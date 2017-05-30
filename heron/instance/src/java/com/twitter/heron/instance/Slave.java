// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.instance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.metrics.MetricsCollector;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.misc.ThreadNames;
import com.twitter.heron.instance.bolt.BoltInstance;
import com.twitter.heron.instance.spout.SpoutInstance;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.system.PhysicalPlans;

/**
 * The slave, which in fact is a InstanceFactory, creates a new spout or bolt according to the PhysicalPlan.
 * First, if the instance is null, it will wait for the PhysicalPlan from inQueue and, if it receives one,
 * will instantiate a new instance (spout or bolt) according to the PhysicalPlanHelper in SingletonRegistry.
 * It is a Runnable so it could be executed in a Thread. During run(), it will begin the SlaveLooper's loop().
 */

public class Slave implements Runnable, AutoCloseable {
  private static final Logger LOG = Logger.getLogger(Slave.class.getName());

  private enum SlaveState {
    WAIT_PHYSICAL_PLAN,
    WAIT_INSTANCE_CONNECT,
    INSTANCE_CONNECTED
  }

  private final SlaveLooper slaveLooper;
  private final NIOLooper gatewayLooper;
  private final MetricsCollector metricsCollector;
  // Communicator
  private final Communicator<HeronTuples.HeronTupleSet> streamInCommunicator;
  private final Communicator<HeronTuples.HeronTupleSet> streamOutCommunicator;
  private final Communicator<InstanceControlMsg> inControlQueue;
  private IInstance instance;
  private PhysicalPlanHelper helper;
  private SystemConfig systemConfig;
  private final Gateway gateway;
  private SlaveState slaveState = SlaveState.WAIT_PHYSICAL_PLAN;
  private final PhysicalPlans.Instance myInstance;

  private boolean isInstanceStarted = false;

  private List<InstanceControlMsg> controlMsgs = new ArrayList<>();
  private Map<Integer, Communicator<HeronTuples.HeronTupleSet>> instanceOutCommunicators =
      new HashMap<>();

  public Slave(Gateway gateway, SlaveLooper slaveLooper, NIOLooper gatewayLooper,
               final Communicator<HeronTuples.HeronTupleSet> streamInCommunicator,
               final Communicator<HeronTuples.HeronTupleSet> streamOutCommunicator,
               final Communicator<InstanceControlMsg> inControlQueue,
               final Communicator<Metrics.MetricPublisherPublishMessage> metricsOutCommunicator,
               PhysicalPlans.Instance instance) {
    this.gateway = gateway;
    this.slaveLooper = slaveLooper;
    this.gatewayLooper = gatewayLooper;
    this.streamInCommunicator = streamInCommunicator;
    this.streamOutCommunicator = streamOutCommunicator;
    this.inControlQueue = inControlQueue;
    this.myInstance = instance;

    this.systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    this.metricsCollector = new MetricsCollector(slaveLooper, metricsOutCommunicator);

    handleControlMessage();
  }

  private void handleControlMessage() {
    Runnable handleControlMessageTask = new Runnable() {
      @Override
      public void run() {
        while (!inControlQueue.isEmpty()) {
          InstanceControlMsg instanceControlMsg = inControlQueue.poll();

          // Handle New Physical Plan
          if (instanceControlMsg.isNewPhysicalPlanHelper()) {
            PhysicalPlanHelper newHelper = instanceControlMsg.getNewPhysicalPlanHelper();

            // update the PhysicalPlanHelper in Slave
            helper = newHelper;
          } else {
            LOG.log(Level.INFO, "Received instance control message: " +
                instanceControlMsg.getTaskId());
            controlMsgs.add(instanceControlMsg);
          }
          handleControlMessages();
          // TODO:- We might handle more control Message in future
        }
      }
    };

    slaveLooper.addTasksOnWakeup(handleControlMessageTask);
  }

  private void handleNewAssignment(PhysicalPlanHelper newHelper) {
    LOG.log(Level.INFO,
        "Incarnating ourselves as {0} with task id {1}",
        new Object[]{newHelper.getMyComponent(), newHelper.getMyTaskId()});

    // During the initiation of instance,
    // we would add a bunch of tasks to slaveLooper's tasksOnWakeup
    if (newHelper.getMySpout() != null) {
      instance =
          new SpoutInstance(newHelper, streamInCommunicator, streamOutCommunicator, slaveLooper);

//      streamInCommunicator.init(systemConfig.getInstanceInternalSpoutReadQueueCapacity(),
//          systemConfig.getInstanceTuningExpectedSpoutReadQueueSize(),
//          systemConfig.getInstanceTuningCurrentSampleWeight());
//      streamOutCommunicator.init(systemConfig.getInstanceInternalSpoutWriteQueueCapacity(),
//          systemConfig.getInstanceTuningExpectedSpoutWriteQueueSize(),
//          systemConfig.getInstanceTuningCurrentSampleWeight());
    } else {
      instance =
          new BoltInstance(newHelper, streamInCommunicator, streamOutCommunicator,
              instanceOutCommunicators, slaveLooper);

//      streamInCommunicator.init(systemConfig.getInstanceInternalBoltReadQueueCapacity(),
//          systemConfig.getInstanceTuningExpectedBoltReadQueueSize(),
//          systemConfig.getInstanceTuningCurrentSampleWeight());
//      streamOutCommunicator.init(systemConfig.getInstanceInternalBoltWriteQueueCapacity(),
//          systemConfig.getInstanceTuningExpectedBoltWriteQueueSize(),
//          systemConfig.getInstanceTuningCurrentSampleWeight());
    }

    if (newHelper.getTopologyState().equals(TopologyAPI.TopologyState.RUNNING)) {
      // We would start the instance only if the TopologyState is RUNNING
      startInstance();
    } else {
      LOG.info("The instance is deployed in deactivated state");
    }
  }

  private void handleControlMessages() {
    if (slaveState == SlaveState.WAIT_PHYSICAL_PLAN) {
      if (helper != null) {
        List<PhysicalPlans.Instance> instances = helper.getInstanceForStmgr(helper.getMyStmgr());

        if (helper.getMySpout() != null) {
          streamInCommunicator.init(systemConfig.getInstanceInternalSpoutReadQueueCapacity(),
              systemConfig.getInstanceTuningExpectedSpoutReadQueueSize(),
              systemConfig.getInstanceTuningCurrentSampleWeight());
          streamOutCommunicator.init(systemConfig.getInstanceInternalSpoutWriteQueueCapacity(),
              systemConfig.getInstanceTuningExpectedSpoutWriteQueueSize(),
              systemConfig.getInstanceTuningCurrentSampleWeight());
        } else {
          streamInCommunicator.init(systemConfig.getInstanceInternalBoltReadQueueCapacity(),
              systemConfig.getInstanceTuningExpectedBoltReadQueueSize(),
              systemConfig.getInstanceTuningCurrentSampleWeight());
          streamOutCommunicator.init(systemConfig.getInstanceInternalBoltWriteQueueCapacity(),
              systemConfig.getInstanceTuningExpectedBoltWriteQueueSize(),
              systemConfig.getInstanceTuningCurrentSampleWeight());
        }

        for (PhysicalPlans.Instance instance : instances) {
          if (instance.getInfo().getTaskId() != helper.getMyTaskId()) {
            Communicator<HeronTuples.HeronTupleSet> instanceOutCommunicator =
                new Communicator<HeronTuples.HeronTupleSet>(slaveLooper, gatewayLooper);
            instanceOutCommunicator.init(systemConfig.getInstanceInternalBoltWriteQueueCapacity(),
                systemConfig.getInstanceTuningExpectedBoltWriteQueueSize(),
                systemConfig.getInstanceTuningCurrentSampleWeight());
            gateway.addInstanceClient(myInstance, instance.getInfo().getTaskId(),
                instanceOutCommunicator);
            instanceOutCommunicators.put(instance.getInfo().getTaskId(), instanceOutCommunicator);
          }
        }
      }
      slaveState = SlaveState.WAIT_INSTANCE_CONNECT;
    } else if (slaveState == SlaveState.WAIT_INSTANCE_CONNECT) {
      List<PhysicalPlans.Instance> instanceIds = helper.getInstanceForStmgr(helper.getMyStmgr());
      boolean find = true;
      String s = "";
      for (InstanceControlMsg msg : controlMsgs) {
        s += msg.getTaskId() + " ";
      }
      LOG.log(Level.INFO, "Constrol msgs: " + s);
      for (PhysicalPlans.Instance i : instanceIds) {
        if (i.getInfo().getTaskId() != helper.getMyTaskId()) {
          int findCount = 0;
          // for each instance we should get 2 control messages, for client and server
          for (InstanceControlMsg msg : controlMsgs) {
            if (i.getInfo().getTaskId() == msg.getTaskId()) {
              findCount++;
            }
          }
          if (findCount != 2) {
            find = false;
            LOG.info(String.format("%d didn't get control from: %d %d",
                helper.getMyTaskId(), i.getInfo().getTaskId(), findCount));
            break;
          }
        }
      }
      if (find) {
        LOG.log(Level.INFO, "All the instances connected, start processing");
        initInstances();
        slaveState = SlaveState.INSTANCE_CONNECTED;
      }
    }
  }

  private void initInstances() {
    PhysicalPlanHelper newHelper = helper;
    // Bind the MetricsCollector with topologyContext
    newHelper.setTopologyContext(metricsCollector);

    if (slaveState == SlaveState.WAIT_INSTANCE_CONNECT) {
      handleNewAssignment(newHelper);
    } else if (slaveState == SlaveState.INSTANCE_CONNECTED) {

      instance.update(newHelper);

      // Handle the state changing
      if (!helper.getTopologyState().equals(newHelper.getTopologyState())) {
        switch (newHelper.getTopologyState()) {
          case RUNNING:
            if (!isInstanceStarted) {
              // Start the instance if it has not yet started
              startInstance();
            }
            instance.activate();
            break;
          case PAUSED:
            instance.deactivate();
            break;
          default:
            throw new RuntimeException("Unexpected TopologyState is updated for spout: "
                + newHelper.getTopologyState());
        }
      } else {
        LOG.info("Topology state remains the same in Slave: " + helper.getTopologyState());
      }
    }
  }

  @Override
  public void run() {
    Thread.currentThread().setName(ThreadNames.THREAD_SLAVE_NAME);

    slaveLooper.loop();
  }

  private void startInstance() {
    instance.start();
    isInstanceStarted = true;
    LOG.info("Started instance.");
  }

  public void close() {
    LOG.info("Closing the Slave Thread");
    this.metricsCollector.forceGatherAllMetrics();
    LOG.info("Cleaning up the instance");
    if (instance != null) {
      instance.stop();
    }
  }
}
