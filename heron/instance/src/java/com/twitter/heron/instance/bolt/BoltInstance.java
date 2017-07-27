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

package com.twitter.heron.instance.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.bolt.IBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.topology.IUpdatable;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.Pair;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.metrics.BoltMetrics;
import com.twitter.heron.common.utils.misc.BroadcastBinaryTree;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.misc.SerializeDeSerializeHelper;
import com.twitter.heron.common.utils.topology.TopologyContextImpl;
import com.twitter.heron.common.utils.tuple.TickTuple;
import com.twitter.heron.common.utils.tuple.TupleImpl;
import com.twitter.heron.instance.IInstance;
import com.twitter.heron.instance.OutgoingTupleCollection;
import com.twitter.heron.instance.grouping.SubTasks;
import com.twitter.heron.proto.system.HeronTuples;

public class BoltInstance implements IInstance {
  private static Logger LOG = Logger.getLogger(BoltInstance.class.getName());

  protected PhysicalPlanHelper helper;
  protected final IBolt bolt;
  protected final BoltOutputCollectorImpl collector;
  protected final IPluggableSerializer serializer;
  protected final BoltMetrics boltMetrics;
  // The bolt will read Data tuples from streamInQueue
  private final Communicator<HeronTuples.HeronTupleSet> streamInQueue;
  private Map<Integer, OutgoingTupleCollection> instanceOutPutters;

  private final SlaveLooper looper;

  private final SystemConfig systemConfig;
  private OutgoingTupleCollection outputter;
  protected SubTasks subTasks;
  private Map<TopologyAPI.StreamId, List<Pair<Integer, Integer>>> broadCastRoutingTable =
      new HashMap<>();

  public BoltInstance(PhysicalPlanHelper helper,
                      Communicator<HeronTuples.HeronTupleSet> streamInQueue,
                      Communicator<HeronTuples.HeronTupleSet> streamOutQueue,
                      Map<Integer, Communicator<HeronTuples.HeronTupleSet>> instanceOutQueues,
                      SlaveLooper looper) {
    this.helper = helper;
    this.looper = looper;
    this.streamInQueue = streamInQueue;
    this.boltMetrics = new BoltMetrics();
    this.boltMetrics.initMultiCountMetrics(helper);
    this.serializer =
        SerializeDeSerializeHelper.getSerializer(helper.getTopologyContext().getTopologyConfig());
    this.systemConfig = (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(
        SystemConfig.HERON_SYSTEM_CONFIG);

    if (helper.getMyBolt() == null) {
      throw new RuntimeException("HeronBoltInstance has no bolt in physical plan.");
    }

    // Get the bolt. Notice, in fact, we will always use the deserialization way to get bolt.
    if (helper.getMyBolt().getComp().hasSerializedObject()) {
      bolt = (IBolt) Utils.deserialize(
          helper.getMyBolt().getComp().getSerializedObject().toByteArray());
    } else if (helper.getMyBolt().getComp().hasClassName()) {
      try {
        String boltClassName = helper.getMyBolt().getComp().getClassName();
        bolt = (IBolt) Class.forName(boltClassName).newInstance();
      } catch (ClassNotFoundException ex) {
        throw new RuntimeException(ex + " Bolt class must be in class path.");
      } catch (InstantiationException ex) {
        throw new RuntimeException(ex + " Bolt class must be concrete.");
      } catch (IllegalAccessException ex) {
        throw new RuntimeException(ex + " Bolt class must have a no-arg constructor.");
      }
    } else {
      throw new RuntimeException("Neither java_object nor java_class_name set for bolt");
    }

    outputter = new OutgoingTupleCollection(helper.getMyComponent(), streamOutQueue);
    instanceOutPutters = new HashMap<>();
    for (Map.Entry<Integer, Communicator<HeronTuples.HeronTupleSet>> out :
        instanceOutQueues.entrySet()) {
      OutgoingTupleCollection outCollection =
          new OutgoingTupleCollection(helper.getMyComponent(), out.getValue());
      instanceOutPutters.put(out.getKey(), outCollection);
    }
    subTasks = new SubTasks(helper, serializer, outputter, boltMetrics,
        streamInQueue, instanceOutPutters);
    initializeBroadcast();
    collector = new BoltOutputCollectorImpl(serializer, helper, outputter, boltMetrics,
        subTasks, streamInQueue, instanceOutPutters);
  }

  private void initializeBroadcast() {
    int interNodeDegree = systemConfig.getCollectiveBroadcastTreeInterNodeDegree();
    int intraNodeDegree = systemConfig.getCollectiveBroadcastTreeIntraNodeDegree();

    BroadcastBinaryTree broadcastBinaryTree = new BroadcastBinaryTree(helper, intraNodeDegree,
        interNodeDegree, TopologyAPI.Grouping.ALL);
    Map<TopologyAPI.StreamId, List<Pair<Integer, Integer>>> bcastTable =
        broadcastBinaryTree.getRoutingTables();

    BroadcastBinaryTree allReduceTree = new BroadcastBinaryTree(helper, intraNodeDegree,
        interNodeDegree, TopologyAPI.Grouping.ALLREDUCE);
    Map<TopologyAPI.StreamId, List<Pair<Integer, Integer>>> allReduceTable =
        allReduceTree.getRoutingTables();

    broadCastRoutingTable.putAll(bcastTable);
    broadCastRoutingTable.putAll(allReduceTable);

    String s = "";
    for (Map.Entry<TopologyAPI.StreamId, List<Pair<Integer, Integer>>> entry :
        broadCastRoutingTable.entrySet()) {
      s += entry.getKey().getId();
      for (Pair<Integer, Integer> p : entry.getValue()) {
        s += "(" + p.first + ", " + p.second+ "), ";
      }
      s += "\n";
    }
    LOG.log(Level.INFO, "Broadcast: " + s);
  }

  @Override
  public void update(PhysicalPlanHelper physicalPlanHelper) {
    if (bolt instanceof IUpdatable) {
      ((IUpdatable) bolt).update(physicalPlanHelper.getTopologyContext());
    }
    collector.updatePhysicalPlanHelper(physicalPlanHelper);

    // Re-prepare the CustomStreamGrouping since the downstream tasks can change
    physicalPlanHelper.prepareForCustomStreamGrouping();
    // Reset the helper
    helper = physicalPlanHelper;
  }

  @Override
  public void start() {
    TopologyContextImpl topologyContext = helper.getTopologyContext();

    // Initialize the GlobalMetrics
    GlobalMetrics.init(topologyContext, systemConfig.getHeronMetricsExportIntervalSec());

    boltMetrics.registerMetrics(topologyContext);

    // Delegate
    bolt.prepare(
        topologyContext.getTopologyConfig(), topologyContext, new OutputCollector(collector));

    // Invoke user-defined prepare task hook
    topologyContext.invokeHookPrepare();

    // Init the CustomStreamGrouping
    helper.prepareForCustomStreamGrouping();

    subTasks.start();

    addBoltTasks();
  }

  @Override
  public void stop() {
    // Invoke clean up hook before clean() is called
    helper.getTopologyContext().invokeHookCleanup();

    // Delegate to user-defined clean-up method
    bolt.cleanup();

    subTasks.stop();

    // Clean the resources we own
    looper.exitLoop();
    streamInQueue.clear();
    collector.clear();
  }

  private void addBoltTasks() {
    // add the readTupleAndExecute() to tasks
    Runnable boltTasks = new Runnable() {
      @Override
      public void run() {
        // Back-pressure -- only when we could send out tuples will we read & execute tuples
        if (collector.isOutQueuesAvailable()) {
          readTuplesAndExecute(streamInQueue);

          // Though we may execute MAX_READ tuples, finally we will packet it as
          // one outgoingPacket and push to out queues
          collector.sendOutTuples();
          // Here we need to inform the Gateway
        } else {
          LOG.log(Level.INFO, "Bolt outqueue not available");
          boltMetrics.updateOutQueueFullCount();
        }

        // If there are more to read, we will wake up itself next time when it doWait()
        if (collector.isOutQueuesAvailable() && !streamInQueue.isEmpty()) {
          looper.wakeUp();
        }
      }
    };
    looper.addTasksOnWakeup(boltTasks);

    PrepareTickTupleTimer();
  }

  @Override
  public void readTuplesAndExecute(Communicator<HeronTuples.HeronTupleSet> inQueue) {
    TopologyContextImpl topologyContext = helper.getTopologyContext();
    long instanceExecuteBatchTime
        = systemConfig.getInstanceExecuteBatchTimeMs() * Constants.MILLISECONDS_TO_NANOSECONDS;

    long startOfCycle = System.nanoTime();
    // Read data from in Queues
    while (!inQueue.isEmpty()) {
      HeronTuples.HeronTupleSet tuples = inQueue.poll();

      // Handle the tuples
      if (tuples.hasControl()) {
        throw new RuntimeException("Bolt cannot get acks/fails from other components");
      }

      // Get meta data of tuples
      TopologyAPI.StreamId stream = tuples.getData().getStream();
      int nValues = topologyContext.getComponentOutputFields(
          stream.getComponentName(), stream.getId()).size();

      // We would reuse the System.nanoTime()
      long currentTime = startOfCycle;
      for (HeronTuples.HeronDataTuple dataTuple : tuples.getData().getTuplesList()) {
        // if we need to broadcast
        handleBroadcast(stream, dataTuple);
        // Create the value list and fill the value
        List<Object> values = new ArrayList<>(nValues);
        for (int i = 0; i < nValues; i++) {
          values.add(serializer.deserialize(dataTuple.getValues(i).toByteArray()));
        }

        // Decode the tuple
        TupleImpl t = new TupleImpl(topologyContext, stream, dataTuple.getKey(),
            dataTuple.getRootsList(), values, currentTime, false, dataTuple.getSourceTask());
        if (!dataTuple.hasSubTaskDest() || !dataTuple.getSubTaskDest()) {
          // Delegate to the use defined bolt
//          LOG.log(Level.INFO, String.format("Subtask dest false %d -> %d",
//              dataTuple.getSourceTask(), helper.getMyTaskId()));
          bolt.execute(t);
        } else {
//          LOG.log(Level.INFO, String.format("Subtask dest true %d -> %d",
//              dataTuple.getSourceTask(), helper.getMyTaskId()));
          subTasks.execute(stream, t);
        }

        // Swap
        long startTime = currentTime;
        currentTime = System.nanoTime();

        long executeLatency = currentTime - startTime;

        // Invoke user-defined execute task hook
        topologyContext.invokeHookBoltExecute(t, executeLatency);

        // Update metrics
        boltMetrics.executeTuple(stream.getId(), stream.getComponentName(), executeLatency);
      }

      // To avoid spending too much time
      if (currentTime - startOfCycle - instanceExecuteBatchTime > 0) {
        break;
      }
    }
  }

  private void handleBroadcast(TopologyAPI.StreamId stream, HeronTuples.HeronDataTuple dataTuple) {
    if (broadCastRoutingTable.containsKey(stream)) {
      // LOG.log(Level.INFO, "Broadcasting to stream");
      List<Pair<Integer, Integer>> routs = broadCastRoutingTable.get(stream);
      if (routs.size() != 0) {
        for (Pair<Integer, Integer> r : routs) {
          if (r.first == helper.getMyTaskId() && r.second != helper.getMyTaskId()) {
            OutgoingTupleCollection t = instanceOutPutters.get(r.second);
//            LOG.log(Level.INFO, "Sending to task: " + r.second + " " +
//                " stream: " + stream.getId());

            HeronTuples.HeronDataTuple.Builder newTuple = dataTuple.toBuilder();
            newTuple.setKey(dataTuple.getKey());
            for (HeronTuples.RootId id : dataTuple.getRootsList()) {
              HeronTuples.RootId.Builder idBuilder = id.toBuilder();
              idBuilder.setKey(id.getKey());
              idBuilder.setTaskid(id.getTaskid());
              newTuple.addRoots(idBuilder);
            }

            for (ByteString b : dataTuple.getValuesList()) {
              newTuple.addValues(b);
            }

            for (int id : dataTuple.getDestTaskIdsList()) {
              newTuple.addDestTaskIds(id);
            }

            if (dataTuple.hasSourceTask()) {
              newTuple.setSourceTask(dataTuple.getSourceTask());
            }
            if (dataTuple.hasSubTaskDest()) {
              newTuple.setSubTaskDest(dataTuple.getSubTaskDest());
            }
            if (dataTuple.hasSubTaskOrigin()) {
              newTuple.setSubTaskOrigin(dataTuple.getSubTaskOrigin());
            }

            t.copyDataTuple(stream, newTuple, 0);
          }
        }
      }
    }
  }

  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  private void PrepareTickTupleTimer() {
    Object tickTupleFreqSecs =
        helper.getTopologyContext().getTopologyConfig().get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);

    if (tickTupleFreqSecs != null) {
      int freq = TypeUtils.getInteger(tickTupleFreqSecs);

      Runnable r = new Runnable() {
        public void run() {
          SendTickTuple();
        }
      };

      looper.registerTimerEventInSeconds(freq, r);
    }
  }

  private void SendTickTuple() {
    TickTuple t = new TickTuple();
    long startTime = System.nanoTime();
    bolt.execute(t);
    long latency = System.nanoTime() - startTime;
    boltMetrics.executeTuple(t.getSourceStreamId(), t.getSourceComponent(), latency);

    collector.sendOutTuples();
    // reschedule ourselves again
    PrepareTickTupleTimer();
  }
}
