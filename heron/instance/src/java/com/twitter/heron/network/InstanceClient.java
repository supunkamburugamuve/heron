//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.network;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.network.HeronClient;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.StatusCode;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.metrics.GatewayMetrics;
import com.twitter.heron.proto.stmgr.StreamManager;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.PhysicalPlans;

public class InstanceClient extends HeronClient {
  private static Logger LOG = Logger.getLogger(InstanceClient.class.getName());

  private final String topologyName;
  private final String topologyId;
  private final int destTaskId;

  private final PhysicalPlans.Instance myInstance;

  // For spout, it will buffer Control tuple, while for bolt, it will buffer data tuple.
  private final Communicator<HeronTuples.HeronTupleSet> inStreamQueue;

  private final Communicator<HeronTuples.HeronTupleSet> outStreamQueue;

  private final Communicator<InstanceControlMsg> inControlQueue;

  private final GatewayMetrics gatewayMetrics;

  private final SystemConfig systemConfig;

  /**
   * Constructor
   *
   * @param s the NIOLooper bind with this socket client
   * @param host the host of remote endpoint to communicate with
   * @param port the port of remote endpoint to communicate with
   */
  public InstanceClient(NIOLooper s, String host, int port, String topologyName, String topologyId,
                        int destTaskId, PhysicalPlans.Instance myInstance, HeronSocketOptions options,
                        Communicator<HeronTuples.HeronTupleSet> inStreamQueue,
                        Communicator<HeronTuples.HeronTupleSet> outStreamQueue,
                        Communicator<InstanceControlMsg> inControlQueue,
                        GatewayMetrics gatewayMetrics) {
    super(s, host, port, options);
    this.topologyName = topologyName;
    this.topologyId = topologyId;
    this.destTaskId = destTaskId;

    this.myInstance = myInstance;
    this.inStreamQueue = inStreamQueue;
    this.outStreamQueue = outStreamQueue;
    this.inControlQueue = inControlQueue;
    this.gatewayMetrics = gatewayMetrics;

    this.systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);
    addStreamManagerClientTasksOnWakeUp();
  }

  private void addStreamManagerClientTasksOnWakeUp() {
    Runnable task = new Runnable() {
      @Override
      public void run() {
        sendStreamMessageIfNeeded();
        readStreamMessageIfNeeded();
      }
    };
    getNIOLooper().addTasksOnWakeup(task);
  }

  private void sendStreamMessageIfNeeded() {
    if (isStreamMgrReadyReceiveTuples()) {
      if (getOutstandingPackets() <= 0) {
        // In order to avoid packets back up in Client side,
        // We would poll message from queue and send them only when there are no outstanding packets
        while (!outStreamQueue.isEmpty()) {
          HeronTuples.HeronTupleSet tupleSet = outStreamQueue.poll();

          gatewayMetrics.updateSentPacketsCount(1);
          gatewayMetrics.updateSentPacketsSize(tupleSet.getSerializedSize());
          // LOG.info("Sending message to instance: " + destTaskId);
          sendMessage(tupleSet);
        }
      }

      if (!outStreamQueue.isEmpty()) {
        // We still have messages to send
        startWriting();
      }
    } else {
      LOG.info("Stop writing due to not yet connected to Stream Manager.");
    }
  }

  private void readStreamMessageIfNeeded() {
    // If client is not connected, just return
    if (isConnected()) {
      if (isInQueuesAvailable()) {
        startReading();
      } else {
        gatewayMetrics.updateInQueueFullCount();
        stopReading();
      }
    } else {
      LOG.info("Stop reading due to not yet connected to Stream Manager.");
    }
  }

  private boolean isStreamMgrReadyReceiveTuples() {
    // The Stream Manager is ready only when:
    // 1. We could connect to it
    // 2. We receive the PhysicalPlan published by Stream Manager
    return isConnected();
  }

  // Return true if we could offer item to the inStreamQueue
  private boolean isInQueuesAvailable() {
    return inStreamQueue.size() < inStreamQueue.getExpectedAvailableCapacity();
  }

  @Override
  public void onError() {
    LOG.severe("Disconnected from Instance.");

    // Dispatch to onConnect(...)
    onConnect(StatusCode.CONNECT_ERROR);
  }

  @Override
  public void onConnect(StatusCode status) {
    if (status != StatusCode.OK) {
      LOG.log(Level.WARNING,
          "Error connecting to Instance with status: {0}, Retrying...", status);
      Runnable r = new Runnable() {
        public void run() {
          start();
        }
      };
      getNIOLooper().registerTimerEventInSeconds(
          systemConfig.getInstanceReconnectStreammgrIntervalSec(), r);
      return;
    }

    // Initialize the register: determine what messages we would like to handle
    registerMessagesToHandle();

    // Build the request and send it.
    LOG.info("Connected to Stream Manager. Ready to send register request");
    sendRegisterRequest();
  }

  private void sendRegisterRequest() {
    StreamManager.RegisterInstanceRequest request =
        StreamManager.RegisterInstanceRequest.newBuilder().
            setInstance(myInstance).setTopologyName(topologyName).setTopologyId(topologyId).
            build();

    // The timeout would be the reconnect-interval-seconds
    LOG.log(Level.INFO, "Send register request with: " +
        systemConfig.getInstanceReconnectStreammgrIntervalSec());
    sendRequest(request, null,
        StreamManager.StrMgrHelloResponse.newBuilder(),
        systemConfig.getInstanceReconnectStreammgrIntervalSec());
  }

  private void registerMessagesToHandle() {
    registerOnMessage(HeronTuples.HeronTupleSet2.newBuilder());
  }

  @Override
  public void onResponse(StatusCode status, Object ctx, Message response) {
    if (status != StatusCode.OK) {
      //TODO:- is this a good thing?
      LOG.info("Got response: " + status);
      throw new RuntimeException("Response from Stream Manager not ok");
    }
    if (response instanceof StreamManager.StrMgrHelloResponse) {
      handleRegisterResponse((StreamManager.StrMgrHelloResponse) response);
    } else {
      throw new RuntimeException("Unknown kind of response received from Stream Manager");
    }
  }

  private void handleRegisterResponse(StreamManager.StrMgrHelloResponse response) {
    if (response.getStatus().getStatus() != Common.StatusCode.OK) {
      throw new RuntimeException("Stream Manager returned a not ok response for register");
    }
    LOG.info("We registered ourselves to the Stream Manager");
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setOutTaskConnect(destTaskId).build();

    inControlQueue.offer(instanceControlMsg);
  }

  @Override
  public void onIncomingMessage(Message message) {
    LOG.log(Level.SEVERE, "We shouldn't receive messages on the clien");
  }

  @Override
  public void onClose() {
    LOG.info("StreamManagerClient exits.");
  }

  // Send out all the data
  public void sendAllMessage() {
    if (!isConnected()) {
      return;
    }

    LOG.info("Flushing all pending data in InstanceClient");
    // Collect all tuples in queue
    int size = outStreamQueue.size();
    for (int i = 0; i < size; i++) {
      HeronTuples.HeronTupleSet tupleSet = outStreamQueue.poll();
      StreamManager.TupleMessage msg = StreamManager.TupleMessage.newBuilder()
          .setSet(tupleSet).build();
      sendMessage(msg);
    }
  }
}
