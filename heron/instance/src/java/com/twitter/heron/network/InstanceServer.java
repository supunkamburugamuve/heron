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

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.network.HeronServer;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.REQID;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.proto.stmgr.StreamManager;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.PhysicalPlans;

public class InstanceServer extends HeronServer {
  private static Logger LOG = Logger.getLogger(InstanceServer.class.getName());

  private final Map<SocketAddress, PhysicalPlans.Instance> inMessageMap;

  private final Communicator<HeronTuples.HeronTupleSet> inStreamQueue;
  private final Communicator<InstanceControlMsg> inControlQueue;
  /**
   * Constructor
   *
   * @param s the NIOLooper bind with this socket server
   * @param host the host of remote endpoint to communicate with
   * @param port the port of remote endpoint to communicate with
   */
  public InstanceServer(NIOLooper s, String host, int port, HeronSocketOptions options,
                        Communicator<HeronTuples.HeronTupleSet> inStreamQueue,
                        Communicator<InstanceControlMsg> inControlQueue) {
    super(s, host, port, options);
    this.inMessageMap = new HashMap<>();
    this.inStreamQueue = inStreamQueue;
    this.inControlQueue = inControlQueue;

    registerMessagesToHandle();
  }

  private void registerMessagesToHandle() {
    registerOnRequest(StreamManager.RegisterInstanceRequest.newBuilder());

    registerOnMessage(HeronTuples.HeronTupleSet.newBuilder());
  }

  @Override
  public void onConnect(SocketChannel channel) {
    LOG.info("Instance Server got a new connection from host:port "
        + channel.socket().getRemoteSocketAddress());
  }

  @Override
  public void onRequest(REQID rid, SocketChannel channel, Message request) {
    if (request instanceof StreamManager.RegisterInstanceRequest) {
      handleRegisterRequest(rid, channel, (StreamManager.RegisterInstanceRequest) request);
    } else {
      LOG.severe("Unknown kind of request received from Metrics Manager");
    }
  }

  private void handleRegisterRequest(
      REQID rid,
      SocketChannel channel,
      StreamManager.RegisterInstanceRequest request) {
    PhysicalPlans.Instance instance = request.getInstance();
    LOG.info("Register request: " + rid);
    LOG.log(Level.INFO, "Got a new register instance from instance: {0},"
            + " component_name: {1} task_id: {2}",
        new Object[] {instance.getInstanceId(), instance.getInfo().getComponentName(),
            instance.getInfo().getTaskId()});

    // Check whether instance has already been registered
    Common.StatusCode responseStatusCode = Common.StatusCode.NOTOK;

    if (inMessageMap.containsKey(channel.socket().getRemoteSocketAddress())) {
      LOG.log(Level.SEVERE , "Got a new register request to existing instance from instance: {0},"
              + " component_name: {1} task_id: {2} sending NOTOK response",
          new Object[] {instance.getInstanceId(), instance.getInfo().getComponentName(),
              instance.getInfo().getTaskId()});
    } else {
      inMessageMap.put(channel.socket().getRemoteSocketAddress(), instance);
      // Add it to the map
      LOG.log(Level.INFO, "Sending OK response to instance: " + instance.getInfo().getTaskId());
      responseStatusCode = Common.StatusCode.OK;
    }

    Common.Status responseStatus = Common.Status.newBuilder().setStatus(responseStatusCode).build();
    StreamManager.StrMgrHelloResponse response =
        StreamManager.StrMgrHelloResponse.newBuilder().setStatus(responseStatus).build();

    // Send the response
    sendResponse(rid, channel, response);

    // notify that we go a connection
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setOutTaskConnect(instance.getInfo().getTaskId()).build();
    inControlQueue.offer(instanceControlMsg);
  }

  @Override
  public void onMessage(SocketChannel channel, Message message) {
    // Fetch the request to append necessary info
    PhysicalPlans.Instance request = inMessageMap.get(channel.socket().getRemoteSocketAddress());
    if (request == null) {
      LOG.severe("Publish message from an unknown socket: " + channel.toString());
      return;
    }

    if (message instanceof HeronTuples.HeronTupleSet) {
      // LOG.log(Level.INFO, "Receiving message from instance: " + request.getInfo().getTaskId());
      handleTupleMessage((HeronTuples.HeronTupleSet) message);
    } else {
      LOG.severe("Unknown kind of message received to Instance Server");
    }
  }

  private void handleTupleMessage(HeronTuples.HeronTupleSet message) {
    inStreamQueue.offer(message);
  }

  @Override
  public void onClose(SocketChannel channel) {
    LOG.log(Level.SEVERE, "Got a connection close from remote socket address: {0}",
        new Object[] {channel.socket().getRemoteSocketAddress()});

    // Unregister the Publisher
    PhysicalPlans.Instance instance =
        inMessageMap.remove(channel.socket().getRemoteSocketAddress());
    if (instance == null) {
      LOG.severe("Unknown connection closed");
    } else {
      LOG.log(Level.SEVERE, "Got a connection close from instance: {0},"
              + " component_name: {1} task_id: {2}",
          new Object[] {instance.getInstanceId(), instance.getInfo().getComponentName(),
              instance.getInfo().getTaskId()});
    }
  }
}
