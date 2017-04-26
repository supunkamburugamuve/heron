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
package com.twitter.heron.instance.grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.grouping.IReduce;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.common.utils.misc.CollectiveBinaryTreeHelper;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.misc.SerializeDeSerializeHelper;
import com.twitter.heron.common.utils.topology.TopologyContextImpl;
import com.twitter.heron.common.utils.tuple.TupleImpl;
import com.twitter.heron.instance.OutgoingTupleCollection;
import com.twitter.heron.proto.system.HeronTuples;

public class ReductionInvoker {
  private Logger LOG = Logger.getLogger(ReductionInvoker.class.getName());

  private List<Integer> incomingTasks = new ArrayList<>();
  private int destTask = -1;

  private final IReduce reduceFunction;
  protected final PhysicalPlanHelper helper;
  private final TopologyAPI.InputStream stream;
  protected final IPluggableSerializer serializer;
  private final OutgoingTupleCollection outputter;
  private final int thisTaskIndex;

  public ReductionInvoker(PhysicalPlanHelper helper, int thisTaskId, IReduce reduceFunction,
                          TopologyAPI.InputStream stream, OutgoingTupleCollection outputter) {
    this.helper = helper;
    this.thisTaskIndex = thisTaskId;
    this.reduceFunction = reduceFunction;
    this.stream = stream;
    this.serializer =
        SerializeDeSerializeHelper.getSerializer(helper.getTopologyContext().getTopologyConfig());
    this.outputter = outputter;
  }

  public void execute(int taskIndex, TupleImpl tuple) {
    List<Object> reductionTuple = reduceFunction.execute(taskIndex, tuple);
    if (reductionTuple != null) {
      admitTuple(reductionTuple);
    }
  }

  private void admitTuple(List<Object> reductionTuple) {
    // Start construct the data tuple
    HeronTuples.HeronDataTuple.Builder bldr = HeronTuples.HeronDataTuple.newBuilder();

    // set the key. This is mostly ignored
    bldr.setKey(0);

    long tupleSizeInBytes = 0;

    // Serialize it
    for (Object obj : reductionTuple) {
      byte[] b = serializer.serialize(obj);
      ByteString bstr = ByteString.copyFrom(b);
      bldr.addValues(bstr);
      tupleSizeInBytes += b.length;
    }
    bldr.setSourceTask(thisTaskIndex);

    if (destTask >= 0) {
      bldr.setSubTaskDest(true);
      bldr.addDestTaskIds(destTask);
    } else {
      bldr.setSubTaskDest(false);
    }
    // submit to outputter
    outputter.addDataTuple(stream.getStream().getId(), bldr, tupleSizeInBytes);
  }

  private TupleImpl createTupleImpl(HeronTuples.HeronDataTuple firstTuple, long currentTime,
                                    TopologyContextImpl topologyContext, int nValues) {
    List<Object> secondValues = new ArrayList<>(nValues);
    for (int i = 0; i < nValues; i++) {
      secondValues.add(serializer.deserialize(firstTuple.getValues(i).toByteArray()));
    }
    return new TupleImpl(topologyContext, stream.getStream(), firstTuple.getKey(),
        firstTuple.getRootsList(), secondValues, currentTime, false);
  }

  public void start() {
    CollectiveBinaryTreeHelper collectiveHelper = new CollectiveBinaryTreeHelper(helper, 2, 2,
        TopologyAPI.Grouping.REDUCE);
    Map<TopologyAPI.InputStream, Map<Integer, Integer>> table = collectiveHelper.getRoutingTables();
    Map<Integer, Integer> routingTable = table.get(stream);
    if (routingTable == null) {
      throw new RuntimeException("Failed to get routing table for reduce instance: " + stream);
    }

    // get the list of incomingTasks
    for (Map.Entry<Integer, Integer> e : routingTable.entrySet()) {
      if (e.getValue() == thisTaskIndex) {
        incomingTasks.add(e.getKey());
      }
    }
    incomingTasks.add(thisTaskIndex);

    if (routingTable.get(thisTaskIndex) != null) {
      destTask = routingTable.get(thisTaskIndex);
    } else {
      // root of the tree
      destTask = -1;
    }

    TopologyContextImpl topologyContext = helper.getTopologyContext();
    // now lets initialize the function
    reduceFunction.prepare(topologyContext.getTopologyConfig(), topologyContext, incomingTasks);
  }

  public void stop() {
    reduceFunction.cleanup();
  }

  public void update(PhysicalPlanHelper physicalPlanHelper) {
  }
}
