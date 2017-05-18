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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.grouping.IReduce;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.common.basics.Pair;
import com.twitter.heron.common.utils.metrics.BoltMetrics;
import com.twitter.heron.common.utils.misc.CollectiveBinaryTreeHelper;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.tuple.TupleImpl;
import com.twitter.heron.instance.OutgoingTupleCollection;

public class SubTasks {
  private static Logger LOG = Logger.getLogger(SubTasks.class.getName());

  private Map<TopologyAPI.StreamId, SubTask> subTaskMap = new HashMap<>();
  // we create this map from the above map in-order to easily query only using the stream id
  private Map<String, SubTask> streamIdToSubTaskMap = new HashMap<>();
  protected PhysicalPlanHelper helper;
  protected final SubTaskOutputCollector subTaskOutputCollector;

  public SubTasks(PhysicalPlanHelper helper, IPluggableSerializer serializer,
                  OutgoingTupleCollection outputter, BoltMetrics boltMetrics) {
    this.helper = helper;
    subTaskOutputCollector = new SubTaskOutputCollector(serializer, helper,
        outputter, boltMetrics, this);
  }

  public void start() {
    try {
      initializeSubTaskInstances();

      for (Map.Entry<TopologyAPI.StreamId, SubTask> e : subTaskMap.entrySet()) {
        streamIdToSubTaskMap.put(e.getKey().getId(), e.getValue());
      }

      for (Map.Entry<TopologyAPI.StreamId, SubTask> entry : subTaskMap.entrySet()) {
        SubTask reductionInvoker = entry.getValue();
        initializeSubTask(entry.getKey(), reductionInvoker);
      }
    } catch (RuntimeException e) {
      LOG.log(Level.SEVERE, "failed to initialize reduce instances", e);
    }
  }

  public void stop() {
    // if there are reduce instances, clean them up
    for (Map.Entry<TopologyAPI.StreamId, SubTask> entry : subTaskMap.entrySet()) {
      SubTask instance = entry.getValue();
      instance.stop();
    }
  }

  private void initializeSubTask(TopologyAPI.StreamId stream, SubTask task) {
    List<Integer> incomingTasks = new ArrayList<>();
    List<Integer> destTask = new ArrayList<>();

    CollectiveBinaryTreeHelper collectiveHelper = new CollectiveBinaryTreeHelper(helper, 2, 2,
        TopologyAPI.Grouping.REDUCE);
    Map<TopologyAPI.StreamId, List<Pair<Integer, Integer>>> table = collectiveHelper.getRoutingTables();
    List<Pair<Integer, Integer>> routingTable = table.get(stream);
    if (routingTable == null) {
      throw new RuntimeException("Failed to get routing table for reduce instance: " + stream);
    }

    String s = "";
    for (Pair<Integer, Integer> entry : routingTable) {
      s += "(" + entry.first + ", " + entry.second + "), ";
    }

    int myTaskId = helper.getMyTaskId();
    LOG.log(Level.INFO, String.format("Routing table for component %s index %d table: %s",
        helper.getMyComponent(), myTaskId, s));

    // get the list of incomingTasks
    for (Pair<Integer, Integer> e : routingTable) {
      if (e.second == myTaskId) {
        incomingTasks.add(e.first);
      }
      if (e.first == myTaskId) {
        destTask.add(e.second);
      }
    }

    task.start(incomingTasks, destTask);
  }

  public void execute(TopologyAPI.StreamId stream, TupleImpl t) {
    SubTask subTask = subTaskMap.get(stream);
    if (subTask != null) {
      subTask.execute(t.getSourceTask(), t);
    } else {
      throw new RuntimeException("Subtask for stream is null: " + stream.getId());
    }
  }

  public List<Integer> getSubTaskDestinations(String stream) {
    List<Integer> ret = new ArrayList<>();
    SubTask subTask = streamIdToSubTaskMap.get(stream);
    if (subTask != null) {
      List<Integer> dest = subTask.getDestinationTask();
      if (dest != null) {
        ret.addAll(dest);
      }
    }
    return ret;
  }

  /**
   * Check weather this stream id is a subtask destination
   * @param streamId
   * @return
   */
  public boolean isSubTaskDestination(String streamId) {
    return streamIdToSubTaskMap.containsKey(streamId);
  }

  private void initializeSubTaskInstances() {
    Map<TopologyAPI.InputStream, IReduce> functions =
        helper.getCollectiveGroupingFunctions(TopologyAPI.Grouping.REDUCE);

    for (Map.Entry<TopologyAPI.InputStream, IReduce> entry : functions.entrySet()) {
      ReductionSubTask instance = new ReductionSubTask(helper,
          helper.getMyTaskId(), entry.getValue(),
          entry.getKey(), subTaskOutputCollector);
      subTaskMap.put(entry.getKey().getStream(), instance);
    }
  }
}
