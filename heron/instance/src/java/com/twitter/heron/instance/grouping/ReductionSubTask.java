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

import java.util.List;
import java.util.logging.Logger;

import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.grouping.IReduce;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.misc.SerializeDeSerializeHelper;
import com.twitter.heron.common.utils.topology.TopologyContextImpl;
import com.twitter.heron.common.utils.tuple.TupleImpl;

public class ReductionSubTask implements SubTask {
  private Logger LOG = Logger.getLogger(ReductionSubTask.class.getName());

  private List<Integer> incomingTasks;
  private List<Integer> destTask;

  private final IReduce reduceFunction;
  protected final PhysicalPlanHelper helper;
  private final TopologyAPI.InputStream stream;
  protected final IPluggableSerializer serializer;
  private final IOutputCollector outputter;
  private final int thisTaskId;

  public ReductionSubTask(PhysicalPlanHelper helper, int thisTaskId, IReduce reduceFunction,
                          TopologyAPI.InputStream stream, IOutputCollector outputter) {
    this.helper = helper;
    this.thisTaskId = thisTaskId;
    this.reduceFunction = reduceFunction;
    this.stream = stream;
    this.serializer =
        SerializeDeSerializeHelper.getSerializer(helper.getTopologyContext().getTopologyConfig());
    this.outputter = outputter;
  }

  public void execute(int taskIndex, TupleImpl tuple) {
    reduceFunction.execute(taskIndex, tuple);
  }

  public void start(List<Integer> incomingTasks, List<Integer> destTask) {
    this.destTask = destTask;
    this.incomingTasks = incomingTasks;
    TopologyContextImpl topologyContext = helper.getTopologyContext();
    // now lets initialize the function
    reduceFunction.prepare(topologyContext.getTopologyConfig(), topologyContext,
        incomingTasks, outputter);
  }

  public void stop() {
    reduceFunction.cleanup();
  }

  @Override
  public List<Integer> getDestinationTask() {
    return destTask;
  }

  @Override
  public List<Integer> getSourceTasks() {
    return incomingTasks;
  }
}
