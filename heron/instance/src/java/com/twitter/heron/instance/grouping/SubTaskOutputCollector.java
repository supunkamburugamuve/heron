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

import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.utils.metrics.BoltMetrics;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.instance.OutgoingTupleCollection;
import com.twitter.heron.instance.bolt.BoltOutputCollectorImpl;
import com.twitter.heron.proto.system.HeronTuples;

public class SubTaskOutputCollector extends BoltOutputCollectorImpl {
  private static Logger LOG = Logger.getLogger(SubTaskOutputCollector.class.getName());

  public SubTaskOutputCollector(IPluggableSerializer serializer, PhysicalPlanHelper helper,
                                OutgoingTupleCollection outputter,
                                BoltMetrics boltMetrics, SubTasks subTasks,
                                Communicator<HeronTuples.HeronTupleSet> streamInQueue,
                                Map<Integer, OutgoingTupleCollection> instanceOutPutters) {
    super(serializer, helper, outputter, boltMetrics, subTasks, streamInQueue, instanceOutPutters);
  }

  @Override
  protected void addSubTasks(HeronTuples.HeronDataTuple.Builder bldr, String streamId) {
    int noOfSubTaskDestinations = 0;
    if (subTasks.isSubTaskDestination(streamId)) {
      List<Integer> subTasksDestinations = subTasks.getSubTaskDestinations(streamId);
      List<Integer> subTasks = new ArrayList<>();
      String s = "";
      for (Integer i : subTasksDestinations) {
        // we are not going to send it to ourselves as this is a subtask
        if (i != helper.getMyTaskId()) {
          subTasks.add(i);
          noOfSubTaskDestinations++;
          s += i + " ";
        }
      }

      // check weather we need to send further
      if (noOfSubTaskDestinations > 0) {
//        LOG.log(Level.INFO, String.format("%d Subtask adding subtask destinations true: %s, %s",
//            helper.getMyTaskId(), streamId, s));
        bldr.setSubTaskDest(true);
        bldr.addAllDestTaskIds(subTasks);
      } else {
//        LOG.log(Level.INFO, String.format("%d Subtask adding subtask destinations false: %s, %s",
//            helper.getMyTaskId(), streamId, s));
        bldr.setSubTaskDest(false);
      }
    }
  }
}
