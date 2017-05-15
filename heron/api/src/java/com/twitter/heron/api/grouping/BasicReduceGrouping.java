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
package com.twitter.heron.api.grouping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;

import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

public abstract class BasicReduceGrouping implements IReduce {
  private static final long serialVersionUID = -2269376231370074763L;
  // we may get multiple inputs depending on the reduce algorithm used
  private Map<Integer, LinkedTransferQueue<Tuple>> taskMessageQueues =
      new HashMap<>();

  private List<Integer> expectedSourceIndexes;
  protected TopologyContext topologyContext;
  protected Map<String, Object> heronConf;
  private List<Integer> currentResultIndexes = new ArrayList<>();
  private List<Object> currentReducedTuple;
  private int currentReduceTupleCount = 0;

  @Override
  public void prepare(Map<String, Object> heronConf, TopologyContext context,
                      List<Integer> expectedSourceIndexes, IOutputCollector collector) {
    this.heronConf = heronConf;
    this.topologyContext = context;
    this.expectedSourceIndexes = expectedSourceIndexes;
    for (int index : expectedSourceIndexes) {
      taskMessageQueues.put(index, new LinkedTransferQueue<Tuple>());
    }
  }

  @Override
  public void execute(int sourceTask, Tuple input) {
    LinkedTransferQueue<Tuple> queue = taskMessageQueues.get(sourceTask);
    queue.put(input);
    Tuple firstTuple = null;
    Tuple secondTuple = null;
    List<Object> ret = null;
    int firstTupleIndex = 0, secondTupleIndex = 0;
    try {
      if (currentReduceTupleCount == 0) {
        for (int i = 0; i < expectedSourceIndexes.size(); i++) {
          queue = taskMessageQueues.get(i);
          if (queue.size() > 0 && !currentResultIndexes.contains(sourceTask)) {
            if (firstTuple == null) {
              firstTuple = queue.take();
              firstTupleIndex = i;
            } else {
              secondTuple = queue.take();
              secondTupleIndex = i;
              break;
            }
          }
        }
        if (firstTuple != null && secondTuple != null) {
          currentReducedTuple = execute(firstTuple, secondTuple);
          currentReduceTupleCount = 2;
          currentResultIndexes.add(firstTupleIndex);
          currentResultIndexes.add(secondTupleIndex);
        }
      } else {
        for (int i = 0; i < expectedSourceIndexes.size(); i++) {
          queue = taskMessageQueues.get(i);
          if (queue.size() > 0 && !currentResultIndexes.contains(sourceTask)) {
            firstTuple = queue.take();
            firstTupleIndex = i;
            break;
          }
        }
        if (firstTuple != null) {
          currentReduceTupleCount++;
          currentResultIndexes.add(firstTupleIndex);
        }
      }

      // we are done processing this set of reductions, clear
      if (currentResultIndexes.size() == expectedSourceIndexes.size()) {
        ret = currentReducedTuple;
        currentReduceTupleCount = 0;
        currentResultIndexes.clear();
        currentReducedTuple = null;
      }
    } catch (InterruptedException ignore) {
    }
  }

  public abstract List<Object> execute(Tuple firstTuple, Tuple secondTuple);

  @Override
  public void cleanup() {
  }
}
