/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "grouping/allreduce-grouping.h"
#include <functional>
#include <iostream>
#include <list>
#include <vector>
#include <string>
#include <algorithm>
#include "grouping/grouping.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "util/collective-tree.h"
#include "config/heron-internals-config-reader.h"

namespace heron {
namespace stmgr {

AllReduceGrouping::AllReduceGrouping(const proto::api::InputStream* _is,
                         proto::system::PhysicalPlan* _pplan,
                         std::string _component, std::string _stmgr,
                         const std::vector<sp_int32>& _task_ids) :
                         Grouping(_task_ids), pplan_(_pplan) {
  int inter_node_tasks = config::HeronInternalsConfigReader::Instance()->
                                  GetHeronCollectiveBroadcastTreeInterNodeDegree();
  int intra_node_tasks = config::HeronInternalsConfigReader::Instance()->
                                  GetHeronCollectiveBroadcastTreeIntraNodeDegree();
  tree_ = new CollectiveTree(pplan_, _is, _stmgr, _component, intra_node_tasks, inter_node_tasks);
  tree_->getStmgrsOfComponent(_is->stream().component_name(), stmgrs_reduce_);
  std::sort(stmgrs_reduce_.begin(), stmgrs_reduce_.end());

  std::vector<int> task_ids_reduce;
  tree_->getTaskIdsOfComponent(stmgrs_reduce_.at(0),
                               _is->stream().component_name(), task_ids_reduce);
  std::sort(task_ids_reduce.begin(), task_ids_reduce.end());

  std::string s = "";
  for (size_t i = 0; i < task_ids_.size(); i++) {
    s += std::to_string(task_ids_.at(i)) + " ";
  }
  LOG(INFO) << "Tasks before: " << s;
  task_ids_.clear();

  tree_->getRoutingTables(stmgrs_reduce_.at(0), task_ids_reduce.at(0), task_routing_);
  s = "";
  for (auto it = task_routing_.begin(); it != task_routing_.end(); ++it) {
    std::vector<int> *tasks = it->second;
    s += std::to_string(it->first) + " : ";
    for (size_t i = 0; i < tasks->size(); i++) {
     s += std::to_string(tasks->at(i)) + " ";
    }
    s += "\n";
  }
  LOG(INFO) << "Tasks after: " << s;

  tree_->getTaskIdsOfComponent(_is->stream().component_name(), reduce_task_ids_);
  tree_->getTaskIdsOfComponent(_component, bcast_task_ids_);
}

AllReduceGrouping::~AllReduceGrouping() {}

void AllReduceGrouping::GetListToSend(proto::system::HeronDataTuple& _tuple,
                                std::vector<sp_int32>& _return) {
  if (_tuple.sub_task_dest() && _tuple.dest_task_ids_size() > 0) {
//    LOG(INFO) << "Sending to reduce tasks: " << _tuple.source_task()
//              << "->" << _tuple.dest_task_ids(0);
    _return.push_back(_tuple.dest_task_ids(0));
  } else {
    int source_task = _tuple.source_task();
//    if (std::find(reduce_task_ids_.begin(), reduce_task_ids_.end(), source_task) !=
//                                                                         reduce_task_ids_.end()) {
//      // this is from reduction, lets send it to first task in broadcast tree
//
//    } else if (std::find(bcast_task_ids_.begin(), bcast_task_ids_.end(), source_task) !=
//                                                                          bcast_task_ids_.end()) {
    std::string s = "";
    for (auto it = task_routing_.begin(); it != task_routing_.end(); ++it) {
      std::vector<int> *tasks = it->second;
      for (size_t i = 0; i < tasks->size(); i++) {
        _return.push_back(tasks->at(i));
        s += std::to_string(tasks->at(i)) + " ";
      }
    }
//    LOG(INFO) << "Sending to broadcast tasks: " << _tuple.source_task() << " " << s;
//    }
  }
}

bool AllReduceGrouping::IsDestTaskCalculationRequired() {
  return true;
}

}  // namespace stmgr
}  // namespace heron
