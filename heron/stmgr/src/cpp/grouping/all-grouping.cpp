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

#include "grouping/all-grouping.h"
#include <functional>
#include <iostream>
#include <list>
#include <vector>
#include <string>
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

AllGrouping::AllGrouping(const proto::api::InputStream* _is, proto::system::PhysicalPlan* _pplan,
                         std::string _component, std::string _stmgr,
                         const std::vector<sp_int32>& _task_ids) :
                         Grouping(_task_ids), pplan_(_pplan) {
  int inter_node_tasks = config::HeronInternalsConfigReader::Instance()->
                                  GetHeronCollectiveBroadcastTreeInterNodeDegree();
  int intra_node_tasks = config::HeronInternalsConfigReader::Instance()->
                                  GetHeronCollectiveBroadcastTreeIntraNodeDegree();
  CollectiveTree tree(pplan_, _is, _stmgr, _component, intra_node_tasks, inter_node_tasks);
  std::string s = "";
  for (size_t i = 0; i < task_ids_.size(); i++) {
    s += std::to_string(task_ids_.at(i)) + " ";
  }
  LOG(INFO) << "Tasks before: " << s;
  task_ids_.clear();

  tree.getRoutingTables(task_routing_);
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
}

AllGrouping::~AllGrouping() {}

void AllGrouping::GetListToSend(const proto::system::HeronDataTuple&,
                                std::vector<sp_int32>& _return) {
  for (auto it = task_routing_.begin(); it != task_routing_.end(); ++it) {
    std::vector<int> *tasks = it->second;
    for (size_t i = 0; i < tasks->size(); i++) {
      _return.push_back(tasks->at(i));
    }
  }
}

bool AllGrouping::IsDestTaskCalculationRequired() {
  return true;
}

}  // namespace stmgr
}  // namespace heron
