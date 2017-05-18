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

#include "grouping/reduce-grouping.h"
#include <functional>
#include <iostream>
#include <list>
#include <vector>
#include "grouping/grouping.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

ReduceGrouping::ReduceGrouping(const std::vector<sp_int32>& _task_ids) : Grouping(_task_ids) {
  next_index_ = rand() % task_ids_.size();
}

ReduceGrouping::~ReduceGrouping() {}

void ReduceGrouping::GetListToSend(const proto::system::HeronDataTuple& _tuple,
                                    std::vector<sp_int32>& _return) {
  if (_tuple.sub_task_dest() && _tuple.dest_task_ids_size() > 0) {
    // LOG(INFO) << "Reduction to: " << _tuple.dest_task_ids(0);
    _return.push_back(_tuple.dest_task_ids(0));
  } else {
    _return.push_back(task_ids_[next_index_]);
    next_index_ = (next_index_ + 1) % task_ids_.size();
//    LOG(INFO) << "shuffle to: " << next_index_;
  }
}

}  // namespace stmgr
}  // namespace heron
