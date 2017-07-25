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

#ifndef SRC_CPP_SVCS_STMGR_SRC_GROUPING_ALL_GROUPING_H_
#define SRC_CPP_SVCS_STMGR_SRC_GROUPING_ALL_GROUPING_H_

#include <list>
#include <vector>
#include <string>
#include "grouping/grouping.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "util/collective-tree.h"

namespace heron {
namespace stmgr {

class AllGrouping : public Grouping {
 public:
  explicit AllGrouping(const proto::api::InputStream * _is, proto::system::PhysicalPlan* _pplan,
                       std::string _component, std::string _stmgr,
                       const std::vector<sp_int32>& _task_ids);
  virtual ~AllGrouping();

  virtual void GetListToSend(proto::system::HeronDataTuple& _tuple,
                             std::vector<sp_int32>& _return);
  virtual bool IsDestTaskCalculationRequired();
 private:
  proto::system::PhysicalPlan* pplan_;
  std::unordered_map<int, std::vector<int>*> task_routing_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_GROUPING_ALL_GROUPING_H_
