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

#ifndef SRC_CPP_SVCS_STMGR_SRC_UTIL_COLLECTIVE_TREE_H_
#define SRC_CPP_SVCS_STMGR_SRC_UTIL_COLLECTIVE_TREE_H_

#include <unordered_map>
#include <utility>
#include <string>
#include <vector>
#include "proto/messages.h"
#include "basics/basics.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

enum NodeType {
  STMGR,
  TASK
};

struct TreeNode {
  std::vector<TreeNode *> children;
  TreeNode *parent;
  int taskId;
  std::string stmgrId;
  std::string instanceId;
  NodeType nodeType;

  TreeNode(TreeNode * _parent, int _taskId, std::string _stmgr,
           std::string _instance, NodeType _t) :
      parent(_parent), taskId(_taskId), stmgrId(_stmgr), instanceId(_instance), nodeType(_t) {}
};

class CollectiveTree {
 public:
  CollectiveTree(proto::system::PhysicalPlan* _pplan, const proto::api::InputStream * _is,
                 std::string _stmgr, std::string _component,
                 int _intra_node_degree, int _inter_node_degree);
  void getRoutingTables(std::unordered_map<int, std::vector<int>*>& list);
  void getRoutingTables(std::string stmgr, int originate_task,
                        std::unordered_map<int, std::vector<int>*> &list);
  void getTaskIdsOfComponent(std::string myComponent, std::vector<int> &taskIds);
  void getStmgrsOfComponent(std::string component, std::vector<std::string> &stmgrs);
  void getTaskIdsOfComponent(std::string stmgr,
                               std::string myComponent, std::vector<int> &taskIds);

 private:
  bool getInstanceIdForComponentId(std::string _component, int _taskId, std::string &instance);
  void getStmgrsOfComponentTaskIds(std::string component,
                                   std::unordered_map<int, std::string> &stmgrs);
  TreeNode* buildInterNodeTree(std::string start_stmgr);
  TreeNode* buildIntraNodeTree(std::string stmgr);
  TreeNode* search(TreeNode *root, std::string stmgr);
  bool getMessageExpectedIndexes(TreeNode *node, std::vector<int> &table);
  void getStmgrOfTaskId(int taskId, std::string &stmgr);

  proto::system::PhysicalPlan* pplan_;
  int intra_node_degree_;
  int inter_node_degree_;
  const proto::api::InputStream *is_;
  std::string stmgr_;
  std::string component_;
  int root_task_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_UTIL_COLLECTIVE_TREE_H_
