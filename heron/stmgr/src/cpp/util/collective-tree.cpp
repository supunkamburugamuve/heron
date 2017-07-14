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

#include "util/collective-tree.h"
#include <iostream>
#include <utility>
#include <string>
#include <set>
#include <algorithm>
#include <queue>
#include <vector>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

CollectiveTree::CollectiveTree(proto::system::PhysicalPlan* _pplan,
                               const proto::api::InputStream* _is, std::string _stmgr,
                               std::string _component,
                               int _intra_node_degree, int _inter_node_degree) :
                               pplan_(_pplan), stmgr_(_stmgr), component_(_component) {
  this->is_ = _is;
  intra_node_degree_ = _intra_node_degree;
  inter_node_degree_ = _inter_node_degree;
}

void CollectiveTree::getRoutingTables(std::vector<int> &list) {
  list.clear();

  TreeNode *root = buildInterNodeTree();

  if (root == NULL) {
    LOG(ERROR) << "Failed to build the tree";
    return;
  }

  TreeNode *s = search(root, stmgr_);
  if (s != NULL) {
    std::vector<int> table;
    getMessageExpectedIndexes(s, table);
    for (size_t i = 0; i < table.size(); i++) {
      list.push_back(table.at(i));
    }
  }
}

bool CollectiveTree::getInstanceIdForComponentId(std::string _component,
                                                 int _taskId, std::string& id) {
  for (int i = 0; i < pplan_->instances_size(); i++) {
    heron::proto::system::Instance instance = pplan_->instances(i);

    if (instance.info().component_name().compare(_component) == 0 &&
        instance.info().task_id() == _taskId) {
      id = instance.instance_id();
      return true;
    }
  }
  return false;
}

void CollectiveTree::getTaskIdsOfComponent(std::string stmgr,
                                           std::string myComponent,
                                           std::vector<int> &taskIds) {
  for (int i = 0; i < pplan_->instances_size(); i++) {
    heron::proto::system::Instance instance = pplan_->instances(i);
    if (instance.stmgr_id().compare(stmgr) == 0) {
      if (instance.info().component_name().compare(myComponent) == 0) {
        taskIds.push_back(instance.info().task_id());
        LOG(INFO) << "GetTaskIDOfComponent: " << stmgr << " " << myComponent
                  << " " << instance.info().task_id();
      }
    }
  }
}

void CollectiveTree::getStmgrsOfComponent(std::string component, std::vector<std::string> &stmgrs) {
  std::set<std::string> stmgr_set;
  for (int i = 0; i < pplan_->instances_size(); i++) {
    heron::proto::system::Instance instance = pplan_->instances(i);
    if (instance.info().component_name().compare(component)) {
      stmgr_set.insert(instance.stmgr_id());
    }
  }
  stmgrs.assign(stmgr_set.begin(), stmgr_set.end());
}

bool CollectiveTree::getMessageExpectedIndexes(TreeNode *node,
                                               std::vector<int> & table) {
  if (stmgr_.compare(node->stmgrId) != 0) {
    LOG(ERROR) << "Unexpected strmgr id: " << node->stmgrId;
    return false;
  }

  if (node->nodeType != STMGR) {
    LOG(ERROR) << "Unexpected type of node";
    return false;
  }

  if (node->children.size() > 0) {
    for (size_t i = 0; i < node->children.size(); i++) {
      table.push_back(node->children.at(i)->taskId);
    }
  }
  // we only need the first one
  return true;
}

TreeNode* CollectiveTree::search(TreeNode *root, std::string stmgr) {
  std::queue<TreeNode *> nodes;
  nodes.push(root);

  while (nodes.size() > 0) {
    TreeNode *current;
    current = nodes.front();
    nodes.pop();

    if (current->nodeType == STMGR && current->stmgrId.compare(stmgr) == 0) {
      return current;
    } else {
      if (current->children.size() > 0) {
        for (size_t i = 0; i < current->children.size(); i++) {
            nodes.push(current->children[i]);
        }
      }
    }
  }
  return NULL;
}

TreeNode* CollectiveTree::buildInterNodeTree() {
  heron::proto::api::StreamId id = is_->stream();
  std::vector<std::string> stmgrs;
  getStmgrsOfComponent(component_, stmgrs);
  LOG(INFO) << "Number of stream managers: " << stmgrs.size();
  if (stmgrs.size() == 0) {
    LOG(ERROR) << "Stream managers for component is zeor: " << component_;
    return NULL;
  }

  std::sort(stmgrs.begin(), stmgrs.end());

  TreeNode *root = buildIntraNodeTree(stmgrs.at(0));
  if (root == NULL) {
    LOG(ERROR) << "Intranode tree didn't built: " << stmgrs.at(0);
    return NULL;
  }

  std::queue<TreeNode *> nodes;

  TreeNode *current = root;
  uint32_t i = 1;
  uint32_t currentInterNodeDegree = current->children.size() + inter_node_degree_;

  while (i < stmgrs.size()) {
    if (current->children.size() < currentInterNodeDegree) {
      TreeNode * e = buildIntraNodeTree(stmgrs.at(i));
      if (e != NULL) {
        current->children.push_back(e);
        e->parent = current;
        nodes.push(e);
      } else {
        LOG(ERROR) << "Stream manager with 0 components for building tree";
      }
      i++;
    } else {
      current = nodes.front();
      nodes.pop();
      currentInterNodeDegree = current->children.size() + inter_node_degree_;
    }
  }

  return root;
}

TreeNode* CollectiveTree::buildIntraNodeTree(std::string stmgr) {
  std::vector<int> taskIds;
  heron::proto::api::StreamId id = is_->stream();

  getTaskIdsOfComponent(stmgr, component_, taskIds);
  if (taskIds.size() == 0) {
    return NULL;
  }

  std::sort(taskIds.begin(), taskIds.end());
  std::string instanceId;

  TreeNode *root = new TreeNode(NULL, -1, stmgr, "", STMGR);
  TreeNode *current = root;

  uint32_t i = 0;
  std::queue<TreeNode *> nodes;

  while (i < taskIds.size()) {
    if (current->children.size() < (uint32_t)intra_node_degree_) {
      getInstanceIdForComponentId(component_, taskIds.at(i), instanceId);
      TreeNode *e = new TreeNode(current, taskIds.at(i), stmgr, instanceId, TASK);
      current->children.push_back(e);
      e->parent = current;
      nodes.push(e);
      i++;
    } else {
      current = nodes.front();
      nodes.pop();
    }
  }
  return root;
}

}  // namespace stmgr
}  // namespace heron
