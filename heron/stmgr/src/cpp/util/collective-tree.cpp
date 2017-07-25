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

void CollectiveTree::getRoutingTables(std::string root_stmgr, int originate_task,
                                      std::unordered_map<int, std::vector<int>*> &list) {
  list.clear();
  // getStmgrOfTaskId(originate_task, reduce_stmgr);

  std::vector<std::string> stmgrs;
  getStmgrsOfComponent(component_, stmgrs);

  LOG(INFO) << "Component ids: " << root_stmgr;
  TreeNode *root = buildInterNodeTree(root_stmgr);
  if (root == NULL) {
    LOG(ERROR) << "Failed to build the tree";
    return;
  }

  TreeNode *s = search(root, stmgr_);
  if (s != NULL) {
    std::vector<int>* table = new std::vector<int>();
    getMessageExpectedIndexes(s, *table);
    for (size_t i = 0; i < table->size(); i++) {
      std::pair<int, std::vector<int>*> p(originate_task, table);
      list.insert(p);
    }
  }
}

void CollectiveTree::getRoutingTables(std::unordered_map<int, std::vector<int>*> &list) {
  list.clear();
  std::unordered_map<int, std::string> comp_stmgrs;
  getStmgrsOfComponentTaskIds(is_->stream().component_name(), comp_stmgrs);

  std::vector<std::string> stmgrs;
  getStmgrsOfComponent(component_, stmgrs);

  for (auto it = comp_stmgrs.begin(); it != comp_stmgrs.end(); ++it) {
    LOG(INFO) << "Component ids: " << it->first << " : " << it->second;
    std::string root_stmgr;
    // we cannot find the stmgr of parent in the component
    root_stmgr = it->second;
    TreeNode *root = buildInterNodeTree(root_stmgr);

    if (root == NULL) {
      LOG(ERROR) << "Failed to build the tree";
      return;
    }

    TreeNode *s = search(root, stmgr_);
    if (s != NULL) {
      std::vector<int>* table = new std::vector<int>();
      getMessageExpectedIndexes(s, *table);
      for (size_t i = 0; i < table->size(); i++) {
        std::pair<int, std::vector<int>*> p(it->first, table);
        list.insert(p);
      }
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

void CollectiveTree::getStmgrOfTaskId(int taskId, std::string &stmgr) {
  for (int i = 0; i < pplan_->instances_size(); i++) {
    heron::proto::system::Instance instance = pplan_->instances(i);
    if (instance.info().task_id() == taskId) {
      stmgr = instance.stmgr_id();
      LOG(INFO) << "GetTaskIDOfComponent: " << stmgr << " " << instance.info().task_id();
      break;
    }
  }
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

void CollectiveTree::getTaskIdsOfComponent(std::string myComponent,
                                           std::vector<int> &taskIds) {
  for (int i = 0; i < pplan_->instances_size(); i++) {
    heron::proto::system::Instance instance = pplan_->instances(i);
    if (instance.info().component_name().compare(myComponent) == 0) {
      taskIds.push_back(instance.info().task_id());
      LOG(INFO) << "GetTaskIDOfComponent: " << myComponent << " " << instance.info().task_id();
    }
  }
}

void CollectiveTree::getStmgrsOfComponentTaskIds(std::string component,
                                                 std::unordered_map<int, std::string> &stmgrs) {
  for (int i = 0; i < pplan_->instances_size(); i++) {
    heron::proto::system::Instance instance = pplan_->instances(i);
    if (instance.info().component_name().compare(component) == 0) {
      stmgrs[instance.info().task_id()] = instance.stmgr_id();
    }
  }
}

void CollectiveTree::getStmgrsOfComponent(std::string component, std::vector<std::string> &stmgrs) {
  std::set<std::string> stmgr_set;
  for (int i = 0; i < pplan_->instances_size(); i++) {
    heron::proto::system::Instance instance = pplan_->instances(i);
    LOG(INFO) << "Instance: " << instance.info().component_name();
    if (instance.info().component_name().compare(component) == 0) {
      stmgr_set.insert(instance.stmgr_id());
      LOG(INFO) << "STMGR: " << instance.stmgr_id();
    }
  }
  //  stmgrs.assign(stmgr_set.begin(), stmgr_set.end());
  std::set<std::string>::iterator it;
  std::string s = "";
  for (it = stmgr_set.begin(); it != stmgr_set.end(); ++it) {
    s += *it + " ";
    stmgrs.push_back(*it);
  }
  LOG(INFO) << "STMGRS: " << s;
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
      if (node->children.at(i)->nodeType == TASK) {
        table.push_back(node->children.at(i)->taskId);
      } else if (node->children.at(i)->nodeType == STMGR) {
        for (size_t j = 0; j < node->children.at(i)->children.size(); j++) {
          TreeNode *temp = node->children.at(i)->children.at(j);
          if (temp->nodeType == TASK) {
            table.push_back(temp->taskId);
            break;
          }
        }
      }
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

TreeNode* CollectiveTree::buildInterNodeTree(std::string start_stmgr) {
  heron::proto::api::StreamId id = is_->stream();
  std::vector<std::string> stmgrs;
  getStmgrsOfComponent(component_, stmgrs);
  LOG(INFO) << "Number of stream managers: " << stmgrs.size();
  if (stmgrs.size() == 0) {
    LOG(ERROR) << "Stream managers for component is zeor: " << component_;
    return NULL;
  }

  std::sort(stmgrs.begin(), stmgrs.end());
  int start_index = 0;
  for (int i = 0; stmgrs.size(); i++) {
    if (stmgrs.at(i).compare(start_stmgr) == 0) {
      start_index = i;
      break;
    }
  }
  LOG(INFO) << "Start index for : " << start_stmgr << " " << start_index;

  TreeNode *root = buildIntraNodeTree(stmgrs.at(start_index));
  if (root == NULL) {
    LOG(ERROR) << "Intranode tree didn't built: " << stmgrs.at(start_index);
    return NULL;
  }

  std::queue<TreeNode *> nodes;

  TreeNode *current = root;
  uint32_t i = 1;
  uint32_t currentInterNodeDegree = current->children.size() + inter_node_degree_;

  while (i < stmgrs.size()) {
    if (current->children.size() < currentInterNodeDegree) {
      TreeNode * e = buildIntraNodeTree(stmgrs.at((i + start_index) % stmgrs.size()));
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
  TreeNode *root = new TreeNode(NULL, -1, stmgr, "", STMGR);

  getTaskIdsOfComponent(stmgr, component_, taskIds);
  if (taskIds.size() == 0) {
    return root;
  }

  std::sort(taskIds.begin(), taskIds.end());
  std::string instanceId;

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
