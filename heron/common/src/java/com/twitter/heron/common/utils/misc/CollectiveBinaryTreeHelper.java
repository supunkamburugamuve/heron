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
package com.twitter.heron.common.utils.misc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.Pair;

public class CollectiveBinaryTreeHelper {
  private Logger LOG = Logger.getLogger(CollectiveBinaryTreeHelper.class.getName());

  private PhysicalPlanHelper helper;
  private int intraNodeDegree;
  private int interNodeDegree;
  private TopologyAPI.Grouping grouping;

  public CollectiveBinaryTreeHelper(PhysicalPlanHelper helper,
                                    int intraNodeDegree, int interNodeDegree,
                                    TopologyAPI.Grouping grouping) {
    this.helper = helper;
    this.interNodeDegree = interNodeDegree;
    if (interNodeDegree <= 0) {
      this.interNodeDegree = Integer.MAX_VALUE;
    }
    if (intraNodeDegree <= 0) {
      this.intraNodeDegree = Integer.MAX_VALUE;
    }
    this.intraNodeDegree = intraNodeDegree;
    this.grouping = grouping;
  }

  private class TreeNode {
    List<TreeNode> children = new ArrayList<>();
    TreeNode parent;
    int taskId;
    String stmgrId;
    String instanceId;

    public TreeNode(TreeNode parent, int taskId, String stmgrId, String instanceId) {
      this.parent = parent;
      this.taskId = taskId;
      this.stmgrId = stmgrId;
      this.instanceId = instanceId;
    }
  }

  public Map<TopologyAPI.StreamId, List<Pair<Integer, Integer>>> getRoutingTables() {
    Map<TopologyAPI.StreamId, List<Pair<Integer, Integer>>> routings = new HashMap<>();
    List<TopologyAPI.InputStream> reduceStreams =
        helper.getCollectiveGroupingStreams(grouping);
    for (TopologyAPI.InputStream stream : reduceStreams) {
      TreeNode tree = buildInterNodeTree(stream.getStream());
      if (tree == null) {
        throw new RuntimeException("Failed to build tree");
      }
      TreeNode search = search(tree, helper.getMyStmgr(), helper.getMyTaskId());
      if (search != null) {
        List<Pair<Integer, Integer>> routingPerStream = getMessageExpectedIndexes(search);
        routings.put(stream.getStream(), routingPerStream);
      } else {
        LOG.log(Level.SEVERE, "Failed find the my task id in the tree");
      }
    }
    return routings;
  }

  private List<Pair<Integer, Integer>> getMessageExpectedIndexes(TreeNode treeNode) {
    List<Pair<Integer, Integer>> table = new ArrayList<>();
    int myTaskId = helper.getMyTaskId();

    if (myTaskId != treeNode.taskId) {
      String format = String.format("Unexpected task id %d != %d",
          myTaskId, treeNode.taskId);
      LOG.log(Level.SEVERE, format);
      throw new RuntimeException(format);
    }

    for (TreeNode child : treeNode.children) {
      table.add(new Pair<Integer, Integer>(child.taskId, myTaskId));
    }

    if (treeNode.parent != null) {
      table.add(new Pair<Integer, Integer>(myTaskId, treeNode.parent.taskId));
    }

    if (treeNode.children.size() > 0) {
      table.add(new Pair<Integer, Integer>(myTaskId, myTaskId));
    }

    return table;
  }

  private TreeNode search(TreeNode root, String stmgrId, int taskId) {
    Queue<TreeNode> queue = new LinkedList<>();
    queue.add(root);

    while (queue.size() > 0) {
      TreeNode current = queue.poll();
      if (current.taskId == taskId && current.stmgrId.equals(stmgrId)) {
        return current;
      } else {
        queue.addAll(current.children);
      }
    }

    return null;
  }

  private TreeNode buildIntraNodeTree(String stmgrId, TopologyAPI.StreamId id) {
    List<Integer> taskIds = helper.getTaskIdsOfComponent(stmgrId, id.getComponentName());

    if (taskIds.size() == 0) {
      return null;
    }

    LOG.log(Level.INFO, "Number of tasks: " + taskIds.size());

    // sort the taskIds to make sure everybody creating the same tree
    Collections.sort(taskIds);

    TreeNode root = new TreeNode(null, taskIds.get(0), stmgrId,
        helper.getInstanceIdForComponentId(id.getComponentName(), taskIds.get(0)));
    Queue<TreeNode> queue = new LinkedList<>();
    queue.add(root);

    TreeNode current = queue.poll();
    int i = 1;
    while (i < taskIds.size()) {
      if (current.children.size() < intraNodeDegree) {
        TreeNode e = new TreeNode(current, taskIds.get(i), stmgrId,
            helper.getInstanceIdForComponentId(id.getComponentName(), taskIds.get(i)));
        current.children.add(e);
        e.parent = current;
        queue.add(e);
        i++;
      } else {
        current = queue.poll();
      }
    }

    return root;
  }

  private TreeNode buildInterNodeTree(TopologyAPI.StreamId id) {
    // get the stmgrs hosting the component
    List<String> stmgrs = helper.getStmgrsHostingComponent(id.getComponentName());
    LOG.log(Level.INFO, "Number of stream managers: " + stmgrs.size());
    if (stmgrs.size() == 0) {
      LOG.log(Level.WARNING, "Stream managers for component is zero: " + id.getComponentName());
      return null;
    }

//    if (interNodeDegree <= intraNodeDegree) {
//      LOG.log(Level.WARNING, "Increasing inter node degree by 64: " + interNodeDegree);
//      interNodeDegree = intraNodeDegree + 2;
//    }

    // sort the list
    Collections.sort(stmgrs);
    TreeNode root = buildIntraNodeTree(stmgrs.get(0), id);
    if (root == null) {
      LOG.log(Level.WARNING, "Intranode tree didn't built: " + stmgrs.get(0) +
          " : " + id.getComponentName());
      return null;
    }
    Queue<TreeNode> queue = new LinkedList<>();
    queue.add(root);

    TreeNode current = queue.poll();
    int i = 1;
    int currentInterNodeDegree = current.children.size() + interNodeDegree;

    while (i < stmgrs.size()) {
      if (current.children.size() < currentInterNodeDegree) {
        TreeNode e = buildIntraNodeTree(stmgrs.get(i), id);
        if (e != null) {
          current.children.add(e);
          e.parent = current;
          queue.add(e);
        } else {
          throw new RuntimeException("Stream manager with 0 components for building tree");
        }
        i++;
      } else {
        current = queue.poll();
        currentInterNodeDegree = current.children.size() + interNodeDegree;
      }
    }
    return root;
  }
}
