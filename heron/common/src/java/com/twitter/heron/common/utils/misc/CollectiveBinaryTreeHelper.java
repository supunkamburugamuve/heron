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
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;

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
    int taskIndex;
    String stmgrId;
    String instanceId;

    public TreeNode(TreeNode parent, int taskIndex, String stmgrId, String instanceId) {
      this.parent = parent;
      this.taskIndex = taskIndex;
      this.stmgrId = stmgrId;
      this.instanceId = instanceId;
    }
  }

  public Map<TopologyAPI.InputStream, Map<Integer, Integer>> getRoutingTables() {
    Map<TopologyAPI.InputStream, Map<Integer, Integer>> routings = new HashMap<>();
    List<TopologyAPI.InputStream> reduceStreams =
        helper.getCollectiveGroupingStreams(grouping);
    for (TopologyAPI.InputStream stream : reduceStreams) {
      TreeNode tree = buildInterNodeTree(stream.getStream());
      if (tree == null) {
        throw new RuntimeException("Failed to build tree");
      }
      Map<Integer, Integer> routingPerStream = getMessageExpectedIndexes(tree);
      routings.put(stream, routingPerStream);
    }
    return routings;
  }

  private Map<Integer, Integer> getMessageExpectedIndexes(TreeNode treeNode) {
    Map<Integer, Integer> table = new HashMap<>();
    int myIndex = helper.getMyInstanceIndex();
    for (TreeNode child : treeNode.children) {
      table.put(child.taskIndex, myIndex);
    }

    if (treeNode.parent != null) {
      table.put(myIndex, treeNode.parent.taskIndex);
    }
    return table;
  }

  private TreeNode buildIntraNodeTree(String stmgrId, TopologyAPI.StreamId id) {
    List<Integer> indexes = helper.getIndexesOfComponent(stmgrId, id.getComponentName());

    if (indexes.size() == 0) {
      return null;
    }

    // sort the indexes to make sure everybody creating the same tree
    Collections.sort(indexes);

    TreeNode root = new TreeNode(null, indexes.get(0), stmgrId,
        helper.getInstanceIdForComponentIndex(id.getComponentName(), indexes.get(0)));
    Queue<TreeNode> queue = new LinkedList<>();
    queue.add(root);

    TreeNode current = queue.poll();
    for (int i = 1; i < indexes.size(); i++) {
      if (current.children.size() < intraNodeDegree) {
        TreeNode e = new TreeNode(current, indexes.get(i), stmgrId,
            helper.getInstanceIdForComponentIndex(id.getComponentName(), indexes.get(i)));
        current.children.add(e);
        queue.add(e);
      } else {
        current = queue.poll();
      }
    }

    return root;
  }

  private TreeNode buildInterNodeTree(TopologyAPI.StreamId id) {
    String myStmgr = helper.getMyStmgr();
    // get the stmgrs hosting the component
    List<String> stmgrs = helper.getStmgrsHostingComponent(id.getComponentName());

    if (stmgrs.size() == 0) {
      return null;
    }


    // sort the list
    Collections.sort(stmgrs);
    TreeNode root = buildIntraNodeTree(stmgrs.get(0), id);
    Queue<TreeNode> queue = new LinkedList<>();
    queue.add(root);

    TreeNode current = queue.poll();
    int currentChildren = 0;
    for (int i = 1; i < stmgrs.size(); i++) {
      if (currentChildren < interNodeDegree) {
        TreeNode e = buildIntraNodeTree(stmgrs.get(i), id);
        if (e != null) {
          current.children.add(e);
          e.parent = current;
          queue.add(e);
          currentChildren++;
        } else {
          throw new RuntimeException("Stream manager with 0 components for building tree");
        }
      } else {
        // lets return the current node for our stream manager
        if (current.stmgrId.equals(myStmgr)) {
          return current;
        }
        currentChildren = 0;
        current = queue.poll();
      }
    }
    return root;
  }
}
