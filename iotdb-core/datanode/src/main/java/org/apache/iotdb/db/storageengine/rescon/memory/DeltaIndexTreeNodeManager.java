/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.rescon.memory;

import org.apache.iotdb.db.storageengine.dataregion.memtable.DeltaIndexTree;

import java.util.ArrayDeque;

public class DeltaIndexTreeNodeManager {
  private static final ArrayDeque[] POOLED_DELTA_TREE_NODE = new ArrayDeque[2];
  private static final int[] LIMITS = new int[2];

  static {
    init();
  }

  private static void init() {
    LIMITS[0] = 1000 * 50;
    LIMITS[1] = 1000 * 1000;
    POOLED_DELTA_TREE_NODE[0] = new ArrayDeque<>(LIMITS[0]);
    POOLED_DELTA_TREE_NODE[1] = new ArrayDeque<>(LIMITS[1]);
  }

  private DeltaIndexTreeNodeManager() {
    // Empty constructor
  }

  public static Object allocate(boolean isLeaf, int degree) {
    Object node;
    int order = isLeaf ? 1 : 0;
    synchronized (POOLED_DELTA_TREE_NODE[order]) {
      node = POOLED_DELTA_TREE_NODE[order].poll();
    }
    if (node == null) {
      node =
          isLeaf
              ? new DeltaIndexTree.DeltaIndexTreeLeafNode(degree)
              : new DeltaIndexTree.DeltaIndexTreeInternalNode(degree);
    }
    return node;
  }

  public static void release(DeltaIndexTree.DeltaIndexTreeNode node) {
    int order = node.isLeaf() ? 1 : 0;
    synchronized (POOLED_DELTA_TREE_NODE[order]) {
      ArrayDeque<Object> arrays = POOLED_DELTA_TREE_NODE[order];
      if (arrays.size() < LIMITS[order]) {
        arrays.add(node);
      }
    }
  }
}
