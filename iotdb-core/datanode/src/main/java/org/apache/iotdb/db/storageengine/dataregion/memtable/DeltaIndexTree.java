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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;

import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeltaIndexTree implements WALEntryValue {
  private final int degree;
  private DeltaIndexTreeNode root;
  // used for in-order leaf traversal
  private final DeltaIndexTreeLeafNode firstLeaf;

  /** Constructor */
  public DeltaIndexTree(int degree) {
    this.degree = degree;
    this.firstLeaf = new DeltaIndexTreeLeafNode();
    this.root = this.firstLeaf;
  }

  public DeltaIndexTreeLeafNode getFirstLeaf() {
    return firstLeaf;
  }

  @Override
  public int serializedSize() {
    return Integer.BYTES + root.serializedSize();
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putInt(degree);
    root.serializeToWAL(buffer);
  }

  public static DeltaIndexTree deserialize(DataInputStream stream) throws IOException {
    int degree = ReadWriteIOUtils.readInt(stream);
    DeltaIndexTree deltaIndexTree = new DeltaIndexTree(degree);
    deltaIndexTree.root = deserialize(stream, null).left;
    return deltaIndexTree;
  }

  private static Pair<DeltaIndexTreeNode, DeltaIndexTreeLeafNode> deserialize(
      DataInputStream stream, DeltaIndexTreeLeafNode leftSibling) throws IOException {
    boolean isLeaf = ReadWriteIOUtils.readBool(stream);
    if (isLeaf) {
      DeltaIndexTreeLeafNode leafNode = new DeltaIndexTreeLeafNode();
      int keySize = ReadWriteIOUtils.readInt(stream);
      for (int i = 0; i < keySize; i++) {
        leafNode.keys.add(ReadWriteIOUtils.readLong(stream));
      }
      int entrySize = ReadWriteIOUtils.readInt(stream);
      for (int i = 0; i < entrySize; i++) {
        leafNode.entries.add(DeltaIndexEntry.deserialize(stream));
      }

      if (leftSibling != null) {
        leftSibling.next = leafNode;
      }
      return new Pair<>(leafNode, leafNode);
    } else {
      DeltaIndexTreeInternalNode internalNode = new DeltaIndexTreeInternalNode();
      DeltaIndexTreeLeafNode leftNode = leftSibling;
      int keySize = ReadWriteIOUtils.readInt(stream);
      for (int i = 0; i < keySize; i++) {
        internalNode.keys.add(ReadWriteIOUtils.readLong(stream));
      }
      int childrenSize = ReadWriteIOUtils.readInt(stream);
      for (int i = 0; i < childrenSize; i++) {
        Pair<DeltaIndexTreeNode, DeltaIndexTreeLeafNode> p = deserialize(stream, leftNode);
        leftNode = p.right;
      }
      return new Pair<>(internalNode, leftSibling);
    }
  }

  public long getMinTime() {
    return firstLeaf.keys.isEmpty() ? Long.MAX_VALUE : firstLeaf.keys.get(0);
  }

  /** Search API */
  public DeltaIndexEntry search(long ts) {
    return search(root, ts);
  }

  private DeltaIndexEntry search(DeltaIndexTreeNode node, long ts) {
    if (node.isLeaf()) {
      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) node;
      int entryIndex = leaf.findRightBoundIndex(ts);
      if (leaf.keys.get(entryIndex) == ts) {
        return leaf.entries.get(entryIndex);
      }
      return null;
    } else {
      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) node;
      int childIndex = internal.findChildIndex(ts);
      return search(internal.children.get(childIndex), ts);
    }
  }

  /** Insert API */
  public void insert(long ts, int stableId, int deltaId) {
    DeltaIndexTreeNode root = this.root;
    if (isFull(root)) {
      DeltaIndexTreeInternalNode newRoot = new DeltaIndexTreeInternalNode();
      newRoot.children.add(root);
      this.root = newRoot;
      splitChild(newRoot, 0, root);
      insertNonFull(newRoot, ts, stableId, deltaId);
    } else {
      insertNonFull(root, ts, stableId, deltaId);
    }
  }

  private void insertNonFull(DeltaIndexTreeNode node, long ts, int stableId, int deltaId) {
    if (node.isLeaf()) {
      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) node;
      leaf.insert(ts, stableId, deltaId);
    } else {
      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) node;
      int childIndex = internal.findChildIndex(ts);
      DeltaIndexTreeNode child = internal.children.get(childIndex);
      if (isFull(child)) {
        splitChild(internal, childIndex, child);
        if (ts > internal.keys.get(childIndex)) {
          childIndex++;
        }
      }
      insertNonFull(internal.children.get(childIndex), ts, stableId, deltaId);
    }
  }

  private void splitChild(DeltaIndexTreeInternalNode parent, int index, DeltaIndexTreeNode child) {
    DeltaIndexTreeNode newNode;
    if (child.isLeaf()) {
      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) child;
      DeltaIndexTreeLeafNode newLeaf = new DeltaIndexTreeLeafNode();
      newNode = newLeaf;
      int mid = degree;
      newLeaf.keys.addAll(leaf.keys.subList(mid, leaf.keys.size()));
      newLeaf.entries.addAll(leaf.entries.subList(mid, leaf.entries.size()));
      leaf.keys.subList(mid, leaf.keys.size()).clear();
      leaf.entries.subList(mid, leaf.entries.size()).clear();
      newLeaf.next = leaf.next;
      leaf.next = newLeaf;
      parent.keys.add(index, newLeaf.keys.get(0));
    } else {
      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) child;
      DeltaIndexTreeInternalNode newInternal = new DeltaIndexTreeInternalNode();
      newNode = newInternal;
      int mid = degree - 1;
      long midTime = internal.keys.get(mid);
      newInternal.keys.addAll(internal.keys.subList(mid + 1, internal.keys.size()));
      internal.keys.subList(mid, internal.keys.size()).clear();
      newInternal.children.addAll(internal.children.subList(mid + 1, internal.children.size()));
      internal.children.subList(mid + 1, internal.children.size()).clear();
      parent.keys.add(index, midTime);
    }
    parent.children.add(index + 1, newNode);
  }

  /** Delete API */
  public void delete(long ts) {
    if (root.keys.isEmpty()) {
      return;
    }
    delete(root, ts);
    if (root.keys.isEmpty() && !root.isLeaf()) {
      root = ((DeltaIndexTreeInternalNode) root).children.get(0);
    }
  }

  private void delete(DeltaIndexTreeNode node, long ts) {
    if (node.isLeaf()) {
      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) node;
      leaf.delete(ts);
    } else {
      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) node;
      int childIndex = internal.findChildIndex(ts);
      DeltaIndexTreeNode child = internal.children.get(childIndex);
      delete(child, ts);
      if (isDeficient(child)) {
        fixShortage(internal, childIndex);
      }
    }
  }

  private void fixShortage(DeltaIndexTreeInternalNode parent, int index) {
    DeltaIndexTreeNode leftSibling = index > 0 ? parent.children.get(index - 1) : null;
    DeltaIndexTreeNode rightSibling =
        index < parent.children.size() - 1 ? parent.children.get(index + 1) : null;

    if (leftSibling != null && isLendable(leftSibling)) {
      borrowFromLeft(parent, index);
    } else if (rightSibling != null && isLendable(rightSibling)) {
      borrowFromRight(parent, index);
    } else {
      if (leftSibling != null) {
        mergeNodes(parent, index - 1);
      } else {
        mergeNodes(parent, index);
      }
    }
  }

  private void borrowFromLeft(DeltaIndexTreeInternalNode parent, int index) {
    DeltaIndexTreeNode leftSibling = parent.children.get(index - 1);
    DeltaIndexTreeNode child = parent.children.get(index);

    if (child.isLeaf()) {
      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) child;
      DeltaIndexTreeLeafNode leftLeaf = (DeltaIndexTreeLeafNode) leftSibling;
      long lastKey = leftLeaf.keys.remove(leftLeaf.keys.size() - 1);
      leaf.keys.add(0, lastKey);
      parent.keys.set(index - 1, leaf.keys.get(0));
    } else {
      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) child;
      DeltaIndexTreeInternalNode leftInternal = (DeltaIndexTreeInternalNode) leftSibling;
      long lastKey = leftInternal.keys.remove(leftInternal.keys.size() - 1);
      DeltaIndexTreeNode lastChild = leftInternal.children.remove(leftInternal.children.size() - 1);
      internal.keys.add(0, parent.keys.get(index - 1));
      internal.children.add(0, lastChild);
      parent.keys.set(index - 1, lastKey);
    }
  }

  private void borrowFromRight(DeltaIndexTreeInternalNode parent, int index) {
    DeltaIndexTreeNode rightSibling = parent.children.get(index + 1);
    DeltaIndexTreeNode child = parent.children.get(index);

    if (child.isLeaf()) {
      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) child;
      DeltaIndexTreeLeafNode rightLeaf = (DeltaIndexTreeLeafNode) rightSibling;
      long firstKey = rightLeaf.keys.remove(0);
      leaf.keys.add(firstKey);
      parent.keys.set(index, rightLeaf.keys.get(0));
    } else {
      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) child;
      DeltaIndexTreeInternalNode rightInternal = (DeltaIndexTreeInternalNode) rightSibling;
      long firstKey = rightInternal.keys.remove(0);
      DeltaIndexTreeNode firstChild = rightInternal.children.remove(0);
      internal.keys.add(parent.keys.get(index));
      internal.children.add(firstChild);
      parent.keys.set(index, firstKey);
    }
  }

  private void mergeNodes(DeltaIndexTreeInternalNode parent, int index) {
    DeltaIndexTreeNode leftChild = parent.children.get(index);
    DeltaIndexTreeNode rightChild = parent.children.get(index + 1);

    if (leftChild.isLeaf()) {
      DeltaIndexTreeLeafNode leftLeaf = (DeltaIndexTreeLeafNode) leftChild;
      DeltaIndexTreeLeafNode rightLeaf = (DeltaIndexTreeLeafNode) rightChild;
      leftLeaf.keys.addAll(rightLeaf.keys);
      leftLeaf.next = rightLeaf.next;
    } else {
      DeltaIndexTreeInternalNode leftInternal = (DeltaIndexTreeInternalNode) leftChild;
      DeltaIndexTreeInternalNode rightInternal = (DeltaIndexTreeInternalNode) rightChild;
      leftInternal.keys.add(parent.keys.get(index));
      leftInternal.keys.addAll(rightInternal.keys);
      leftInternal.children.addAll(rightInternal.children);
    }
    parent.keys.remove(index);
    parent.children.remove(index + 1);
  }

  private boolean isFull(DeltaIndexTreeNode node) {
    return node.keys.size() == 2 * degree - 1;
  }

  private boolean isDeficient(DeltaIndexTreeNode node) {
    return node.keys.size() < degree - 1;
  }

  private boolean isLendable(DeltaIndexTreeNode node) {
    return node.keys.size() > degree - 1;
  }

  public abstract static class DeltaIndexTreeNode implements WALEntryValue {
    protected List<Long> keys;
    protected boolean isLeaf;

    public boolean isLeaf() {
      return isLeaf;
    }
  }

  public static class DeltaIndexTreeInternalNode extends DeltaIndexTreeNode {
    protected List<DeltaIndexTreeNode> children;

    public DeltaIndexTreeInternalNode() {
      this.isLeaf = false;
      this.keys = new ArrayList<>();
      this.children = new ArrayList<>();
    }

    public int findChildIndex(long ts) {
      int i = keys.size() - 1;
      while (i >= 0 && ts < keys.get(i)) {
        i--;
      }
      return i + 1;
    }

    public DeltaIndexTreeNode getChild(int index) {
      if (index < 0 || index > children.size()) {
        return null;
      }
      return children.get(index);
    }

    @Override
    public int serializedSize() {
      // isLeaf
      int size = Byte.BYTES;
      // length & values for keys
      size += Integer.BYTES + keys.size() * Long.BYTES;
      // length & values for entries
      size += Integer.BYTES;
      for (DeltaIndexTreeNode node : children) {
        size += node.serializedSize();
      }
      return size;
    }

    @Override
    public void serializeToWAL(IWALByteBufferView buffer) {
      buffer.put(BytesUtils.boolToByte(isLeaf));
      buffer.putInt(keys.size());
      for (Long key : keys) {
        buffer.putLong(key);
      }
      buffer.putInt(children.size());
      for (DeltaIndexTreeNode node : children) {
        node.serializeToWAL(buffer);
      }
    }
  }

  public static class DeltaIndexTreeLeafNode extends DeltaIndexTreeNode {
    protected List<DeltaIndexEntry> entries;
    protected DeltaIndexTreeLeafNode next;

    public DeltaIndexTreeLeafNode() {
      this.isLeaf = true;
      this.keys = new ArrayList<>();
      this.entries = new ArrayList<>();
      this.next = null;
    }

    public void delete(long ts) {
      int entryIndex = findRightBoundIndex(ts);
      while (entryIndex >= 0 && keys.get(entryIndex) == ts) {
        keys.remove(entryIndex);
        entries.remove(entryIndex);
        entryIndex--;
      }
    }

    public void insert(long ts, int stableId, int deltaId) {
      int entryIndex = findRightBoundIndex(ts) + 1;
      keys.add(entryIndex, ts);
      entries.add(entryIndex, new DeltaIndexEntry(stableId, deltaId));
    }

    public int findRightBoundIndex(long ts) {
      int left = 0;
      int right = keys.size();
      while (left < right) {
        int mid = left + (right - left) / 2;
        if (keys.get(mid) <= ts) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
      return right - 1;
    }

    @Override
    public int serializedSize() {
      // isLeaf
      int size = Byte.BYTES;
      // length & values for keys
      size += Integer.BYTES + keys.size() * Long.BYTES;
      // length & values for entries
      size += Integer.BYTES + entries.size() * DeltaIndexEntry.serializedSize();
      return size;
    }

    @Override
    public void serializeToWAL(IWALByteBufferView buffer) {
      buffer.put(BytesUtils.boolToByte(isLeaf));
      buffer.putInt(keys.size());
      for (Long key : keys) {
        buffer.putLong(key);
      }
      buffer.putInt(entries.size());
      for (DeltaIndexEntry e : entries) {
        e.serializeToWAL(buffer);
      }
    }
  }
}
