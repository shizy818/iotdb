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
import org.apache.iotdb.db.storageengine.rescon.memory.DeltaIndexTreeNodeManager;

import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

public class DeltaIndexTree implements WALEntryValue {
  private final int degree;
  private DeltaIndexTreeNode root;
  // used for in-order leaf traversal
  private final DeltaIndexTreeLeafNode firstLeaf;

  /** Constructor */
  public DeltaIndexTree(int degree) {
    this.degree = degree;
    this.firstLeaf = (DeltaIndexTreeLeafNode) DeltaIndexTreeNodeManager.allocate(true, degree);
    this.root = this.firstLeaf;
  }

  public void clear() {
    root.clear();
    root = null;
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
    deltaIndexTree.root = deserialize(stream, null, degree).left;
    return deltaIndexTree;
  }

  private static Pair<DeltaIndexTreeNode, DeltaIndexTreeLeafNode> deserialize(
      DataInputStream stream, DeltaIndexTreeLeafNode leftSibling, int degree) throws IOException {
    boolean isLeaf = ReadWriteIOUtils.readBool(stream);
    if (isLeaf) {
      DeltaIndexTreeLeafNode leafNode =
          (DeltaIndexTreeLeafNode) DeltaIndexTreeNodeManager.allocate(true, degree);
      int keySize = ReadWriteIOUtils.readInt(stream);
      for (int i = 0; i < keySize; i++) {
        leafNode.keys[i] = ReadWriteIOUtils.readLong(stream);
      }
      int entrySize = ReadWriteIOUtils.readInt(stream);
      for (int i = 0; i < entrySize; i++) {
        leafNode.stableIds[i] = ReadWriteIOUtils.readInt(stream);
        leafNode.deltaIds[i] = ReadWriteIOUtils.readInt(stream);
      }

      if (leftSibling != null) {
        leftSibling.next = leafNode;
      }
      return new Pair<>(leafNode, leafNode);
    } else {
      DeltaIndexTreeInternalNode internalNode =
          (DeltaIndexTreeInternalNode) DeltaIndexTreeNodeManager.allocate(false, degree);
      DeltaIndexTreeLeafNode leftNode = leftSibling;
      int keySize = ReadWriteIOUtils.readInt(stream);
      for (int i = 0; i < keySize; i++) {
        internalNode.keys[i] = ReadWriteIOUtils.readLong(stream);
      }
      int childrenSize = ReadWriteIOUtils.readInt(stream);
      for (int i = 0; i < childrenSize; i++) {
        Pair<DeltaIndexTreeNode, DeltaIndexTreeLeafNode> p = deserialize(stream, leftNode, degree);
        leftNode = p.right;
      }
      return new Pair<>(internalNode, leftSibling);
    }
  }

  public long getMinTime() {
    return firstLeaf.isEmpty() ? Long.MAX_VALUE : firstLeaf.keys[0];
  }

  //  /** Search API */
  //  public DeltaIndexEntry search(long ts) {
  //    return search(root, ts);
  //  }
  //
  //  private DeltaIndexEntry search(DeltaIndexTreeNode node, long ts) {
  //    if (node.isLeaf()) {
  //      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) node;
  //      int entryIndex = leaf.findRightBoundIndex(ts);
  //      if (leaf.keys.get(entryIndex) == ts) {
  //        return leaf.entries.get(entryIndex);
  //      }
  //      return null;
  //    } else {
  //      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) node;
  //      int childIndex = internal.findChildIndex(ts);
  //      return search(internal.children.get(childIndex), ts);
  //    }
  //  }

  /** Insert API */
  public void insert(long ts, int stableId, int deltaId) {
    DeltaIndexTreeNode root = this.root;
    if (isFull(root)) {
      DeltaIndexTreeInternalNode newRoot =
          (DeltaIndexTreeInternalNode) DeltaIndexTreeNodeManager.allocate(false, degree);
      newRoot.children[0] = root;
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
      DeltaIndexTreeNode child = internal.children[childIndex];
      if (isFull(child)) {
        splitChild(internal, childIndex, child);
        if (ts > internal.keys[childIndex]) {
          childIndex++;
        }
      }
      insertNonFull(internal.children[childIndex], ts, stableId, deltaId);
    }
  }

  private void splitChild(DeltaIndexTreeInternalNode parent, int index, DeltaIndexTreeNode child) {
    if (child.isLeaf()) {
      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) child;
      DeltaIndexTreeLeafNode newLeaf =
          (DeltaIndexTreeLeafNode) DeltaIndexTreeNodeManager.allocate(true, degree);
      leaf.moveTo(newLeaf, degree, leaf.count);
      newLeaf.next = leaf.next;
      leaf.next = newLeaf;
      parent.add(index, newLeaf.keys[0], newLeaf);
    } else {
      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) child;
      DeltaIndexTreeInternalNode newInternal =
          (DeltaIndexTreeInternalNode) DeltaIndexTreeNodeManager.allocate(false, degree);
      long midTime = internal.moveTo(newInternal, degree, internal.count);
      parent.add(index, midTime, newInternal);
    }
  }

  //  /** Delete API */
  //  public void delete(long ts) {
  //    if (root.keys.isEmpty()) {
  //      return;
  //    }
  //    delete(root, ts);
  //    if (root.keys.isEmpty() && !root.isLeaf()) {
  //      root = ((DeltaIndexTreeInternalNode) root).children.get(0);
  //    }
  //  }
  //
  //  private void delete(DeltaIndexTreeNode node, long ts) {
  //    if (node.isLeaf()) {
  //      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) node;
  //      leaf.delete(ts);
  //    } else {
  //      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) node;
  //      int childIndex = internal.findChildIndex(ts);
  //      DeltaIndexTreeNode child = internal.children.get(childIndex);
  //      delete(child, ts);
  //      if (isDeficient(child)) {
  //        fixShortage(internal, childIndex);
  //      }
  //    }
  //  }
  //
  //  private void fixShortage(DeltaIndexTreeInternalNode parent, int index) {
  //    DeltaIndexTreeNode leftSibling = index > 0 ? parent.children.get(index - 1) : null;
  //    DeltaIndexTreeNode rightSibling =
  //        index < parent.children.size() - 1 ? parent.children.get(index + 1) : null;
  //
  //    if (leftSibling != null && isLendable(leftSibling)) {
  //      borrowFromLeft(parent, index);
  //    } else if (rightSibling != null && isLendable(rightSibling)) {
  //      borrowFromRight(parent, index);
  //    } else {
  //      if (leftSibling != null) {
  //        mergeNodes(parent, index - 1);
  //      } else {
  //        mergeNodes(parent, index);
  //      }
  //    }
  //  }
  //
  //  private void borrowFromLeft(DeltaIndexTreeInternalNode parent, int index) {
  //    DeltaIndexTreeNode leftSibling = parent.children.get(index - 1);
  //    DeltaIndexTreeNode child = parent.children.get(index);
  //
  //    if (child.isLeaf()) {
  //      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) child;
  //      DeltaIndexTreeLeafNode leftLeaf = (DeltaIndexTreeLeafNode) leftSibling;
  //      long lastKey = leftLeaf.keys.remove(leftLeaf.keys.size() - 1);
  //      leaf.keys.add(0, lastKey);
  //      parent.keys.set(index - 1, leaf.keys.get(0));
  //    } else {
  //      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) child;
  //      DeltaIndexTreeInternalNode leftInternal = (DeltaIndexTreeInternalNode) leftSibling;
  //      long lastKey = leftInternal.keys.remove(leftInternal.keys.size() - 1);
  //      DeltaIndexTreeNode lastChild = leftInternal.children.remove(leftInternal.children.size() -
  // 1);
  //      internal.keys.add(0, parent.keys.get(index - 1));
  //      internal.children.add(0, lastChild);
  //      parent.keys.set(index - 1, lastKey);
  //    }
  //  }
  //
  //  private void borrowFromRight(DeltaIndexTreeInternalNode parent, int index) {
  //    DeltaIndexTreeNode rightSibling = parent.children.get(index + 1);
  //    DeltaIndexTreeNode child = parent.children.get(index);
  //
  //    if (child.isLeaf()) {
  //      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) child;
  //      DeltaIndexTreeLeafNode rightLeaf = (DeltaIndexTreeLeafNode) rightSibling;
  //      long firstKey = rightLeaf.keys.remove(0);
  //      leaf.keys.add(firstKey);
  //      parent.keys.set(index, rightLeaf.keys.get(0));
  //    } else {
  //      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) child;
  //      DeltaIndexTreeInternalNode rightInternal = (DeltaIndexTreeInternalNode) rightSibling;
  //      long firstKey = rightInternal.keys.remove(0);
  //      DeltaIndexTreeNode firstChild = rightInternal.children.remove(0);
  //      internal.keys.add(parent.keys.get(index));
  //      internal.children.add(firstChild);
  //      parent.keys.set(index, firstKey);
  //    }
  //  }
  //
  //  private void mergeNodes(DeltaIndexTreeInternalNode parent, int index) {
  //    DeltaIndexTreeNode leftChild = parent.children.get(index);
  //    DeltaIndexTreeNode rightChild = parent.children.get(index + 1);
  //
  //    if (leftChild.isLeaf()) {
  //      DeltaIndexTreeLeafNode leftLeaf = (DeltaIndexTreeLeafNode) leftChild;
  //      DeltaIndexTreeLeafNode rightLeaf = (DeltaIndexTreeLeafNode) rightChild;
  //      leftLeaf.keys.addAll(rightLeaf.keys);
  //      leftLeaf.next = rightLeaf.next;
  //    } else {
  //      DeltaIndexTreeInternalNode leftInternal = (DeltaIndexTreeInternalNode) leftChild;
  //      DeltaIndexTreeInternalNode rightInternal = (DeltaIndexTreeInternalNode) rightChild;
  //      leftInternal.keys.add(parent.keys.get(index));
  //      leftInternal.keys.addAll(rightInternal.keys);
  //      leftInternal.children.addAll(rightInternal.children);
  //    }
  //    parent.keys.remove(index);
  //    parent.children.remove(index + 1);
  //  }

  private boolean isFull(DeltaIndexTreeNode node) {
    return node.count == 2 * degree - 1;
  }

  private boolean isDeficient(DeltaIndexTreeNode node) {
    return node.count < degree - 1;
  }

  private boolean isLendable(DeltaIndexTreeNode node) {
    return node.count > degree - 1;
  }

  public abstract static class DeltaIndexTreeNode implements WALEntryValue {
    protected long[] keys;
    protected int count;
    protected boolean isLeaf;

    public boolean isLeaf() {
      return isLeaf;
    }

    public boolean isEmpty() {
      return count == 0;
    }

    public abstract void clear();
  }

  public static class DeltaIndexTreeInternalNode extends DeltaIndexTreeNode {
    protected DeltaIndexTreeNode[] children;

    public DeltaIndexTreeInternalNode(int degree) {
      this.isLeaf = false;
      this.keys = new long[2 * degree - 1];
      this.count = 0;
      this.children = new DeltaIndexTreeNode[2 * degree];
    }

    public int findChildIndex(long ts) {
      int i = count - 1;
      while (i >= 0 && ts < keys[i]) {
        i--;
      }
      return i + 1;
    }

    public DeltaIndexTreeNode getChild(int index) {
      if (index < 0 || index >= count + 1) {
        return null;
      }
      return children[index];
    }

    public void add(int index, long ts, DeltaIndexTreeNode node) {
      if (index < count) {
        System.arraycopy(keys, index, keys, index + 1, count - index);
        System.arraycopy(children, index + 1, children, index + 2, count - index);
      }
      keys[index] = ts;
      children[index + 1] = node;
      count++;
    }

    @Override
    public int serializedSize() {
      // isLeaf
      int size = Byte.BYTES;
      // length & values for keys
      size += Integer.BYTES + count * Long.BYTES;
      // length & values for entries
      size += Integer.BYTES;
      for (int i = 0; i < count + 1; i++) {
        size += children[i].serializedSize();
      }
      return size;
    }

    @Override
    public void serializeToWAL(IWALByteBufferView buffer) {
      buffer.put(BytesUtils.boolToByte(isLeaf));
      buffer.putInt(count);
      for (int i = 0; i < count; i++) {
        buffer.putLong(keys[i]);
      }
      buffer.putInt(count + 1);
      for (int i = 0; i < count + 1; i++) {
        children[i].serializeToWAL(buffer);
      }
    }

    @Override
    public void clear() {
      for (int i = 0; i < count + 1; i++) {
        children[i].clear();
      }
      Arrays.fill(keys, 0);
      Arrays.fill(children, null);
      this.count = 0;
      DeltaIndexTreeNodeManager.release(this);
    }

    public long moveTo(DeltaIndexTreeInternalNode dest, int start, int end) {
      System.arraycopy(this.keys, start, dest.keys, 0, end - start);
      System.arraycopy(this.children, start, dest.children, 0, end - start + 1);
      dest.count = end - start;

      int mid = start - 1;
      this.count = mid;
      return this.keys[mid];
    }
  }

  public static class DeltaIndexTreeLeafNode extends DeltaIndexTreeNode {
    protected int[] stableIds;
    protected int[] deltaIds;
    protected DeltaIndexTreeLeafNode next;

    public DeltaIndexTreeLeafNode(int degree) {
      this.isLeaf = true;
      this.keys = new long[2 * degree - 1];
      this.stableIds = new int[2 * degree - 1];
      this.deltaIds = new int[2 * degree - 1];
      this.count = 0;
      this.next = null;
    }

    public void delete(long ts) {
      int entryIndex = findRightBoundIndex(ts);
      while (entryIndex >= 0 && keys[entryIndex] == ts) {
        remove(entryIndex);
        entryIndex--;
      }
    }

    public void insert(long ts, int stableId, int deltaId) {
      if (count >= keys.length) {
        throw new ArrayIndexOutOfBoundsException(count);
      }
      int entryIndex = findRightBoundIndex(ts) + 1;
      add(entryIndex, ts, stableId, deltaId);
    }

    public int findRightBoundIndex(long ts) {
      int left = 0;
      int right = count;
      while (left < right) {
        int mid = left + (right - left) / 2;
        if (keys[mid] <= ts) {
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
      size += Integer.BYTES + count * Long.BYTES;
      // length & values for entries
      size += Integer.BYTES + count * 2 * Integer.BYTES;
      return size;
    }

    @Override
    public void serializeToWAL(IWALByteBufferView buffer) {
      buffer.put(BytesUtils.boolToByte(isLeaf));
      buffer.putInt(count);
      for (int i = 0; i < count; i++) {
        buffer.putLong(keys[i]);
      }
      buffer.putInt(count);
      for (int i = 0; i < count; i++) {
        buffer.putInt(stableIds[i]);
        buffer.putInt(deltaIds[i]);
      }
    }

    @Override
    public void clear() {
      Arrays.fill(keys, 0);
      Arrays.fill(stableIds, -1);
      Arrays.fill(deltaIds, -1);
      this.count = 0;
      this.next = null;
      DeltaIndexTreeNodeManager.release(this);
    }

    public void moveTo(DeltaIndexTreeLeafNode dest, int start, int end) {
      System.arraycopy(this.keys, start, dest.keys, 0, end - start);
      System.arraycopy(this.stableIds, start, dest.stableIds, 0, end - start);
      System.arraycopy(this.deltaIds, start, dest.deltaIds, 0, end - start);
      this.count = start;
      dest.count = end - start;
    }

    private void remove(int index) {
      if (index == count - 1) {
        keys[index] = 0;
        stableIds[index] = -1;
        deltaIds[index] = -1;
      } else {
        System.arraycopy(keys, index + 1, keys, index, count - index - 1);
        System.arraycopy(stableIds, index + 1, stableIds, index, count - index - 1);
        System.arraycopy(deltaIds, index + 1, deltaIds, index, count - index - 1);
      }
      count--;
    }

    private void add(int index, long ts, int stableId, int deltaId) {
      if (index == count) {
        keys[index] = ts;
        stableIds[index] = stableId;
        deltaIds[index] = deltaId;
      } else {
        System.arraycopy(keys, index, keys, index + 1, count - index);
        System.arraycopy(stableIds, index, stableIds, index + 1, count - index);
        System.arraycopy(deltaIds, index, deltaIds, index + 1, count - index);
        keys[index] = ts;
        stableIds[index] = stableId;
        deltaIds[index] = deltaId;
      }
      count++;
    }
  }
}
