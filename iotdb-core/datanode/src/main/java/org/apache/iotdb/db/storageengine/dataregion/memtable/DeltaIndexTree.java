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
    deltaIndexTree.root = deserialize(stream, null).left;
    return deltaIndexTree;
  }

  private static Pair<DeltaIndexTreeNode, DeltaIndexTreeLeafNode> deserialize(
      DataInputStream stream, DeltaIndexTreeLeafNode leftSibling) throws IOException {
    boolean isLeaf = ReadWriteIOUtils.readBool(stream);
    int degree = ReadWriteIOUtils.readInt(stream);
    int count = ReadWriteIOUtils.readInt(stream);
    if (isLeaf) {
      DeltaIndexTreeLeafNode leafNode =
          (DeltaIndexTreeLeafNode) DeltaIndexTreeNodeManager.allocate(true, degree);
      for (int i = 0; i < count; i++) {
        leafNode.keys[i] = ReadWriteIOUtils.readLong(stream);
      }
      for (int i = 0; i < count; i++) {
        int stableId = ReadWriteIOUtils.readInt(stream);
        int deltaId = ReadWriteIOUtils.readInt(stream);
        leafNode.entries[i].setId(stableId, deltaId);
      }

      if (leftSibling != null) {
        leftSibling.next = leafNode;
      }
      return new Pair<>(leafNode, leafNode);
    } else {
      DeltaIndexTreeInternalNode internalNode =
          (DeltaIndexTreeInternalNode) DeltaIndexTreeNodeManager.allocate(false, degree);
      DeltaIndexTreeLeafNode leftNode = leftSibling;
      for (int i = 0; i < count; i++) {
        internalNode.keys[i] = ReadWriteIOUtils.readLong(stream);
      }
      for (int i = 0; i < count + 1; i++) {
        Pair<DeltaIndexTreeNode, DeltaIndexTreeLeafNode> p = deserialize(stream, leftNode);
        leftNode = p.right;
      }
      return new Pair<>(internalNode, leftSibling);
    }
  }

  public long getMinTime() {
    return firstLeaf.isEmpty() ? Long.MAX_VALUE : firstLeaf.keys[0];
  }

  /** Search API */
  public DeltaIndexEntry search(long ts) {
    return search(root, ts);
  }

  private DeltaIndexEntry search(DeltaIndexTreeNode node, long ts) {
    if (node.isLeaf()) {
      DeltaIndexTreeLeafNode leaf = (DeltaIndexTreeLeafNode) node;
      int entryIndex = leaf.findRightBoundIndex(ts);
      if (leaf.keys[entryIndex] == ts) {
        return leaf.entries[entryIndex];
      }
      return null;
    } else {
      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) node;
      int childIndex = internal.findChildIndex(ts);
      return search(internal.children[childIndex], ts);
    }
  }

  /** Insert API */
  public void insert(long ts, int stableId, int deltaId) {
    DeltaIndexTreeNode root = this.root;
    if (root.isFull()) {
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
      if (child.isFull()) {
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
      int mid = degree;
      // new & old leaf node
      System.arraycopy(leaf.keys, mid, newLeaf.keys, 0, leaf.count - mid);
      for (int i = mid; i < leaf.count; i++) {
        DeltaIndexEntry e = leaf.entries[i];
        newLeaf.entries[i - mid].setId(e.getStableId(), e.getDeltaId());
      }
      newLeaf.count = leaf.count - mid;
      leaf.count = mid;
      // update leaf node link
      newLeaf.next = leaf.next;
      leaf.next = newLeaf;
      // add parent keys
      parent.addChild(index, newLeaf.keys[0], newLeaf);
    } else {
      DeltaIndexTreeInternalNode internal = (DeltaIndexTreeInternalNode) child;
      DeltaIndexTreeInternalNode newInternal =
          (DeltaIndexTreeInternalNode) DeltaIndexTreeNodeManager.allocate(false, degree);
      int mid = degree - 1;
      // new & old leaf node
      System.arraycopy(internal.keys, mid + 1, newInternal.keys, 0, internal.count - mid - 1);
      System.arraycopy(internal.children, mid + 1, newInternal.children, 0, internal.count - mid);
      newInternal.count = internal.count - mid - 1;
      internal.count = mid;
      // add parent keys
      parent.addChild(index, internal.keys[mid], newInternal);
    }
  }

  public abstract static class DeltaIndexTreeNode implements WALEntryValue {
    protected int degree;
    protected long[] keys;
    protected int count;
    protected boolean isLeaf;

    public DeltaIndexTreeNode(int degree) {
      this.degree = degree;
      this.count = 0;
      this.keys = new long[2 * degree - 1];
    }

    public boolean isLeaf() {
      return isLeaf;
    }

    public boolean isEmpty() {
      return count == 0;
    }

    public boolean isFull() {
      return count == 2 * degree - 1;
    }

    public boolean isDeficient() {
      return count < degree - 1;
    }

    public boolean isLendable() {
      return count > degree - 1;
    }

    public abstract void clear();
  }

  public static class DeltaIndexTreeInternalNode extends DeltaIndexTreeNode {
    protected DeltaIndexTreeNode[] children;

    public DeltaIndexTreeInternalNode(int degree) {
      super(degree);
      this.isLeaf = false;
      this.children = new DeltaIndexTreeNode[2 * degree];
    }

    public void addChild(int index, long key, DeltaIndexTreeNode child) {
      System.arraycopy(keys, index, keys, index + 1, count - index);
      System.arraycopy(children, index + 1, children, index + 2, count - index);
      keys[index] = key;
      children[index + 1] = child;
      count++;
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

    @Override
    public int serializedSize() {
      // isLeaf & degree & count
      int size = Byte.BYTES + 2 * Integer.BYTES;
      // keys
      size += count * Long.BYTES;
      // entries
      size += Integer.BYTES;
      for (int i = 0; i < count + 1; i++) {
        size += children[i].serializedSize();
      }
      return size;
    }

    @Override
    public void serializeToWAL(IWALByteBufferView buffer) {
      buffer.put(BytesUtils.boolToByte(isLeaf));
      buffer.putInt(degree);
      buffer.putInt(count);
      for (int i = 0; i < count; i++) {
        buffer.putLong(keys[i]);
      }
      for (int i = 0; i < count + 1; i++) {
        children[i].serializeToWAL(buffer);
      }
    }

    @Override
    public void clear() {
      for (DeltaIndexTreeNode node : children) {
        if (node != null) {
          node.clear();
        }
      }
      Arrays.fill(keys, 0);
      Arrays.fill(children, null);
      this.count = 0;
      DeltaIndexTreeNodeManager.release(this);
    }
  }

  public static class DeltaIndexTreeLeafNode extends DeltaIndexTreeNode {
    protected DeltaIndexEntry[] entries;
    protected DeltaIndexTreeLeafNode next;

    public DeltaIndexTreeLeafNode(int degree) {
      super(degree);
      this.isLeaf = true;
      this.entries = new DeltaIndexEntry[2 * degree - 1];
      for (int i = 0; i < 2 * degree - 1; i++) {
        this.entries[i] = new DeltaIndexEntry(0, 0);
      }
      this.next = null;
    }

    public void delete(long ts) {
      int endIndex = findRightBoundIndex(ts);
      if (keys[endIndex] != ts) {
        return;
      }
      int startIndex = endIndex;
      while (startIndex >= 0 && keys[startIndex] == ts) {
        startIndex--;
      }
      int numMoved = count - endIndex - 1;
      System.arraycopy(keys, endIndex + 1, keys, startIndex, numMoved);
      System.arraycopy(entries, endIndex + 1, entries, startIndex, numMoved);
      int numDeleted = endIndex - startIndex + 1;
      count -= numDeleted;
    }

    public void insert(long ts, int stableId, int deltaId) {
      int entryIndex = findRightBoundIndex(ts) + 1;
      int numMoved = count - entryIndex;
      System.arraycopy(keys, entryIndex, keys, entryIndex + 1, numMoved);
      keys[entryIndex] = ts;
      for (int i = count; i > entryIndex; i--) {
        DeltaIndexEntry e = entries[i - 1];
        entries[i].setId(e.getStableId(), e.getDeltaId());
      }
      entries[entryIndex].setId(stableId, deltaId);
      count++;
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
      // isLeaf & degree & count
      int size = Byte.BYTES + 2 * Integer.BYTES;
      // keys
      size += count * Long.BYTES;
      // entries
      size += Integer.BYTES + count * DeltaIndexEntry.serializedSize();
      return size;
    }

    @Override
    public void serializeToWAL(IWALByteBufferView buffer) {
      buffer.put(BytesUtils.boolToByte(isLeaf));
      buffer.putInt(degree);
      buffer.putInt(count);
      for (int i = 0; i < count; i++) {
        buffer.putLong(keys[i]);
      }
      for (int i = 0; i < count; i++) {
        entries[i].serializeToWAL(buffer);
      }
    }

    @Override
    public void clear() {
      Arrays.fill(keys, 0);
      for (DeltaIndexEntry entry : entries) {
        entry.reset();
      }
      this.count = 0;
      this.next = null;
      DeltaIndexTreeNodeManager.release(this);
    }
  }
}
