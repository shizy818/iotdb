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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MergeSortMultiTVListIterator extends MultiTVListIterator {
  private List<Integer> probeIterators;
  private final PriorityQueue<Pair<Long, Integer>> minHeap =
      new PriorityQueue<>(
          (a, b) -> a.left.equals(b.left) ? b.right.compareTo(a.right) : a.left.compareTo(b.left));

  public MergeSortMultiTVListIterator(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding) {
    super(tsDataType, tvLists, deletionList, floatPrecision, encoding);
    this.probeIterators =
        IntStream.range(0, tvListIterators.size()).boxed().collect(Collectors.toList());
  }

  private MergeSortMultiTVListIterator() {
    super();
  }

  @Override
  public void setTVListOffsets(int[] tvListOffsets) {
    for (int i = 0; i < tvListIterators.size(); i++) {
      tvListIterators.get(i).setIndex(tvListOffsets[i]);
      this.tvListOffsets[i] = tvListOffsets[i];
    }
    minHeap.clear();
    probeIterators.clear();
    for (int i = 0; i < tvListIterators.size(); i++) {
      probeIterators.add(i);
    }
    probeNext = false;
  }

  @Override
  public MultiTVListIterator clone() {
    MergeSortMultiTVListIterator cloneIterator = new MergeSortMultiTVListIterator();
    cloneIterator.tsDataType = tsDataType;
    cloneIterator.tvListIterators = new ArrayList<>(tvListIterators.size());
    for (int i = 0; i < tvListIterators.size(); i++) {
      cloneIterator.tvListIterators.add(tvListIterators.get(i).clone());
    }
    cloneIterator.tvListOffsets = new int[tvListIterators.size()];
    cloneIterator.probeIterators =
        IntStream.range(0, tvListIterators.size()).boxed().collect(Collectors.toList());
    return cloneIterator;
  }

  protected void prepareNext() {
    hasNext = false;
    for (int i : probeIterators) {
      TVList.TVListIterator iterator = tvListIterators.get(i);
      if (iterator.hasNext()) {
        minHeap.add(new Pair<>(iterator.currentTime(), i));
      }
    }
    probeIterators.clear();

    if (!minHeap.isEmpty()) {
      Pair<Long, Integer> top = minHeap.poll();
      iteratorIndex = top.right;
      probeIterators.add(iteratorIndex);
      rowIndex = tvListIterators.get(iteratorIndex).getIndex();
      hasNext = true;
      while (!minHeap.isEmpty() && minHeap.peek().left.longValue() == top.left.longValue()) {
        Pair<Long, Integer> element = minHeap.poll();
        probeIterators.add(element.right);
      }
    }
    probeNext = true;
  }

  protected void next() {
    for (int index : probeIterators) {
      TVList.TVListIterator iterator = tvListIterators.get(index);
      iterator.step();
      tvListOffsets[index] = iterator.getIndex();
    }
    probeNext = false;
  }
}
