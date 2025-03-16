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

import java.util.ArrayList;
import java.util.List;

public class OrderedMultiAlignedTVListIterator extends MultiAlignedTVListIterator {
  private int iteratorIndex = 0;

  private OrderedMultiAlignedTVListIterator() {
    super();
  }

  public OrderedMultiAlignedTVListIterator(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    super(
        alignedTvLists,
        tsDataTypes,
        columnIndexList,
        floatPrecision,
        encodingList,
        ignoreAllNullRows);
  }

  @Override
  protected void prepareNextRow() {
    currentTvPair = null;
    while (iteratorIndex < alignedTvListIterators.size() - 1
        && !alignedTvListIterators.get(iteratorIndex).hasNext()) {
      iteratorIndex++;
    }
    TVList.TVListIterator iterator = alignedTvListIterators.get(iteratorIndex);
    if (iterator.hasNext()) {
      currentTvPair = iterator.current();
    }
    probeNext = true;
  }

  @Override
  public void step() {
    AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators.get(iteratorIndex);
    iterator.step();
    alignedTvListOffsets[iteratorIndex] = iterator.getIndex();
    probeNext = false;
  }

  @Override
  public void setAlignedTVListOffsets(int[] alignedTvListOffsets) {
    for (int i = 0; i < alignedTvListIterators.size(); i++) {
      alignedTvListIterators.get(i).setIndex(alignedTvListOffsets[i]);
      this.alignedTvListOffsets[i] = alignedTvListOffsets[i];
    }
    probeNext = false;
  }

  @Override
  public MultiAlignedTVListIterator clone() {
    OrderedMultiAlignedTVListIterator cloneIterator = new OrderedMultiAlignedTVListIterator();
    cloneIterator.alignedTvListIterators = new ArrayList<>(alignedTvListIterators.size());
    for (int i = 0; i < alignedTvListIterators.size(); i++) {
      cloneIterator.alignedTvListIterators.add(alignedTvListIterators.get(i).clone());
    }
    cloneIterator.alignedTvListOffsets = new int[alignedTvListIterators.size()];
    cloneIterator.iteratorIndex = 0;
    return cloneIterator;
  }
}
