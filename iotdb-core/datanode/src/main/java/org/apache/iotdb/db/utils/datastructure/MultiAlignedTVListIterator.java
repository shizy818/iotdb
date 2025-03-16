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
import org.apache.tsfile.read.TimeValuePair;

import java.util.ArrayList;
import java.util.List;

public abstract class MultiAlignedTVListIterator {
  protected List<AlignedTVList.AlignedTVListIterator> alignedTvListIterators;
  protected int[] alignedTvListOffsets;

  protected boolean probeNext = false;
  protected TimeValuePair currentTvPair;

  protected MultiAlignedTVListIterator() {}

  public MultiAlignedTVListIterator(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    this.alignedTvListIterators = new ArrayList<>(alignedTvLists.size());
    for (AlignedTVList alignedTvList : alignedTvLists) {
      alignedTvListIterators.add(
          alignedTvList.iterator(
              tsDataTypes, columnIndexList, floatPrecision, encodingList, ignoreAllNullRows));
    }
    this.alignedTvListOffsets = new int[alignedTvLists.size()];
  }

  public boolean hasNextTimeValuePair() {
    if (!probeNext) {
      prepareNextRow();
    }
    return currentTvPair != null;
  }

  public TimeValuePair nextTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    step();
    return currentTvPair;
  }

  public TimeValuePair currentTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    return currentTvPair;
  }

  public long getUsedMemorySize() {
    // not used
    return 0;
  }

  public int[] getAlignedTVListOffsets() {
    return alignedTvListOffsets;
  }

  public abstract void setAlignedTVListOffsets(int[] alignedTvListOffsets);

  public abstract void step();

  public abstract MultiAlignedTVListIterator clone();

  protected abstract void prepareNextRow();
}
