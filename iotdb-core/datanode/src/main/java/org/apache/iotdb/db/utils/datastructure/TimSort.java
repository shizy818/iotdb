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

import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;

import org.apache.tsfile.enums.TSDataType;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;

public class TimSort {
  /** when array size <= 32, it's better to use binarysort. */
  public static int SMALL_ARRAY_LENGTH = 32;

  protected final TVList tvList;

  private long[][] sortedTimestamps;
  private long pivotTime;

  private int[][] sortedIndices;
  private int pivotIndex;

  public TimSort(TVList tvList) {
    this.tvList = tvList;
  }

  /** the same as the 'set' function in TVList, the reason is to avoid two equal functions. */
  public void tim_set(int src, int dest) {
    tvList.set(src, dest);
  }

  public void setToSorted(int src, int dest) {
    sortedTimestamps[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = tvList.getTime(src);
    sortedIndices[dest / ARRAY_SIZE][dest % ARRAY_SIZE] = tvList.getValueIndex(src);
  }

  public void saveAsPivot(int pos) {
    pivotTime = tvList.getTime(pos);
    pivotIndex = tvList.getValueIndex(pos);
  }

  public void setFromSorted(int src, int dest) {
    tvList.set(
        dest,
        sortedTimestamps[src / ARRAY_SIZE][src % ARRAY_SIZE],
        sortedIndices[src / ARRAY_SIZE][src % ARRAY_SIZE]);
  }

  public void setPivotTo(int pos) {
    tvList.set(pos, pivotTime, pivotIndex);
  }

  /**
   * The arrays for sorting are not including in write memory now, the memory usage is considered as
   * temporary memory.
   */
  public void clearSortedTime() {
    if (sortedTimestamps != null) {
      sortedTimestamps = null;
    }
  }

  public void clearSortedValue() {
    if (sortedIndices != null) {
      sortedIndices = null;
    }
  }

  /** compare the timestamps in idx1 and idx2 */
  public int compare(int idx1, int idx2) {
    long t1 = tvList.getTime(idx1);
    long t2 = tvList.getTime(idx2);
    return Long.compare(t1, t2);
  }

  /** From TimSort.java */
  public void reverseRange(int lo, int hi) {
    hi--;
    while (lo < hi) {
      long loT = tvList.getTime(lo);
      int loV = tvList.getValueIndex(lo);
      long hiT = tvList.getTime(hi);
      int hiV = tvList.getValueIndex(hi);
      tvList.set(lo++, hiT, hiV);
      tvList.set(hi--, loT, loV);
    }
  }

  public void checkSortedTimestampsAndIndices() {
    if (sortedTimestamps == null
        || sortedTimestamps.length < PrimitiveArrayManager.getArrayRowCount(tvList.rowCount())) {
      sortedTimestamps =
          (long[][])
              PrimitiveArrayManager.createDataListsByType(TSDataType.INT64, tvList.rowCount());
    }
    if (sortedIndices == null
        || sortedIndices.length < PrimitiveArrayManager.getArrayRowCount(tvList.rowCount())) {
      sortedIndices =
          (int[][])
              PrimitiveArrayManager.createDataListsByType(TSDataType.INT32, tvList.rowCount());
    }
  }

  /**
   * the entrance of tim_sort; 1. array_size <= 32, use binary sort. 2. recursively invoke merge
   * sort.
   */
  public void sort(int lo, int hi) {
    if (lo == hi) {
      return;
    }
    if (hi - lo <= SMALL_ARRAY_LENGTH) {
      int initRunLen = countRunAndMakeAscending(lo, hi);
      binarySort(lo, hi, lo + initRunLen);
      return;
    }
    int mid = (lo + hi) >>> 1;
    sort(lo, mid);
    sort(mid, hi);
    merge(lo, mid, hi);
  }

  public int countRunAndMakeAscending(int lo, int hi) {
    assert lo < hi;
    int runHi = lo + 1;
    if (runHi == hi) {
      return 1;
    }
    // Find end of run, and reverse range if descending
    if (compare(runHi++, lo) == -1) { // Descending
      while (runHi < hi && compare(runHi, runHi - 1) == -1) {
        runHi++;
      }
      reverseRange(lo, runHi);
    } else { // Ascending
      while (runHi < hi && compare(runHi, runHi - 1) >= 0) {
        runHi++;
      }
    }

    return runHi - lo;
  }

  public void binarySort(int lo, int hi, int start) {
    assert lo <= start && start <= hi;
    if (start == lo) {
      start++;
    }
    for (; start < hi; start++) {

      saveAsPivot(start);
      // Set left (and right) to the index where a[start] (pivot) belongs
      int left = lo;
      int right = start;
      assert left <= right;
      /*
       * Invariants:
       *   pivot >= all in [lo, left).
       *   pivot <  all in [right, start).
       */
      while (left < right) {
        int mid = (left + right) >>> 1;
        if (compare(start, mid) < 0) {
          right = mid;
        } else {
          left = mid + 1;
        }
      }
      assert left == right;

      /*
       * The invariants still hold: pivot >= all in [lo, left) and
       * pivot < all in [left, start), so pivot belongs at left.  Note
       * that if there are elements equal to pivot, left points to the
       * first slot after them -- that's why this sort is stable.
       * Slide elements over to make room for pivot.
       */
      int n = start - left; // The number of elements to move
      for (int i = n; i >= 1; i--) {
        tim_set(left + i - 1, left + i);
      }
      setPivotTo(left);
    }
    for (int i = lo; i < hi; i++) {
      setToSorted(i, i);
    }
  }

  /** merge arrays [lo, mid) [mid, hi] */
  public void merge(int lo, int mid, int hi) {
    // end of sorting buffer
    int tmpIdx = 0;

    // start of unmerged parts of each sequence
    int leftIdx = lo;
    int rightIdx = mid;

    // copy the minimum elements to sorting buffer until one sequence is exhausted
    int endSide = 0;
    while (endSide == 0) {
      if (compare(leftIdx, rightIdx) <= 0) {
        setToSorted(leftIdx, lo + tmpIdx);
        tmpIdx++;
        leftIdx++;
        if (leftIdx == mid) {
          endSide = 1;
        }
      } else {
        setToSorted(rightIdx, lo + tmpIdx);
        tmpIdx++;
        rightIdx++;
        if (rightIdx == hi) {
          endSide = 2;
        }
      }
    }

    // copy the remaining elements of another sequence
    int start;
    int end;
    if (endSide == 1) {
      start = rightIdx;
      end = hi;
    } else {
      start = leftIdx;
      end = mid;
    }
    for (; start < end; start++) {
      setToSorted(start, lo + tmpIdx);
      tmpIdx++;
    }

    // copy from sorting buffer to the original arrays so that they can be further sorted
    // potential speed up: change the place of sorting buffer and origin data between merge
    // iterations
    for (int i = lo; i < hi; i++) {
      setFromSorted(i, i);
    }
  }
}
