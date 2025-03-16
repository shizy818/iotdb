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

import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;

import java.util.ArrayList;
import java.util.List;

public abstract class MultiTVListIterator {
  protected List<TVList.TVListIterator> tvListIterators;
  protected int[] tvListOffsets;

  protected boolean probeNext = false;
  protected TimeValuePair currentTvPair;

  protected MultiTVListIterator() {}

  public MultiTVListIterator(List<TVList> tvLists, Integer floatPrecision, TSEncoding encoding) {
    tvListIterators = new ArrayList<>(tvLists.size());
    for (TVList tvList : tvLists) {
      tvListIterators.add(tvList.iterator(floatPrecision, encoding));
    }
    this.tvListOffsets = new int[tvLists.size()];
  }

  public boolean hasNextTimeValuePair() {
    if (!probeNext) {
      prepareNext();
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

  public int[] getTVListOffsets() {
    return tvListOffsets;
  }

  public abstract void setTVListOffsets(int[] tvListOffsets);

  public abstract void step();

  public abstract MultiTVListIterator clone();

  protected abstract void prepareNext();
}
