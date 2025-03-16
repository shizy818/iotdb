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

import java.util.ArrayList;
import java.util.List;

public class OrderedMultiTVListIterator extends MultiTVListIterator {
  private int iteratorIndex = 0;

  private OrderedMultiTVListIterator() {
    super();
  }

  public OrderedMultiTVListIterator(
      List<TVList> tvLists, Integer floatPrecision, TSEncoding encoding) {
    super(tvLists, floatPrecision, encoding);
  }

  @Override
  protected void prepareNext() {
    currentTvPair = null;
    while (iteratorIndex < tvListIterators.size() - 1
        && !tvListIterators.get(iteratorIndex).hasNext()) {
      iteratorIndex++;
    }
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    if (iterator.hasNext()) {
      currentTvPair = iterator.current();
    }
    probeNext = true;
  }

  @Override
  public void step() {
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    iterator.step();
    tvListOffsets[iteratorIndex] = iterator.getIndex();
    probeNext = false;
  }

  @Override
  public void setTVListOffsets(int[] tvListOffsets) {
    for (int i = 0; i < tvListIterators.size(); i++) {
      tvListIterators.get(i).setIndex(tvListOffsets[i]);
      this.tvListOffsets[i] = tvListOffsets[i];
    }
    probeNext = false;
  }

  @Override
  public MultiTVListIterator clone() {
    OrderedMultiTVListIterator cloneIterator = new OrderedMultiTVListIterator();
    cloneIterator.tvListIterators = new ArrayList<>(tvListIterators.size());
    for (int i = 0; i < tvListIterators.size(); i++) {
      cloneIterator.tvListIterators.add(tvListIterators.get(i).clone());
    }
    cloneIterator.tvListOffsets = new int[tvListIterators.size()];
    cloneIterator.iteratorIndex = 0;
    return cloneIterator;
  }
}
