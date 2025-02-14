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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.IOException;

public class DeltaIndexEntry {
  // records to read in stable table before this entry
  private int stableId;
  // index of delta table of this entry
  private int deltaId;

  // records to read in delta index before this entry
  // private int count;

  public DeltaIndexEntry(int stableId, int deltaId) {
    this.stableId = stableId;
    this.deltaId = deltaId;
  }

  public static int serializedSize() {
    return Integer.BYTES * 2;
  }

  public int getStableId() {
    return stableId;
  }

  public int getDeltaId() {
    return deltaId;
  }

  public void reset() {
    this.stableId = -1;
    this.deltaId = -1;
  }

  public void set(int stableId, int deltaId) {
    this.stableId = stableId;
    this.deltaId = deltaId;
  }

  public void serializeToWAL(IWALByteBufferView buffer) {
    buffer.putInt(stableId);
    buffer.putInt(deltaId);
  }

  public static DeltaIndexEntry deserialize(DataInputStream stream) throws IOException {
    int sId = ReadWriteIOUtils.readInt(stream);
    int dId = ReadWriteIOUtils.readInt(stream);
    return new DeltaIndexEntry(sId, dId);
  }
}
