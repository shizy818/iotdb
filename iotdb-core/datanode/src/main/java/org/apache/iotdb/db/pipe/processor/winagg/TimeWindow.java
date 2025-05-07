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

package org.apache.iotdb.db.pipe.processor.winagg;

public class TimeWindow extends Window {
  private final String partitionKey;
  private final long start;
  private final long end;

  public TimeWindow(long start, long end, String partitionKey) {
    this.start = start;
    this.end = end;
    this.partitionKey = partitionKey;
  }

  @Override
  public long startTime() {
    return start;
  }

  @Override
  public long endTime() {
    return end;
  }

  @Override
  public long maxTimestamp() {
    return end - 1;
  }

  @Override
  public int hashCode() {
    return (int) (start + modInverse((int) (end << 1) + 1));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimeWindow window = (TimeWindow) o;

    return end == window.end && start == window.start && partitionKey.equals(window.partitionKey);
  }

  @Override
  public int compareTo(Window o) {
    TimeWindow that = (TimeWindow) o;
    if (this.start == that.start) {
      if (this.end == that.end) {
        return this.partitionKey.compareTo(that.partitionKey);
      } else {
        return Long.compare(this.end, that.end);
      }
    } else {
      return Long.compare(this.start, that.start);
    }
  }

  private int modInverse(int x) {
    // Cube gives inverse mod 2^4, as x^4 == 1 (mod 2^4) for all odd x.
    int inverse = x * x * x;
    // Newton iteration doubles correct bits at each step.
    inverse *= 2 - x * inverse;
    inverse *= 2 - x * inverse;
    inverse *= 2 - x * inverse;
    return inverse;
  }
}
