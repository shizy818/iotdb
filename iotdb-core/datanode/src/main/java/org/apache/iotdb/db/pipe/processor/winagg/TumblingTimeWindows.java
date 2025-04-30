package org.apache.iotdb.db.pipe.processor.winagg;

import java.util.Collections;
import java.util.List;

public class TumblingTimeWindows extends WindowAssigner<Object, TimeWindow> {
  private final long offset;
  private final long size;

  TumblingTimeWindows(long size) {
    this.size = size;
    this.offset = 0;
  }

  TumblingTimeWindows(long size, long offset) {
    this.size = size;
    this.offset = offset;
  }

  @Override
  public List<TimeWindow> assignWindows(Object element, long timestamp) {
    long window_start = (timestamp - offset) / size * size;
    String partitionKey = "";
    return Collections.singletonList(
        new TimeWindow(window_start, window_start + size, partitionKey));
  }
}
