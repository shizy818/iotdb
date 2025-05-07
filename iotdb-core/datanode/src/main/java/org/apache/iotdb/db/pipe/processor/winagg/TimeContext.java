package org.apache.iotdb.db.pipe.processor.winagg;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TimeContext {
  private final long interval;
  private long watermark;
  private final Map<Window, Long> lastProcessTimeMap;

  public TimeContext(long interval) {
    this.interval = interval;
    this.watermark = 0;
    lastProcessTimeMap = new ConcurrentHashMap<>();
  }

  public void advanceWatermark(long eventTime) {
    this.watermark = eventTime - interval;
  }

  public void advanceLastProcessTime(Window w, long systemTime) {
    lastProcessTimeMap.put(w, systemTime);
  }

  public long watermark() {
    return watermark;
  }

  public long lastProcessTime(Window w) {
    return lastProcessTimeMap.getOrDefault(w, Long.MIN_VALUE);
  }
}
