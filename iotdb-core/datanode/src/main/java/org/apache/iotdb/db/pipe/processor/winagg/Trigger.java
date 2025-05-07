package org.apache.iotdb.db.pipe.processor.winagg;

public abstract class Trigger<Window> {

  public abstract Action onElement(long timestamp, Window window, long watermark);

  // public abstract Status onEventTime(long time, W window, long watermark);

  public abstract void clear(Window window);

  public enum Action {
    CONTINUE,
    FIRE,
    PURGE
  }
}
