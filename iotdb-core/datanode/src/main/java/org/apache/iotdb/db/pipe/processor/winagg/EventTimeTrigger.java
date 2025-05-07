package org.apache.iotdb.db.pipe.processor.winagg;

public class EventTimeTrigger extends Trigger<Window> {
  @Override
  public Action onElement(long timestamp, Window window, long watermark) {
    if (window.maxTimestamp() <= watermark) {
      return Action.PURGE;
    } else {
      return Action.CONTINUE;
    }
  }

  @Override
  public void clear(Window window) {}
}
