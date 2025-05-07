package org.apache.iotdb.db.pipe.processor.winagg;

import java.util.List;

public abstract class WindowAssigner<T, W extends Window> {

  public abstract List<W> assignWindows(T element, long timestamp);
}
