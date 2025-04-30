package org.apache.iotdb.db.pipe.processor.winagg;

import java.util.List;

public abstract class WindowAssigner<T, W extends Window> {
  private static final long serialVersionUID = 1L;

  public abstract List<W> assignWindows(T element, long timestamp);
}
