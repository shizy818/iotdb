package org.apache.iotdb.db.pipe.processor.winagg;

import java.util.List;

public interface WindowFunction<IN, OUT, KEY, W extends Window> {
  void process(KEY key, W window, WindowContext context, IN input, List<OUT> out) throws Exception;

  void clear(W window, WindowContext context) throws Exception;

  interface WindowContext {
    long currentProcessingTime();

    long currentWatermark();

    //    KeyedStateStore windowState();
    //
    //    KeyedStateStore globalState();
  }
}
