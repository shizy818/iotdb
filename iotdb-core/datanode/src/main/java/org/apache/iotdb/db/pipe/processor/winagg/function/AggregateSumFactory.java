package org.apache.iotdb.db.pipe.processor.winagg.function;

import org.apache.tsfile.enums.TSDataType;

public class AggregateSumFactory {
  public static AggregateFunction create(TSDataType dataType) {
    switch (dataType) {
      case INT32:
        return new AggregateIntegerSum();
      case INT64:
        return new AggregateLongSum();
      default:
        return null;
    }
  }
}
