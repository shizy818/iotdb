package org.apache.iotdb.db.pipe.processor.winagg.function;

import org.apache.tsfile.enums.TSDataType;

public class AggregateAvgFactory {
  public static AggregateFunction create(TSDataType dataType) {
    switch (dataType) {
      case INT32:
        return new AggregateIntegerAvg();
      case INT64:
        return new AggregateLongAvg();
      default:
        return null;
    }
  }
}
