package org.apache.iotdb.db.pipe.processor.winagg;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.processor.winagg.function.AggregateAvgFactory;
import org.apache.iotdb.db.pipe.processor.winagg.function.AggregateFunction;
import org.apache.iotdb.db.pipe.processor.winagg.function.AggregateSumFactory;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeRuntimeEnvironment;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OPERATORS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OPERATORS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MEASUREMENTS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_OUTPUT_MEASUREMENTS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_TUMBLING_SIZE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_TUMBLING_SIZE_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_WATERMAKR_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_WATERMARK;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_WINDOWING_STRATEGY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_WINDOWING_STRATEGY_KEY;

@TreeModel
public class WindowAggregateProcessor implements PipeProcessor {
  private String pipeName;
  private String databaseWithPathSeparator;
  private PipeTaskMeta pipeTaskMeta;
  private long outputMaxDelayMilliseconds;

  private final Map<String, Set<AggregateFunction>> aggregateFunctionMap = new HashMap<>();
  private final Map<String, Map<String, Set<AggregateState>>> windowStateMap = new HashMap<>();

  private String dataBaseName;
  private Boolean isTableModel;

  WindowAssigner<Object, ? extends Window> windowAssigner;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();
    validator
        .validate(
            arg -> !((String) arg).isEmpty(),
            String.format("The parameter %s must not be empty.", PROCESSOR_OPERATORS_KEY),
            parameters.getStringOrDefault(
                PROCESSOR_OPERATORS_KEY, PROCESSOR_OPERATORS_DEFAULT_VALUE))
        .validate(
            arg -> !((String) arg).isEmpty(),
            String.format("The parameter %s must not be empty.", PROCESSOR_WINDOWING_STRATEGY_KEY),
            parameters.getStringOrDefault(
                PROCESSOR_WINDOWING_STRATEGY_KEY, PROCESSOR_WINDOWING_STRATEGY_DEFAULT_VALUE))
        .validate(
            arg ->
                Arrays.stream(((String) arg).replace(" ", "").split(","))
                    .allMatch(this::isLegalMeasurement),
            String.format(
                "The output measurements %s contains illegal measurements, the measurements must be the last level of a legal path",
                parameters.getStringOrDefault(
                    PROCESSOR_OUTPUT_MEASUREMENTS_KEY,
                    PROCESSOR_OUTPUT_MEASUREMENTS_DEFAULT_VALUE)),
            parameters.getStringOrDefault(
                PROCESSOR_OUTPUT_MEASUREMENTS_KEY, PROCESSOR_OUTPUT_MEASUREMENTS_DEFAULT_VALUE));
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    final PipeRuntimeEnvironment environment = configuration.getRuntimeEnvironment();
    pipeName = environment.getPipeName();
    dataBaseName =
        StorageEngine.getInstance()
            .getDataRegion(new DataRegionId(environment.getRegionId()))
            .getDatabaseName();
    if (dataBaseName != null) {
      isTableModel = PathUtils.isTableModelDatabase(dataBaseName);
    }

    databaseWithPathSeparator =
        StorageEngine.getInstance()
                .getDataRegion(
                    new DataRegionId(configuration.getRuntimeEnvironment().getRegionId()))
                .getDatabaseName()
            + TsFileConstant.PATH_SEPARATOR;

    // Load parameters
    final long outputMaxDelaySeconds =
        parameters.getLongOrDefault(
            PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_KEY,
            PROCESSOR_OUTPUT_MAX_DELAY_SECONDS_DEFAULT_VALUE);
    // The output max delay milliseconds must be set to at least 1
    // to guarantee the correctness of the CAS in last receive time
    outputMaxDelayMilliseconds =
        outputMaxDelaySeconds < 0 ? Long.MAX_VALUE : Math.max(outputMaxDelaySeconds * 1000, 1);

    // Set output name
    final List<String> operatorList =
        Arrays.stream(
                parameters
                    .getStringOrDefault(PROCESSOR_OPERATORS_KEY, PROCESSOR_OPERATORS_DEFAULT_VALUE)
                    .replace(" ", "")
                    .split(","))
            .collect(Collectors.toList());
    System.out.println(operatorList);
    translateToAggregateFunctionMap(operatorList);

    final String outputMeasurementString =
        parameters.getStringOrDefault(
            PROCESSOR_OUTPUT_MEASUREMENTS_KEY, PROCESSOR_OUTPUT_MEASUREMENTS_DEFAULT_VALUE);
    final List<String> outputMeasurementNameList =
        outputMeasurementString.isEmpty()
            ? Collections.emptyList()
            : Arrays.stream(outputMeasurementString.replace(" ", "").split(","))
                .collect(Collectors.toList());
    System.out.println(outputMeasurementNameList);

    // watermark
    final long watermark =
        parameters.getLongOrDefault(PROCESSOR_WATERMARK, PROCESSOR_WATERMAKR_SECONDS_DEFAULT_VALUE);

    // get window type
    final String windowType =
        parameters.getStringOrDefault(
            PROCESSOR_WINDOWING_STRATEGY_KEY, PROCESSOR_WINDOWING_STRATEGY_DEFAULT_VALUE);
    if (windowType.equals("tumbling")) {
      long tumblingSize =
          parameters.getLongOrDefault(
              PROCESSOR_TUMBLING_SIZE, PROCESSOR_TUMBLING_SIZE_SECONDS_DEFAULT_VALUE);
      windowAssigner = new TumblingTimeWindows(tumblingSize);
    }

    // Restore window state (TODO)
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      eventCollector.collect(tabletInsertionEvent);
      return;
    }

    final AtomicReference<String> deviceSuffix = new AtomicReference<>();
    final AtomicReference<Exception> exception = new AtomicReference<>();

    final Iterable<TabletInsertionEvent> outputEvents =
        tabletInsertionEvent.processRowByRow(
            (row, rowCollector) -> {
              // To reduce the memory usage, we use the device suffix
              // instead of the full path as the key.
              if (deviceSuffix.get() == null) {
                deviceSuffix.set(
                    row.getDeviceId().replaceFirst(this.databaseWithPathSeparator, ""));
              }
              processRow(row, rowCollector, deviceSuffix.get(), exception);
            });

    outputEvents.forEach(
        event -> {
          try {
            eventCollector.collect(event);
          } catch (Exception e) {
            exception.set(e);
          }
        });

    if (Objects.nonNull(exception.get())) {
      throw exception.get();
    }
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {}

  @Override
  public void close() throws Exception {}

  private boolean isLegalMeasurement(final String measurement) {
    try {
      PathUtils.isLegalPath("root." + measurement);
    } catch (final IllegalPathException e) {
      return false;
    }
    return measurement.startsWith("`") && measurement.endsWith("`") || !measurement.contains(".");
  }

  private void translateToAggregateFunctionMap(List<String> operatorList) {
    for (String operator : operatorList) {
      String[] s = operator.replace(")", "").split("\\(");
      String aggFunc = s[0].toLowerCase().trim();
      // Now consider one argument only
      String arg = s[1].toLowerCase().trim();

      if (!aggregateFunctionMap.containsKey(arg)) {
        aggregateFunctionMap.put(arg, new HashSet<>());
      }

      // create aggregate function
      TSDataType dataType = getArgumentDataType(arg);
      switch (aggFunc) {
        case "sum":
          aggregateFunctionMap.get(arg).add(AggregateSumFactory.create(dataType));
          break;
        case "avg":
          aggregateFunctionMap.get(arg).add(AggregateAvgFactory.create(dataType));
          break;
        default:
          break;
      }
    }
  }

  private TSDataType getArgumentDataType(String argument) {
    if (argument.equals("s1")) {
      return TSDataType.INT32;
    } else if (argument.equals("s2")) {
      return TSDataType.INT64;
    }
    return TSDataType.UNKNOWN;
  }

  private void processRow(
      Row row,
      RowCollector rowCollector,
      String deviceSuffix,
      AtomicReference<Exception> exception) {
    System.out.println(row.getTime());
    final long timestamp = row.getTime();

    // assign windows
    List<? extends Window> windows = windowAssigner.assignWindows(row, timestamp);
    for (Window w : windows) {
      if (timestamp >= w.startTime() && timestamp < w.endTime()) {
        String windowId = w.uniqueId();
        if (!windowStateMap.containsKey(windowId)) {
          windowStateMap.put(windowId, new HashMap<>());
          aggregateFunctionMap.forEach(
              (arg, funcSet) -> {
                windowStateMap.get(windowId).put(arg, new HashSet<>());
                funcSet.forEach(
                    func -> windowStateMap.get(windowId).get(arg).add(func.createState()));
              });
        }

        for (int index = 0, size = row.size(); index < size; ++index) {
          // Do not calculate null values
          if (row.isNull(index)) {
            continue;
          }

          String columnName = row.getColumnName(index);
          Object columnValue = row.getObject(index);
          Set<AggregateState> stateSet = windowStateMap.get(windowId).get(columnName);
          stateSet.forEach(aggState -> aggState.add(columnValue));
        }
      }
    }
  }
}
