/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.processor.reactive;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.protocol.writeback.WriteBackConnector;
import org.apache.iotdb.db.pipe.event.common.row.PipeResetTabletRow;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.component.ResultColumn;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeRuntimeEnvironment;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_EXPR;
import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_SOURCE_PATH;

@TreeModel
@TableModel
public class ExpressionProcessor implements PipeProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionProcessor.class);

  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();
  private IClientSession treeSession;

  private static final Context graalVMConext = Context.create("js");

  private String expr;
  private String databaseName;
  private String inputTimeSeries;
  private String outputTimeSeries;

  private MeasurementSchema[] measurementSchemaList;
  private String[] measurementStringList;
  private TSDataType[] valueColumnTypes;

  private final Map<String, Integer> measurementIndexMap = new HashMap<>();
  private final Set<String> variables = new HashSet<>();
  private final List<Expression> expressions = new ArrayList<>();

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(PROCESSOR_EXPR);
    validator.validateRequiredAttribute(PROCESSOR_SOURCE_PATH);

    final PipeParameters parameters = validator.getParameters();
    expr = parameters.getString(PROCESSOR_EXPR).toLowerCase().trim();
    String pathKey = parameters.getString(PROCESSOR_SOURCE_PATH).toLowerCase().trim();
    String[] pathNodes = pathKey.split("[.]");
    inputTimeSeries = pathNodes[pathNodes.length - 2];
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    final PipeRuntimeEnvironment environment = configuration.getRuntimeEnvironment();
    // source database
    databaseName =
        StorageEngine.getInstance()
            .getDataRegion(new DataRegionId(environment.getRegionId()))
            .getDatabaseName();
    outputTimeSeries = databaseName + TsFileConstant.PATH_SEPARATOR + "t2";

    // client session
    treeSession =
            new InternalClientSession(
                    String.format(
                            "%s_%s_%s_%s_tree",
                            WriteBackConnector.class.getSimpleName(),
                            environment.getPipeName(),
                            environment.getCreationTime(),
                            environment.getRegionId()));
    treeSession.setUsername(AuthorityChecker.SUPER_USER);
    treeSession.setClientVersion(IoTDBConstant.ClientVersion.V_1_0);
    treeSession.setZoneId(ZoneId.systemDefault());

    // prepare source series meta info
    fetchAlignedSeriesMeta();

    // analyze statement
    analyzeStmt();
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      eventCollector.collect(tabletInsertionEvent);
      return;
    }

    final Iterable<TabletInsertionEvent> outputEvents =
        tabletInsertionEvent.processRowByRow(this::processRow);

    outputEvents.forEach(
        event -> {
          try {
            eventCollector.collect(event);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws IOException {
    eventCollector.collect(tsFileInsertionEvent);
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    eventCollector.collect(event);
  }

  @Override
  public void close() throws Exception {
    if (treeSession != null) {
      SESSION_MANAGER.closeSession(treeSession, COORDINATOR::cleanupQueryExecution);
    }
  }

  private void processRow(final Row row, final RowCollector rowCollector) {
    // time
    final long[] timestampColumn = new long[] {row.getTime()};

    // bind variable in the expression
    Value bindings = graalVMConext.getBindings("js");
    for(String var: variables) {
      int columnIndex = measurementIndexMap.get(var);
      switch (valueColumnTypes[columnIndex]) {
        case INT32:
          bindings.putMember(var, row.getInt(columnIndex));
          break;
        case FLOAT:
          bindings.putMember(var, row.getFloat(columnIndex));
          break;
        default:
          break;
      }
    }

    // value
    Object[] valueColumns = new Object[expressions.size()];
    BitMap[] bitMaps = new BitMap[expressions.size()];

    for (int resultIndex = 0; resultIndex < expressions.size(); ++resultIndex) {
      String exprStr = expressions.get(resultIndex).getExpressionString();
      LOGGER.info("exprStr {}", exprStr);
      Value result = graalVMConext.eval("js", exprStr);

      // should judge based on output column type
      switch (valueColumnTypes[resultIndex]) {
        case INT32:
          valueColumns[resultIndex] = new int[1];
          ((int[]) valueColumns[resultIndex])[0] = result.asInt();
          break;
        case FLOAT:
          valueColumns[resultIndex] = new float[1];
          ((float[]) valueColumns[resultIndex])[0] = (float) result.asDouble();
          break;
        default:
          break;
      }
      bitMaps[resultIndex] = new BitMap(1);
    }

    try {
      rowCollector.collectRow(
          new PipeResetTabletRow(
              0,
              outputTimeSeries,
              true,
              measurementSchemaList,
              timestampColumn,
              valueColumnTypes,
              valueColumns,
              bitMaps,
              measurementStringList));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void fetchAlignedSeriesMeta() {
    String devicePath = databaseName + TsFileConstant.PATH_SEPARATOR + inputTimeSeries + TsFileConstant.PATH_SEPARATOR + "*";
    Long queryId = null;
    try {
      Statement showTimeSeriesStatement =
              new ShowTimeSeriesStatement(new PartialPath(devicePath), false);

      treeSession.setDatabaseName(databaseName);
      treeSession.setSqlDialect(IClientSession.SqlDialect.TREE);
      SESSION_MANAGER.registerSession(treeSession);

      queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result = Coordinator.getInstance()
              .executeForTreeModel(
                      showTimeSeriesStatement,
                      queryId,
                      SESSION_MANAGER.getSessionInfo(treeSession),
                      "",
                      ClusterPartitionFetcher.getInstance(),
                      ClusterSchemaFetcher.getInstance(),
                      IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold(),
                      false);
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
              && result.status.code != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()
              && result.status.code != TSStatusCode.DATABASE_CONFLICT.getStatusCode()) {
        LOGGER.error(
                "Show TimeSeries error, statement: {}, result status : {}.",
                showTimeSeriesStatement, result.status);
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (optionalTsBlock.isPresent()) {
        TsBlock tsBlock = optionalTsBlock.get();
        int rowCount = tsBlock.getPositionCount();
        measurementStringList = new String[rowCount];
        measurementSchemaList = new MeasurementSchema[rowCount];
        valueColumnTypes = new TSDataType[rowCount];

        for(int i = 0; i < rowCount; i++) {
          // Timeseries
          String[] names = tsBlock.getColumn(0).getBinary(i).toString().split("[.]");
          measurementStringList[i] = names[names.length - 1];
          measurementIndexMap.put(names[names.length - 1], i);

          // Column Type
          String columnType = tsBlock.getColumn(3).getBinary(i).toString();
          valueColumnTypes[i] = getTsDataType(columnType);

          // Measurement
          measurementSchemaList[i] = new MeasurementSchema(
                  measurementStringList[i], valueColumnTypes[i]
          );
        }
      }
    } catch (IoTDBException e) {
      throw new RuntimeException(e);
    } finally {
      if (queryId != null) {
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  private void analyzeStmt() {
    String selectSql = String.format("select %s from %s.%s", expr, databaseName, inputTimeSeries);
    Statement queryStmt = StatementGenerator.createStatement(selectSql, ZoneId.systemDefault());
    List<ResultColumn> resultColumns = ((QueryStatement)queryStmt).getSelectComponent().getResultColumns();
    for(ResultColumn rc: resultColumns) {
      Expression resultExpr = rc.getExpression();
      expressions.add(resultExpr);
      analyzeExpr(resultExpr);
    }
  }

  private void analyzeExpr(Expression expression) {
    if (expression == null) {
      return;
    }

    if (expression instanceof TimeSeriesOperand) {
      String measurement = ((TimeSeriesOperand)expression).getPath().getMeasurement();
      if (!measurementIndexMap.containsKey(measurement)) {
        throw new RuntimeException("Unknown measurement in the expression");
      }
      variables.add(measurement);
    }

    for(Expression e: expression.getExpressions()) {
      analyzeExpr(e);
    }
  }

  private TSDataType getTsDataType(String tsType) {
    switch (tsType) {
      case "INT32":
        return TSDataType.INT32;
      case "FLOAT":
        return TSDataType.FLOAT;
      default:
        return null;
    }
  }
}
