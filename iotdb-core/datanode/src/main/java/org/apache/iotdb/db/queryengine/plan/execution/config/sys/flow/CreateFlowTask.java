package org.apache.iotdb.db.queryengine.plan.execution.config.sys.flow;

import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateFlow;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.flow.CreateFlowStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class CreateFlowTask implements IConfigTask {
  private final CreateFlowStatement createFlowStatement;

  public CreateFlowTask(CreateFlow createFlow) {
    createFlowStatement = new CreateFlowStatement(StatementType.CREATE_FLOW);
    createFlowStatement.setFlowName(createFlow.getFlowName());
    createFlowStatement.setIfNotExists(createFlow.hasIfNotExistsCondition());
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.createFlow(createFlowStatement);
  }
}
