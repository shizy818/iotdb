package org.apache.iotdb.db.queryengine.plan.statement.metadata.flow;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;

public class CreateFlowStatement extends Statement implements IConfigStatement {
  private String flowName;
  private boolean ifNotExistsCondition;

  public CreateFlowStatement(StatementType createFlowStatement) {
    this.statementType = createFlowStatement;
  }

  public String getFlowName() {
    return flowName;
  }

  public boolean hasIfNotExistsCondition() {
    return ifNotExistsCondition;
  }

  public void setFlowName(String flowName) {
    this.flowName = flowName;
  }

  public void setIfNotExists(boolean ifNotExistsCondition) {
    this.ifNotExistsCondition = ifNotExistsCondition;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.USE_PIPE),
        PrivilegeType.USE_PIPE);
  }
}
