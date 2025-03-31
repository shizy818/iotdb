package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class CreateFlow extends Statement {
  private final String flowName;
  private final boolean ifNotExistsCondition;

  public CreateFlow(final String flowName, final boolean ifNotExistsCondition) {
    super(null);
    this.flowName = flowName;
    this.ifNotExistsCondition = ifNotExistsCondition;
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  public String getFlowName() {
    return flowName;
  }

  public boolean hasIfNotExistsCondition() {
    return ifNotExistsCondition;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateFlow(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(flowName, ifNotExistsCondition);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CreateFlow other = (CreateFlow) obj;
    return Objects.equals(flowName, other.flowName)
        && Objects.equals(ifNotExistsCondition, other.ifNotExistsCondition);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("flowName", flowName)
        .add("ifNotExistsCondition", ifNotExistsCondition)
        .toString();
  }
}
