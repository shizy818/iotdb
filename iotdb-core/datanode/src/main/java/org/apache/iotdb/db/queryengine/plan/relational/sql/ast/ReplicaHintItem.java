package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

public class ReplicaHintItem extends Node {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ReplicaHintItem.class);
  private static final String HINT_NAME_ITEM = "replica";

  private @Nullable final QualifiedName table;
  private final int replicaIndex;

  public ReplicaHintItem(@Nullable QualifiedName table, int replicaIndex) {
    super(null);
    this.table = table;
    this.replicaIndex = replicaIndex;
  }

  public @Nullable QualifiedName getTable() {
    return table;
  }

  public int getReplicaIndex() {
    return replicaIndex;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitReplicaHintItem(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, replicaIndex);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ReplicaHintItem other = (ReplicaHintItem) obj;
    return this.table.equals(other.table) && this.replicaIndex == other.replicaIndex;
  }

  @Override
  public String toString() {
    return HINT_NAME_ITEM + (table == null ? "" : "-" + table) + "(" + replicaIndex + ")";
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + AstMemoryEstimationHelper.getEstimatedSizeOfAccountableObject(table);
  }
}
