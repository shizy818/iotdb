package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.statement.component.IntoComponent.DUPLICATE_TARGET_COLUMN_ERROR_MSG;

public class IntoScheme {
  // List<(sourceColumn, targetColumn)>
  private final List<Pair<String, String>> sourceTargetPairList;

  // sourceColumn -> dataType
  private Map<String, TSDataType> sourceToDataTypeMap;

  public IntoScheme() {
    this.sourceTargetPairList = new ArrayList<>();
    this.sourceToDataTypeMap = new HashMap<>();
  }

  public IntoScheme(
          List<Pair<String, String>> sourceTargetPairList) {
    this.sourceTargetPairList = sourceTargetPairList;
  }

  public void addSourceTargetPair(
          String sourceColumn, String targetColumn) {
    sourceTargetPairList.add(new Pair<>(sourceColumn, targetColumn));
  }

  public void addSourceColumnDataType(String sourceColumn, TSDataType dataType) {
    sourceToDataTypeMap.put(sourceColumn, dataType);
  }

  public void validate() {
    List<String> targetColumns =
            sourceTargetPairList.stream().map(Pair::getRight).collect(Collectors.toList());
    if (targetColumns.size() > new HashSet<>(targetColumns).size()) {
      throw new SemanticException(DUPLICATE_TARGET_COLUMN_ERROR_MSG);
    }
  }

  public List<Pair<String, String>> getSourceTargetPairList() {
    return sourceTargetPairList;
  }

  public void serialize(ByteBuffer byteBuffer) {}

  public void serialize(DataOutputStream stream) throws IOException {}

  public static IntoScheme deserialize(ByteBuffer byteBuffer) {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IntoScheme that = (IntoScheme) o;
    return sourceTargetPairList.equals(that.sourceTargetPairList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceTargetPairList);
  }
}
