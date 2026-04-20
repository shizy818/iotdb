package org.apache.iotdb.db.queryengine.plan.relational.utils.hint;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.QualifiedName;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

public class RegionRouteHint extends Hint {
  public static String HINT_NAME = "region_route";
  public static int ANY_TABLE = -1;

  private final QualifiedName table;
  private final Map<Integer, Integer> regionDatanodeMap;

  public RegionRouteHint(QualifiedName table, Map<Integer, Integer> regionDatanodeMa) {
    super(HINT_NAME);
    this.table = table;
    this.regionDatanodeMap = regionDatanodeMa;
  }

  @Override
  public String getKey() {
    return HINT_NAME + (table == null ? "" : "-" + table);
  }

  @Override
  public String toString() {
    return HINT_NAME + (table == null ? "" : "-" + table) + "(" + regionDatanodeMap + ")";
  }

  /**
   * Selects data node locations based on the region_route strategy.
   *
   * @param dataNodeLocations the available data node locations
   * @param regionId the region ID to route
   * @return the selected locations based on region_route hint strategy, or null if no match
   */
  public List<TDataNodeLocation> selectLocations(
      List<TDataNodeLocation> dataNodeLocations, int regionId) {
    if (dataNodeLocations == null || dataNodeLocations.isEmpty()) {
      return null;
    }

    Integer datanodeId = regionDatanodeMap.getOrDefault(regionId, regionDatanodeMap.get(ANY_TABLE));
    if (datanodeId == null) {
      return null;
    }

    return dataNodeLocations.stream()
        .filter(location -> location.getDataNodeId() == datanodeId)
        .findFirst()
        .map(ImmutableList::of)
        .orElse(null);
  }
}
