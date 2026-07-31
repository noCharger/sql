/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.ScriptedMetric;
import org.opensearch.sql.common.cluster.ClusterConfigFingerprint;
import org.opensearch.sql.common.cluster.HierarchicalClusterMerge;
import org.opensearch.sql.common.cluster.LocalClusterSummary;
import org.opensearch.sql.common.cluster.MatchMode;

/**
 * Coordinator Reduce stage for the distributed {@code cluster} command
 * ({@code showcount=false, labelonly=false}).
 *
 * <p>Each shard runs greedy leader clustering in a {@code scripted_metric} Map (init/map/combine)
 * and emits one shard state. With a pass-through {@code reduce_script} ({@code return states}) the
 * coordinator receives {@link ScriptedMetric#aggregation()} as a {@code List} of per-shard states,
 * one per shard, in non-deterministic response order.
 *
 * <p>This parser reconstitutes the shard states into {@link LocalClusterSummary} objects, assigns a
 * deterministic {@code shardOrdinal} by ranking the stable per-shard {@code shardKey}, and delegates
 * the canonical-order greedy merge to {@link HierarchicalClusterMerge}. It then emits one
 * representative row per global cluster (the representative row's original fields plus the cluster
 * label, and the count when requested).
 *
 * <p>Shard-state contract (produced by the Map combine script):
 *
 * <pre>
 * { "shardKey": &lt;String stable per-shard order key&gt;,
 *   "clusters": [ { "ord": &lt;int localCreationOrdinal&gt;,
 *                   "rep": &lt;String representative field text&gt;,
 *                   "row": &lt;Map representative source row&gt;,
 *                   "count": &lt;long local size&gt; }, ... ] }
 * </pre>
 */
public class ClusterScriptedMetricParser implements MetricParser {

  private final String name;
  private final String labelField;
  private final String countField;
  private final boolean showCount;
  private final double threshold;
  private final MatchMode matchMode;
  private final String delims;

  public ClusterScriptedMetricParser(
      String name,
      String labelField,
      String countField,
      boolean showCount,
      double threshold,
      MatchMode matchMode,
      String delims) {
    this.name = name;
    this.labelField = labelField;
    this.countField = countField;
    this.showCount = showCount;
    this.threshold = threshold;
    this.matchMode = matchMode;
    this.delims = delims;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Map<String, Object>> parse(Aggregation aggregation) {
    ScriptedMetric scriptedMetric = (ScriptedMetric) aggregation;
    Object states = scriptedMetric.aggregation();
    if (!(states instanceof List<?> shardStates) || shardStates.isEmpty()) {
      return List.of();
    }

    // Rank shards by their stable shardKey so the merge order is reproducible regardless of the
    // response arrival order of the per-shard states.
    List<Map<String, Object>> orderedShards = new ArrayList<>();
    for (Object state : shardStates) {
      if (state instanceof Map<?, ?> shardState) {
        orderedShards.add((Map<String, Object>) shardState);
      }
    }
    orderedShards.sort(Comparator.comparing(s -> String.valueOf(s.getOrDefault("shardKey", ""))));

    String fingerprint = ClusterConfigFingerprint.of(matchMode, threshold, delims);
    List<LocalClusterSummary> summaries = new ArrayList<>();
    for (int shardOrdinal = 0; shardOrdinal < orderedShards.size(); shardOrdinal++) {
      Map<String, Object> shardState = orderedShards.get(shardOrdinal);
      Object clusters = shardState.get("clusters");
      if (!(clusters instanceof List<?> clusterList)) {
        continue;
      }
      for (Object c : clusterList) {
        if (!(c instanceof Map<?, ?> cluster)) {
          continue;
        }
        int ord = toInt(cluster.get("ord"));
        String rep = cluster.get("rep") == null ? "" : String.valueOf(cluster.get("rep"));
        Map<String, Object> row =
            cluster.get("row") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        long count = toLong(cluster.get("count"));
        summaries.add(
            new LocalClusterSummary(shardOrdinal, ord, ord, rep, row, count, fingerprint));
      }
    }

    List<HierarchicalClusterMerge.GlobalCluster> globals =
        HierarchicalClusterMerge.merge(summaries, threshold, matchMode, delims);

    List<Map<String, Object>> rows = new ArrayList<>(globals.size());
    for (HierarchicalClusterMerge.GlobalCluster g : globals) {
      Map<String, Object> row = new LinkedHashMap<>();
      if (g.representativeRow() != null) {
        row.putAll(g.representativeRow());
      }
      row.put(labelField, g.label());
      if (showCount) {
        row.put(countField, g.count());
      }
      rows.add(row);
    }
    return rows;
  }

  private static int toInt(Object o) {
    return o instanceof Number n ? n.intValue() : 0;
  }

  private static long toLong(Object o) {
    return o instanceof Number n ? n.longValue() : 0L;
  }
}
