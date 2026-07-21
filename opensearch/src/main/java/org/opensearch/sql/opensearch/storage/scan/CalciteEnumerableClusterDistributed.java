/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.List;
import java.util.Optional;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.calcite.plan.rel.ClusterDistributed;
import org.opensearch.sql.common.cluster.ClusterLabeler;
import org.opensearch.sql.common.cluster.ClusterPartialState;
import org.opensearch.sql.common.cluster.MatchMode;
import org.opensearch.sql.common.cluster.TextSimilarityClustering;
import org.opensearch.sql.opensearch.cluster.ClusterAggregationBuilder;
import org.opensearch.sql.opensearch.cluster.InternalClusterResult;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.util.OpenSearchRelOptUtil;
import org.opensearch.transport.client.node.NodeClient;

/**
 * OpenSearch physical operator for the distributed {@code cluster} path. At execution its {@code
 * scan()} runs two phases: phase one builds the global cluster model (wired to the {@code
 * ppl_cluster} aggregation in a later sub-step), phase two labels each input row against that model
 * using {@link ClusterLabeler}. Modeled on {@code CalciteEnumerableGraphLookup}, which likewise
 * issues its work during execution.
 */
public class CalciteEnumerableClusterDistributed extends ClusterDistributed
    implements EnumerableRel {

  public CalciteEnumerableClusterDistributed(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      String sourceField,
      double threshold,
      String matchMode,
      String delims,
      int maxClusters,
      String labelField) {
    super(
        cluster, traits, input, sourceField, threshold, matchMode, delims, maxClusters, labelField);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new CalciteEnumerableClusterDistributed(
        getCluster(),
        traitSet,
        inputs.get(0),
        sourceField,
        threshold,
        matchMode,
        delims,
        maxClusters,
        labelField);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    BlockBuilder builder = new BlockBuilder();
    Result inputResult = implementor.visitChild(this, 0, (EnumerableRel) getInput(), Prefer.ARRAY);
    PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            OpenSearchRelOptUtil.replaceDot(getCluster().getTypeFactory(), getRowType()),
            pref.preferArray());
    Expression inputEnumerable = builder.append("clusterDistributedInput", inputResult.block);
    Expression op = implementor.stash(this, CalciteEnumerableClusterDistributed.class);
    builder.add(
        Expressions.return_(
            null,
            Expressions.call(
                op, "labelAll", Expressions.convert_(inputEnumerable, Enumerable.class))));
    return implementor.result(physType, builder.toBlock());
  }

  /**
   * Phase two invoked from generated code: label each input row against the global model. Consuming
   * the materialized child enumerable (rather than requiring a {@code Scannable} input) lets this
   * work whether or not pushdown is enabled.
   */
  public Enumerable<@Nullable Object> labelAll(Enumerable<Object> input) {
    return new ClusterDistributedEnumerable(this, input);
  }

  /**
   * Phase one: issue the {@code ppl_cluster} aggregation on the source index and return the global
   * cluster representative texts (in label order). Runs on the coordinating node during execution,
   * mirroring how {@code CalciteEnumerableGraphLookup} issues its own queries.
   */
  List<String> buildModel() {
    OpenSearchIndex index = extractIndex(getInput());
    if (index == null) {
      return List.of();
    }
    Optional<NodeClient> maybeClient = index.getClient().getNodeClient();
    if (maybeClient.isEmpty()) {
      return List.of();
    }
    SearchSourceBuilder source =
        new SearchSourceBuilder()
            .size(0)
            .aggregation(
                new ClusterAggregationBuilder(MODEL_AGG_NAME)
                    .field(sourceField)
                    .threshold(threshold)
                    .matchMode(matchMode)
                    .delims(delims)
                    .maxClusters(maxClusters));
    SearchRequest request = new SearchRequest(index.getIndexName().getIndexNames()).source(source);
    SearchResponse response = maybeClient.get().search(request).actionGet();
    Aggregation agg =
        response.getAggregations() == null ? null : response.getAggregations().get(MODEL_AGG_NAME);
    if (!(agg instanceof InternalClusterResult result)) {
      return List.of();
    }
    return result.state().representatives().stream()
        .map(ClusterPartialState.Representative::text)
        .toList();
  }

  private static final String MODEL_AGG_NAME = "ppl_cluster_model";

  private static OpenSearchIndex extractIndex(RelNode node) {
    if (node instanceof AbstractCalciteIndexScan scan) {
      return scan.getOsIndex();
    }
    if (node instanceof RelSubset subset) {
      RelNode delegate = subset.getBest() != null ? subset.getBest() : subset.getOriginal();
      return delegate == null ? null : extractIndex(delegate);
    }
    for (RelNode input : node.getInputs()) {
      OpenSearchIndex index = extractIndex(input);
      if (index != null) {
        return index;
      }
    }
    return null;
  }

  /** Phase two: append the label computed against the global model to a single input row. */
  static Object[] labelRow(
      Object row,
      int arity,
      int sourceIndex,
      List<String> model,
      TextSimilarityClustering similarity,
      double threshold) {
    Object[] in = (row instanceof Object[]) ? (Object[]) row : new Object[] {row};
    Object[] out = new Object[arity + 1];
    System.arraycopy(in, 0, out, 0, Math.min(arity, in.length));
    String text =
        (sourceIndex >= 0 && sourceIndex < in.length && in[sourceIndex] != null)
            ? in[sourceIndex].toString()
            : "";
    out[arity] = ClusterLabeler.labelOf(text, model, similarity, threshold);
    return out;
  }

  private int sourceFieldIndex() {
    return getInput().getRowType().getFieldNames().indexOf(sourceField);
  }

  private static class ClusterDistributedEnumerable extends AbstractEnumerable<@Nullable Object> {
    private final CalciteEnumerableClusterDistributed rel;
    private final Enumerable<Object> input;

    ClusterDistributedEnumerable(
        CalciteEnumerableClusterDistributed rel, Enumerable<Object> input) {
      this.rel = rel;
      this.input = input;
    }

    @Override
    public Enumerator<@Nullable Object> enumerator() {
      List<String> model = rel.buildModel();
      TextSimilarityClustering similarity =
          new TextSimilarityClustering(
              rel.threshold, MatchMode.fromString(rel.matchMode), rel.delims);
      int arity = rel.getInput().getRowType().getFieldCount();
      int sourceIndex = rel.sourceFieldIndex();
      Enumerator<Object> inputEnum = input.enumerator();
      return new Enumerator<>() {
        @Override
        public @Nullable Object current() {
          return labelRow(
              inputEnum.current(), arity, sourceIndex, model, similarity, rel.threshold);
        }

        @Override
        public boolean moveNext() {
          return inputEnum.moveNext();
        }

        @Override
        public void reset() {
          inputEnum.reset();
        }

        @Override
        public void close() {
          inputEnum.close();
        }
      };
    }
  }
}
