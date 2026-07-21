/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.cluster;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.sql.calcite.udf.udaf.ClusterLabelAggFunction;
import org.opensearch.sql.common.cluster.MatchMode;

/**
 * Aggregation builder for the distributed {@code cluster} path. Carries the source field and the
 * clustering parameters, builds a {@link ClusterAggregatorFactory} per shard, and serializes over
 * the wire so a coordinating node can send it to data nodes.
 */
public class ClusterAggregationBuilder
    extends AbstractAggregationBuilder<ClusterAggregationBuilder> {

  public static final String NAME = InternalClusterResult.NAME;

  private String field;
  private double threshold = 0.8;
  private MatchMode matchMode = MatchMode.DEFAULT;
  private String delims = " ";
  private int maxClusters = ClusterLabelAggFunction.DEFAULT_MAX_CLUSTERS;

  public static final ObjectParser<ClusterAggregationBuilder, Void> PARSER =
      new ObjectParser<>(NAME);

  static {
    PARSER.declareString(ClusterAggregationBuilder::field, new ParseField("field"));
    PARSER.declareDouble(ClusterAggregationBuilder::threshold, new ParseField("threshold"));
    PARSER.declareString(ClusterAggregationBuilder::matchMode, new ParseField("match"));
    PARSER.declareString(ClusterAggregationBuilder::delims, new ParseField("delims"));
    PARSER.declareInt(ClusterAggregationBuilder::maxClusters, new ParseField("max_clusters"));
  }

  public static ClusterAggregationBuilder parse(String aggregationName, XContentParser parser)
      throws IOException {
    return PARSER.parse(parser, new ClusterAggregationBuilder(aggregationName), null);
  }

  public ClusterAggregationBuilder(String name) {
    super(name);
  }

  public ClusterAggregationBuilder(StreamInput in) throws IOException {
    super(in);
    this.field = in.readString();
    this.threshold = in.readDouble();
    this.matchMode = MatchMode.valueOf(in.readString());
    this.delims = in.readString();
    this.maxClusters = in.readVInt();
  }

  protected ClusterAggregationBuilder(
      ClusterAggregationBuilder clone,
      AggregatorFactories.Builder factoriesBuilder,
      Map<String, Object> metadata) {
    super(clone, factoriesBuilder, metadata);
    this.field = clone.field;
    this.threshold = clone.threshold;
    this.matchMode = clone.matchMode;
    this.delims = clone.delims;
    this.maxClusters = clone.maxClusters;
  }

  public ClusterAggregationBuilder field(String field) {
    this.field = field;
    return this;
  }

  public ClusterAggregationBuilder threshold(double threshold) {
    this.threshold = threshold;
    return this;
  }

  public ClusterAggregationBuilder matchMode(String matchMode) {
    this.matchMode = MatchMode.fromString(matchMode);
    return this;
  }

  public ClusterAggregationBuilder delims(String delims) {
    this.delims = delims;
    return this;
  }

  public ClusterAggregationBuilder maxClusters(int maxClusters) {
    this.maxClusters = maxClusters;
    return this;
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeString(field);
    out.writeDouble(threshold);
    out.writeString(matchMode.name());
    out.writeString(delims);
    out.writeVInt(maxClusters);
  }

  @Override
  protected AggregatorFactory doBuild(
      QueryShardContext queryShardContext,
      AggregatorFactory parent,
      AggregatorFactories.Builder subFactoriesBuilder)
      throws IOException {
    return new ClusterAggregatorFactory(
        name,
        field,
        threshold,
        matchMode,
        delims,
        maxClusters,
        queryShardContext,
        parent,
        subFactoriesBuilder,
        metadata);
  }

  @Override
  protected XContentBuilder internalXContent(XContentBuilder builder, Params params)
      throws IOException {
    builder.startObject();
    builder.field("field", field);
    builder.field("threshold", threshold);
    builder.field("match", matchMode.name());
    builder.field("delims", delims);
    builder.field("max_clusters", maxClusters);
    builder.endObject();
    return builder;
  }

  @Override
  protected AggregationBuilder shallowCopy(
      AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
    return new ClusterAggregationBuilder(this, factoriesBuilder, metadata);
  }

  @Override
  public BucketCardinality bucketCardinality() {
    return BucketCardinality.NONE;
  }

  @Override
  public String getType() {
    return NAME;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass() || !super.equals(obj)) {
      return false;
    }
    ClusterAggregationBuilder other = (ClusterAggregationBuilder) obj;
    return Double.compare(threshold, other.threshold) == 0
        && maxClusters == other.maxClusters
        && Objects.equals(field, other.field)
        && matchMode == other.matchMode
        && Objects.equals(delims, other.delims);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), field, threshold, matchMode, delims, maxClusters);
  }
}
