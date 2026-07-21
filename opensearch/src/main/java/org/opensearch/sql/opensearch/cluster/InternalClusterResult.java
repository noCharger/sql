/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.sql.common.cluster.ClusterPartialState;
import org.opensearch.sql.common.cluster.ClusterRepresentativeMerge;
import org.opensearch.sql.common.cluster.MatchMode;

/**
 * Server-side aggregation result for the distributed map/reduce {@code cluster} path. Each shard
 * emits one of these carrying its shard-local {@link ClusterPartialState} (cluster representatives
 * plus member counts); the coordinating node reduces them into a global set of representatives via
 * {@link ClusterRepresentativeMerge}.
 *
 * <p>Serialization is a cross-version wire contract. Representatives are carried as raw text plus a
 * count so the contract stays minimal. New fields must be added behind a {@code out.getVersion()}
 * guard so mixed-version clusters during a rolling upgrade stay compatible.
 *
 * <p>Note: an aggregation yields the cluster summary (representatives and counts), not a per-row
 * label. Producing the command's per-row {@code cluster_label} output requires a second labeling
 * pass over the rows against this global model; that pass is out of scope for this class.
 */
public class InternalClusterResult extends InternalAggregation {

  public static final String NAME = "ppl_cluster";

  private final ClusterPartialState state;

  public InternalClusterResult(
      String name, ClusterPartialState state, Map<String, Object> metadata) {
    super(name, metadata);
    this.state = state;
  }

  public InternalClusterResult(StreamInput in) throws IOException {
    super(in);
    double threshold = in.readDouble();
    MatchMode matchMode = MatchMode.valueOf(in.readString());
    String delims = in.readString();
    int size = in.readVInt();
    List<ClusterPartialState.Representative> reps = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      String text = in.readString();
      long count = in.readVLong();
      reps.add(new ClusterPartialState.Representative(text, count));
    }
    this.state = new ClusterPartialState(threshold, matchMode, delims, reps);
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeDouble(state.threshold());
    out.writeString(state.matchMode().name());
    out.writeString(state.delims());
    out.writeVInt(state.representatives().size());
    for (ClusterPartialState.Representative rep : state.representatives()) {
      out.writeString(rep.text());
      out.writeVLong(rep.count());
    }
  }

  @Override
  public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext context) {
    List<ClusterPartialState> shardStates = new ArrayList<>(aggregations.size());
    for (InternalAggregation agg : aggregations) {
      shardStates.add(((InternalClusterResult) agg).state);
    }
    List<ClusterPartialState.Representative> merged =
        ClusterRepresentativeMerge.merge(
            shardStates, state.threshold(), state.matchMode(), state.delims());
    ClusterPartialState global =
        new ClusterPartialState(state.threshold(), state.matchMode(), state.delims(), merged);
    return new InternalClusterResult(getName(), global, getMetadata());
  }

  @Override
  protected boolean mustReduceOnSingleInternalAgg() {
    return false;
  }

  @Override
  public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
    builder.startArray("clusters");
    for (ClusterPartialState.Representative rep : state.representatives()) {
      builder
          .startObject()
          .field("representative", rep.text())
          .field("count", rep.count())
          .endObject();
    }
    builder.endArray();
    return builder;
  }

  @Override
  public Object getProperty(List<String> path) {
    throw new UnsupportedOperationException("cluster aggregation does not expose sub-properties");
  }

  @Override
  public String getWriteableName() {
    return NAME;
  }

  public ClusterPartialState state() {
    return state;
  }
}
