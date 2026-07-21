/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.BigArrays;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.opensearch.sql.common.cluster.ClusterPartialState;
import org.opensearch.sql.common.cluster.ClusterPartialState.Representative;
import org.opensearch.sql.common.cluster.MatchMode;

class InternalClusterResultTest {

  private static InternalClusterResult result(double threshold, Representative... reps) {
    return new InternalClusterResult(
        "c", new ClusterPartialState(threshold, MatchMode.TERMLIST, " ", List.of(reps)), Map.of());
  }

  @Test
  void serializationRoundTrip() throws IOException {
    InternalClusterResult original =
        result(0.8, new Representative("login error", 3), new Representative("disk full", 7));
    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    InternalClusterResult copy = new InternalClusterResult(out.bytes().streamInput());

    assertEquals(0.8, copy.state().threshold());
    assertEquals(MatchMode.TERMLIST, copy.state().matchMode());
    assertEquals(2, copy.state().representatives().size());
    assertEquals("login error", copy.state().representatives().get(0).text());
    assertEquals(3, copy.state().representatives().get(0).count());
    assertEquals(7, copy.state().representatives().get(1).count());
  }

  @Test
  void reduceMergesShardResults() {
    InternalClusterResult a = result(0.5, new Representative("login login login", 3));
    InternalClusterResult b = result(0.5, new Representative("login login login", 2));
    ReduceContext ctx =
        ReduceContext.forFinalReduction(
            BigArrays.NON_RECYCLING_INSTANCE, null, count -> {}, PipelineTree.EMPTY);

    InternalClusterResult merged = (InternalClusterResult) a.reduce(List.of(a, b), ctx);

    assertEquals(1, merged.state().representatives().size());
    assertEquals(5, merged.state().representatives().get(0).count());
  }
}
