/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies the reduce phase of the distributed map/reduce cluster aggregation: similar
 * representatives merge with summed counts, dissimilar ones stay separate, and the outcome is
 * shard-order dependent (the documented approximation).
 */
class ClusterRepresentativeMergeTest {

  private static ClusterPartialState shard(ClusterPartialState.Representative... reps) {
    return new ClusterPartialState(0.5, MatchMode.TERMLIST, " ", List.of(reps));
  }

  private static ClusterPartialState.Representative rep(String text, long count) {
    return new ClusterPartialState.Representative(text, count);
  }

  @Test
  void mergesSimilarRepresentativesAndSumsCounts() {
    List<ClusterPartialState.Representative> merged =
        ClusterRepresentativeMerge.merge(
            List.of(shard(rep("login login login", 3)), shard(rep("login login login", 2))),
            0.5,
            MatchMode.TERMLIST,
            " ");
    assertEquals(1, merged.size());
    assertEquals(5, merged.get(0).count());
  }

  @Test
  void keepsDissimilarRepresentativesSeparate() {
    List<ClusterPartialState.Representative> merged =
        ClusterRepresentativeMerge.merge(
            List.of(shard(rep("login error auth", 4)), shard(rep("disk full quota", 7))),
            0.5,
            MatchMode.TERMLIST,
            " ");
    assertEquals(2, merged.size());
    long total = merged.stream().mapToLong(ClusterPartialState.Representative::count).sum();
    assertEquals(11, total);
  }

  @Test
  void reduceIsApproximateBecauseRepresentativesStandInForMembers() {
    // A representative stands in only for itself during the reduce, so total member count is
    // always preserved even though cluster boundaries depend on shard layout.
    List<ClusterPartialState> states =
        List.of(shard(rep("alpha alpha", 1), rep("beta beta", 1)), shard(rep("alpha alpha", 1)));
    List<ClusterPartialState.Representative> merged =
        ClusterRepresentativeMerge.merge(states, 0.5, MatchMode.TERMLIST, " ");
    long total = merged.stream().mapToLong(ClusterPartialState.Representative::count).sum();
    assertEquals(3, total);
    assertTrue(merged.size() >= 1 && merged.size() <= 3);
  }
}
