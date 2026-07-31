/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HierarchicalClusterMergeTest {

  private static final double T = 0.8;
  private static final MatchMode MODE = MatchMode.TERMLIST;
  private static final String DELIMS = " ";
  private static final String FP = ClusterConfigFingerprint.of(MODE, T, DELIMS);

  private static LocalClusterSummary summary(
      int shard, int localId, int creationOrdinal, String text, long count) {
    return new LocalClusterSummary(
        shard, localId, creationOrdinal, text, Map.of("message", text), count, FP);
  }

  @Test
  void emptyInputYieldsNoClusters() {
    assertThat(HierarchicalClusterMerge.merge(List.of(), T, MODE, DELIMS)).isEmpty();
    assertThat(HierarchicalClusterMerge.merge(null, T, MODE, DELIMS)).isEmpty();
  }

  @Test
  void distinctRepresentativesGetSequentialLabelsInCreationOrder() {
    List<LocalClusterSummary> summaries =
        List.of(
            summary(0, 1, 0, "connection failed host", 3),
            summary(0, 2, 1, "disk full error code", 2));

    List<HierarchicalClusterMerge.GlobalCluster> result =
        HierarchicalClusterMerge.merge(summaries, T, MODE, DELIMS);

    assertThat(result).hasSize(2);
    assertThat(result.get(0).label()).isEqualTo(1);
    assertThat(result.get(0).count()).isEqualTo(3);
    assertThat(result.get(0).representativeRow()).containsEntry("message", "connection failed host");
    assertThat(result.get(1).label()).isEqualTo(2);
    assertThat(result.get(1).count()).isEqualTo(2);
  }

  @Test
  void similarRepresentativesAcrossShardsMergeAndSumCounts() {
    List<LocalClusterSummary> summaries =
        List.of(
            summary(0, 1, 0, "connection failed host", 3),
            summary(1, 1, 0, "connection failed host", 4));

    List<HierarchicalClusterMerge.GlobalCluster> result =
        HierarchicalClusterMerge.merge(summaries, T, MODE, DELIMS);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).label()).isEqualTo(1);
    assertThat(result.get(0).count()).isEqualTo(7);
  }

  @Test
  void resultIsIndependentOfInputOrderCanonicalMergeKey() {
    List<LocalClusterSummary> canonical =
        List.of(
            summary(0, 1, 0, "alpha alpha alpha", 2), // creates cluster 1
            summary(0, 2, 1, "beta beta beta", 1), // creates cluster 2
            summary(1, 1, 0, "alpha alpha alpha", 5)); // joins cluster 1

    List<HierarchicalClusterMerge.GlobalCluster> expected =
        HierarchicalClusterMerge.merge(canonical, T, MODE, DELIMS);

    // Feed the same summaries in several shuffled orders; the canonical (shard, creationOrdinal)
    // sort must produce identical labels, representatives, and counts every time.
    List<LocalClusterSummary> shuffled = new ArrayList<>(canonical);
    for (int seed = 0; seed < 5; seed++) {
      Collections.shuffle(shuffled, new java.util.Random(seed));
      List<HierarchicalClusterMerge.GlobalCluster> actual =
          HierarchicalClusterMerge.merge(shuffled, T, MODE, DELIMS);
      assertThat(actual).isEqualTo(expected);
    }

    assertThat(expected).hasSize(2);
    assertThat(expected.get(0).label()).isEqualTo(1);
    assertThat(expected.get(0).count()).isEqualTo(7); // 2 + 5
    assertThat(expected.get(0).representativeRow()).containsEntry("message", "alpha alpha alpha");
    assertThat(expected.get(1).label()).isEqualTo(2);
    assertThat(expected.get(1).count()).isEqualTo(1);
  }

  @Test
  void representativeIsTheEarliestCreatorAndNotUpdated() {
    // Both summaries are identical text; the shard-0 creator (canonical first) must win as the
    // surviving representative even though a later summary carries a different row payload.
    LocalClusterSummary first =
        new LocalClusterSummary(0, 1, 0, "same tokens here", Map.of("id", "first"), 1, FP);
    LocalClusterSummary second =
        new LocalClusterSummary(1, 1, 0, "same tokens here", Map.of("id", "second"), 1, FP);

    List<HierarchicalClusterMerge.GlobalCluster> result =
        HierarchicalClusterMerge.merge(List.of(second, first), T, MODE, DELIMS);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).representativeRow()).containsEntry("id", "first");
    assertThat(result.get(0).count()).isEqualTo(2);
  }

  @Test
  void mismatchedFingerprintsAreRejected() {
    LocalClusterSummary good = summary(0, 1, 0, "connection failed host", 1);
    LocalClusterSummary bad =
        new LocalClusterSummary(
            1, 1, 0, "connection failed host", Map.of("message", "x"), 1, "mm=TERMSET;t=0.5;...");

    assertThatThrownBy(
            () -> HierarchicalClusterMerge.merge(List.of(good, bad), T, MODE, DELIMS))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("fingerprint");
  }
}
