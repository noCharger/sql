/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.cluster.ClusterLabeler;
import org.opensearch.sql.common.cluster.MatchMode;
import org.opensearch.sql.common.cluster.TextSimilarityClustering;

/** Verifies phase-two row labeling: the label column is appended and computed against the model. */
class CalciteEnumerableClusterDistributedTest {

  private static final List<String> MODEL = List.of("login error alpha", "disk full beta");

  private static TextSimilarityClustering sim() {
    return new TextSimilarityClustering(0.5, MatchMode.TERMSET, " ");
  }

  @Test
  void appendsLabelForMatchingRow() {
    Object[] out =
        CalciteEnumerableClusterDistributed.labelRow(
            new Object[] {"disk full beta", 42}, 2, 0, MODEL, sim(), 0.5);
    assertEquals(3, out.length);
    assertEquals("disk full beta", out[0]);
    assertEquals(42, out[1]);
    assertEquals(1, out[2]);
  }

  @Test
  void appendsUnassignedForOutlierRow() {
    Object[] out =
        CalciteEnumerableClusterDistributed.labelRow(
            new Object[] {"totally unrelated payload", 7}, 2, 0, MODEL, sim(), 0.5);
    assertEquals(ClusterLabeler.UNASSIGNED, out[2]);
  }

  @Test
  void emptyModelLabelsEverythingUnassigned() {
    Object[] out =
        CalciteEnumerableClusterDistributed.labelRow(
            new Object[] {"login error alpha"}, 1, 0, List.of(), sim(), 0.5);
    assertEquals(2, out.length);
    assertEquals(ClusterLabeler.UNASSIGNED, out[1]);
  }
}
