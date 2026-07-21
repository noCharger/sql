/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies phase-two per-row labeling assigns rows to the nearest representative or marks outliers.
 */
class ClusterLabelerTest {

  private static final List<String> MODEL = List.of("login error alpha", "disk full beta");

  private int label(String text) {
    TextSimilarityClustering sim = new TextSimilarityClustering(0.5, MatchMode.TERMSET, " ");
    return ClusterLabeler.labelOf(text, MODEL, sim, 0.5);
  }

  @Test
  void assignsRowToNearestRepresentative() {
    assertEquals(0, label("login error alpha"));
    assertEquals(1, label("disk full beta"));
  }

  @Test
  void assignsPartialMatchAboveThreshold() {
    // Shares two of three tokens with representative 0 (cosine ~0.67 >= 0.5).
    assertEquals(0, label("login error gamma"));
  }

  @Test
  void marksOutlierWhenNoRepresentativeMeetsThreshold() {
    assertEquals(ClusterLabeler.UNASSIGNED, label("totally unrelated payload"));
  }
}
