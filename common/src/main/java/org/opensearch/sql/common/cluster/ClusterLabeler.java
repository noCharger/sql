/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import java.util.List;

/**
 * Phase two of the distributed {@code cluster} path: per-row labeling against a fixed global model.
 *
 * <p>Phase one (the {@code ppl_cluster} aggregation and its reduce) produces the global cluster
 * representatives. This kernel then assigns each individual row to the most similar representative,
 * using the same threshold and similarity as clustering. It is a pure function of the row text and
 * the frozen model, so it can run per document on a data node (with the model passed in) to produce
 * the command's per-row {@code cluster_label} output.
 */
public final class ClusterLabeler {

  /** Label returned for a row that matches no representative at or above the threshold. */
  public static final int UNASSIGNED = -1;

  private ClusterLabeler() {}

  /**
   * Assign a row to a cluster in the global model.
   *
   * @param text the row's source value
   * @param representatives global cluster representative texts, in label order
   * @param similarity clustering similarity configured with the same threshold/matchMode/delims as
   *     the model was built with
   * @param threshold the clustering threshold
   * @return the index (label) of the most similar representative if that similarity meets the
   *     threshold, otherwise {@link #UNASSIGNED}
   */
  public static int labelOf(
      String text,
      List<String> representatives,
      TextSimilarityClustering similarity,
      double threshold) {
    int best = UNASSIGNED;
    double bestSim = 0.0;
    for (int i = 0; i < representatives.size(); i++) {
      double sim = similarity.computeSimilarity(text, representatives.get(i));
      if (sim > bestSim) {
        bestSim = sim;
        best = i;
      }
    }
    return (best >= 0 && bestSim >= threshold - 1e-9) ? best : UNASSIGNED;
  }
}
