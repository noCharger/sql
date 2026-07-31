/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import java.util.Locale;

/**
 * Stable identity of a clustering configuration.
 *
 * <p>The distributed cluster command forms clusters on the shards (Map) and merges the resulting
 * representatives on the coordinator (Reduce). A shard-local summary is only comparable to another
 * when both were produced with the same feature contract: match mode, similarity threshold,
 * delimiter policy, token-normalization version, and vectorization version. Nodes must never merge
 * cluster states produced with different fingerprints, otherwise the similarity comparisons on the
 * coordinator would be meaningless.
 *
 * <p>Bumping {@link #NORMALIZATION_VERSION} or {@link #VECTORIZATION_VERSION} whenever the
 * tokenization / vectorization behaviour changes guarantees that summaries produced by mixed-version
 * nodes (during a rolling upgrade) are rejected rather than silently mismerged.
 */
public final class ClusterConfigFingerprint {

  /** Bump when token normalization (e.g. numeric masking) changes. */
  public static final int NORMALIZATION_VERSION = 1;

  /** Bump when sparse-vector construction (term/ngram layout) changes. */
  public static final int VECTORIZATION_VERSION = 1;

  private ClusterConfigFingerprint() {}

  /**
   * Compute the deterministic fingerprint string for a clustering configuration.
   *
   * @param matchMode feature-extraction mode
   * @param threshold similarity threshold in (0, 1)
   * @param delims delimiter policy ({@code "non-alphanumeric"} or an explicit delimiter set)
   * @return a stable, order-independent identity string
   */
  public static String of(MatchMode matchMode, double threshold, String delims) {
    MatchMode mode = matchMode != null ? matchMode : MatchMode.DEFAULT;
    String delimPolicy = delims != null ? delims : " ";
    // Fixed-precision threshold formatting so 0.8 and 0.80 map to the same fingerprint and float
    // formatting differences across nodes cannot fork the identity.
    return String.format(
        Locale.ROOT,
        "mm=%s;t=%.6f;delims=%s;norm=%d;vec=%d",
        mode.name(),
        threshold,
        delimPolicy,
        NORMALIZATION_VERSION,
        VECTORIZATION_VERSION);
  }
}
