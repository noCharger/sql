/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies the index-time-reuse contract: {@link TextSimilarityClustering#extractKeys} produces the
 * stable key form that an index-time processor would store, and rebuilding the vector from those
 * stored keys yields the same similarity as clustering the raw text at query time (no re-tokenize
 * needed).
 */
class TextSimilarityClusteringKeysTest {

  @Test
  void termListKeysArePositionPrefixedAndNumericNormalized() {
    TextSimilarityClustering c = new TextSimilarityClustering(0.8, MatchMode.TERMLIST, " ");
    assertEquals(List.of("0-login", "1-error", "2-*"), c.extractKeys("login error 500"));
  }

  @Test
  void termSetKeysCountAsBagOfWords() {
    TextSimilarityClustering c = new TextSimilarityClustering(0.8, MatchMode.TERMSET, " ");
    Map<CharSequence, Integer> vector = TextSimilarityClustering.countKeys(c.extractKeys("a a b"));
    assertEquals(2, vector.get("a"));
    assertEquals(1, vector.get("b"));
  }

  @Test
  void ngramSetKeysAreTrigrams() {
    TextSimilarityClustering c = new TextSimilarityClustering(0.8, MatchMode.NGRAMSET, " ");
    assertEquals(List.of("log", "ogi", "gin"), c.extractKeys("login"));
  }

  @Test
  void storedKeysPreserveSimilarity() {
    // Rebuilding vectors from stored keys must match the live computeSimilarity path exactly, so an
    // index-time processor can store extractKeys(...) and query time skips re-tokenization.
    TextSimilarityClustering c = new TextSimilarityClustering(0.8, MatchMode.TERMLIST, " ");
    String a = "login error auth";
    String b = "login error timeout";

    double live = c.computeSimilarity(a, b);
    double fromStoredKeys =
        c.cosine(
            TextSimilarityClustering.countKeys(c.extractKeys(a)),
            TextSimilarityClustering.countKeys(c.extractKeys(b)));

    assertEquals(live, fromStoredKeys, 1e-12);
  }
}
