/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import java.util.ArrayList;
import java.util.List;

/**
 * Partial clustering state produced by one shard (the map phase) of a distributed map/reduce
 * cluster aggregation. It carries only the shard-local cluster representatives and their member
 * counts, not the raw rows, so the volume crossing the wire to the coordinating node is bounded by
 * the number of shard-local clusters rather than by the number of documents.
 *
 * <p>Representatives are carried as their raw text (not as pre-computed token vectors) so the wire
 * form stays minimal and stable; the coordinating node re-vectorizes only the representatives
 * during the reduce, which is bounded by shards times the per-shard cluster cap.
 *
 * <p>This is a plain value object with no OpenSearch wire dependency on purpose. The server-side
 * InternalAggregation that carries it across the wire (implemented in the wiring stage) owns
 * serialization and its cross-version compatibility contract.
 */
public class ClusterPartialState {

  /** A shard-local cluster representative and how many rows it stands for. */
  public static final class Representative {
    private final String text;
    private final long count;

    public Representative(String text, long count) {
      this.text = text;
      this.count = count;
    }

    public String text() {
      return text;
    }

    public long count() {
      return count;
    }
  }

  private final double threshold;
  private final MatchMode matchMode;
  private final String delims;
  private final List<Representative> representatives;

  public ClusterPartialState(
      double threshold, MatchMode matchMode, String delims, List<Representative> representatives) {
    this.threshold = threshold;
    this.matchMode = matchMode;
    this.delims = delims;
    this.representatives = new ArrayList<>(representatives);
  }

  public double threshold() {
    return threshold;
  }

  public MatchMode matchMode() {
    return matchMode;
  }

  public String delims() {
    return delims;
  }

  public List<Representative> representatives() {
    return representatives;
  }
}
