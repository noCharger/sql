/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.cluster;

import java.util.Map;

/**
 * One shard-local cluster, emitted by the Map stage and merged by the coordinator Reduce stage.
 *
 * <p>Rows assigned to the same shard-local cluster are never transferred individually; only this
 * summary crosses the wire. The coordinator applies the same greedy algorithm to the
 * representatives, processing them in the canonical order {@code (shardOrdinal,
 * localCreationOrdinal)} so that network arrival order cannot affect the result.
 *
 * @param shardOrdinal deterministic shard identity (part of the canonical merge key)
 * @param localClusterId shard-local cluster id (1-based, creation order on that shard)
 * @param localCreationOrdinal order in which this cluster was created on its shard (canonical key)
 * @param representativeText the clustered field value of the representative row; re-vectorized on
 *     the coordinator so vectorization stays a single source of truth
 * @param representativeRow the representative output row (source projection) surfaced to the client
 *     for {@code labelonly=false}; may be null when only counts/labels are needed
 * @param localCount number of shard-local rows assigned to this cluster
 * @param configFingerprint {@link ClusterConfigFingerprint} the summary was produced under; the
 *     coordinator rejects a merge across differing fingerprints
 */
public record LocalClusterSummary(
    int shardOrdinal,
    int localClusterId,
    int localCreationOrdinal,
    String representativeText,
    Map<String, Object> representativeRow,
    long localCount,
    String configFingerprint) {}
