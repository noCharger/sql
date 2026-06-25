/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.Set;
import org.apache.calcite.rel.RelNode;

/**
 * Interface for scan nodes that support schema-on-read (RFC <a
 * href="https://github.com/opensearch-project/sql/issues/4984">#4984</a>). When schema-on-read is
 * enabled, the scan carries a catch-all {@value DynamicFieldsConstants#DYNAMIC_FIELDS_MAP} column
 * resolved at read time, so that any field reference absent from the index mapping resolves through
 * it. Like highlight, this is applied eagerly during plan construction rather than via an optimizer
 * rule.
 *
 * <p>The column is added <b>unconditionally</b> (no per-command field list): the name resolver is
 * self-gating on the presence of {@code _MAP}, which is what makes dynamic-field support
 * command-agnostic.
 */
public interface SchemaOnReadPushDown {
  /**
   * Return a scan whose schema carries the {@code _MAP} catch-all column, or the same scan when the
   * column is already present (idempotent).
   */
  RelNode applyDynamicFieldsColumn();

  /**
   * Restrict the read-time {@code _source} fetch to the given dynamic field names (RFC #4984 Step
   * 5). These are collected generically from the built logical plan (every {@code ITEM(_MAP, ...)} /
   * {@code MAP_FILTER_KEYS(_MAP, ...)} reference), so the optimization is command-agnostic. A no-op
   * when this scan does not carry the {@code _MAP} column or {@code includes} is empty; the request
   * builder then falls back to fetching the full {@code _source}.
   */
  void applyDynamicFieldSourceIncludes(Set<String> includes);
}
