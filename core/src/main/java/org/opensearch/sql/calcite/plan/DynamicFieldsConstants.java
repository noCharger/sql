/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import lombok.experimental.UtilityClass;

/**
 * Constants for schema-on-read dynamic field handling.
 *
 * <p>Schema-on-read (RFC <a href="https://github.com/opensearch-project/sql/issues/4984">#4984</a>)
 * lets PPL queries reference fields that exist in document {@code _source} but are not declared in
 * the index mapping. Such fields cannot be part of the static Calcite row type, so they are carried
 * through a single catch-all map column named {@value #DYNAMIC_FIELDS_MAP}. References to an
 * unmapped field {@code f} are rewritten to {@code ITEM(_MAP, 'f')}.
 */
@UtilityClass
public class DynamicFieldsConstants {
  /** Name of the catch-all map column holding fields resolved at read time. */
  public static final String DYNAMIC_FIELDS_MAP = "_MAP";
}
