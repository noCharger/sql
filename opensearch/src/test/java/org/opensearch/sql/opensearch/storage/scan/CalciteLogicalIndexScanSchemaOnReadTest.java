/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.calcite.plan.DynamicFieldsConstants;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/**
 * Step 3 (schema-on-read, RFC #4984) — unit tests for injecting the {@code _MAP} catch-all column
 * into {@link CalciteLogicalIndexScan} when the query references fields absent from the index
 * mapping.
 */
@ExtendWith(MockitoExtension.class)
public class CalciteLogicalIndexScanSchemaOnReadTest {
  static final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  @Mock private RelOptCluster cluster;
  @Mock private RelOptTable table;
  @Mock private OpenSearchIndex osIndex;

  private RelDataType mappedRowType;

  @BeforeEach
  void setUp() {
    RelTraitSet traitSet = mock(RelTraitSet.class);
    lenient().when(cluster.traitSetOf(any(Convention.class))).thenReturn(traitSet);
    lenient().when(cluster.getTypeFactory()).thenReturn(typeFactory);
    mappedRowType =
        typeFactory
            .builder()
            .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("age", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .build();
    lenient().when(table.getRowType()).thenReturn(mappedRowType);
  }

  @Test
  void applyDynamicFieldsColumn_addsMapColumn() {
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    CalciteLogicalIndexScan newScan = scan.applyDynamicFieldsColumn();

    // _MAP is appended after the mapped fields, preserving their order.
    assertEquals(
        List.of("name", "age", DynamicFieldsConstants.DYNAMIC_FIELDS_MAP),
        newScan.getRowType().getFieldNames());

    RelDataType mapType =
        newScan
            .getRowType()
            .getField(DynamicFieldsConstants.DYNAMIC_FIELDS_MAP, false, false)
            .getType();
    assertEquals(SqlTypeName.MAP, mapType.getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, mapType.getKeyType().getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, mapType.getValueType().getSqlTypeName());
  }

  @Test
  void applyDynamicFieldsColumn_isIdempotent_whenMapAlreadyPresent() {
    CalciteLogicalIndexScan scan = new CalciteLogicalIndexScan(cluster, table, osIndex);

    CalciteLogicalIndexScan once = scan.applyDynamicFieldsColumn();
    CalciteLogicalIndexScan twice = once.applyDynamicFieldsColumn();

    // The second call is a no-op: exactly one _MAP column, no duplication, same instance returned.
    assertSame(once, twice);
    assertEquals(
        List.of("name", "age", DynamicFieldsConstants.DYNAMIC_FIELDS_MAP),
        twice.getRowType().getFieldNames());
  }
}
