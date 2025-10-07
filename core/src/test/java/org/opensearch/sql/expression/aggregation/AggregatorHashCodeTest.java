/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;

class AggregatorHashCodeTest {

  @Test
  void testAggregatorHashCodeStableAfterMutation() {
    // Create aggregator
    CountAggregator aggregator = new CountAggregator(List.of(DSL.ref("field", INTEGER)), INTEGER);
    
    // Get initial hashCode
    int initialHashCode = aggregator.hashCode();
    
    // Put in HashMap
    Map<Expression, String> map = new HashMap<>();
    map.put(aggregator, "value");
    
    // Mutate condition and distinct
    aggregator.condition(DSL.literal(true));
    aggregator.distinct(true);
    
    // HashCode should remain the same (condition/distinct excluded)
    assertEquals(initialHashCode, aggregator.hashCode());
    
    // HashMap lookup should still work
    assertEquals("value", map.get(aggregator));
  }

  @Test
  void testTwoAggregatorsWithDifferentConditionsAreEqual() {
    CountAggregator agg1 = new CountAggregator(List.of(DSL.ref("field", INTEGER)), INTEGER);
    CountAggregator agg2 = new CountAggregator(List.of(DSL.ref("field", INTEGER)), INTEGER);
    
    agg1.condition(DSL.literal(true));
    agg2.condition(DSL.literal(false));
    
    // Should be equal because condition is excluded from equals
    assertEquals(agg1, agg2);
    assertEquals(agg1.hashCode(), agg2.hashCode());
  }

  @Test
  void testTwoAggregatorsWithDifferentDistinctAreEqual() {
    CountAggregator agg1 = new CountAggregator(List.of(DSL.ref("field", INTEGER)), INTEGER);
    CountAggregator agg2 = new CountAggregator(List.of(DSL.ref("field", INTEGER)), INTEGER);
    
    agg1.distinct(true);
    agg2.distinct(false);
    
    // Should be equal because distinct is excluded from equals
    assertEquals(agg1, agg2);
    assertEquals(agg1.hashCode(), agg2.hashCode());
  }
}
