/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.WildcardUtils;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * UDF MAP_FILTER_KEYS(map, pattern) — returns a new MAP containing only the entries whose keys
 * match the given wildcard pattern. Used to implement partial-wildcard field selection after spath:
 *
 * <pre>
 *   source=idx | spath input=log output=result | fields result.user.*
 *   → MAP_FILTER_KEYS(result, 'user.*')
 * </pre>
 *
 * <p>Keys in the returned map are in lexicographic order (inherited from spath's TreeMap output).
 * The pattern uses {@code *} as the sole wildcard character (same semantics as the PPL {@code
 * fields} wildcard).
 */
public class MapFilterKeysFunctionImpl extends ImplementorUDF {

  public MapFilterKeysFunctionImpl() {
    super(new MapFilterKeysImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.explicit(
        TYPE_FACTORY.createMapType(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            true));
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.MAP, SqlTypeFamily.STRING));
  }

  public static class MapFilterKeysImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          Types.lookupMethod(
              MapFilterKeysFunctionImpl.class, "mapFilterKeys", Object.class, Object.class),
          translatedOperands.get(0),
          translatedOperands.get(1));
    }
  }

  /**
   * Filters the MAP, retaining only entries whose keys match {@code pattern}. The pattern uses
   * {@code *} as a wildcard. An empty result map is returned when nothing matches.
   */
  @SuppressWarnings("unchecked")
  public static Object mapFilterKeys(Object mapArg, Object patternArg) {
    if (mapArg == null) {
      return null;
    }
    if (patternArg == null) {
      return mapArg;
    }

    Map<String, String> map = (Map<String, String>) mapArg;
    String pattern = (String) patternArg;

    // "*" with no other characters → return full map copy preserving order
    if ("*".equals(pattern)) {
      return new LinkedHashMap<>(map);
    }

    Map<String, String> result = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (WildcardUtils.matchesWildcardPattern(pattern, entry.getKey())) {
        result.put(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }
}
