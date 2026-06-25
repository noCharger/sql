/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Schema-on-read (RFC #4984, Step 3) integration tests. Exercises a {@code dynamic:false} index
 * that carries fields in {@code _source} which are not declared in the mapping, and verifies they
 * can be read at query time through the {@code _MAP} catch-all column when {@code
 * plugins.calcite.schema_on_read.enabled} is on. Gated behind the setting; {@code @AfterClass
 * wipeAllClusterSettings} in the base resets it for other suites.
 */
public class CalcitePPLSchemaOnReadIT extends PPLIntegTestCase {

  private static final String INDEX = "schema_on_read_test";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    enableSchemaOnRead();

    // dynamic:false → undeclared fields are stored in _source but NOT added to the mapping,
    // which is exactly the schema-on-read scenario. Only "name" is mapped.
    if (!TestUtils.isIndexExist(client(), INDEX)) {
      TestUtils.createIndexByRestClient(
          client(),
          INDEX,
          "{\"mappings\":{\"dynamic\":false,\"properties\":{\"name\":{\"type\":\"keyword\"}}}}");

      Request doc1 = new Request("PUT", "/" + INDEX + "/_doc/1?refresh=true");
      doc1.setJsonEntity("{\"name\":\"Alice\",\"undeclared_field\":\"hidden_value_A\",\"age\":30}");
      client().performRequest(doc1);

      Request doc2 = new Request("PUT", "/" + INDEX + "/_doc/2?refresh=true");
      doc2.setJsonEntity("{\"name\":\"Bob\",\"undeclared_field\":\"hidden_value_B\",\"age\":25}");
      client().performRequest(doc2);
    }
  }

  private void enableSchemaOnRead() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_SCHEMA_ON_READ_ENABLED.getKeyValue(), "true"));
  }

  private void disableSchemaOnRead() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_SCHEMA_ON_READ_ENABLED.getKeyValue(), "false"));
  }

  @Test
  public void testProjectionOfUnmappedField() throws IOException {
    JSONObject result = executeQuery("source=" + INDEX + " | fields undeclared_field");
    verifySchema(result, schema("undeclared_field", "string"));
    verifyDataRows(result, rows("hidden_value_A"), rows("hidden_value_B"));
  }

  @Test
  public void testMixedMappedAndUnmappedFields() throws IOException {
    // 'name' is mapped (keyword); 'undeclared_field' and 'age' come from _source via _MAP. Values
    // are STRING-typed (RFC Step 1's "treat as STRING"), matching spath/JSON_EXTRACT_ALL — so age
    // is "30", not 30. Numeric usage relies on implicit VARCHAR->numeric coercion (see stats
    // tests).
    JSONObject result = executeQuery("source=" + INDEX + " | fields name, undeclared_field, age");
    verifySchema(
        result,
        schema("name", "string"),
        schema("undeclared_field", "string"),
        schema("age", "string"));
    verifyDataRows(
        result, rows("Alice", "hidden_value_A", "30"), rows("Bob", "hidden_value_B", "25"));
  }

  @Test
  public void testWhereOnUnmappedStringField() throws IOException {
    // Filter on an unmapped field: the ITEM(_MAP,...) predicate must NOT be pushed to the DSL.
    JSONObject result =
        executeQuery(
            "source="
                + INDEX
                + " | where undeclared_field = 'hidden_value_A' | fields name, undeclared_field");
    verifySchema(result, schema("name", "string"), schema("undeclared_field", "string"));
    verifyDataRows(result, rows("Alice", "hidden_value_A"));
  }

  @Test
  public void testWhereOnUnmappedNumericField() throws IOException {
    // Numeric comparison against a STRING-typed unmapped field works via implicit coercion.
    JSONObject result = executeQuery("source=" + INDEX + " | where age = 25 | fields name, age");
    verifySchema(result, schema("name", "string"), schema("age", "string"));
    verifyDataRows(result, rows("Bob", "25"));
  }

  @Test
  public void testSortByUnmappedField() throws IOException {
    // Sort on an unmapped field: the ITEM(_MAP,...) sort key must NOT be pushed as a script sort.
    JSONObject result = executeQuery("source=" + INDEX + " | sort age | fields name, age");
    verifySchema(result, schema("name", "string"), schema("age", "string"));
    verifyDataRowsInOrder(result, rows("Bob", "25"), rows("Alice", "30"));
  }

  @Test
  public void testStatsSumOnUnmappedField() throws IOException {
    // Step 4: aggregation over an unmapped field. The STRING values coerce to numeric, and the
    // _MAP-referencing aggregate must NOT be pushed to OpenSearch (it is evaluated in Calcite).
    JSONObject result = executeQuery("source=" + INDEX + " | stats sum(age) as total_age");
    verifySchema(result, schema("total_age", "double"));
    verifyDataRows(result, rows(55.0));
  }

  @Test
  public void testStatsAvgOnUnmappedField() throws IOException {
    JSONObject result = executeQuery("source=" + INDEX + " | stats avg(age) as avg_age");
    verifySchema(result, schema("avg_age", "double"));
    verifyDataRows(result, rows(27.5));
  }

  @Test
  public void testStatsCountByUnmappedField() throws IOException {
    JSONObject result =
        executeQuery("source=" + INDEX + " | stats count() as c by undeclared_field");
    verifySchema(result, schema("c", "bigint"), schema("undeclared_field", "string"));
    verifyDataRows(result, rows(1, "hidden_value_A"), rows(1, "hidden_value_B"));
  }

  @Test
  public void testEvalArithmeticOnUnmappedField() throws IOException {
    // Arithmetic on a STRING-typed unmapped field coerces to numeric (double).
    JSONObject result =
        executeQuery("source=" + INDEX + " | eval next_age = age + 1 | fields name, next_age");
    verifySchema(result, schema("name", "string"), schema("next_age", "double"));
    verifyDataRows(result, rows("Alice", 31.0), rows("Bob", 26.0));
  }

  @Test
  public void testEvalFromUnmappedField() throws IOException {
    JSONObject result =
        executeQuery("source=" + INDEX + " | eval label = undeclared_field | fields name, label");
    verifySchema(result, schema("name", "string"), schema("label", "string"));
    verifyDataRows(result, rows("Alice", "hidden_value_A"), rows("Bob", "hidden_value_B"));
  }

  @Test
  public void testMappedFieldUnaffected() throws IOException {
    // Regression guard: ordinary mapped-field queries behave exactly as before.
    JSONObject result = executeQuery("source=" + INDEX + " | where name = 'Alice' | fields name");
    verifySchema(result, schema("name", "string"));
    verifyDataRows(result, rows("Alice"));
  }

  @Test
  public void testTargetedSourceFetchForUnmappedFields() throws IOException {
    // RFC Step 5: _source is restricted to the referenced fields (mapped + unmapped) instead of
    // fetching the whole document. Guards against silently regressing to a full-source fetch (which
    // is the safe fallback but loses the optimization). Full-source shows "_source":true with no
    // includes; the optimized form carries an "includes" list.
    String explain = explainQueryToString("source=" + INDEX + " | fields name, undeclared_field");
    assertTrue(
        "Expected a targeted _source includes list in explain, got: " + explain,
        explain.contains("includes") && explain.contains("undeclared_field"));
    assertFalse(
        "Expected NOT to fetch the full _source, got: " + explain,
        explain.replace(" ", "").contains("\"_source\":true"));
  }

  @Test
  public void testGateDisabledThrowsFieldNotFound() throws IOException {
    // With schema-on-read off, an unmapped field is an error again (opt-in is preserved).
    try {
      disableSchemaOnRead();
      Throwable e =
          assertThrowsWithReplace(
              IllegalStateException.class,
              () -> executeQuery("source=" + INDEX + " | fields undeclared_field"));
      verifyErrorMessageContains(e, "Field [undeclared_field] not found.");
    } finally {
      enableSchemaOnRead();
    }
  }

  @Test
  public void testDedupOnUnmappedField_commandAgnostic() throws IOException {
    // Command-agnostic proof: `dedup` never had a per-command entry in the (now-removed)
    // SchemaOnReadFieldCollector — only project/where/sort/eval/stats were handled. Under the
    // plan-layer design, the scan carries _MAP unconditionally and the name resolver self-gates on
    // it, so `dedup` resolves the unmapped field automatically with NO command-specific code.
    JSONObject result =
        executeQuery(
            "source=" + INDEX + " | dedup undeclared_field | fields name, undeclared_field");
    verifySchema(result, schema("name", "string"), schema("undeclared_field", "string"));
    verifyDataRows(
        result, rows("Alice", "hidden_value_A"), rows("Bob", "hidden_value_B"));
  }

  @Test
  public void testHeadAfterUnmappedReference_commandAgnostic() throws IOException {
    // `head` likewise had no collector entry. Chaining it after an unmapped-field reference still
    // resolves correctly, confirming the support is not tied to any specific command surface.
    JSONObject result =
        executeQuery(
            "source="
                + INDEX
                + " | sort age | fields name, undeclared_field, age | head 1");
    verifySchema(
        result,
        schema("name", "string"),
        schema("undeclared_field", "string"),
        schema("age", "string"));
    verifyDataRowsInOrder(result, rows("Bob", "hidden_value_B", "25"));
  }
}
