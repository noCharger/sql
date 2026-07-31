/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteClusterCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
  }

  @Test
  public void testBasicCluster() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'user login failed' | cluster message | fields"
                    + " cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "int"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithCustomThreshold() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'error connecting to database' | cluster message"
                    + " t=0.8 | fields cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "int"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithTermsetMatch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'user authentication failed' | cluster message"
                    + " match=termset | fields cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "int"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithNgramsetMatch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'connection timeout error' | cluster message"
                    + " match=ngramset | fields cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "int"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithCustomLabelField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'database error occurred' | cluster message"
                    + " labelfield=my_cluster | fields my_cluster | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("my_cluster", null, "int"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithShowCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'server unavailable' | cluster message"
                    + " showcount=true | fields cluster_label, cluster_count | head 1",
                TEST_INDEX_BANK));
    verifySchema(
        result, schema("cluster_label", null, "int"), schema("cluster_count", null, "bigint"));
    verifyDataRows(result, rows(1, 7));
  }

  @Test
  public void testClusterWithCustomCountField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'server unavailable' | cluster message"
                    + " countfield=my_count showcount=true | fields cluster_label, my_count"
                    + " | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "int"), schema("my_count", null, "bigint"));
    verifyDataRows(result, rows(1, 7));
  }

  @Test
  public void testClusterWithDelimiters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'user-login-failed' | cluster message delims='-'"
                    + " | fields cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "int"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testClusterWithAllParameters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'system error detected' | cluster message t=0.7"
                    + " match=termset labelfield=custom_label countfield=custom_count"
                    + " showcount=true | fields custom_label, custom_count | head 1",
                TEST_INDEX_BANK));
    verifySchema(
        result, schema("custom_label", null, "int"), schema("custom_count", null, "bigint"));
    verifyDataRows(result, rows(1, 7));
  }

  @Test
  public void testClusterPreservesOtherFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = 'system alert' | cluster message | fields"
                    + " account_number, message, cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("account_number", null, "bigint"),
        schema("message", null, "string"),
        schema("cluster_label", null, "int"));
    verifyDataRows(result, rows(1, "system alert", 1));
  }

  @Test
  public void testClusterGroupsSimilarMessages() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = case(account_number=1, 'login failed for user"
                    + " admin', account_number=6, 'login failed for user root' else 'login"
                    + " failed for user guest') | cluster message match=termset showcount=true"
                    + " | fields message, cluster_label, cluster_count",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("message", null, "string"),
        schema("cluster_label", null, "int"),
        schema("cluster_count", null, "bigint"));
    // All similar messages should dedup to one representative row
    verifyDataRows(result, rows("login failed for user admin", 1, 7));
  }

  @Test
  public void testClusterDedupsByDefault() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = case(account_number=1, 'login failed for user"
                    + " admin', account_number=6, 'login failed for user root' else 'login"
                    + " failed for user guest') | cluster message match=termset showcount=true"
                    + " | fields message, cluster_label, cluster_count",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("message", null, "string"),
        schema("cluster_label", null, "int"),
        schema("cluster_count", null, "bigint"));
    verifyDataRows(result, rows("login failed for user admin", 1, 7));
  }

  @Test
  public void testClusterLabelOnlyKeepsAllRows() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval message = case(account_number=1, 'login failed for user"
                    + " admin', account_number=6, 'login failed for user root' else 'login"
                    + " failed for user guest') | cluster message match=termset labelonly=true"
                    + " showcount=true | fields message, cluster_label, cluster_count | head 3",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("message", null, "string"),
        schema("cluster_label", null, "int"),
        schema("cluster_count", null, "bigint"));
    verifyDataRows(
        result,
        rows("login failed for user admin", 1, 7),
        rows("login failed for user root", 1, 7),
        rows("login failed for user guest", 1, 7));
  }

  // ---------------------------------------------------------------------------
  // Distributed path (Increment 2): showcount=false, labelonly=false.
  //
  // When cluster is applied directly over a real indexed field, the Map phase is pushed to the
  // shards as a scripted_metric aggregation (shard-local greedy clustering) and the coordinator
  // merges the per-shard summaries (HierarchicalClusterMerge). The eval-derived-field cases above
  // stay on the coordinator window path because a computed field cannot be read from _source on a
  // shard. These cases require the visitCluster wiring (Step 2) and a running cluster; they assert
  // the distributed shape rather than data-brittle exact clusters.
  // ---------------------------------------------------------------------------

  @Test
  public void testDistributedClusterExplainPushesScriptedMetricToShards() throws IOException {
    // Clustering an indexed field must push a scripted_metric aggregation down to the shards.
    String explained =
        explainQueryToString(
            String.format("search source=%s | cluster address | fields cluster_label", TEST_INDEX_BANK));
    assertThat(
        "distributed cluster should push a scripted_metric aggregation to the shards",
        explained,
        containsString("scripted_metric"));
  }

  @Test
  public void testDistributedClusterFirstRepresentativeLabelIsOne() throws IOException {
    // The first representative always receives label 1 regardless of the underlying data, so this
    // is a stable invariant of the distributed representative output.
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | cluster address | fields cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("cluster_label", null, "int"));
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testDistributedClusterRepresentativeKeepsOriginalFields() throws IOException {
    // labelonly=false returns one representative row per cluster with the original fields plus the
    // cluster label. Assert the output schema shape (representative row + label).
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | cluster address | fields address, cluster_label | head 1",
                TEST_INDEX_BANK));
    verifySchema(
        result, schema("address", null, "string"), schema("cluster_label", null, "int"));
  }

  @Test
  public void testDistributedClusterShowCountSchema() throws IOException {
    // showcount=true adds the coordinator-merged final size per representative.
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | cluster address showcount=true"
                    + " | fields cluster_label, cluster_count | head 1",
                TEST_INDEX_BANK));
    verifySchema(
        result, schema("cluster_label", null, "int"), schema("cluster_count", null, "bigint"));
  }

  @Test
  public void testDistributedClusterTermsetMatchExplain() throws IOException {
    // match mode is threaded into the scripted_metric params; verify pushdown still occurs.
    String explained =
        explainQueryToString(
            String.format(
                "search source=%s | cluster address match=termset t=0.7 | fields cluster_label",
                TEST_INDEX_BANK));
    assertThat(
        "termset distributed cluster should still push scripted_metric",
        explained,
        containsString("scripted_metric"));
  }
}
