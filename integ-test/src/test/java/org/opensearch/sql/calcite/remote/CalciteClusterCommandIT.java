/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.isIndexExist;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
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

  @Test
  public void testClusterNoStateLeakAcrossQueries() throws IOException {
    String twoClusterQuery =
        String.format(
            "search source=%s | eval message = case(account_number=1, 'alpha alpha alpha' else"
                + " 'beta beta beta') | cluster message match=termset | stats count() as k",
            TEST_INDEX_BANK);
    String threeClusterQuery =
        String.format(
            "search source=%s | eval message = case(account_number=1, 'gamma gamma gamma',"
                + " account_number=6, 'delta delta delta' else 'epsilon epsilon epsilon') | cluster"
                + " message match=termset | stats count() as k",
            TEST_INDEX_BANK);

    verifyDataRows(executeQuery(twoClusterQuery), rows(2));
    verifyDataRows(executeQuery(threeClusterQuery), rows(3));
    verifyDataRows(executeQuery(twoClusterQuery), rows(2));
    verifyDataRows(executeQuery(threeClusterQuery), rows(3));
  }

  @Test
  public void testDistributedClusterAggregationEndToEnd() throws IOException {
    String index = "cluster_agg_e2e";
    if (!isIndexExist(client(), index)) {
      Request create = new Request("PUT", "/" + index);
      create.setJsonEntity("{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0}}");
      client().performRequest(create);
      String[] docs = {
        "login error alpha",
        "login error alpha",
        "login error alpha",
        "disk full beta",
        "disk full beta"
      };
      for (int i = 0; i < docs.length; i++) {
        Request doc = new Request("PUT", "/" + index + "/_doc/" + (i + 1));
        doc.setJsonEntity("{\"message\": \"" + docs[i] + "\"}");
        client().performRequest(doc);
      }
      // Refresh the whole index: a per-document refresh only refreshes that document's shard.
      client().performRequest(new Request("POST", "/" + index + "/_refresh"));
    }

    Request search = new Request("POST", "/" + index + "/_search");
    search.setJsonEntity(
        "{\"size\":0,\"aggs\":{\"c\":{\"ppl_cluster\":{"
            + "\"field\":\"message\",\"threshold\":0.5,\"match\":\"termset\","
            + "\"delims\":\" \",\"max_clusters\":100}}}}");

    long startNanos = System.nanoTime();
    String responseBody =
        new String(
            client().performRequest(search).getEntity().getContent().readAllBytes(),
            StandardCharsets.UTF_8);
    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

    JSONArray clusters =
        new JSONObject(responseBody)
            .getJSONObject("aggregations")
            .getJSONObject("c")
            .getJSONArray("clusters");

    assertEquals(2, clusters.length());
    Set<Long> counts = new HashSet<>();
    long total = 0;
    for (int i = 0; i < clusters.length(); i++) {
      long count = clusters.getJSONObject(i).getLong("count");
      counts.add(count);
      total += count;
    }
    assertEquals(5L, total);
    assertTrue(counts.contains(3L) && counts.contains(2L));
    logger.info(
        "ppl_cluster distributed aggregation over 3 shards returned {} clusters in {} ms",
        clusters.length(),
        elapsedMs);
  }

  @Test
  public void testDistributedClusterCommandEndToEnd() throws IOException {
    String index = "cluster_dist_e2e";
    if (!isIndexExist(client(), index)) {
      Request create = new Request("PUT", "/" + index);
      create.setJsonEntity("{\"settings\":{\"number_of_shards\":3,\"number_of_replicas\":0}}");
      client().performRequest(create);
      String[] docs = {
        "login error alpha",
        "login error alpha",
        "login error alpha",
        "disk full beta",
        "disk full beta"
      };
      for (int i = 0; i < docs.length; i++) {
        Request doc = new Request("PUT", "/" + index + "/_doc/" + (i + 1));
        doc.setJsonEntity("{\"message\": \"" + docs[i] + "\"}");
        client().performRequest(doc);
      }
      client().performRequest(new Request("POST", "/" + index + "/_refresh"));
    }

    Request enable = new Request("PUT", "/_cluster/settings");
    enable.setJsonEntity("{\"transient\":{\"plugins.ppl.cluster.distributed\":true}}");
    client().performRequest(enable);
    try {
      JSONObject result =
          executeQuery("source=" + index + " | cluster message | fields message, cluster_label");
      JSONArray rows = result.getJSONArray("datarows");
      assertEquals(5, rows.length());
      Set<Integer> labels = new java.util.HashSet<>();
      for (int i = 0; i < rows.length(); i++) {
        labels.add(rows.getJSONArray(i).getInt(1));
      }
      assertEquals(2, labels.size());
      assertTrue(labels.stream().allMatch(l -> l >= 0));
    } finally {
      Request reset = new Request("PUT", "/_cluster/settings");
      reset.setJsonEntity("{\"transient\":{\"plugins.ppl.cluster.distributed\":null}}");
      client().performRequest(reset);
    }
  }

  @Test
  public void testClusterPreservesUpstreamSortOrder() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + TEST_INDEX_BANK
                + " | sort age | eval message = firstname"
                + " | cluster message match=termset labelonly=true | fields age");
    JSONArray rows = result.getJSONArray("datarows");
    int previous = Integer.MIN_VALUE;
    for (int i = 0; i < rows.length(); i++) {
      int age = rows.getJSONArray(i).getInt(0);
      assertTrue(age >= previous);
      previous = age;
    }
  }
}
