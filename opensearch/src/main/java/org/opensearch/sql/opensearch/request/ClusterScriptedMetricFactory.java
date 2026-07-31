/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.metrics.ScriptedMetricAggregationBuilder;

/**
 * Builds the {@code scripted_metric} aggregation that runs the Map stage of the distributed cluster
 * command: shard-local greedy leader clustering over one text field.
 *
 * <p>The Painless is a faithful, regex-free re-implementation of {@code
 * org.opensearch.sql.common.cluster.TextSimilarityClustering} so the shards and the coordinator use
 * the same feature contract (see {@code ClusterConfigFingerprint}):
 *
 * <ul>
 *   <li>tokenization replicates {@code String.split} semantics (delimiter runs collapse; a leading
 *       delimiter run yields a leading empty token that still advances the {@code termlist}
 *       position; trailing empty tokens are dropped) without using regex, which Painless disables by
 *       default;
 *   <li>numeric tokens ({@code ^\d+$}) normalize to {@code "*"};
 *   <li>{@code termlist} keys are {@code position + "-" + token}; {@code termset} keys are the bare
 *       token; {@code ngramset} keys are character trigrams (character frequency for values shorter
 *       than 3);
 *   <li>cosine similarity over the sparse integer vectors, with the empty-text short-circuits of
 *       {@code computeSimilarity} (empty vs empty = 1.0, empty vs non-empty = 0.0);
 *   <li>greedy assignment joins the best cluster when {@code similarity >= threshold - 1e-9},
 *       otherwise creates a new cluster whose representative is never updated.
 * </ul>
 *
 * <p>The map script reads {@code params._source} so it can both cluster on the text field and carry
 * the representative's full row (needed for {@code labelonly=false} output). The reduce script is a
 * pass-through ({@code return states}) so the coordinator receives the per-shard combine outputs
 * and performs the canonical-order merge in {@link
 * org.opensearch.sql.opensearch.response.agg.ClusterScriptedMetricParser}.
 */
public final class ClusterScriptedMetricFactory {

  private ClusterScriptedMetricFactory() {}

  private static final String INIT_SCRIPT = "state.clusters = new ArrayList();";

  private static final String MAP_SCRIPT =
      """
      boolean isDelimChar(char ch, String delims, boolean nonAlnum) {
        if (nonAlnum) {
          int cc = (int) ch;
          boolean alnum = (cc >= 48 && cc <= 57)   // 0-9
              || (cc >= 65 && cc <= 90)             // A-Z
              || (cc >= 97 && cc <= 122)            // a-z
              || cc == 95;                          // _
          return !alnum;
        }
        return delims.indexOf(String.valueOf(ch)) >= 0;
      }
      List tokenize(String value, String delims) {
        boolean nonAlnum = delims.equals("non-alphanumeric");
        ArrayList segs = new ArrayList();
        StringBuilder cur = new StringBuilder();
        int i = 0; int n = value.length();
        while (i < n) {
          char ch = value.charAt(i);
          if (isDelimChar(ch, delims, nonAlnum)) {
            segs.add(cur.toString()); cur.setLength(0);
            while (i < n && isDelimChar(value.charAt(i), delims, nonAlnum)) { i++; }
          } else {
            cur.append(value.substring(i, i + 1)); i++;
          }
        }
        segs.add(cur.toString());
        while (segs.size() > 0 && ((String)segs.get(segs.size() - 1)).length() == 0) {
          segs.remove(segs.size() - 1);
        }
        return segs;
      }
      String normalizeToken(String token) {
        if (token.length() == 0) { return token; }
        boolean allDigits = true;
        for (int i = 0; i < token.length(); i++) {
          int cc = (int) token.charAt(i);
          if (cc < 48 || cc > 57) { allDigits = false; break; }
        }
        return allDigits ? "*" : token;
      }
      void inc(Map v, String key) {
        if (v.containsKey(key)) { v.put(key, ((int)v.get(key)) + 1); }
        else { v.put(key, 1); }
      }
      Map vectorize(String value, String match, String delims) {
        Map v = new HashMap();
        if (match.equals("ngramset")) {
          if (value.length() < 3) {
            for (int i = 0; i < value.length(); i++) { inc(v, value.substring(i, i + 1)); }
            return v;
          }
          for (int i = 0; i <= value.length() - 3; i++) { inc(v, value.substring(i, i + 3)); }
          return v;
        }
        List toks = tokenize(value, delims);
        if (match.equals("termset")) {
          for (int i = 0; i < toks.size(); i++) {
            String t = (String)toks.get(i);
            if (t.length() > 0) { inc(v, normalizeToken(t)); }
          }
        } else {
          for (int i = 0; i < toks.size(); i++) {
            String t = (String)toks.get(i);
            if (t.length() > 0) { inc(v, "" + i + "-" + normalizeToken(t)); }
          }
        }
        return v;
      }
      double cosine(Map a, Map b) {
        double dot = 0.0;
        for (def e : a.entrySet()) {
          if (b.containsKey(e.getKey())) { dot += ((int)e.getValue()) * ((int)b.get(e.getKey())); }
        }
        double na = 0.0;
        for (def val : a.values()) { na += ((int)val) * ((int)val); }
        double nb = 0.0;
        for (def val : b.values()) { nb += ((int)val) * ((int)val); }
        if (na == 0.0 || nb == 0.0) { return 0.0; }
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
      }

      def src = params._source;
      String field = params.field;
      def raw = (src == null) ? null : src.get(field);
      String text = (raw == null) ? "" : String.valueOf(raw);
      Map vec = vectorize(text, params.match, params.delims);
      double bestSim = -1.0;
      def best = null;
      double t = (double) params.threshold;
      for (def c : state.clusters) {
        String rep = (String) c.get("rep");
        double sim;
        boolean textEmpty = (text.length() == 0);
        boolean repEmpty = (rep.length() == 0);
        if (textEmpty && repEmpty) { sim = 1.0; }
        else if (textEmpty || repEmpty) { sim = 0.0; }
        else { sim = cosine(vec, (Map) c.get("vec")); }
        if (sim > bestSim) { bestSim = sim; best = c; }
      }
      if (best != null && bestSim >= t - 1e-9) {
        best.put("count", ((long) best.get("count")) + 1L);
      } else {
        Map cluster = new HashMap();
        cluster.put("ord", state.clusters.size());
        cluster.put("rep", text);
        cluster.put("row", src);
        cluster.put("count", 1L);
        cluster.put("vec", vec);
        state.clusters.add(cluster);
      }
      """;

  private static final String COMBINE_SCRIPT =
      """
      Map out = new HashMap();
      ArrayList clusters = new ArrayList();
      String shardKey = "";
      for (def c : state.clusters) {
        Map o = new HashMap();
        o.put("ord", c.get("ord"));
        o.put("rep", c.get("rep"));
        o.put("row", c.get("row"));
        o.put("count", c.get("count"));
        clusters.add(o);
        if (((int) c.get("ord")) == 0) { shardKey = (String) c.get("rep"); }
      }
      out.put("shardKey", shardKey);
      out.put("clusters", clusters);
      return out;
      """;

  // Pass-through: the coordinator merge is performed in Java by ClusterScriptedMetricParser, which
  // needs the raw per-shard combine states rather than a single reduced value.
  private static final String REDUCE_SCRIPT = "return states;";

  /**
   * Build the scripted_metric aggregation for shard-local clustering.
   *
   * @param aggName aggregation name (must equal the parser's name)
   * @param field source field root name (read via {@code params._source})
   * @param threshold similarity threshold in (0, 1)
   * @param matchMode {@code termlist} | {@code termset} | {@code ngramset} (lower-case)
   * @param delims delimiter policy ({@code non-alphanumeric} or an explicit delimiter set)
   */
  public static ScriptedMetricAggregationBuilder build(
      String aggName, String field, double threshold, String matchMode, String delims) {
    Map<String, Object> params = new HashMap<>();
    params.put("field", field);
    params.put("threshold", threshold);
    params.put("match", matchMode);
    params.put("delims", delims);
    return AggregationBuilders.scriptedMetric(aggName)
        .params(params)
        .initScript(new Script(INIT_SCRIPT))
        .mapScript(new Script(MAP_SCRIPT))
        .combineScript(new Script(COMBINE_SCRIPT))
        .reduceScript(new Script(REDUCE_SCRIPT));
  }
}
