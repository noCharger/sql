/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.calcite.plan.rule.PPLClusterConvertRule;

public class CalcitePPLClusterTest extends CalcitePPLAbstractTest {

  public CalcitePPLClusterTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  /**
   * {@link #getRelNode} returns the raw visitor output, which now tops out at {@link
   * org.opensearch.sql.calcite.plan.rel.LogicalCluster}. The in-process executable plan (the
   * buffered-window lowering) is produced by {@link PPLClusterConvertRule} during optimization.
   * SparkSQL assertions apply the convert rule first so the window plan is what gets rendered — this
   * verifies the convert rule reproduces the exact same window plan end-to-end.
   */
  private RelNode lowerCluster(RelNode root) {
    HepProgram program =
        new HepProgramBuilder()
            .addRuleInstance(PPLClusterConvertRule.CLUSTER_CONVERT_RULE)
            .build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(root);
    return planner.findBestExp();
  }

  @Test
  public void testBasicCluster() {
    String ppl = "source=EMP | cluster ENAME";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalCluster(source=[$1], threshold=[0.8], match=[termlist],"
            + " delims=[non-alphanumeric], labelField=[cluster_label], showCount=[false],"
            + " labelOnly=[false])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`, ROW_NUMBER() OVER (PARTITION BY `cluster_label`)"
            + " `_cluster_convergence_row_num`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `_cluster_labels_array`[CAST(ROW_NUMBER() OVER () AS INTEGER)] `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`(`ENAME`, 8E-1, 'termlist', 'non-alphanumeric', 50000, 10000) OVER"
            + " (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `_cluster_labels_array`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `ENAME` IS NOT NULL) `t0`) `t1`) `t2`\n"
            + "WHERE `_cluster_convergence_row_num` = 1";
    verifyPPLToSparkSQL(lowerCluster(root), expectedSparkSql);
  }

  @Test
  public void testClusterWithThreshold() {
    String ppl = "source=EMP | cluster ENAME t=0.8";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalCluster(source=[$1], threshold=[0.8], match=[termlist],"
            + " delims=[non-alphanumeric], labelField=[cluster_label], showCount=[false],"
            + " labelOnly=[false])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testClusterWithTermsetMatch() {
    String ppl = "source=EMP | cluster ENAME match=termset";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalCluster(source=[$1], threshold=[0.8], match=[termset],"
            + " delims=[non-alphanumeric], labelField=[cluster_label], showCount=[false],"
            + " labelOnly=[false])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testClusterWithNgramsetMatch() {
    String ppl = "source=EMP | cluster ENAME match=ngramset";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalCluster(source=[$1], threshold=[0.8], match=[ngramset],"
            + " delims=[non-alphanumeric], labelField=[cluster_label], showCount=[false],"
            + " labelOnly=[false])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testClusterWithCustomFields() {
    String ppl = "source=EMP | cluster ENAME labelfield=my_cluster countfield=my_count";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalCluster(source=[$1], threshold=[0.8], match=[termlist],"
            + " delims=[non-alphanumeric], labelField=[my_cluster], showCount=[false],"
            + " labelOnly=[false])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testClusterWithAllParameters() {
    String ppl =
        "source=EMP | cluster ENAME t=0.7 match=termset labelfield=cluster_id"
            + " countfield=cluster_size showcount=true labelonly=false delims=' '";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalCluster(source=[$1], threshold=[0.7], match=[termset], delims=[ ],"
            + " labelField=[cluster_id], countField=[cluster_size], showCount=[true],"
            + " labelOnly=[false])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testClusterOnDifferentField() {
    String ppl = "source=EMP | cluster JOB";
    RelNode root = getRelNode(ppl);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`, ROW_NUMBER() OVER (PARTITION BY `cluster_label`)"
            + " `_cluster_convergence_row_num`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `_cluster_labels_array`[CAST(ROW_NUMBER() OVER () AS INTEGER)] `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`(`JOB`, 8E-1, 'termlist', 'non-alphanumeric', 50000, 10000) OVER"
            + " (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `_cluster_labels_array`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `JOB` IS NOT NULL) `t0`) `t1`) `t2`\n"
            + "WHERE `_cluster_convergence_row_num` = 1";
    verifyPPLToSparkSQL(lowerCluster(root), expectedSparkSql);
  }

  @Test
  public void testClusterLabelOnly() {
    String ppl = "source=EMP | cluster ENAME labelonly=true";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalCluster(source=[$1], threshold=[0.8], match=[termlist],"
            + " delims=[non-alphanumeric], labelField=[cluster_label], showCount=[false],"
            + " labelOnly=[true])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `_cluster_labels_array`[CAST(ROW_NUMBER() OVER () AS INTEGER)] `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`(`ENAME`, 8E-1, 'termlist', 'non-alphanumeric', 50000, 10000) OVER"
            + " (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `_cluster_labels_array`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `ENAME` IS NOT NULL) `t0`";
    verifyPPLToSparkSQL(lowerCluster(root), expectedSparkSql);
  }

  @Test
  public void testClusterLabelOnlyWithShowCount() {
    String ppl = "source=EMP | cluster ENAME labelonly=true showcount=true";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalCluster(source=[$1], threshold=[0.8], match=[termlist],"
            + " delims=[non-alphanumeric], labelField=[cluster_label], countField=[cluster_count],"
            + " showCount=[true], labelOnly=[true])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`, COUNT(*) OVER (PARTITION BY `cluster_label` RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) `cluster_count`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `_cluster_labels_array`[CAST(ROW_NUMBER() OVER () AS INTEGER)] `cluster_label`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `cluster_label`(`ENAME`, 8E-1, 'termlist', 'non-alphanumeric', 50000, 10000) OVER"
            + " (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `_cluster_labels_array`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `ENAME` IS NOT NULL) `t0`) `t1`";
    verifyPPLToSparkSQL(lowerCluster(root), expectedSparkSql);
  }
}
