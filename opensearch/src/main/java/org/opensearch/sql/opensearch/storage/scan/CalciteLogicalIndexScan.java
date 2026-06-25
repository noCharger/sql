/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.ast.tree.HighlightConfig;
import org.opensearch.sql.calcite.plan.DynamicFieldsConstants;
import org.opensearch.sql.calcite.plan.HighlightPushDown;
import org.opensearch.sql.calcite.plan.SchemaOnReadPushDown;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PPLHintUtils;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.HighlightExpression;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.planner.rules.OpenSearchIndexRules;
import org.opensearch.sql.opensearch.request.AggregateAnalyzer;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.QueryExpression;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.context.AbstractAction;
import org.opensearch.sql.opensearch.storage.scan.context.AggSpec;
import org.opensearch.sql.opensearch.storage.scan.context.FilterDigest;
import org.opensearch.sql.opensearch.storage.scan.context.LimitDigest;
import org.opensearch.sql.opensearch.storage.scan.context.OSRequestBuilderAction;
import org.opensearch.sql.opensearch.storage.scan.context.ProjectDigest;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownType;
import org.opensearch.sql.opensearch.storage.scan.context.RareTopDigest;
import org.opensearch.sql.utils.Utils;

/** The logical relational operator representing a scan of an OpenSearchIndex type. */
@Getter
public class CalciteLogicalIndexScan extends AbstractCalciteIndexScan
    implements HighlightPushDown, SchemaOnReadPushDown {
  private static final Logger LOG = LogManager.getLogger(CalciteLogicalIndexScan.class);

  public CalciteLogicalIndexScan(
      RelOptCluster cluster, RelOptTable table, OpenSearchIndex osIndex) {
    this(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        ImmutableList.of(),
        table,
        osIndex,
        table.getRowType(),
        new PushDownContext(osIndex));
  }

  /**
   * Append the schema-on-read catch-all column to this scan. A single map column named {@value
   * org.opensearch.sql.calcite.plan.DynamicFieldsConstants#DYNAMIC_FIELDS_MAP} of type {@code
   * MAP<VARCHAR, VARCHAR>} is added to the scan's row type so that any field reference absent from
   * the index mapping can be resolved to {@code ITEM(_MAP, '<field>')} downstream by {@link
   * org.opensearch.sql.calcite.QualifiedNameResolver}. VARCHAR values mirror {@code
   * spath}/{@code JSON_EXTRACT_ALL} (RFC #4984 Step 1's "treat as STRING"), so the existing
   * implicit VARCHAR→numeric coercion handles arithmetic and aggregations. Structurally mirrors
   * {@link #pushDownHighlight}.
   *
   * <p>This is applied <b>unconditionally</b> whenever schema-on-read is enabled, independent of
   * which fields the query references. That makes dynamic-field support command-agnostic: the
   * resolver fallback is self-gating on the presence of {@code _MAP}, so every command (current and
   * future) resolves unmapped references for free, with no per-command pre-pass. The targeted
   * {@code _source} fetch (RFC #4984 Step 5) is recovered separately by a generic plan-layer
   * collector that scans the built logical plan for {@code ITEM(_MAP, ...)} references; until that
   * runs, {@link org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder} safely falls back
   * to fetching the full {@code _source}.
   *
   * @return a new scan whose schema carries the {@code _MAP} column, or {@code this} when the column
   *     is already present (idempotent)
   */
  @Override
  public CalciteLogicalIndexScan applyDynamicFieldsColumn() {
    if (getRowType().getField(DynamicFieldsConstants.DYNAMIC_FIELDS_MAP, true, false) != null) {
      return this;
    }
    RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    RelDataType mapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.VARCHAR));
    RelDataTypeFactory.Builder schemaBuilder = typeFactory.builder();
    schemaBuilder.addAll(getRowType().getFieldList());
    schemaBuilder.add(DynamicFieldsConstants.DYNAMIC_FIELDS_MAP, mapType);
    return copyWithNewSchema(schemaBuilder.build());
  }

  /**
   * RFC #4984 Step 5: restrict the read-time {@code _source} fetch to the given dynamic field
   * names, collected generically from the built logical plan by {@link
   * org.opensearch.sql.calcite.plan.DynamicFieldSourceCollector}. No-op when this scan does not
   * carry the {@code _MAP} column (so it never narrows a plain scan) or when {@code includes} is
   * empty (request builder then fetches the full {@code _source}).
   */
  @Override
  public void applyDynamicFieldSourceIncludes(java.util.Set<String> includes) {
    if (includes == null
        || includes.isEmpty()
        || getRowType().getField(DynamicFieldsConstants.DYNAMIC_FIELDS_MAP, true, false) == null) {
      return;
    }
    getPushDownContext().setDynamicFieldsSourceIncludes(includes);
  }

  public RelNode pushDownHighlight(HighlightConfig highlightConfig) {
    RelDataTypeFactory.Builder schemaBuilder = getCluster().getTypeFactory().builder();
    schemaBuilder.addAll(getRowType().getFieldList());
    schemaBuilder.add(
        HighlightExpression.HIGHLIGHT_FIELD,
        getCluster().getTypeFactory().createSqlType(SqlTypeName.ANY));
    CalciteLogicalIndexScan newScan = copyWithNewSchema(schemaBuilder.build());
    newScan
        .getPushDownContext()
        .add(
            PushDownType.HIGHLIGHT,
            highlightConfig.fieldNames(),
            (OSRequestBuilderAction)
                requestBuilder -> applyHighlightPushDown(requestBuilder, highlightConfig));
    return newScan;
  }

  protected CalciteLogicalIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    super(cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
  }

  @Override
  protected AbstractCalciteIndexScan buildScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    return new CalciteLogicalIndexScan(
        cluster, traitSet, hints, table, osIndex, schema, pushDownContext);
  }

  @Override
  public CalciteLogicalIndexScan copy() {
    return new CalciteLogicalIndexScan(
        getCluster(), traitSet, hints, table, osIndex, schema, pushDownContext.clone());
  }

  public CalciteLogicalIndexScan copyWithNewSchema(RelDataType schema) {
    // Do shallow copy for requestBuilder, thus requestBuilder among different plans produced in the
    // optimization process won't affect each other.
    return new CalciteLogicalIndexScan(
        getCluster(), traitSet, hints, table, osIndex, schema, pushDownContext.clone());
  }

  public CalciteLogicalIndexScan copyWithNewTraitSet(RelTraitSet traitSet) {
    return new CalciteLogicalIndexScan(
        getCluster(), traitSet, hints, table, osIndex, schema, pushDownContext.clone());
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    for (RelOptRule rule : OpenSearchIndexRules.OPEN_SEARCH_NON_PUSHDOWN_RULES) {
      planner.addRule(rule);
    }
    if ((Boolean) osIndex.getSettings().getSettingValue(Settings.Key.CALCITE_PUSHDOWN_ENABLED)) {
      for (RelOptRule rule : OpenSearchIndexRules.OPEN_SEARCH_PUSHDOWN_RULES) {
        planner.addRule(rule);
      }
    }
  }

  public AbstractRelNode pushDownFilter(Filter filter) {
    // Schema-on-read (RFC #4984): a filter that references the synthetic _MAP column cannot be
    // translated to an OpenSearch DSL query (the column does not exist in the index). Returning
    // null leaves the filter above the scan so Calcite evaluates it against the materialized _MAP.
    if (referencesDynamicFieldsMap(filter.getCondition())) {
      return null;
    }
    try {
      RelDataType rowType = this.getRowType();
      List<String> schema = buildSchema();
      Map<String, ExprType> fieldTypes = this.osIndex.getAllFieldTypes();
      QueryExpression queryExpression =
          PredicateAnalyzer.analyzeExpression(
              filter.getCondition(), schema, fieldTypes, rowType, getCluster());
      // TODO: handle the case where condition contains a score function
      CalciteLogicalIndexScan newScan = this.copy();
      newScan.pushDownContext.add(
          queryExpression.getScriptCount() > 0 ? PushDownType.SCRIPT : PushDownType.FILTER,
          new FilterDigest(
              queryExpression.getScriptCount(),
              queryExpression.isPartial()
                  ? constructCondition(
                      queryExpression.getAnalyzedNodes(), getCluster().getRexBuilder())
                  : filter.getCondition()),
          (OSRequestBuilderAction)
              requestBuilder -> requestBuilder.pushDownFilterForCalcite(queryExpression.builder()));

      // If the query expression is partial, we need to replace the input of the filter with the
      // partial pushed scan and the filter condition with non-pushed-down conditions.
      if (queryExpression.isPartial()) {
        // Only CompoundQueryExpression could be partial.
        List<RexNode> conditions = queryExpression.getUnAnalyzableNodes();
        RexNode newCondition = constructCondition(conditions, getCluster().getRexBuilder());
        return filter.copy(filter.getTraitSet(), newScan, newCondition);
      }
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the filter condition.", e);
      }
    }
    return null;
  }

  /** Whether {@code condition} references the schema-on-read {@code _MAP} catch-all column. */
  private boolean referencesDynamicFieldsMap(RexNode condition) {
    int mapIndex = getRowType().getFieldNames().indexOf(DynamicFieldsConstants.DYNAMIC_FIELDS_MAP);
    if (mapIndex < 0) {
      return false;
    }
    return RelOptUtil.InputFinder.bits(condition).get(mapIndex);
  }

  /**
   * Whether an aggregate (optionally with an intermediate project) reads the schema-on-read {@code
   * _MAP} column of this scan. When a {@code project} sits between the scan and the aggregate, the
   * aggregate addresses the project's outputs, so it is enough to inspect the project's
   * expressions; otherwise the aggregate's group set and call arguments index the scan directly.
   */
  private boolean aggregateReferencesDynamicFieldsMap(
      Aggregate aggregate, @Nullable Project project) {
    int mapIndex = getRowType().getFieldNames().indexOf(DynamicFieldsConstants.DYNAMIC_FIELDS_MAP);
    if (mapIndex < 0) {
      return false;
    }
    if (project != null) {
      return project.getProjects().stream()
          .anyMatch(expr -> RelOptUtil.InputFinder.bits(expr).get(mapIndex));
    }
    if (aggregate.getGroupSet().get(mapIndex)) {
      return true;
    }
    return aggregate.getAggCallList().stream()
        .anyMatch(call -> call.getArgList().contains(mapIndex));
  }

  /**
   * Build schema for the current scan. Schema is the combination of all index fields and nested
   * fields in index fields.
   *
   * @return All current outputs fields plus nested paths.
   */
  private List<String> buildSchema() {
    List<String> schema = new ArrayList<>(this.getRowType().getFieldNames());
    // Add nested paths to schema if it has
    List<String> nestedPaths =
        schema.stream()
            .map(field -> Utils.resolveNestedPath(field, this.osIndex.getAllFieldTypes()))
            .filter(Objects::nonNull)
            .distinct()
            .toList();
    schema.addAll(nestedPaths);
    return schema;
  }

  private static RexNode constructCondition(List<RexNode> conditions, RexBuilder rexBuilder) {
    return conditions.size() > 1
        ? rexBuilder.makeCall(SqlStdOperatorTable.AND, conditions)
        : conditions.get(0);
  }

  public CalciteLogicalIndexScan pushDownCollapse(Project finalOutput, String fieldName) {
    ExprType fieldType = osIndex.getFieldTypes().get(fieldName);
    if (fieldType == null) {
      // the fieldName must be one of index fields
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the dedup '{}' due to it is not a index field", fieldName);
      }
      return null;
    }
    ExprType originalExprType = fieldType.getOriginalExprType();
    String originalFieldName = originalExprType.getOriginalPath().orElse(fieldName);
    if (!ExprCoreType.numberTypes().contains(originalExprType)
        && !originalExprType.legacyTypeName().equals("KEYWORD")
        && !originalExprType.legacyTypeName().equals("TEXT")) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Cannot pushdown the dedup '{}' due to only keyword and number type are accepted, but"
                + " its type is {}",
            originalFieldName,
            originalExprType.legacyTypeName());
      }
      return null;
    }
    // For text, use its subfield if exists.
    String field = OpenSearchTextType.toKeywordSubField(originalFieldName, fieldType);
    if (field == null) {
      LOG.debug("Cannot pushdown the dedup due to no keyword subfield for {}.", fieldName);
      return null;
    }
    CalciteLogicalIndexScan newScan = this.copyWithNewSchema(finalOutput.getRowType());
    newScan.pushDownContext.add(
        PushDownType.AGGREGATION,
        fieldName,
        (OSRequestBuilderAction) requestBuilder -> requestBuilder.pushDownCollapse(field));
    return newScan;
  }

  /**
   * When pushing down a project, we need to create a new CalciteLogicalIndexScan with the updated
   * schema since we cannot override getRowType() which is defined to be final.
   */
  public CalciteLogicalIndexScan pushDownProject(List<Integer> selectedColumns) {
    final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
    final List<RelDataTypeField> fieldList = this.getRowType().getFieldList();
    for (int project : selectedColumns) {
      builder.add(fieldList.get(project));
    }
    RelDataType newSchema = builder.build();

    // To prevent circular pushdown for some edge cases where plan has pattern(see TPCH Q1):
    // `Project($1, $0) -> Sort(sort0=[1]) -> Project($1, $0) -> Aggregate(group={0},...)...`
    // We don't support pushing down the duplicated project here otherwise it will cause dead-loop.
    // `ProjectMergeRule` will help merge the duplicated projects.
    if (this.getPushDownContext().containsDigest(newSchema.getFieldNames())) return null;

    // Projection may alter indicies in the collations.
    // E.g. When sorting age
    // `Project(age) - TableScan(schema=[name, age], collation=[$1 ASC])` should become
    // `TableScan(schema=[age], collation=[$0 ASC])` after pushing down project.
    RelTraitSet traitSetWithReIndexedCollations = reIndexCollations(selectedColumns);

    CalciteLogicalIndexScan newScan =
        new CalciteLogicalIndexScan(
            getCluster(),
            traitSetWithReIndexedCollations,
            hints,
            table,
            osIndex,
            newSchema,
            pushDownContext.clone());

    AbstractAction<?> action;
    if (pushDownContext.isAggregatePushed()) {
      // For aggregate, we do nothing on query builder but only change the schema of the scan.
      action = (OSRequestBuilderAction) requestBuilder -> {};
    } else {
      action =
          (OSRequestBuilderAction)
              requestBuilder ->
                  requestBuilder.pushDownProjectStream(
                      newSchema.getFieldNames().stream()
                          .filter(f -> !HighlightExpression.HIGHLIGHT_FIELD.equals(f)));
    }
    newScan.pushDownContext.add(
        PushDownType.PROJECT,
        new ProjectDigest(newSchema.getFieldNames(), selectedColumns),
        action);
    return newScan;
  }

  private RelTraitSet reIndexCollations(List<Integer> selectedColumns) {
    RelTraitSet newTraitSet;
    RelCollation relCollation = getTraitSet().getCollation();
    if (!Objects.isNull(relCollation) && !relCollation.getFieldCollations().isEmpty()) {
      List<RelFieldCollation> newCollations =
          relCollation.getFieldCollations().stream()
              .filter(collation -> selectedColumns.contains(collation.getFieldIndex()))
              .map(
                  collation ->
                      collation.withFieldIndex(selectedColumns.indexOf(collation.getFieldIndex())))
              .collect(Collectors.toList());
      newTraitSet = getTraitSet().plus(RelCollations.of(newCollations));
    } else {
      newTraitSet = getTraitSet();
    }
    return newTraitSet;
  }

  public CalciteLogicalIndexScan pushDownSortAggregateMeasure(Sort sort) {
    try {
      AggSpec aggSpec = pushDownContext.getAggSpec();
      if (aggSpec == null || !aggSpec.isCompositeAggregation()) {
        return null;
      }
      List<String> collationNames = getCollationNames(sort.getCollation().getFieldCollations());
      if (!isAnyCollationNameInAggregators(collationNames)) {
        return null;
      }
      CalciteLogicalIndexScan newScan = copyWithNewTraitSet(sort.getTraitSet());
      newScan.pushDownContext.setAggSpec(
          aggSpec.withSortMeasure(
              sort.getCollation().getFieldCollations(), rowType.getFieldNames()));
      AbstractAction<?> action =
          (OSRequestBuilderAction) requestAction -> requestAction.resetRequestTotal();
      Object digest = sort.getCollation().getFieldCollations();
      newScan.pushDownContext.add(PushDownType.SORT_AGG_METRICS, digest, action);
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the sort aggregate {}", sort, e);
      }
    }
    return null;
  }

  public CalciteLogicalIndexScan pushDownRareTop(Project project, RareTopDigest digest) {
    try {
      CalciteLogicalIndexScan newScan = copyWithNewSchema(project.getRowType());
      newScan.pushDownContext.setAggSpec(pushDownContext.getAggSpec().withRareTop(digest));
      AbstractAction<?> action =
          (OSRequestBuilderAction) requestAction -> requestAction.resetRequestTotal();
      newScan.pushDownContext.add(PushDownType.RARE_TOP, digest, action);
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown {}", digest, e);
      }
      return null;
    }
  }

  public AbstractRelNode pushDownAggregate(Aggregate aggregate, @Nullable Project project) {
    // Schema-on-read (RFC #4984): an aggregation that reads the synthetic _MAP column cannot be
    // translated to an OpenSearch aggregation (the column does not exist in the index). Returning
    // null keeps the aggregate above the scan so Calcite evaluates it on the materialized _MAP.
    if (aggregateReferencesDynamicFieldsMap(aggregate, project)) {
      return null;
    }
    try {
      CalciteLogicalIndexScan newScan =
          new CalciteLogicalIndexScan(
              getCluster(),
              traitSet,
              hints,
              table,
              osIndex,
              aggregate.getRowType(),
              pushDownContext.cloneForAggregate(aggregate, project));
      List<String> schema = buildSchema();
      Map<String, ExprType> fieldTypes =
          this.osIndex.getAllFieldTypes().entrySet().stream()
              .filter(entry -> schema.contains(entry.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      List<String> outputFields = aggregate.getRowType().getFieldNames();
      List<String> bucketNames = outputFields.subList(0, aggregate.getGroupSet().cardinality());
      if (bucketNames.stream()
          .map(b -> fieldTypes.get(b))
          .filter(Objects::nonNull)
          .anyMatch(expr -> expr.getOriginalType() == ExprCoreType.ARRAY)) {
        // TODO https://github.com/opensearch-project/sql/issues/5006
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cannot pushdown the aggregate due to bucket contains array (nested) type");
        }
        return null;
      }
      int queryBucketSize = osIndex.getQueryBucketSize();
      boolean bucketNullable = !PPLHintUtils.ignoreNullBucket(aggregate);
      AggregateAnalyzer.AggregateBuilderHelper helper =
          new AggregateAnalyzer.AggregateBuilderHelper(
              getRowType(), fieldTypes, getCluster(), bucketNullable, queryBucketSize);
      final Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> builderAndParser =
          AggregateAnalyzer.analyze(aggregate, project, outputFields, helper);
      Map<String, OpenSearchDataType> extendedTypeMapping =
          aggregate.getRowType().getFieldList().stream()
              .collect(
                  Collectors.toMap(
                      RelDataTypeField::getName,
                      field ->
                          OpenSearchDataType.of(
                              OpenSearchTypeFactory.convertRelDataTypeToExprType(
                                  field.getType()))));
      AggSpec aggSpec = AggSpec.create(extendedTypeMapping, bucketNames, builderAndParser);
      // AggPushDownAction is lazily materialized by AggSpec.buildAction() and then this action
      // will materialize agg request builder.
      // The AGGREGATION pushdown operation in PushDownContext remains a no-op marker here.
      newScan.pushDownContext.setAggSpec(aggSpec);
      newScan.pushDownContext.add(
          PushDownType.AGGREGATION, aggregate, (OSRequestBuilderAction) requestBuilder -> {});
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the aggregate {}", aggregate, e);
      }
    }
    return null;
  }

  public AbstractRelNode pushDownLimit(LogicalSort sort, Integer limit, Integer offset) {
    try {
      if (pushDownContext.isAggregatePushed()) {
        int totalSize = limit + offset;
        AggSpec aggSpec = pushDownContext.getAggSpec();
        boolean canReduceEstimatedRowsCount =
            !pushDownContext.isLimitPushed()
                || pushDownContext.getQueue().reversed().stream()
                    .takeWhile(op -> op.type() != PushDownType.AGGREGATION)
                    .filter(op -> op.type() == PushDownType.LIMIT)
                    .findFirst()
                    .map(op -> (LimitDigest) op.digest())
                    .map(d -> totalSize < d.offset() + d.limit())
                    .orElse(true);
        boolean canUpdate =
            canReduceEstimatedRowsCount || aggSpec.canPushDownLimitIntoBucketSize(totalSize);
        if (!canUpdate && offset > 0) return null;
        CalciteLogicalIndexScan newScan = this.copyWithNewSchema(getRowType());
        if (canUpdate) {
          newScan.pushDownContext.setAggSpec(aggSpec.withLimit(limit + offset));
        }
        AbstractAction action;
        if (newScan.pushDownContext.getAggSpec().isCompositeAggregation()) {
          action =
              (OSRequestBuilderAction)
                  requestBuilder -> requestBuilder.pushDownLimitToRequestTotal(limit, offset);
        } else {
          action = (OSRequestBuilderAction) requestBuilder -> {};
        }
        newScan.pushDownContext.add(PushDownType.LIMIT, new LimitDigest(limit, offset), action);
        return offset > 0 ? sort.copy(sort.getTraitSet(), List.of(newScan)) : newScan;
      } else {
        LimitDigest digest = new LimitDigest(limit, offset);
        if (pushDownContext.containsDigestOnTop(digest)) {
          // avoid stack overflow
          return null;
        }
        CalciteLogicalIndexScan newScan = this.copyWithNewSchema(getRowType());
        newScan.pushDownContext.add(
            PushDownType.LIMIT,
            digest,
            (OSRequestBuilderAction) requestBuilder -> requestBuilder.pushDownLimit(limit, offset));
        return newScan;
      }
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown limit {} with offset {}", limit, offset, e);
      }
    }
    return null;
  }
}
