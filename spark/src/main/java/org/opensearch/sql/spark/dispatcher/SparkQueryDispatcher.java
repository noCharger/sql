/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.AUTO_REFRESH;
import static org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions.INCREMENTAL_REFRESH;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.client.Client;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryActionType;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.utils.SQLQueryUtils;

/** This class takes care of understanding query and dispatching job query to emr serverless. */
@AllArgsConstructor
public class SparkQueryDispatcher {
  private static final Logger LOG = LogManager.getLogger();
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

  public static final String INDEX_TAG_KEY = "index";
  public static final String DATASOURCE_TAG_KEY = "datasource";
  public static final String CLUSTER_NAME_TAG_KEY = "domain_ident";
  public static final String JOB_TYPE_TAG_KEY = "type";

  private EMRServerlessClientFactory emrServerlessClientFactory;

  private DataSourceService dataSourceService;

  private DataSourceUserAuthorizationHelperImpl dataSourceUserAuthorizationHelper;

  private JobExecutionResponseReader jobExecutionResponseReader;

  private FlintIndexMetadataService flintIndexMetadataService;

  private Client client;

  private SessionManager sessionManager;

  private LeaseManager leaseManager;

  private StateStore stateStore;

  public DispatchQueryResponse dispatch(DispatchQueryRequest dispatchQueryRequest) {
    EMRServerlessClient emrServerlessClient = emrServerlessClientFactory.getClient();
    DataSourceMetadata dataSourceMetadata =
        this.dataSourceService.verifyDataSourceAccessAndGetRawMetadata(
            dispatchQueryRequest.getDatasource());
    AsyncQueryHandler asyncQueryHandler =
        sessionManager.isEnabled()
            ? new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader, leaseManager)
            : new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader, leaseManager);
    AsyncQueryId queryId = AsyncQueryId.newAsyncQueryId(dataSourceMetadata.getName());
    DispatchQueryContext.DispatchQueryContextBuilder contextBuilder =
        DispatchQueryContext.builder()
            .dataSourceMetadata(dataSourceMetadata)
            .tags(getDefaultTagsForJobSubmission(dispatchQueryRequest))
            .queryId(queryId);

    // override asyncQueryHandler with specific.
    if (LangType.SQL.equals(dispatchQueryRequest.getLangType())
        && SQLQueryUtils.isFlintExtensionQuery(dispatchQueryRequest.getQuery())) {
      IndexQueryDetails indexQueryDetails =
          SQLQueryUtils.extractIndexDetails(dispatchQueryRequest.getQuery());
      fillMissingDetails(dispatchQueryRequest, indexQueryDetails);
      contextBuilder.indexQueryDetails(indexQueryDetails);

      if (isEligibleForIndexDMLHandling(indexQueryDetails)) {
        asyncQueryHandler = createIndexDMLHandler(emrServerlessClient);
      } else if (IndexQueryActionType.CREATE.equals(indexQueryDetails.getIndexQueryActionType())
          && indexQueryDetails.getFlintIndexOptions().autoRefresh()) {
        String query = dispatchQueryRequest.getQuery();
        indexQueryDetails.getFlintIndexOptions().setOption(AUTO_REFRESH, "false");
        indexQueryDetails.getFlintIndexOptions().setOption(INCREMENTAL_REFRESH, "true");

        LOG.info("BatchQueryHandler - original query: " + query);
        query =
            query.replace(
                "auto_refresh = true", "auto_refresh = false, incremental_refresh = true");
        LOG.info("BatchQueryHandler - newQuery: " + query);

        asyncQueryHandler =
            new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader, leaseManager);

        AsyncQueryHandler refreshQueryHandler =
            new RefreshQueryHandler(
                emrServerlessClient,
                jobExecutionResponseReader,
                flintIndexMetadataService,
                stateStore,
                leaseManager);
        registerRefreshJob(
            queryId,
            indexQueryDetails,
            refreshQueryHandler,
            dispatchQueryRequest,
            contextBuilder.build());
      } else if (isEligibleForStreamingQuery(indexQueryDetails)) {
        asyncQueryHandler =
            new StreamingQueryHandler(
                emrServerlessClient, jobExecutionResponseReader, leaseManager);
      } else if (IndexQueryActionType.REFRESH.equals(indexQueryDetails.getIndexQueryActionType())) {
        // manual refresh should be handled by batch handler
        asyncQueryHandler =
            new RefreshQueryHandler(
                emrServerlessClient,
                jobExecutionResponseReader,
                flintIndexMetadataService,
                stateStore,
                leaseManager);
      }
    }
    return asyncQueryHandler.submit(dispatchQueryRequest, contextBuilder.build());
  }

  private boolean isEligibleForStreamingQuery(IndexQueryDetails indexQueryDetails) {
    Boolean isAlterQuery =
        IndexQueryActionType.ALTER.equals(indexQueryDetails.getIndexQueryActionType());
    return isAlterQuery;
  }

  private boolean isEligibleForIndexDMLHandling(IndexQueryDetails indexQueryDetails) {
    return IndexQueryActionType.DROP.equals(indexQueryDetails.getIndexQueryActionType())
        || IndexQueryActionType.VACUUM.equals(indexQueryDetails.getIndexQueryActionType())
        || (IndexQueryActionType.ALTER.equals(indexQueryDetails.getIndexQueryActionType())
            && (indexQueryDetails
                    .getFlintIndexOptions()
                    .getProvidedOptions()
                    .containsKey("auto_refresh")
                && !indexQueryDetails.getFlintIndexOptions().autoRefresh()));
  }

  public JSONObject getQueryResponse(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    EMRServerlessClient emrServerlessClient = emrServerlessClientFactory.getClient();
    if (asyncQueryJobMetadata.getSessionId() != null) {
      return new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader, leaseManager)
          .getQueryResponse(asyncQueryJobMetadata);
    } else if (IndexDMLHandler.isIndexDMLQuery(asyncQueryJobMetadata.getJobId())) {
      return createIndexDMLHandler(emrServerlessClient).getQueryResponse(asyncQueryJobMetadata);
    } else {
      return new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader, leaseManager)
          .getQueryResponse(asyncQueryJobMetadata);
    }
  }

  public String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    EMRServerlessClient emrServerlessClient = emrServerlessClientFactory.getClient();
    AsyncQueryHandler queryHandler;
    if (asyncQueryJobMetadata.getSessionId() != null) {
      queryHandler =
          new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader, leaseManager);
    } else if (IndexDMLHandler.isIndexDMLQuery(asyncQueryJobMetadata.getJobId())) {
      queryHandler = createIndexDMLHandler(emrServerlessClient);
    } else if (asyncQueryJobMetadata.getJobType() == JobType.BATCH) {
      queryHandler =
          new RefreshQueryHandler(
              emrServerlessClient,
              jobExecutionResponseReader,
              flintIndexMetadataService,
              stateStore,
              leaseManager);
    } else if (asyncQueryJobMetadata.getJobType() == JobType.STREAMING) {
      queryHandler =
          new StreamingQueryHandler(emrServerlessClient, jobExecutionResponseReader, leaseManager);
    } else {
      queryHandler =
          new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader, leaseManager);
    }
    return queryHandler.cancelJob(asyncQueryJobMetadata);
  }

  private IndexDMLHandler createIndexDMLHandler(EMRServerlessClient emrServerlessClient) {
    return new IndexDMLHandler(
        emrServerlessClient,
        jobExecutionResponseReader,
        flintIndexMetadataService,
        stateStore,
        client);
  }

  // TODO: Revisit this logic.
  // Currently, Spark if datasource is not provided in query.
  // Spark Assumes the datasource to be catalog.
  // This is required to handle drop index case properly when datasource name is not provided.
  private static void fillMissingDetails(
      DispatchQueryRequest dispatchQueryRequest, IndexQueryDetails indexQueryDetails) {
    if (indexQueryDetails.getFullyQualifiedTableName() != null
        && indexQueryDetails.getFullyQualifiedTableName().getDatasourceName() == null) {
      indexQueryDetails
          .getFullyQualifiedTableName()
          .setDatasourceName(dispatchQueryRequest.getDatasource());
    }
  }

  private static Map<String, String> getDefaultTagsForJobSubmission(
      DispatchQueryRequest dispatchQueryRequest) {
    Map<String, String> tags = new HashMap<>();
    tags.put(CLUSTER_NAME_TAG_KEY, dispatchQueryRequest.getClusterName());
    tags.put(DATASOURCE_TAG_KEY, dispatchQueryRequest.getDatasource());
    return tags;
  }

  private void registerRefreshJob(
      AsyncQueryId queryId,
      IndexQueryDetails indexQueryDetails,
      AsyncQueryHandler refreshQueryHandler,
      DispatchQueryRequest dispatchQueryRequest,
      DispatchQueryContext context) {
    String periodString =
        indexQueryDetails.getFlintIndexOptions().getOption("refresh_interval").get();
    long period = parsePeriodString(periodString);
    LOG.info("Refresh query period: " + period);

    String query = dispatchQueryRequest.getQuery();
    int indexOfOpeningParenthesis = query.indexOf("(");
    String prefix = query.substring(0, indexOfOpeningParenthesis);
    String modifiedPrefix = prefix.replaceFirst("CREATE", "REFRESH");
    LOG.info("Refresh query: " + modifiedPrefix);
    DispatchQueryRequest request =
        new DispatchQueryRequest(
            dispatchQueryRequest.getApplicationId(),
            modifiedPrefix,
            dispatchQueryRequest.getDatasource(),
            dispatchQueryRequest.getLangType(),
            dispatchQueryRequest.getExecutionRoleARN(),
            dispatchQueryRequest.getClusterName(),
            dispatchQueryRequest.getExtraSparkSubmitParams(),
            "");
    Runnable refreshJob =
        () -> {
          try {
            // Logic for refreshing the index
            LOG.info(
                "Executing scheduled refresh job for "
                    + queryId.getId()
                    + " at "
                    + System.currentTimeMillis());
            refreshQueryHandler.submit(request, context);
            // TODO: submit refresh job
          } catch (Exception e) {
            LOG.error("Error executing refresh job: " + e.getMessage());
          }
        };

    scheduler.scheduleAtFixedRate(refreshJob, period, period, TimeUnit.MINUTES);
  }

  private long parsePeriodString(String periodString) {
    if (periodString.startsWith("'") && periodString.endsWith("'")) {
      periodString = periodString.substring(1, periodString.length() - 1);
    }

    String[] parts = periodString.trim().split("\\s+");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid refresh interval format");
    }

    long value;
    try {
      value = Long.parseLong(parts[0]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid refresh interval value");
    }

    String unit = parts[1].toLowerCase();
    switch (unit) {
      case "minute":
      case "minutes":
        return value;
      case "second":
      case "seconds":
        return TimeUnit.SECONDS.toMinutes(value);
      case "hour":
      case "hours":
        return TimeUnit.HOURS.toMinutes(value);
      default:
        throw new IllegalArgumentException("Invalid refresh interval unit");
    }
  }
}
