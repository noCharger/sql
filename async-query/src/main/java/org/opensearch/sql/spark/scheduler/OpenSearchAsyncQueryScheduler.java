/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;
import org.opensearch.sql.datasources.storage.OpenSearchDataSourceMetadataStorage;
import org.opensearch.sql.spark.scheduler.job.OpenSearchRefreshIndexJob;
import org.opensearch.sql.spark.scheduler.job.OpenSearchRefreshIndexJobRequest;
import org.opensearch.threadpool.ThreadPool;

public class OpenSearchAsyncQueryScheduler implements AsyncQueryScheduler {
  public static final String SCHEDULER_INDEX_NAME = ".async-query-scheduler";
  public static final String SCHEDULER_PLUGIN_JOB_TYPE = "async-query-scheduler";
  private static final String SCHEDULER_INDEX_MAPPING_FILE_NAME =
      "async-query-scheduler-index-mapping.yml";
  private static final Logger LOG = LogManager.getLogger();

  private final Client client;
  private final ClusterService clusterService;
  private final ThreadPool threadPool;

  /**
   * This class implements DataSourceMetadataStorage interface using OpenSearch as underlying
   * storage.
   *
   * @param client opensearch NodeClient.
   * @param clusterService ClusterService.
   */
  public OpenSearchAsyncQueryScheduler(
      Client client, ClusterService clusterService, ThreadPool threadPool) {
    this.client = client;
    this.clusterService = clusterService;
    this.threadPool = threadPool;
  }

  public ScheduledJobRunner getJobRunner() {
    OpenSearchRefreshIndexJob openSearchRefreshIndexJob =
        OpenSearchRefreshIndexJob.getJobRunnerInstance();
    openSearchRefreshIndexJob.setClusterService(clusterService);
    openSearchRefreshIndexJob.setThreadPool(threadPool);
    openSearchRefreshIndexJob.setClient(client);
    return openSearchRefreshIndexJob;
  }

  public ScheduledJobParser getJobParser() {
    return (parser, id, jobDocVersion) -> {
      OpenSearchRefreshIndexJobRequest.Builder builder =
          new OpenSearchRefreshIndexJobRequest.Builder();
      XContentParserUtils.ensureExpectedToken(
          XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

      while (!parser.nextToken().equals(XContentParser.Token.END_OBJECT)) {
        String fieldName = parser.currentName();
        parser.nextToken();
        switch (fieldName) {
          case OpenSearchRefreshIndexJobRequest.NAME_FIELD:
            builder.withJobName(parser.text());
            break;
          case OpenSearchRefreshIndexJobRequest.ENABLED_FILED:
            builder.withEnabled(parser.booleanValue());
            break;
          case OpenSearchRefreshIndexJobRequest.ENABLED_TIME_FILED:
            builder.withEnabledTime(parseInstantValue(parser));
            break;
          case OpenSearchRefreshIndexJobRequest.LAST_UPDATE_TIME_FIELD:
            builder.withLastUpdateTime(parseInstantValue(parser));
            break;
          case OpenSearchRefreshIndexJobRequest.SCHEDULE_FIELD:
            builder.withSchedule(ScheduleParser.parse(parser).toString());
            break;
          case OpenSearchRefreshIndexJobRequest.INDEX_NAME_FIELD:
            builder.withIndexToWatch(parser.text());
            break;
          case OpenSearchRefreshIndexJobRequest.LOCK_DURATION_SECONDS:
            builder.withLockDurationSeconds(parser.longValue());
            break;
          case OpenSearchRefreshIndexJobRequest.JITTER:
            builder.withJitter(parser.doubleValue());
            break;
          default:
            XContentParserUtils.throwUnknownToken(parser.currentToken(), parser.getTokenLocation());
        }
      }
      return builder.build();
    };
  }

  @Override
  public void scheduleJob(AsyncQuerySchedulerJobRequest request) {
    // Implementation to schedule the job
  }

  @Override
  public void unscheduleJob(AsyncQuerySchedulerJobRequest request) {
    // Implementation to unschedule the job
  }

  @Override
  public void removeJob(AsyncQuerySchedulerJobRequest request) {
    // Implementation to remove the job
  }

  @Override
  public void updateJob(AsyncQuerySchedulerJobRequest request) {
    // Implementation to update the job
  }

  private void createAsyncQuerySchedulerIndex() {
    try {
      InputStream mappingFileStream =
          OpenSearchDataSourceMetadataStorage.class
              .getClassLoader()
              .getResourceAsStream(SCHEDULER_INDEX_MAPPING_FILE_NAME);
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(SCHEDULER_INDEX_NAME);
      createIndexRequest.mapping(
          IOUtils.toString(mappingFileStream, StandardCharsets.UTF_8), XContentType.YAML);
      ActionFuture<CreateIndexResponse> createIndexResponseActionFuture;
      try (ThreadContext.StoredContext ignored =
          client.threadPool().getThreadContext().stashContext()) {
        createIndexResponseActionFuture = client.admin().indices().create(createIndexRequest);
      }
      CreateIndexResponse createIndexResponse = createIndexResponseActionFuture.actionGet();
      if (createIndexResponse.isAcknowledged()) {
        LOG.info("Index: {} creation Acknowledged", SCHEDULER_INDEX_NAME);
      } else {
        throw new RuntimeException("Index creation is not acknowledged.");
      }
    } catch (Throwable e) {
      throw new RuntimeException(
          "Internal server error while creating"
              + SCHEDULER_INDEX_NAME
              + " index:: "
              + e.getMessage());
    }
  }

  private Instant parseInstantValue(XContentParser parser) throws IOException {
    if (XContentParser.Token.VALUE_NULL.equals(parser.currentToken())) {
      return null;
    }
    if (parser.currentToken().isValue()) {
      return Instant.ofEpochMilli(parser.longValue());
    }
    XContentParserUtils.throwUnknownToken(parser.currentToken(), parser.getTokenLocation());
    return null;
  }
}
