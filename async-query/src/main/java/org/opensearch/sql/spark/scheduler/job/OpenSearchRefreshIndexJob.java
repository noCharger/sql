/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.job;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.plugins.Plugin;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.scheduler.model.OpenSearchScheduleQueryJobRequest;
import org.opensearch.threadpool.ThreadPool;

/**
 * The job runner class for scheduling refresh index query.
 *
 * <p>The job runner should be a singleton class if it uses OpenSearch client or other objects
 * passed from OpenSearch. Because when registering the job runner to JobScheduler plugin,
 * OpenSearch has not invoked plugins' createComponents() method. That is saying the plugin is not
 * completely initialized, and the OpenSearch {@link org.opensearch.client.Client}, {@link
 * ClusterService} and other objects are not available to plugin and this job runner.
 *
 * <p>So we have to move this job runner initialization to {@link Plugin} createComponents() method,
 * and using singleton job runner to ensure we register a usable job runner instance to JobScheduler
 * plugin.
 */
public class OpenSearchRefreshIndexJob implements ScheduledJobRunner {

  private static final Logger log = LogManager.getLogger(OpenSearchRefreshIndexJob.class);

  public static OpenSearchRefreshIndexJob INSTANCE = new OpenSearchRefreshIndexJob();

  public static OpenSearchRefreshIndexJob getJobRunnerInstance() {
    return INSTANCE;
  }

  private AsyncQueryExecutorService asyncQueryExecutorService;
  private ClusterService clusterService;
  private ThreadPool threadPool;
  private Client client;

  private OpenSearchRefreshIndexJob() {
    // Singleton class, use getJobRunnerInstance method instead of constructor
  }

  public void setClusterService(ClusterService clusterService) {
    this.clusterService = clusterService;
  }

  public void setThreadPool(ThreadPool threadPool) {
    this.threadPool = threadPool;
  }

  public void setClient(Client client) {
    this.client = client;
  }

  @Override
  public void runJob(ScheduledJobParameter jobParameter, JobExecutionContext context) {
    if (!(jobParameter instanceof OpenSearchScheduleQueryJobRequest)) {
      throw new IllegalStateException(
              "Job parameter is not instance of OpenSearchRefreshIndexJobRequest, type: "
                      + jobParameter.getClass().getCanonicalName());
    }

    if (this.clusterService == null) {
      throw new IllegalStateException("ClusterService is not initialized.");
    }

    if (this.threadPool == null) {
      throw new IllegalStateException("ThreadPool is not initialized.");
    }

    if (this.client == null) {
      throw new IllegalStateException("Client is not initialized.");
    }

    Runnable runnable =
            () -> {
              doRefresh((OpenSearchScheduleQueryJobRequest) jobParameter);
            };
    threadPool.generic().submit(runnable);
  }

  void doRefresh(OpenSearchScheduleQueryJobRequest jobParameter) {
    log.info("Scheduled refresh index job with jobId: " + jobParameter.getName());
    CreateAsyncQueryRequest request =
            new CreateAsyncQueryRequest(
                    jobParameter.getScheduledQuery(), jobParameter.getDataSource(), LangType.SQL);
    asyncQueryExecutorService.createAsyncQuery(request, new NullAsyncQueryRequestContext());
  }
}