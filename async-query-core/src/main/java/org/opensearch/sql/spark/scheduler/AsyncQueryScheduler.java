/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler;

public interface AsyncQueryScheduler {
  void scheduleJob(AsyncQuerySchedulerJobRequest request);

  void unscheduleJob(AsyncQuerySchedulerJobRequest request);

  void removeJob(AsyncQuerySchedulerJobRequest request);

  void updateJob(AsyncQuerySchedulerJobRequest request);
}
