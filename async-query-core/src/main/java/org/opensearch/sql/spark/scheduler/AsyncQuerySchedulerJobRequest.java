/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler;

public abstract class AsyncQuerySchedulerJobRequest {
  private final String jobName;
  private final String jobType;
  private final String schedule;
  private final boolean enabled;

  protected AsyncQuerySchedulerJobRequest(Builder<?> builder) {
    this.jobName = builder.jobName;
    this.jobType = builder.jobType;
    this.schedule = builder.schedule;
    this.enabled = builder.enabled;
  }

  public String getJobName() {
    return jobName;
  }

  public String getJobType() {
    return jobType;
  }

  public String getRawSchedule() {
    return schedule;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public abstract static class Builder<T extends Builder<T>> {
    private String jobName;
    private String jobType;
    private String schedule;
    private boolean enabled;

    public T withJobName(String jobName) {
      this.jobName = jobName;
      return self();
    }

    public T withJobType(String jobType) {
      this.jobType = jobType;
      return self();
    }

    public T withSchedule(String schedule) {
      this.schedule = schedule;
      return self();
    }

    public T withEnabled(boolean enabled) {
      this.enabled = enabled;
      return self();
    }

    protected abstract T self();

    public abstract AsyncQuerySchedulerJobRequest build();
  }
}
