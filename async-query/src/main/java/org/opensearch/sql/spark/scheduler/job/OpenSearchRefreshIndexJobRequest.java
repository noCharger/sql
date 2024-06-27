/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.job;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.sql.spark.scheduler.AsyncQuerySchedulerJobRequest;

/**
 * A sample job parameter.
 *
 * <p>It adds an additional "indexToWatch" field to {@link ScheduledJobParameter}, which stores the
 * index the job runner will watch.
 */
public class OpenSearchRefreshIndexJobRequest extends AsyncQuerySchedulerJobRequest
    implements ScheduledJobParameter {
  public static final String NAME_FIELD = "name";
  public static final String ENABLED_FILED = "enabled";
  public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
  public static final String LAST_UPDATE_TIME_FIELD_READABLE = "last_update_time_field";
  public static final String SCHEDULE_FIELD = "schedule";
  public static final String ENABLED_TIME_FILED = "enabled_time";
  public static final String ENABLED_TIME_FILED_READABLE = "enabled_time_field";
  public static final String INDEX_NAME_FIELD = "index_name_to_watch";
  public static final String LOCK_DURATION_SECONDS = "lock_duration_seconds";
  public static final String JITTER = "jitter";

  private Instant lastUpdateTime;
  private Instant enabledTime;
  private String indexToWatch;
  private Long lockDurationSeconds;
  private Double jitter;

  private OpenSearchRefreshIndexJobRequest(Builder builder) {
    super(builder);
    this.indexToWatch = builder.indexToWatch;
    this.lockDurationSeconds = builder.lockDurationSeconds;
    this.jitter = builder.jitter;
    this.enabledTime = builder.enabledTime;
    this.lastUpdateTime = builder.lastUpdatedTime;
  }

  public String getIndexToWatch() {
    return this.indexToWatch;
  }

  @Override
  public String getName() {
    return super.getJobName();
  }

  @Override
  public Instant getLastUpdateTime() {
    return this.lastUpdateTime;
  }

  @Override
  public Instant getEnabledTime() {
    return this.enabledTime;
  }

  @Override
  public Schedule getSchedule() {
    // TODO: Optimize parser
    return new IntervalSchedule(
        Instant.now(), Integer.parseInt(this.getRawSchedule()), ChronoUnit.MINUTES);
  }

  @Override
  public Long getLockDurationSeconds() {
    return this.lockDurationSeconds;
  }

  @Override
  public Double getJitter() {
    return jitter;
  }

  public static class Builder extends AsyncQuerySchedulerJobRequest.Builder<Builder> {
    private String indexToWatch;
    private Long lockDurationSeconds;
    private Double jitter;

    private Instant enabledTime;

    private Instant lastUpdatedTime;

    public Builder withIndexToWatch(String indexToWatch) {
      this.indexToWatch = indexToWatch;
      return this;
    }

    public Builder withLockDurationSeconds(Long lockDurationSeconds) {
      this.lockDurationSeconds = lockDurationSeconds;
      return this;
    }

    public Builder withJitter(Double jitter) {
      this.jitter = jitter;
      return this;
    }

    public Builder withEnabledTime(Instant enabledTime) {
      this.enabledTime = enabledTime;
      return this;
    }

    public Builder withLastUpdateTime(Instant lastUpdateTime) {
      this.lastUpdatedTime = lastUpdatedTime;
      return this;
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    public OpenSearchRefreshIndexJobRequest build() {
      return new OpenSearchRefreshIndexJobRequest(this);
    }
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params)
      throws IOException {
    builder.startObject();
    builder
        .field(NAME_FIELD, getJobName())
        .field(ENABLED_FILED, isEnabled())
        .field(SCHEDULE_FIELD, getRawSchedule())
        .field(INDEX_NAME_FIELD, this.indexToWatch);
    if (getEnabledTime() != null) {
      builder.timeField(
          ENABLED_TIME_FILED, ENABLED_TIME_FILED_READABLE, getEnabledTime().toEpochMilli());
    }
    if (getLastUpdateTime() != null) {
      builder.timeField(
          LAST_UPDATE_TIME_FIELD,
          LAST_UPDATE_TIME_FIELD_READABLE,
          getLastUpdateTime().toEpochMilli());
    }
    if (this.lockDurationSeconds != null) {
      builder.field(LOCK_DURATION_SECONDS, this.lockDurationSeconds);
    }
    if (this.jitter != null) {
      builder.field(JITTER, this.jitter);
    }
    builder.endObject();
    return builder;
  }

  @Override
  public String getJobType() {
    return "OpenSearchRefreshIndexJob";
  }

  @Override
  public boolean isFragment() {
    return false;
  }
}
