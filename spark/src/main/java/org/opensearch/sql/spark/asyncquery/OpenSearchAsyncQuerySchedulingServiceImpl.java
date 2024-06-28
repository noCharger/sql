package org.opensearch.sql.spark.asyncquery;

import java.io.IOException;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.dispatcher.AsyncQueryHandler;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.threadpool.ThreadPool;

@RequiredArgsConstructor
public class OpenSearchAsyncQuerySchedulingServiceImpl implements AsyncQueryScheduler {
  static final String JOB_INDEX_NAME = ".scheduler_sample_extension";
  private final Client client;
  private final ClusterService clusterService;
  private final ThreadPool threadPool;
  AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService;

  public ScheduledJobRunner getJobRunner() {
    SampleJobRunner sampleJobRunner = SampleJobRunner.getJobRunnerInstance();
    sampleJobRunner.setClusterService(clusterService);
    sampleJobRunner.setThreadPool(threadPool);
    sampleJobRunner.setClient(client);
    return sampleJobRunner;
  }

  public ScheduledJobParser getJobParser() {
    return (parser, id, jobDocVersion) -> {
      SampleJobParameter jobParameter = new SampleJobParameter();
      XContentParserUtils.ensureExpectedToken(
          XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

      while (!parser.nextToken().equals(XContentParser.Token.END_OBJECT)) {
        String fieldName = parser.currentName();
        parser.nextToken();
        switch (fieldName) {
          case SampleJobParameter.NAME_FIELD:
            jobParameter.setJobName(parser.text());
            break;
          case SampleJobParameter.ENABLED_FILED:
            jobParameter.setEnabled(parser.booleanValue());
            break;
          case SampleJobParameter.ENABLED_TIME_FILED:
            jobParameter.setEnabledTime(parseInstantValue(parser));
            break;
          case SampleJobParameter.LAST_UPDATE_TIME_FIELD:
            jobParameter.setLastUpdateTime(parseInstantValue(parser));
            break;
          case SampleJobParameter.SCHEDULE_FIELD:
            jobParameter.setSchedule(ScheduleParser.parse(parser));
            break;
          case SampleJobParameter.INDEX_NAME_FIELD:
            jobParameter.setIndexToWatch(parser.text());
            break;
          case SampleJobParameter.LOCK_DURATION_SECONDS:
            jobParameter.setLockDurationSeconds(parser.longValue());
            break;
          case SampleJobParameter.JITTER:
            jobParameter.setJitter(parser.doubleValue());
            break;
          default:
            XContentParserUtils.throwUnknownToken(parser.currentToken(), parser.getTokenLocation());
        }
      }
      return jobParameter;
    };
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

  @Override
  public void scheduleQuery() {
    asyncQueryJobMetadataStorageService.storeJobMetadata();
  }

  @Override
  public void disableScheduledQuery() {

  }

  @Override
  public void cancelScheduledQuery() {

  }
}
