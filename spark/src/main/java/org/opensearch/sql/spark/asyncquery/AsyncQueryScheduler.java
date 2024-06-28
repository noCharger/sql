package org.opensearch.sql.spark.asyncquery;

import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;

public interface AsyncQueryScheduler {
    public void scheduleQuery();

    public void disableScheduledQuery();

    public void cancelScheduledQuery();
}
