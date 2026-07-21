/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;

class ClusterAggregationBuilderTest {

  @Test
  void serializationRoundTrip() throws IOException {
    ClusterAggregationBuilder original =
        new ClusterAggregationBuilder("c")
            .field("message")
            .threshold(0.7)
            .matchMode("termset")
            .delims(" ,;")
            .maxClusters(500);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    ClusterAggregationBuilder copy = new ClusterAggregationBuilder(out.bytes().streamInput());

    assertEquals(original, copy);
  }
}
