/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import org.jooq.*;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Span;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.StorageAdapters;

import static zipkin.internal.ApplyTimestampAndDuration.authoritativeTimestamp;
import static zipkin.internal.ApplyTimestampAndDuration.guessTimestamp;
import static zipkin.storage.mysql.internal.generated.tables.ZipkinAnnotations.ZIPKIN_ANNOTATIONS;
import static zipkin.storage.mysql.internal.generated.tables.ZipkinSpans.ZIPKIN_SPANS;

final class MySQLSpanConsumer implements StorageAdapters.SpanConsumer {
  private final DataSource datasource;
  private final DSLContexts context;
  private final Schema schema;
  private Integer annotationCount = 0;
  private InsertValuesStep11<Record, Long, Long, String, byte[], Integer, Long, Long, String, Integer, byte[], Short>
          annotationColumns;


  MySQLSpanConsumer(DataSource datasource, DSLContexts context, Schema schema) {
    this.datasource = datasource;
    this.context = context;
    this.schema = schema;
  }

  /** Blocking version of {@link AsyncSpanConsumer#accept} */
  @Override public void accept(List<Span> spans) {
    if (spans.isEmpty()) return;
    try (Connection conn = datasource.getConnection()) {
      DSLContext create = context.get(conn);
      synchronized (MySQLSpanConsumer.class) {
        if (annotationColumns == null) {
          annotationColumns = create.insertInto(ZIPKIN_ANNOTATIONS)
                  .columns(
                          ZIPKIN_ANNOTATIONS.TRACE_ID,
                          ZIPKIN_ANNOTATIONS.SPAN_ID,
                          ZIPKIN_ANNOTATIONS.A_KEY,
                          ZIPKIN_ANNOTATIONS.A_VALUE,
                          ZIPKIN_ANNOTATIONS.A_TYPE,
                          ZIPKIN_ANNOTATIONS.A_TIMESTAMP,
                          ZIPKIN_ANNOTATIONS.TRACE_ID_HIGH,
                          ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME,
                          ZIPKIN_ANNOTATIONS.ENDPOINT_IPV4,
                          ZIPKIN_ANNOTATIONS.ENDPOINT_IPV6,
                          ZIPKIN_ANNOTATIONS.ENDPOINT_PORT
                  );
        }
      }
      List<Query> inserts = new ArrayList<>();

      for (Span span : spans) {
        Long overridingTimestamp = authoritativeTimestamp(span);
        Long timestamp = overridingTimestamp != null ? overridingTimestamp : guessTimestamp(span);

        Map<TableField<Record, ?>, Object> updateFields = new LinkedHashMap<>();
        if (!span.name.equals("") && !span.name.equals("unknown")) {
          updateFields.put(ZIPKIN_SPANS.NAME, span.name);
        }
        // replace any tentative timestamp with the authoritative one.
        if (overridingTimestamp != null) {
          updateFields.put(ZIPKIN_SPANS.START_TS, overridingTimestamp);
        }
        if (span.duration != null) {
          updateFields.put(ZIPKIN_SPANS.DURATION, span.duration);
        }

        InsertSetMoreStep<Record> insertSpan = create.insertInto(ZIPKIN_SPANS)
            .set(ZIPKIN_SPANS.TRACE_ID, span.traceId)
            .set(ZIPKIN_SPANS.ID, span.id)
            .set(ZIPKIN_SPANS.PARENT_ID, span.parentId)
            .set(ZIPKIN_SPANS.NAME, span.name)
            .set(ZIPKIN_SPANS.DEBUG, span.debug)
            .set(ZIPKIN_SPANS.START_TS, timestamp)
            .set(ZIPKIN_SPANS.DURATION, span.duration);

        if (span.traceIdHigh != 0 && schema.hasTraceIdHigh) {
          insertSpan.set(ZIPKIN_SPANS.TRACE_ID_HIGH, span.traceIdHigh);
        }

        inserts.add(updateFields.isEmpty() ?
            insertSpan.onDuplicateKeyIgnore() :
            insertSpan.onDuplicateKeyUpdate().set(updateFields));

        synchronized (MySQLSpanConsumer.class) {
          for (Annotation annotation : span.annotations) {
            Long traceIdHigh = 0L;
            String serviceName = null;
            Integer ipv4 = null;
            byte[] ipv6 = null;
            Short port = null;
            if (span.traceIdHigh != 0 && schema.hasTraceIdHigh) {
              traceIdHigh = span.traceIdHigh;
            }
            if (annotation.endpoint != null) {
              serviceName = annotation.endpoint.serviceName;
              ipv4 = annotation.endpoint.ipv4;
              if (annotation.endpoint.ipv6 != null && schema.hasIpv6) {
                ipv6 = annotation.endpoint.ipv6;
              }
              port = annotation.endpoint.port;
            }
            annotationColumns.values(
                    span.traceId,
                    span.id,
                    annotation.value,
                    null,
                    -1,
                    annotation.timestamp,
                    traceIdHigh,
                    serviceName,
                    ipv4,
                    ipv6,
                    port
            );
            annotationCount ++;
          }

          for (BinaryAnnotation annotation : span.binaryAnnotations) {
            Long traceIdHigh = 0L;
            String serviceName = null;
            Integer ipv4 = null;
            byte[] ipv6 = null;
            Short port = null;
            if (span.traceIdHigh != 0 && schema.hasTraceIdHigh) {
              traceIdHigh = span.traceIdHigh;
            }
            if (annotation.endpoint != null) {
              serviceName = annotation.endpoint.serviceName;
              ipv4 = annotation.endpoint.ipv4;
              if (annotation.endpoint.ipv6 != null && schema.hasIpv6) {
                ipv6 = annotation.endpoint.ipv6;
              }
              port = annotation.endpoint.port;
            }
            annotationColumns.values(
                    span.traceId,
                    span.id,
                    annotation.key,
                    annotation.value,
                    annotation.type.value,
                    timestamp,
                    traceIdHigh,
                    serviceName,
                    ipv4,
                    ipv6,
                    port
            );
            annotationCount ++;
          }
        }
      }
      synchronized (MySQLSpanConsumer.class) {
        if (annotationCount >= 10) {
          annotationColumns.onDuplicateKeyIgnore().execute();
          annotationColumns = null;
          annotationCount = 0;
        }
      }
      create.batch(inserts).execute();
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO
    }
  }
}
