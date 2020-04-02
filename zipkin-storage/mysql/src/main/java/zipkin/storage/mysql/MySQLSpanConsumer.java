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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
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

  private static final int bufferSize = 1000;
  private final static Queue<SpanRow> spanRows = new ConcurrentLinkedQueue<>();
  private final static Queue<AnnotationRow> annotationRows = new ConcurrentLinkedQueue<>();
  private static final Logger LOG = Logger.getLogger(MySQLSpanConsumer.class.getName());


  MySQLSpanConsumer(DataSource datasource, DSLContexts context, Schema schema) {
    this.datasource = datasource;
    this.context = context;
    this.schema = schema;
  }

  private void doAnnotationInsert(
          InsertValuesStep11<Record, Long, Long, String, byte[], Integer, Long, Long, String, Integer, byte[], Short> insert
  ) throws DataAccessException {
    boolean retry;
    int tries = 0;
    do {
      retry = false;
      try {
        insert.onDuplicateKeyIgnore().execute();
      } catch (DataAccessException ex) {
        if (ex.getMessage().contains("Deadlock")) {
          tries ++;
          if (tries > 1){
            System.out.println("Deadlock detected for attempt " + tries);
          }
          retry = true;
        } else {
          throw ex;
        }
      }
    } while (retry);
  }

  public void maybePersistAnnotations(){
    try (Connection conn = datasource.getConnection()) {
      DSLContext create = context.get(conn);
      InsertValuesStep11<Record, Long, Long, String, byte[], Integer, Long, Long, String, Integer, byte[], Short>
              insert = create.insertInto(ZIPKIN_ANNOTATIONS)
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
      int rowCount = 0;
      while (!annotationRows.isEmpty()) {
        AnnotationRow row = annotationRows.poll();
        if (row != null) {
          insert.values(
                  row.traceId,
                  row.spanId,
                  row.key,
                  row.value,
                  row.type,
                  row.timestamp,
                  row.traceIdHigh,
                  row.serviceName,
                  row.ipv4,
                  row.ipv6,
                  row.port
          );
          rowCount++;
          if (rowCount >= bufferSize) {
            doAnnotationInsert(insert);
            insert = create.insertInto(ZIPKIN_ANNOTATIONS)
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
            rowCount = 0;
          }
        }
      }
      if (rowCount > 0) {
        doAnnotationInsert(insert);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO
    }
  }

  public void maybePersistSpans() {
    try (Connection conn = datasource.getConnection()) {
      DSLContext create = context.get(conn);
      InsertValuesStep8<Record, Long, Long, Long, String, Boolean, Long, Long, Long> insert =
              create.insertInto(ZIPKIN_SPANS)
                      .columns(
                              ZIPKIN_SPANS.TRACE_ID,
                              ZIPKIN_SPANS.ID,
                              ZIPKIN_SPANS.PARENT_ID,
                              ZIPKIN_SPANS.NAME,
                              ZIPKIN_SPANS.DEBUG,
                              ZIPKIN_SPANS.START_TS,
                              ZIPKIN_SPANS.DURATION,
                              ZIPKIN_SPANS.TRACE_ID_HIGH
                      );
      int rowCount = 0;
      while (!spanRows.isEmpty()) {
        SpanRow row = spanRows.poll();
        if (row != null) {
          insert.values(
                  row.traceId,
                  row.id,
                  row.parentId,
                  row.name,
                  row.debug,
                  row.timestamp,
                  row.duration,
                  row.traceIdHigh
          );
          rowCount++;
          if (rowCount >= bufferSize) {
            insert.onDuplicateKeyUpdate()
                    .set(ZIPKIN_SPANS.NAME, UpsertDSL.values(ZIPKIN_SPANS.NAME))
                    .set(ZIPKIN_SPANS.START_TS, UpsertDSL.values(ZIPKIN_SPANS.START_TS))
                    .set(ZIPKIN_SPANS.DURATION, UpsertDSL.values(ZIPKIN_SPANS.DURATION))
                    .execute();
            insert = create.insertInto(ZIPKIN_SPANS)
                    .columns(
                            ZIPKIN_SPANS.TRACE_ID,
                            ZIPKIN_SPANS.ID,
                            ZIPKIN_SPANS.PARENT_ID,
                            ZIPKIN_SPANS.NAME,
                            ZIPKIN_SPANS.DEBUG,
                            ZIPKIN_SPANS.START_TS,
                            ZIPKIN_SPANS.DURATION,
                            ZIPKIN_SPANS.TRACE_ID_HIGH
                    );
            rowCount = 0;
          }
        }
      }
      if (rowCount > 0) {
        insert.onDuplicateKeyUpdate()
                .set(ZIPKIN_SPANS.NAME, UpsertDSL.values(ZIPKIN_SPANS.NAME))
                .set(ZIPKIN_SPANS.START_TS, UpsertDSL.values(ZIPKIN_SPANS.START_TS))
                .set(ZIPKIN_SPANS.DURATION, UpsertDSL.values(ZIPKIN_SPANS.DURATION))
                .execute();
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Some SQL exception int spans", e);
      System.out.println("Some error in spans sql " + e + e.getMessage());
      e.printStackTrace();
    }
  }



  /** Blocking version of {@link AsyncSpanConsumer#accept} */
  @Override public void accept(List<Span> spans) {
    if (spans.isEmpty()) return;
    try (Connection conn = datasource.getConnection()) {
      for (Span span : spans) {
        Long overridingTimestamp = authoritativeTimestamp(span);
        Long timestamp = overridingTimestamp != null ? overridingTimestamp : guessTimestamp(span);

        Long traceIdHigh = 0L;
        if (span.traceIdHigh != 0 && schema.hasTraceIdHigh) {
          traceIdHigh = span.traceIdHigh;
        }

        spanRows.add(
                new SpanRow(
                        span.traceId,
                        span.id,
                        span.parentId,
                        span.name,
                        span.debug,
                        timestamp,
                        span.duration,
                        traceIdHigh
                )
        );
        for (Annotation annotation : span.annotations) {
          String serviceName = null;
          Integer ipv4 = null;
          byte[] ipv6 = null;
          Short port = null;
          if (annotation.endpoint != null) {
            serviceName = annotation.endpoint.serviceName;
            ipv4 = annotation.endpoint.ipv4;
            if (annotation.endpoint.ipv6 != null && schema.hasIpv6) {
              ipv6 = annotation.endpoint.ipv6;
            }
            port = annotation.endpoint.port;
          }
          if (serviceName != null) {
            String lowerServiceName = serviceName.toLowerCase();
            SpanName.Store.spans.putIfAbsent(lowerServiceName, Collections.newSetFromMap(new ConcurrentHashMap<>(16, 0.9f, 2)));
            SpanName.Store.spans.get(lowerServiceName).add(span.name);
          }
          annotationRows.add(
                  new AnnotationRow(
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
                  )
          );
        }
        for (BinaryAnnotation annotation : span.binaryAnnotations) {
          String serviceName = null;
          Integer ipv4 = null;
          byte[] ipv6 = null;
          Short port = null;
          if (annotation.endpoint != null) {
            serviceName = annotation.endpoint.serviceName;
            ipv4 = annotation.endpoint.ipv4;
            if (annotation.endpoint.ipv6 != null && schema.hasIpv6) {
              ipv6 = annotation.endpoint.ipv6;
            }
            port = annotation.endpoint.port;
          }
          annotationRows.add(
                  new AnnotationRow(
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
                  )
          );
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e); // TODO
    }
  }

  class SpanRow {
    private Long traceId;
    private Long id;
    private Long parentId;
    private String name;
    private Boolean debug;
    private Long timestamp;
    private Long duration;
    private Long traceIdHigh;
    public SpanRow(Long traceId, Long id, Long parentId, String name, Boolean debug, Long timestamp, Long duration, Long traceIdHigh) {
      this.traceId = traceId;
      this.id = id;
      this.parentId = parentId;
      this.name = name;
      this.debug = debug;
      this.timestamp = timestamp;
      this.duration = duration;
      this.traceIdHigh = traceIdHigh;
    }
  }

  class AnnotationRow {
    private Long traceId;
    private Long spanId;
    private String key;
    private byte[] value;
    private Integer type;
    private Long timestamp;
    private Long traceIdHigh;
    private String serviceName;
    private Integer ipv4;
    private byte[] ipv6;
    private Short port;

    public AnnotationRow(Long traceId, Long spanId, String key, byte[] value, Integer type, Long timestamp, Long traceIdHigh, String serviceName, Integer ipv4, byte[] ipv6, Short port) {
      this.traceId = traceId;
      this.spanId = spanId;
      this.key = key;
      this.value = value;
      this.type = type;
      this.timestamp = timestamp;
      this.traceIdHigh = traceIdHigh;
      this.serviceName = serviceName;
      this.ipv4 = ipv4;
      this.ipv6 = ipv6;
      this.port = port;
    }
  }
}
