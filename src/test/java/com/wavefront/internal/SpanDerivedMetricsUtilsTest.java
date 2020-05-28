package com.wavefront.internal;

import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.java_sdk.com.google.common.collect.ImmutableList;
import com.wavefront.java_sdk.com.google.common.collect.ImmutableSet;
import com.wavefront.java_sdk.com.google.common.collect.Sets;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.common.annotation.Nullable;
import com.wavefront.sdk.entities.histograms.HistogramGranularity;
import com.wavefront.sdk.entities.tracing.SpanLog;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.wavefront.internal.SpanDerivedMetricsUtils.reportHeartbeats;
import static com.wavefront.internal.SpanDerivedMetricsUtils.reportWavefrontGeneratedData;
import static com.wavefront.sdk.common.Constants.DELTA_PREFIX;
import static com.wavefront.sdk.common.Constants.DELTA_PREFIX_2;
import static com.wavefront.sdk.common.Constants.HEART_BEAT_METRIC;
import static com.wavefront.sdk.common.Utils.histogramToLineData;
import static com.wavefront.sdk.common.Utils.metricToLineData;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link SpanDerivedMetricsUtils}.
 *
 * @author Hao Song (songhao@vmware.com).
 */
public class SpanDerivedMetricsUtilsTest {

  private static WavefrontSender wavefrontSender;
  private static WavefrontInternalReporter wavefrontInternalReporter;
  private static AtomicLong wavefrontHistogramClock;
  private static final Set<MetricRecord> heartbeatMetricsEmitted = new HashSet<>();
  private static final Set<MetricRecord> deltaMetricsEmitted = new HashSet<>();
  private static final Set<MetricRecord> metricsEmitted = new HashSet<>();
  private static final Set<HistogramRecord> histogramsEmitted = new HashSet<>();

  static class MetricRecord {
    String name;
    double value;
    @Nullable
    String source;
    @Nullable
    Map<String, String> tags;

    public MetricRecord(String name, double value, @Nullable String source, @Nullable Map<String, String> tags) {
      this.name = name;
      this.value = value;
      this.source = source;
      this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MetricRecord that = (MetricRecord) o;

      if (Double.compare(that.value, value) != 0) return false;
      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(source, that.source)) return false;
      return Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
      int result;
      long temp;
      result = name != null ? name.hashCode() : 0;
      temp = Double.doubleToLongBits(value);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      result = 31 * result + (source != null ? source.hashCode() : 0);
      result = 31 * result + (tags != null ? tags.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return metricToLineData(name, value,  null, source, tags, "default-source");
    }
  }

  static class HistogramRecord {
    String name;
    List<Pair<Double, Integer>> centroids;
    Set<HistogramGranularity> histogramGranularities;
    @Nullable
    Long timestamp;
    @Nullable
    String source;
    @Nullable
    Map<String, String> tags;

    public HistogramRecord(String name, List<Pair<Double, Integer>> centroids,
                           Set<HistogramGranularity> histogramGranularities, @Nullable Long timestamp,
                           @Nullable String source, @Nullable Map<String, String> tags) {
      this.name = name;
      this.centroids = centroids;
      this.histogramGranularities = histogramGranularities;
      this.timestamp = timestamp;
      this.source = source;
      this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      HistogramRecord that = (HistogramRecord) o;

      if (!Objects.equals(name, that.name)) return false;
      if (!Objects.equals(centroids, that.centroids))
        return false;
      if (!Objects.equals(histogramGranularities, that.histogramGranularities))
        return false;
      if (!Objects.equals(timestamp, that.timestamp))
        return false;
      if (!Objects.equals(source, that.source)) return false;
      return Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (centroids != null ? centroids.hashCode() : 0);
      result = 31 * result + (histogramGranularities != null ? histogramGranularities.hashCode() : 0);
      result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
      result = 31 * result + (source != null ? source.hashCode() : 0);
      result = 31 * result + (tags != null ? tags.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return histogramToLineData(name, centroids, histogramGranularities, timestamp, source, tags,
          "default-source");
    }
  }

  @BeforeClass
  public static void classSetup() {
    wavefrontSender = new WavefrontSender() {
      @Override
      public String getClientId() {
        return "";
      }

      @Override
      public void flush() {
        metricsEmitted.clear();
        histogramsEmitted.clear();
      }

      @Override
      public int getFailureCount() {
        return 0;
      }

      @Override
      public void sendDistribution(String name, List<Pair<Double, Integer>> centroids,
                                   Set<HistogramGranularity> histogramGranularities,
                                   @Nullable Long timestamp, @Nullable String source,
                                   @Nullable Map<String, String> tags) {
        HistogramRecord histogramRecord = new HistogramRecord(name, centroids,
            histogramGranularities, timestamp, source, tags);
        histogramsEmitted.add(histogramRecord);
      }

      @Override
      public void sendMetric(String name, double value, @Nullable Long timestamp,
                             @Nullable String source, @Nullable Map<String, String> tags) {
        MetricRecord metricRecord = new MetricRecord(name, value, source, tags);
        if (name.equalsIgnoreCase(HEART_BEAT_METRIC)) {
          heartbeatMetricsEmitted.add(metricRecord);
        } else if (name.startsWith(DELTA_PREFIX) || name.startsWith(DELTA_PREFIX_2)) {
          deltaMetricsEmitted.add(metricRecord);
        } else {
          metricsEmitted.add(metricRecord);
        }
      }

      @Override
      public void sendFormattedMetric(String point) {
        throw new NotImplementedException();
      }

      @Override
      public void sendSpan(String name, long startMillis, long durationMillis, @Nullable String source,
                           UUID traceId, UUID spanId, @Nullable List<UUID> parents,
                           @Nullable List<UUID> followsFrom, @Nullable List<Pair<String, String>> tags,
                           @Nullable List<SpanLog> spanLogs) {
        throw new NotImplementedException();
      }

      @Override
      public void close() {
        this.flush();
      }
    };
  }

  @Before
  public void setUp() throws IOException {
    // Clear metrics and histograms records.
    wavefrontSender.flush();

    // Realign the clock to System.currentTimeMillis().
    wavefrontHistogramClock = new AtomicLong(System.currentTimeMillis());

    // Reset wavefrontInternalReporter.
    wavefrontInternalReporter = new WavefrontInternalReporter.Builder().
        prefixedWith("tracing.derived").withSource("default-source").reportMinuteDistribution().
        build(wavefrontSender);
  }

  @Test
  public void testHeartbeatMetrics() throws IOException {
    Pair<Map<String, String>, String> heartbeatTags = reportWavefrontGeneratedData(
        wavefrontInternalReporter, "orderShirt", "beachshirts", "shopping", "us-west", "primary",
        "localhost", "jdbc", false, 10000L, Sets.newHashSet("location"),
        ImmutableList.of(new Pair<>("location", "SF")), true, () -> wavefrontHistogramClock.get());

    // Assert return pointTags of Heartbeat metrics.
    assertEquals(Stream.of(new String[][]{
        {"component", "jdbc"},
        {"application", "beachshirts"},
        {"service", "shopping"},
        {"cluster", "us-west"},
        {"shard", "primary"},
        {"span.kind", "none"},
        {"location", "SF"}
    }).collect(Collectors.toMap(data -> data[0], data -> data[1])), heartbeatTags._1);

    // Assert return source value of Heartbeat metrics.
    assertEquals("localhost", heartbeatTags._2);

    reportHeartbeats(wavefrontSender, Sets.newHashSet(heartbeatTags));

    Set<MetricRecord> heatbeatMetricsExpected = new HashSet<>();
    heatbeatMetricsExpected.add(new MetricRecord(
        "~component.heartbeat",
        1.0,
        "localhost",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"span.kind", "none"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))));
    assertEquals(heatbeatMetricsExpected, heartbeatMetricsEmitted);
    heartbeatMetricsEmitted.clear();

    reportHeartbeats(wavefrontSender, Sets.newHashSet(heartbeatTags), "jaeger");
    heatbeatMetricsExpected.add(new MetricRecord(
        "~component.heartbeat",
        1.0,
        "localhost",
        Stream.of(new String[][]{
            {"component", "jaeger"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"span.kind", "none"},
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))));
    assertEquals(2, heartbeatMetricsEmitted.size());
  }

  @Test
  public void testNonErroneousDeltaSpanRedMetrics() {
    reportWavefrontGeneratedData(
        wavefrontInternalReporter, "orderShirt", "beachshirts", "shopping", "us-west", "primary",
        "localhost", "jdbc", false, 10000L, Sets.newHashSet("location"),
        ImmutableList.of(new Pair<>("location", "SF")), true, () -> wavefrontHistogramClock.get());
    long histogramEmittedMinuteMillis = (wavefrontHistogramClock.get() / 60000L) * 60000L;

    wavefrontHistogramClock.addAndGet(60000L + 1);
    wavefrontInternalReporter.report();

    Set<MetricRecord> deltaCounterExpected = new HashSet<>();
    Set<HistogramRecord> histogramExpected = new HashSet<>();

    deltaCounterExpected.add(new MetricRecord(
        "∆tracing.derived.beachshirts.shopping.orderShirt.invocation.count",
        1.0,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))));

    deltaCounterExpected.add(new MetricRecord(
        "∆tracing.derived.beachshirts.shopping.orderShirt.total_time.millis.count",
        10.0,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
    ));

    histogramExpected.add(new HistogramRecord(
        "tracing.derived.beachshirts.shopping.orderShirt.duration.micros",
        ImmutableList.of(new Pair<>(10000.0, 1)),
        ImmutableSet.of(HistogramGranularity.MINUTE),
        histogramEmittedMinuteMillis,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
    ));

    // Assert metrics data part of RED metrics.
    assertEquals(deltaCounterExpected, deltaMetricsEmitted);

    // Assert histogram data part of RED metrics.
    assertEquals(histogramExpected, histogramsEmitted);
  }

  @Test
  public void testErroneousDeltaSpanRedMetrics() {
    reportWavefrontGeneratedData(
        wavefrontInternalReporter, "orderShirt", "beachshirts", "shopping", "us-west", "primary",
        "localhost", "jdbc", true, 10000L, Sets.newHashSet("location"),
        ImmutableList.of(new Pair<>("location", "SF")), true, () -> wavefrontHistogramClock.get());
    long histogramEmittedMinuteMillis = (wavefrontHistogramClock.get() / 60000L) * 60000L;

    wavefrontHistogramClock.addAndGet(60000L + 1);
    wavefrontInternalReporter.report();

    Set<MetricRecord> deltaCounterExpected = new HashSet<>();
    Set<HistogramRecord> histogramExpected = new HashSet<>();

    deltaCounterExpected.add(new MetricRecord(
        "∆tracing.derived.beachshirts.shopping.orderShirt.invocation.count",
        1.0,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))));

    deltaCounterExpected.add(new MetricRecord(
        "∆tracing.derived.beachshirts.shopping.orderShirt.error.count",
        1.0,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
    ));

    deltaCounterExpected.add(new MetricRecord(
        "∆tracing.derived.beachshirts.shopping.orderShirt.total_time.millis.count",
        10.0,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
    ));

    histogramExpected.add(new HistogramRecord(
        "tracing.derived.beachshirts.shopping.orderShirt.duration.micros",
        ImmutableList.of(new Pair<>(10000.0, 1)),
        ImmutableSet.of(HistogramGranularity.MINUTE),
        histogramEmittedMinuteMillis,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"error", "true"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
    ));

    // Assert metrics data part of RED metrics.
    assertEquals(deltaCounterExpected, deltaMetricsEmitted);

    // Assert histogram data part of RED metrics.
    assertEquals(histogramExpected, histogramsEmitted);
  }

  @Test
  public void testNonErroneousSpanRedMetrics() {
    reportWavefrontGeneratedData(
        wavefrontInternalReporter, "orderShirt", "beachshirts", "shopping", "us-west", "primary",
        "localhost", "jdbc", false, 10000L, Sets.newHashSet("location"),
        ImmutableList.of(new Pair<>("location", "SF")), false, () -> wavefrontHistogramClock.get());
    long histogramEmittedMinuteMillis = (wavefrontHistogramClock.get() / 60000L) * 60000L;

    wavefrontHistogramClock.addAndGet(60000L + 1);
    wavefrontInternalReporter.report();

    Set<MetricRecord> counterExpected = new HashSet<>();
    Set<HistogramRecord> histogramExpected = new HashSet<>();

    counterExpected.add(new MetricRecord(
        "tracing.derived.beachshirts.shopping.orderShirt.invocation.count",
        1.0,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))));

    counterExpected.add(new MetricRecord(
        "tracing.derived.beachshirts.shopping.orderShirt.total_time.millis.count",
        10.0,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
    ));

    histogramExpected.add(new HistogramRecord(
        "tracing.derived.beachshirts.shopping.orderShirt.duration.micros",
        ImmutableList.of(new Pair<>(10000.0, 1)),
        ImmutableSet.of(HistogramGranularity.MINUTE),
        histogramEmittedMinuteMillis,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
    ));

    // Assert metrics data part of RED metrics.
    assertEquals(counterExpected, metricsEmitted);

    // Assert histogram data part of RED metrics.
    assertEquals(histogramExpected, histogramsEmitted);
  }

  @Test
  public void testErroneousSpanRedMetrics() {
    reportWavefrontGeneratedData(
        wavefrontInternalReporter, "orderShirt", "beachshirts", "shopping", "us-west", "primary",
        "localhost", "jdbc", true, 10000L, Sets.newHashSet("location"),
        ImmutableList.of(new Pair<>("location", "SF")), false, () -> wavefrontHistogramClock.get());
    long histogramEmittedMinuteMillis = (wavefrontHistogramClock.get() / 60000L) * 60000L;

    wavefrontHistogramClock.addAndGet(60000L + 1);
    wavefrontInternalReporter.report();

    Set<MetricRecord> counterExpected = new HashSet<>();
    Set<HistogramRecord> histogramExpected = new HashSet<>();

    counterExpected.add(new MetricRecord(
        "tracing.derived.beachshirts.shopping.orderShirt.invocation.count",
        1.0,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))));

    counterExpected.add(new MetricRecord(
        "tracing.derived.beachshirts.shopping.orderShirt.error.count",
        1.0,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
    ));

    counterExpected.add(new MetricRecord(
        "tracing.derived.beachshirts.shopping.orderShirt.total_time.millis.count",
        10.0,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
    ));

    histogramExpected.add(new HistogramRecord(
        "tracing.derived.beachshirts.shopping.orderShirt.duration.micros",
        ImmutableList.of(new Pair<>(10000.0, 1)),
        ImmutableSet.of(HistogramGranularity.MINUTE),
        histogramEmittedMinuteMillis,
        "default-source",
        Stream.of(new String[][]{
            {"component", "jdbc"},
            {"application", "beachshirts"},
            {"service", "shopping"},
            {"cluster", "us-west"},
            {"shard", "primary"},
            {"operationName", "orderShirt"},
            {"span.kind", "none"},
            {"source", "localhost"},
            {"error", "true"},
            {"location", "SF"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]))
    ));

    // Assert metrics data part of RED metrics.
    assertEquals(counterExpected, metricsEmitted);

    // Assert histogram data part of RED metrics.
    assertEquals(histogramExpected, histogramsEmitted);
  }
}
