package com.wavefront.internal;

import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.common.WavefrontSender;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.dropwizard.metrics5.MetricName;

import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.COMPONENT_TAG_KEY;
import static com.wavefront.sdk.common.Constants.HEART_BEAT_METRIC;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SOURCE_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Utils.sanitizeWithoutQuotes;

/**
 * Util methods to generate data (metrics/histograms/heartbeats) from tracing spans
 *
 * @author Hao Song (songhao@vmware.com).
 */
public class SpanDerivedMetricsUtils {

  public final static String TRACING_DERIVED_PREFIX = "tracing.derived";
  public final static String ERROR_SPAN_TAG_KEY = "error";
  public final static String ERROR_SPAN_TAG_VAL = "true";
  public final static String DEBUG_SPAN_TAG_KEY = "debug";
  public final static String DEBUG_SPAN_TAG_VAL = "true";

  private final static String INVOCATION_SUFFIX = ".invocation";
  private final static String ERROR_SUFFIX = ".error";
  private final static String DURATION_SUFFIX = ".duration.micros";
  private final static String TOTAL_TIME_SUFFIX = ".total_time.millis";
  private final static String OPERATION_NAME_TAG = "operationName";
  private static final String SPAN_KIND_KEY = "span.kind";
  private static final String HTTP_STATUS_KEY = "http.status_code";

  /**
   * Report generated metrics and histograms from the wavefront tracing span using delta counter.
   *
   * @param operationName             span operation name.
   * @param application               name of the application.
   * @param service                   name of the service.
   * @param cluster                   name of the cluster.
   * @param shard                     name of the shard.
   * @param source                    reporting source.
   * @param componentTagValue         component tag value.
   * @param isError                   indicates if the span is erroneous.
   * @param spanDurationMicros        Original span duration (both Zipkin and Jaeger support micros
   *                                  duration).
   * @param traceDerivedCustomTagKeys custom tags added to derived RED metrics.
   * @param spanTags                  span tags.
   * @return Pair of Heartbeat custom tags and source.
   */
  @Nonnull
  public static Pair<Map<String, String>, String> reportWavefrontGeneratedData(
      @Nonnull WavefrontInternalReporter wfInternalReporter, @Nonnull String operationName,
      @Nonnull String application, @Nonnull String service, String cluster, String shard,
      String source, String componentTagValue, boolean isError, long spanDurationMicros,
      Set<String> traceDerivedCustomTagKeys, List<Pair<String, String>> spanTags) {
    return reportWavefrontGeneratedData(wfInternalReporter, operationName, application, service,
        cluster, shard, source, componentTagValue, isError, spanDurationMicros,
        traceDerivedCustomTagKeys, spanTags, true, System::currentTimeMillis);
  }

  /**
   * Report generated metrics and histograms from the wavefront tracing span.
   *
   * @param operationName             span operation name.
   * @param application               name of the application.
   * @param service                   name of the service.
   * @param cluster                   name of the cluster.
   * @param shard                     name of the shard.
   * @param source                    reporting source.
   * @param componentTagValue         component tag value.
   * @param isError                   indicates if the span is erroneous.
   * @param spanDurationMicros        Original span duration (both Zipkin and Jaeger support micros
   *                                  duration).
   * @param traceDerivedCustomTagKeys custom tags added to derived RED metrics.
   * @param spanTags                  span tags.
   * @param isReportingDelta          reporting delta counters or normal counters.
   * @return Pair of Heartbeat custom tags and source.
   */
  @Nonnull
  public static Pair<Map<String, String>, String> reportWavefrontGeneratedData(
      @Nonnull WavefrontInternalReporter wfInternalReporter, @Nonnull String operationName,
      @Nonnull String application, @Nonnull String service, String cluster, String shard,
      String source, String componentTagValue, boolean isError, long spanDurationMicros,
      Set<String> traceDerivedCustomTagKeys, List<Pair<String, String>> spanTags,
      boolean isReportingDelta) {
    return reportWavefrontGeneratedData(wfInternalReporter, operationName, application, service,
        cluster, shard, source, componentTagValue, isError, spanDurationMicros,
        traceDerivedCustomTagKeys, spanTags, isReportingDelta, System::currentTimeMillis);
  }

  @Nonnull
  protected static Pair<Map<String, String>, String> reportWavefrontGeneratedData(
      @Nonnull WavefrontInternalReporter wfInternalReporter, String operationName, String application,
      String service, String cluster, String shard, String source, String componentTagValue,
      boolean isError, long spanDurationMicros, Set<String> traceDerivedCustomTagKeys,
      List<Pair<String, String>> spanTags, boolean isReportingDelta, Supplier<Long> clock) {

    Map<String, String> pointTags = new HashMap<>();
    source = getNonEmptyOrDefaultValue(source, "unknown_source");

    /*
     * 1) Can only propagate mandatory application/service and optional cluster/shard tags.
     * 2) Cannot convert ApplicationTags.customTags unfortunately as those are not well-known.
     * 3) Both Jaeger and Zipkin support error=true tag for erroneous spans
     */
    pointTags.put(APPLICATION_TAG_KEY, getNonEmptyOrDefaultValue(application, "unknown_application"));
    pointTags.put(SERVICE_TAG_KEY, getNonEmptyOrDefaultValue(service, "unknown_service"));
    pointTags.put(CLUSTER_TAG_KEY, getNonEmptyOrDefaultValue(cluster, NULL_TAG_VAL));
    pointTags.put(SHARD_TAG_KEY, getNonEmptyOrDefaultValue(shard, NULL_TAG_VAL));
    pointTags.put(OPERATION_NAME_TAG, getNonEmptyOrDefaultValue(operationName, "unknown_operation"));
    pointTags.put(COMPONENT_TAG_KEY, getNonEmptyOrDefaultValue(componentTagValue, NULL_TAG_VAL));
    pointTags.put(SOURCE_KEY, source);

    boolean hasTraceDerivedCustomTagKeys = traceDerivedCustomTagKeys != null && traceDerivedCustomTagKeys.size() > 0;
    spanTags.forEach((tag) -> {
      String tagKey = tag._1;
      String tagValue = tag._2;

      // propagate trace derived custom tags
      if (hasTraceDerivedCustomTagKeys && traceDerivedCustomTagKeys.contains(tagKey)) {
        pointTags.put(tagKey, tagValue);
      }

      // propagate http status
      if (tagKey.equalsIgnoreCase(HTTP_STATUS_KEY)) {
        pointTags.put(HTTP_STATUS_KEY, tagValue);
      }
    });

    // span.kind tag will be promoted by default
    pointTags.putIfAbsent(SPAN_KIND_KEY, NULL_TAG_VAL);

    // tracing.derived.<application>.<service>.<operation>.invocation.count
    incCounter(wfInternalReporter, new MetricName(sanitizeWithoutQuotes(application +
        "." + service + "." + operationName + INVOCATION_SUFFIX), pointTags), isReportingDelta);

    if (isError) {
      // tracing.derived.<application>.<service>.<operation>.error.count
      incCounter(wfInternalReporter, new MetricName(sanitizeWithoutQuotes(application +
          "." + service + "." + operationName + ERROR_SUFFIX), pointTags), isReportingDelta);
    }

    // tracing.derived.<application>.<service>.<operation>.duration.micros.m
    if (isError) {
      Map<String, String> errorPointTags = new HashMap<>(pointTags);
      errorPointTags.put("error", "true");
      wfInternalReporter.newWavefrontHistogram(new MetricName(sanitizeWithoutQuotes(application +
          "." + service + "." + operationName + DURATION_SUFFIX), errorPointTags), clock).
          update(spanDurationMicros);
    } else {
      wfInternalReporter.newWavefrontHistogram(new MetricName(sanitizeWithoutQuotes(application +
          "." + service + "." + operationName + DURATION_SUFFIX), pointTags), clock).
          update(spanDurationMicros);
    }

    // tracing.derived.<application>.<service>.<operation>.total_time.millis.count
    incCounter(wfInternalReporter, new MetricName(sanitizeWithoutQuotes(application +
        "." + service + "." + operationName + TOTAL_TIME_SUFFIX), pointTags), isReportingDelta,
        spanDurationMicros / 1000);

    // Remove operation tag and source tag from tags list before sending RED heartbeat.
    pointTags.remove(OPERATION_NAME_TAG);
    pointTags.remove(SOURCE_KEY);

    return new Pair<>(pointTags, source);
  }

  private static void incCounter(WavefrontInternalReporter wfInternalReporter,
                                 MetricName metric, boolean isReportingDelta) {
    incCounter(wfInternalReporter, metric, isReportingDelta, 1L);
  }

  private static void incCounter(WavefrontInternalReporter wfInternalReporter,
                                 MetricName metric, boolean isReportingDelta, long n) {
    if (isReportingDelta) {
      wfInternalReporter.newDeltaCounter(metric).inc(n);
    } else {
      wfInternalReporter.newCounter(metric).inc(n);
    }
  }

  /**
   * Report discovered heartbeats to Wavefront.
   *
   * @param wavefrontSender            Wavefront sender via proxy.
   * @param discoveredHeartbeatMetrics Discovered heartbeats.
   */
  public static void reportHeartbeats(WavefrontSender wavefrontSender,
                                      Set<Pair<Map<String, String>, String>> discoveredHeartbeatMetrics)
      throws IOException {
    reportHeartbeats(wavefrontSender, discoveredHeartbeatMetrics, "");
  }

  /**
   * Report discovered heartbeats to Wavefront.
   *
   * @param wavefrontSender            Wavefront sender via proxy.
   * @param discoveredHeartbeatMetrics Discovered heartbeats.
   * @param secondaryComponent         Secondary component value need to be sent in heartbeats
   *                                   metric.
   */
  public static void reportHeartbeats(WavefrontSender wavefrontSender,
                                      Set<Pair<Map<String, String>, String>> discoveredHeartbeatMetrics,
                                      @Nonnull String secondaryComponent) throws IOException {
    if (wavefrontSender == null) {
      // should never happen
      return;
    }
    Iterator<Pair<Map<String, String>, String>> iter = discoveredHeartbeatMetrics.iterator();
    while (iter.hasNext()) {
      Pair<Map<String, String>, String> key = iter.next();

      Map<String, String> tags = new HashMap<>(key._1);
      String source = key._2;

      wavefrontSender.sendMetric(HEART_BEAT_METRIC, 1.0, System.currentTimeMillis(), source, tags);
      if (!secondaryComponent.trim().isEmpty()) {
        tags.put(COMPONENT_TAG_KEY, secondaryComponent);
        wavefrontSender.sendMetric(HEART_BEAT_METRIC, 1.0, System.currentTimeMillis(), source, tags);
      }
      // remove from discovered list so that it is only reported on subsequent discovery
      discoveredHeartbeatMetrics.remove(key);
    }
  }

  private static String getNonEmptyOrDefaultValue(@Nullable String inputValue,
                                                  String defaultValue) {
    if (inputValue == null || inputValue.trim().isEmpty()) {
      return defaultValue;
    }
    return inputValue;
  }
}
