package io.dropwizard.metrics5;

import com.wavefront.sdk.common.Constants;

/**
 * Wavefront Delta Counter
 *
 * @author Sushant Dewan (sushant@wavefront.com).
 */
public class DeltaCounter extends Counter {

  public static synchronized DeltaCounter get(MetricRegistry registry, MetricName metricName) {

    if (registry == null || metricName == null || metricName.getKey().isEmpty()) {
      throw new IllegalArgumentException("Invalid arguments");
    }

    if (!metricName.getKey().startsWith(Constants.DELTA_PREFIX) &&
        !metricName.getKey().startsWith(Constants.DELTA_PREFIX_2)) {
      metricName = new MetricName(Constants.DELTA_PREFIX + metricName.getKey(),
          metricName.getTags());
    }
    DeltaCounter counter = new DeltaCounter();
    try {
      Metric metric = registry.getMetrics().get(metricName);
      if (metric instanceof DeltaCounter) {
        return (DeltaCounter) metric;
      }
      return registry.register(metricName, counter);
    } catch (IllegalArgumentException e) {
      Counter existing = registry.counter(metricName);
      if (existing instanceof DeltaCounter) {
        return (DeltaCounter) existing;
      } else {
        throw new IllegalStateException("Existing metric of type: Counter found registered to " +
            "metricName: " + metricName);
      }
    }
  }
}
