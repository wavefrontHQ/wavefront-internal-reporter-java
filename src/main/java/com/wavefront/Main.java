package com.wavefront;

import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.sdk.direct.ingestion.WavefrontDirectIngestionClient;
import com.wavefront.sdk.proxy.WavefrontProxyClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.dropwizard.metrics5.Counter;
import io.dropwizard.metrics5.DeltaCounter;
import io.dropwizard.metrics5.Gauge;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.Meter;
import io.dropwizard.metrics5.MetricName;
import io.dropwizard.metrics5.MetricRegistry;
import io.dropwizard.metrics5.SlidingTimeWindowArrayReservoir;
import io.dropwizard.metrics5.SlidingTimeWindowReservoir;
import io.dropwizard.metrics5.Timer;
import io.dropwizard.metrics5.WavefrontHistogram;

/**
 * Driver class for ad-hoc experiments
 *
 * @author Sushant Dewan (sushant@wavefront.com).
 */
public class Main {

  public static void main(String[] args) throws InterruptedException, IOException {
    String wavefrontServer = args[0];
    String token = args[1];
    String proxyHost = args.length < 3 ? null : args[2];
    String metricsPort = args.length < 4 ? null : args[3];
    String distributionPort = args.length < 5 ? null : args[4];

    WavefrontProxyClient.Builder proxyBuilder = new WavefrontProxyClient.Builder(proxyHost);
    if (metricsPort != null) {
      proxyBuilder.metricsPort(Integer.parseInt(metricsPort));
    }
    if (distributionPort != null) {
      proxyBuilder.distributionPort(Integer.parseInt(distributionPort));
    }
    WavefrontProxyClient wavefrontProxyClient = proxyBuilder.build();

    WavefrontDirectIngestionClient wavefrontDirectIngestionClient =
        new WavefrontDirectIngestionClient.Builder(wavefrontServer, token).build();

    WavefrontInternalReporter.Builder builder = new WavefrontInternalReporter.Builder();

    /* Set the source for your metrics and histograms */
    builder.withSource("mySource");

    /* Invoke this method to report your metrics and histograms with given prefix */
    builder.prefixedWith("myInternal");

    Map<String, String> reporterTagsMap = new HashMap<String, String>() {{
      put("env", "Staging");
      put("location", "SF");
    }};

    /* Set reporter level point tags map for your metrics and histograms */
    builder.withReporterPointTags(reporterTagsMap);

    /* Add a specific reporter level point tag key value for your metrics and histograms */
    builder.withReporterPointTag("cluster", "us-west");

    /* Invoke this method if you want to report minute bin Wavefront histograms */
    builder.reportMinuteDistribution();

    /* Invoke this method if you want to report hour bin Wavefront histograms  */
    builder.reportHourDistribution();

    /* Invoke this method if you want to report day bin Wavefront histograms */
    builder.reportDayDistribution();

    WavefrontInternalReporter internalReporter =
        builder.build(wavefrontDirectIngestionClient);

    /*
     * Instead of direct ingestion, you can also report the metrics and histograms to Wavefront
     * via proxy using the below line of code
     */
    //WavefrontInternalReporter internalReporter = builder.build(wavefrontProxyClient);

    /* Report metrics and histograms to Wavefront every 30 seconds */
    internalReporter.start(30, TimeUnit.SECONDS);

    Map<String, String> pointTagMap = new HashMap<String, String>() {{
      put("application", "Wavefront");
    }};
    Counter counter = internalReporter.newCounter(new MetricName("myCounter", pointTagMap));

    DeltaCounter deltaCounter = internalReporter.newDeltaCounter(
        new MetricName("myDeltaCounter", pointTagMap));
    AtomicInteger bufferSize = new AtomicInteger();
    Gauge genericGauge = internalReporter.newGauge(new MetricName("myGauge", pointTagMap),
        () -> () -> (double) bufferSize.get());
    // Unchecked assignment (cannnot prevent this with current WavefrontInternalReporter design).
    Gauge<Double> doubleGauge = genericGauge;
    System.out.println("Current gauge value: " + doubleGauge.getValue());
    Meter meter = internalReporter.newMeter(new MetricName("myMeter", pointTagMap));
    Timer timer = internalReporter.newTimer(new MetricName("myTimer", pointTagMap));
    Histogram histogram = internalReporter.newHistogram(new MetricName("myHistogram",
        pointTagMap));
    WavefrontHistogram wavefrontHistogram = internalReporter.newWavefrontHistogram(
        new MetricName("myWavefrontHistogram", pointTagMap));

    for (int i = 0; i < 50; i++) {
      counter.inc();
      deltaCounter.inc();
      bufferSize.set(10 * i);
      meter.mark(i);
      timer.update(i, TimeUnit.SECONDS);
      histogram.update(i);
      wavefrontHistogram.update(i);
      Thread.sleep(50);
    }
  }
}
