# wavefront-internal-reporter-java [![build status][ci-img]][ci] [![Released Version][maven-img]][maven]

[ci-img]: https://travis-ci.com/wavefrontHQ/wavefront-internal-reporter-java.svg?branch=master
[ci]: https://travis-ci.com/wavefrontHQ/wavefront-internal-reporter-java
[maven-img]: https://img.shields.io/maven-central/v/com.wavefront/wavefront-internal-reporter-java.svg?maxAge=604800
[maven]: http://search.maven.org/#search%7Cga%7C1%7Cwavefront-internal-reporter-java

Internal metrics reporter sdk which offers common metrics reporting functionality to other Observability SDKs in Java. <br />
**Note:** Users are not supposed to use this SDK directly as it is internal building block for other sdks.

## Internal Diagnostic Metrics

This SDK automatically collects a set of diagnostic metrics that allow you to monitor your `WavefrontInternalReporter` instance. These metrics are collected once per minute and are reported to Wavefront using your `WavefrontSender` instance.

The following is a list of the diagnostic metrics that are collected:

|Metric Name|Metric Type|Description|
|:---|:---:|:---|
|~sdk.java.internal_reporter.gauges.reported.count                |Counter    |Times that gauges are reported|
|~sdk.java.internal_reporter.delta_counters.reported.count        |Counter    |Times that delta counters are reported|
|~sdk.java.internal_reporter.counters.reported.count              |Counter    |Times that non-delta counters are reported|
|~sdk.java.internal_reporter.wavefront_histograms.reported.count  |Counter    |Times that Wavefront histograms are reported|
|~sdk.java.internal_reporter.histograms.reported.count            |Counter    |Times that non-Wavefront histograms are reported|
|~sdk.java.internal_reporter.meters.reported.count                |Counter    |Times that meters are reported|
|~sdk.java.internal_reporter.timers.reported.count                |Counter    |Times that timers are reported|
|~sdk.java.internal_reporter.errors.count                         |Counter    |Exceptions encountered while reporting|

Each of the above metrics is reported with the same source and application tags that are specified for your `WavefrontInternalReporter`.

For information regarding diagnostic metrics for your `WavefrontSender` instance, [see here](https://github.com/wavefrontHQ/wavefront-sdk-java/tree/master/docs/internal_metrics.md).
