package org.apache.blur.metrics;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.blur.BlurConfiguration;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.CsvReporter;
import com.yammer.metrics.reporting.GangliaReporter;
import com.yammer.metrics.reporting.GraphiteReporter;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public abstract class ReporterSetup {

  private static final Log LOG = LogFactory.getLog(ReporterSetup.class);

  public static final String BLUR_SHARD_METRICS_REPORTERS = "blur.metrics.reporters";
  public static final String BLUR_SHARD_METRICS_REPORTER_PREFIX = "blur.metrics.reporter.";

  public enum TYPES {
    ConsoleReporter, CsvReporter, GangliaReporter, GraphiteReporter
  }

  public static void setupReporters(BlurConfiguration configuration) {
    String reportersStr = configuration.get(BLUR_SHARD_METRICS_REPORTERS);
    if (reportersStr == null || reportersStr.trim().isEmpty()) {
      return;
    }
    String[] reporters = reportersStr.split(",");
    for (String reporter : reporters) {
      setupReporter(configuration, reporter);
    }
  }

  public static void setupReporter(BlurConfiguration configuration, String reporter) {
    reporter = reporter.trim();
    try {
      TYPES type = TYPES.valueOf(reporter.trim());
      switch (type) {
      case ConsoleReporter:
        setupConsoleReporter(configuration);
        return;
      case CsvReporter:
        setupCsvReporter(configuration);
        return;
      case GangliaReporter:
        setupGangliaReporter(configuration);
        return;
      case GraphiteReporter:
        setupGraphiteReporter(configuration);
        return;
      default:
        break;
      }
    } catch (IllegalArgumentException e) {
      LOG.info("Cannot resolve reporter of type [{0}] trying to find class.", reporter);
      try {
        Class<?> reporterClazz = Class.forName(reporter);
        reflectiveSetup(reporterClazz, configuration, reporter);
      } catch (ClassNotFoundException ex) {
        LOG.error("Cannot find class [{0}]", reporter);
      }
    }
  }

  private static void reflectiveSetup(Class<?> reporterClazz, BlurConfiguration configuration, String reporter) {
    try {
      Object o = reporterClazz.newInstance();
      if (o instanceof ReporterSetup) {
        ReporterSetup reporterSetup = (ReporterSetup) o;
        reporterSetup.setup(configuration);
      } else {
        LOG.error("Could not setup [{0}] because it does not extends [{1}]", reporter, ReporterSetup.class.getName());
      }
    } catch (InstantiationException e) {
      LOG.error("Could not instantiate [{0}]", e, reporter);
    } catch (IllegalAccessException e) {
      LOG.error("Could not instantiate [{0}]", e, reporter);
    }
  }

  private static void setupGraphiteReporter(BlurConfiguration configuration) {
    long period = configuration.getLong(BLUR_SHARD_METRICS_REPORTER_PREFIX + "graphite." + "period", 5l);
    TimeUnit unit = TimeUnit.valueOf(configuration.get(BLUR_SHARD_METRICS_REPORTER_PREFIX + "graphite." + "unit",
        "SECONDS").toUpperCase());
    String host = configuration.get(BLUR_SHARD_METRICS_REPORTER_PREFIX + "graphite." + "host", "localhost");
    int port = configuration.getInt(BLUR_SHARD_METRICS_REPORTER_PREFIX + "graphite." + "port", -1);
    String prefix = configuration.get(BLUR_SHARD_METRICS_REPORTER_PREFIX + "graphite." + "prefix", "");
    GraphiteReporter.enable(period, unit, host, port, prefix);
  }

  private static void setupGangliaReporter(BlurConfiguration configuration) {
    long period = configuration.getLong(BLUR_SHARD_METRICS_REPORTER_PREFIX + "ganglia." + "period", 5l);
    TimeUnit unit = TimeUnit.valueOf(configuration.get(BLUR_SHARD_METRICS_REPORTER_PREFIX + "ganglia." + "unit",
        "SECONDS").toUpperCase());
    String host = configuration.get(BLUR_SHARD_METRICS_REPORTER_PREFIX + "ganglia." + "host", "localhost");
    int port = configuration.getInt(BLUR_SHARD_METRICS_REPORTER_PREFIX + "ganglia." + "port", -1);
    String prefix = configuration.get(BLUR_SHARD_METRICS_REPORTER_PREFIX + "ganglia." + "prefix", "");
    boolean compressPackageNames = configuration.getBoolean(BLUR_SHARD_METRICS_REPORTER_PREFIX + "ganglia."
        + "compressPackageNames", false);
    GangliaReporter.enable(Metrics.defaultRegistry(), period, unit, host, port, prefix, MetricPredicate.ALL,
        compressPackageNames);
  }

  private static void setupCsvReporter(BlurConfiguration configuration) {
    long period = configuration.getLong(BLUR_SHARD_METRICS_REPORTER_PREFIX + "csv." + "period", 5l);
    TimeUnit unit = TimeUnit.valueOf(configuration.get(BLUR_SHARD_METRICS_REPORTER_PREFIX + "csv." + "unit", "SECONDS")
        .toUpperCase());
    File outputDir = new File(configuration.get(BLUR_SHARD_METRICS_REPORTER_PREFIX + "csv." + "outputDir", "."));
    CsvReporter.enable(outputDir, period, unit);
  }

  private static void setupConsoleReporter(BlurConfiguration configuration) {
    long period = configuration.getLong(BLUR_SHARD_METRICS_REPORTER_PREFIX + "console." + "period", 5l);
    TimeUnit unit = TimeUnit.valueOf(configuration.get(BLUR_SHARD_METRICS_REPORTER_PREFIX + "console." + "unit",
        "SECONDS").toUpperCase());
    ConsoleReporter.enable(period, unit);
  }

  public abstract void setup(BlurConfiguration configuration);

}
