/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.blur.shell;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import jline.Terminal;
import jline.console.ConsoleReader;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Metric;

public class TopCommand extends Command {

  public enum SCREEN {
    HELP, TOP
  }

  private static final String SC = "seg cnt";
  private static final String IC = "idx cnt";
  private static final String TC = "tble cnt";
  private static final String RE = "rd rec";
  private static final String IM = "idx mem";
  private static final String RO = "rd row";
  private static final String CS = "bc size";
  private static final String CE = "bc evt";
  private static final String CM = "bc miss";
  private static final String CH = "bc hit";
  private static final String IQ = "in qry";
  private static final String EQ = "ex qry";
  private static final String SHARD_SERVER = "Shard Server";
  private static final String INTERNAL_QUERIES = "\"org.apache.blur\":type=\"Blur\",name=\"Internal Queries/s\"";
  private static final String EXTERNAL_QUERIES = "\"org.apache.blur\":type=\"Blur\",name=\"External Queries/s\"";

  private static final String CACHE_HIT = "\"org.apache.blur\":type=\"Cache\",name=\"Hit\"";
  private static final String CACHE_MISS = "\"org.apache.blur\":type=\"Cache\",name=\"Miss\"";
  private static final String CACHE_EVICTION = "\"org.apache.blur\":type=\"Cache\",name=\"Eviction\"";
  private static final String CACHE_SIZE = "\"org.apache.blur\":type=\"Cache\",name=\"Size\"";

  private static final String READ_RECORDS = "\"org.apache.blur\":type=\"Blur\",name=\"Read Records/s\"";
  private static final String READ_ROWS = "\"org.apache.blur\":type=\"Blur\",name=\"Read Row/s\"";

  private static final String INDEX_MEMORY_USAGE = "\"org.apache.blur\":type=\"Blur\",scope=\"default\",name=\"Index Memory Usage\"";
  private static final String TABLE_COUNT = "\"org.apache.blur\":type=\"Blur\",scope=\"default\",name=\"Table Count\"";
  private static final String INDEX_COUNT = "\"org.apache.blur\":type=\"Blur\",scope=\"default\",name=\"Index Count\"";
  private static final String SEGMENT_COUNT = "\"org.apache.blur\":type=\"Blur\",scope=\"default\",name=\"Segment Count\"";
  private static final double ONE_MILLION = 1000000;
  private static final double ONE_BILLION = 1000 * ONE_MILLION;
  private static final double ONE_TRILLION = 1000 * ONE_BILLION;
  private static final double ONE_QUADRILLION = 1000 * ONE_TRILLION;

  private int _width = Integer.MAX_VALUE;
  private int _height;

  @Override
  public void doit(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {
    try {
      doitInternal(out, client, args);
    } finally {
      ConsoleReader reader = this.getConsoleReader();
      if (reader != null) {
        reader.setPrompt(Main.PROMPT);
      }
    }
  }

  public void doitInternal(PrintWriter out, Blur.Iface client, String[] args) throws CommandException, TException,
      BlurException {

    AtomicBoolean quit = new AtomicBoolean();

    Map<String, String> metricNames = new HashMap<String, String>();
    metricNames.put(IQ, INTERNAL_QUERIES);
    metricNames.put(EQ, EXTERNAL_QUERIES);
    metricNames.put(CH, CACHE_HIT);
    metricNames.put(CM, CACHE_MISS);
    metricNames.put(CE, CACHE_EVICTION);
    metricNames.put(CS, CACHE_SIZE);
    metricNames.put(RO, READ_RECORDS);
    metricNames.put(RE, READ_ROWS);
    metricNames.put(IM, INDEX_MEMORY_USAGE);
    metricNames.put(TC, TABLE_COUNT);
    metricNames.put(IC, INDEX_COUNT);
    metricNames.put(SC, SEGMENT_COUNT);

    Object[] labels = new Object[] { SHARD_SERVER, EQ, IQ, CH, CM, CE, CS, RO, RE, IM, TC, IC, SC };

    Set<String> sizes = new HashSet<String>();
    sizes.add(IM);

    Set<String> keys = new HashSet<String>(metricNames.values());

    String cluster;
    if (args.length != 2) {
      cluster = Main.getCluster(client, "Invalid args: " + help());
    } else {
      cluster = args[1];
    }

    List<String> shardServerList = client.shardServerList(cluster);

    ConsoleReader reader = this.getConsoleReader();
    if (reader != null) {
      Terminal terminal = reader.getTerminal();
      _height = terminal.getHeight() - 2;
      _width = terminal.getWidth() - 2;
      try {
        reader.setPrompt("");
        reader.clearScreen();
      } catch (IOException e) {
        if (Main.debug) {
          e.printStackTrace();
        }
      }
      startCommandWatcher(reader, quit, this);
    }

    Map<String, Blur.Iface> shardClients = new HashMap<String, Blur.Iface>();
    for (String sc : shardServerList) {
      shardClients.put(sc, BlurClient.getClient(sc));
    }

    int longestServerName = Math.max(getSizeOfLongestKey(shardClients), SHARD_SERVER.length());

    StringBuilder header = new StringBuilder("%" + longestServerName + "s");
    for (int i = 1; i < labels.length; i++) {
      header.append(" %10s");
    }
    header.append("%n");

    do {
      if (quit.get()) {
        return;
      }
      out.printf(truncate(header.toString()), labels);
      for (Entry<String, Blur.Iface> e : shardClients.entrySet()) {
        String shardServer = e.getKey();
        Iface shardClient = e.getValue();
        Object[] cols = new Object[labels.length];
        int c = 0;
        cols[c++] = shardServer;
        StringBuilder sb = new StringBuilder("%" + longestServerName + "s");

        Map<String, Metric> metrics = shardClient.metrics(keys);
        for (int i = 1; i < labels.length; i++) {
          String mn = metricNames.get(labels[i]);
          Metric metric = metrics.get(mn);
          Map<String, Double> doubleMap = metric.getDoubleMap();
          Double value = doubleMap.get("oneMinuteRate");
          if (value == null) {
            value = doubleMap.get("value");
          }
          cols[c++] = humanize(value, sizes.contains(mn));
          sb.append(" %10s");
        }
        sb.append("%n");
        out.printf(truncate(sb.toString()), cols);
      }
      out.flush();
      if (reader != null) {
        try {
          synchronized (this) {
            wait(3000);
          }
        } catch (InterruptedException e) {
          return;
        }
        try {
          reader.clearScreen();
        } catch (IOException e) {
          if (Main.debug) {
            e.printStackTrace();
          }
        }
        Terminal terminal = reader.getTerminal();
        _height = terminal.getHeight() - 2;
        _width = terminal.getWidth() - 2;
      }
    } while (reader != null);

  }

  private void startCommandWatcher(final ConsoleReader reader, final AtomicBoolean quit, final Object lock) {
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          while (true) {
            int readCharacter = reader.readCharacter();
            if (readCharacter == 'q') {
              quit.set(true);
              synchronized (lock) {
                lock.notify();
              }
              return;
            }
          }
        } catch (IOException e) {
          if (Main.debug) {
            e.printStackTrace();
          }
        }
      }
    });
    thread.setDaemon(true);
    thread.start();
  }

  private String truncate(String s) {
    return s.substring(0, Math.min(_width, s.length()));
  }

  private String humanize(double value, boolean size) {
    long v = (long) (value / ONE_QUADRILLION);
    if (v > 0) {
      return String.format("%7.2f%s", value / ONE_QUADRILLION, size ? "Q" : "P");
    }
    v = (long) (value / ONE_TRILLION);
    if (v > 0) {
      return String.format("%7.2f%s", value / ONE_TRILLION, "T");
    }
    v = (long) (value / ONE_BILLION);
    if (v > 0) {
      return String.format("%7.2f%s", value / ONE_BILLION, size ? "B" : "G");
    }
    v = (long) (value / ONE_MILLION);
    if (v > 0) {
      return String.format("%7.2f%s", value / ONE_MILLION, "M");
    }
    return String.format("%7.2f", value);
  }

  private int getSizeOfLongestKey(Map<String, ?> map) {
    int i = 0;
    for (String s : map.keySet()) {
      int length = s.length();
      if (i < length) {
        i = length;
      }
    }
    return i;
  }

  @Override
  public String help() {
    return "top arg; cluster";
  }
}
