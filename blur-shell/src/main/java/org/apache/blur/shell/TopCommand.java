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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import jline.Terminal;
import jline.console.ConsoleReader;

import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thirdparty.thrift_0_9_0.transport.TTransportException;
import org.apache.blur.thrift.BlurClientManager;
import org.apache.blur.thrift.Connection;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Client;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.Metric;

public class TopCommand extends Command {

  private static final String HELP = ".help";
  private static final String TOP = "top.";
  private static final String LONGNAME = ".longname";
  private static final String SHORTNAME = ".shortname";
  private static final String UNKNOWN = "Unknown";
  private static final String TOP_SHARD_SERVER_SHORTNAME = "top.SHARD_SERVER.shortname";

  public enum SCREEN {
    HELP, TOP
  }

  private static final double ONE_THOUSAND = 1000;
  private static final double ONE_MILLION = ONE_THOUSAND * ONE_THOUSAND;
  private static final double ONE_BILLION = ONE_THOUSAND * ONE_MILLION;
  private static final double ONE_TRILLION = ONE_THOUSAND * ONE_BILLION;
  private static final double ONE_QUADRILLION = ONE_THOUSAND * ONE_TRILLION;

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
    AtomicBoolean help = new AtomicBoolean();

    Properties properties = new Properties();
    try {
      properties.load(getClass().getResourceAsStream("top.properties"));
    } catch (IOException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
      throw new CommandException(e.getMessage());
    }

    String cluster;
    if (args.length != 2) {
      cluster = Main.getCluster(client, "Invalid args: " + help());
    } else {
      cluster = args[1];
    }

    Map<String, String> metricNames = new HashMap<String, String>();
    Map<String, String> helpMap = new HashMap<String, String>();
    Set<Object> keySet = properties.keySet();
    for (Object k : keySet) {
      String key = k.toString();
      if (isShortName(key)) {
        String shortName = getShortName(key, properties);
        String longName = getLongName(getLongNameKey(key), properties);
        longName = longName.replace("<CLUSTER_NAME>", cluster);
        metricNames.put(shortName, longName);
      } else if (isHelpName(key)) {
        int indexOf = key.indexOf(HELP);
        String strKey = key.substring(0, indexOf);
        Object shortNameKey = properties.get(strKey + SHORTNAME);
        Object helpMessage = properties.get(key);
        if (shortNameKey != null && helpMessage != null) {
          helpMap.put(shortNameKey.toString(), helpMessage.toString());
        }
      }
    }

    String labelsStr = properties.getProperty("top.columns");
    String[] labels = resolveShortNames(labelsStr.split(","), properties);

    String sizesStr = properties.getProperty("top.sizes");
    Set<String> sizes = new HashSet<String>(Arrays.asList(resolveShortNames(sizesStr.split(","), properties)));
    Set<String> keys = new HashSet<String>(metricNames.values());

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

      startCommandWatcher(reader, quit, help, this);
    }

    List<String> shardServerList = new ArrayList<String>(client.shardServerList(cluster));
    Collections.sort(shardServerList);
    Map<String, AtomicReference<Client>> shardClients = setupClients(shardServerList);

    String shardServerLabel = properties.getProperty(TOP_SHARD_SERVER_SHORTNAME);
    int longestServerName = Math.max(getSizeOfLongestKey(shardClients), shardServerLabel.length());

    StringBuilder header = new StringBuilder("%" + longestServerName + "s");
    for (int i = 1; i < labels.length; i++) {
      header.append(" %10s");
    }

    do {
      int lineCount = 0;
      StringBuilder output = new StringBuilder();
      if (quit.get()) {
        return;
      } else if (help.get()) {
        showHelp(output, labels, helpMap);
      } else {
        output.append(truncate(String.format(header.toString(), (Object[]) labels)) + "\n");
        lineCount++;
        SERVER: for (Entry<String, AtomicReference<Client>> e : new TreeMap<String, AtomicReference<Client>>(
            shardClients).entrySet()) {
          String shardServer = e.getKey();
          AtomicReference<Client> ref = e.getValue();
          Map<String, Metric> metrics = getMetrics(shardServer, ref, keys);
          if (metrics == null) {
            String line = String.format("%" + longestServerName + "s*%n", shardServer);
            output.append(line);
            lineCount++;
            if (tooLong(lineCount)) {
              break SERVER;
            }
          } else {
            Object[] cols = new Object[labels.length];
            int c = 0;
            cols[c++] = shardServer;
            StringBuilder sb = new StringBuilder("%" + longestServerName + "s");

            for (int i = 1; i < labels.length; i++) {
              String mn = metricNames.get(labels[i]);
              Metric metric = metrics.get(mn);
              Double value;
              if (metric == null) {
                value = null;
              } else {
                Map<String, Double> doubleMap = metric.getDoubleMap();
                value = doubleMap.get("oneMinuteRate");
                if (value == null) {
                  value = doubleMap.get("value");
                }
              }
              if (value == null) {
                value = 0.0;
              }
              cols[c++] = humanize(value, sizes.contains(mn));
              sb.append(" %10s");
            }
            output.append(truncate(String.format(sb.toString(), cols)) + "\n");
            lineCount++;
            if (tooLong(lineCount)) {
              break SERVER;
            }
          }
        }
      }
      if (reader != null) {
        try {
          reader.clearScreen();
        } catch (IOException e) {
          if (Main.debug) {
            e.printStackTrace();
          }
        }
      }
      out.print(output.toString());
      out.flush();
      if (reader != null) {
        try {
          synchronized (this) {
            wait(3000);
          }
        } catch (InterruptedException e) {
          return;
        }
        Terminal terminal = reader.getTerminal();
        _height = terminal.getHeight() - 2;
        _width = terminal.getWidth() - 2;

        List<String> currentShardServerList = new ArrayList<String>(client.shardServerList(cluster));
        Collections.sort(currentShardServerList);
        if (!shardServerList.equals(currentShardServerList)) {
          close(shardClients);
          shardClients = setupClients(shardServerList);
        }
      }
    } while (reader != null);
  }

  private boolean tooLong(int lineCount) {
    if (lineCount >= _height) {
      return true;
    }
    return false;
  }

  private boolean isHelpName(String key) {
    return key.endsWith(HELP);
  }

  private void close(Map<String, AtomicReference<Client>> shardClients) {
    for (AtomicReference<Client> client : shardClients.values()) {
      tryToClose(client);
    }
  }

  private Map<String, AtomicReference<Client>> setupClients(List<String> shardServerList) {
    Map<String, AtomicReference<Client>> shardClients = new ConcurrentHashMap<String, AtomicReference<Client>>();
    for (String sc : shardServerList) {
      AtomicReference<Client> ref = shardClients.get(sc);
      if (ref == null) {
        ref = new AtomicReference<Client>();
        shardClients.put(sc, ref);
      }
      tryToConnect(sc, ref);
    }
    return shardClients;
  }

  private boolean tryToConnect(String sc, AtomicReference<Client> ref) {
    try {
      Client c = BlurClientManager.newClient(new Connection(sc));
      ref.set(c);
      return true;
    } catch (TTransportException e) {
      ref.set(null);
      if (Main.debug) {
        e.printStackTrace();
      }
    } catch (IOException e) {
      ref.set(null);
      if (Main.debug) {
        e.printStackTrace();
      }
    }
    return false;
  }

  private Map<String, Metric> getMetrics(String sc, AtomicReference<Client> ref, Set<String> keys) {
    if (ref.get() == null) {
      if (!tryToConnect(sc, ref)) {
        return null;
      }
    }
    try {
      return ref.get().metrics(keys);
    } catch (BlurException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
    } catch (TException e) {
      if (Main.debug) {
        e.printStackTrace();
      }
      tryToClose(ref);
    }
    return null;
  }

  private void tryToClose(AtomicReference<Client> ref) {
    Client client = ref.get();
    if (client != null) {
      ref.set(null);
      try {
        client.getInputProtocol().getTransport().close();
      } catch (Exception e) {
        if (Main.debug) {
          e.printStackTrace();
        }
      }
    }
  }

  private String[] resolveShortNames(String[] keys, Properties properties) {
    String[] labels = new String[keys.length];
    int i = 0;
    for (String key : keys) {
      String shortName = properties.getProperty(TOP + key + SHORTNAME);
      labels[i++] = shortName;
    }
    return labels;
  }

  private String getLongNameKey(String key) {
    return key.replace(SHORTNAME, LONGNAME);
  }

  private String getShortName(String key, Properties properties) {
    return properties.getProperty(key, UNKNOWN);
  }

  private String getLongName(String key, Properties properties) {
    return properties.getProperty(key, UNKNOWN);
  }

  private boolean isShortName(String key) {
    return key.endsWith(SHORTNAME);
  }

  private void showHelp(StringBuilder output, Object[] labels, Map<String, String> helpMap) {
    output.append("Help\n");
    for (int i = 0; i < labels.length; i++) {
      String shortName = (String) labels[i];
      String helpMessage = helpMap.get(shortName);
      output.append(String.format("%15s", shortName));
      output.append(" - ");
      output.append(helpMessage);
      output.append('\n');
    }

  }

  private void startCommandWatcher(final ConsoleReader reader, final AtomicBoolean quit, final AtomicBoolean help,
      final Object lock) {
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
            } else if (readCharacter == 'h') {
              help.set(!help.get());
              synchronized (lock) {
                lock.notify();
              }
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
    v = (long) (value / ONE_THOUSAND);
    if (v > 0) {
      return String.format("%7.2f%s", value / ONE_THOUSAND, "K");
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
  public String description() {
    return "Top for watching shard clusters.";
  }

  @Override
  public String usage() {
    return "[<cluster>]";
  }

  @Override
  public String name() {
    return "top";
  }
}
