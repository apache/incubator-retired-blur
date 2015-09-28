/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.blur.spark;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.blur.command.stream.StreamClient;
import org.apache.blur.command.stream.StreamFunction;
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;
import org.apache.blur.utils.BlurConstants;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.google.common.base.Splitter;
import com.google.common.io.Closer;

@SuppressWarnings("serial")
public class BlurRDD implements Serializable {

  private final static String CLASS_LOADER_ID = UUID.randomUUID().toString();

  private final transient Iface _client;
  private final transient SparkConf _sparkConf;
  private final List<String> _jars = new ArrayList<String>();
  private final int _timeout = 60000;

  public BlurRDD(String zooKeeperConnectionStr, SparkConf sparkConf) throws IOException {
    this(BlurClient.getClientFromZooKeeperConnectionStr(zooKeeperConnectionStr), sparkConf);
  }

  public BlurRDD(Iface client, SparkConf sparkConf) throws IOException {
    _client = client;
    _sparkConf = sparkConf;
    if (_sparkConf.contains("spark.jars")) {
      String jars = _sparkConf.get("spark.jars");
      for (String jar : Splitter.on(',').split(jars)) {
        _jars.add(jar);
      }
    }
  }

  public <T> JavaRDD<T> executeStream(JavaSparkContext context, String table, StreamFunction<T> streamFunction) {
    User user = UserContext.getUser();
    List<BlurSparkSplit> splits = getSplits(table, user, CLASS_LOADER_ID);
    return context.parallelize(splits).flatMap(new FlatMapFunction<BlurSparkSplit, T>() {
      @Override
      public Iterable<T> call(BlurSparkSplit t) throws Exception {
        return new Iterable<T>() {
          @Override
          public Iterator<T> iterator() {
            Closer closer = Closer.create();
            try {
              String host = t.getHost();
              int port = t.getPort();
              int timeout = t.getTimeout();
              StreamClient streamClient = closer.register(new StreamClient(host, port, timeout));
              String classLoaderId = t.getClassLoaderId();
              if (!streamClient.isClassLoaderAvailable(classLoaderId)) {
                streamClient.loadJars(classLoaderId, _jars);
              }
              return wrapClose(closer, streamClient.executeStream(t, streamFunction).iterator());
            } catch (IOException e) {
              IOUtils.closeQuietly(closer);
              throw new RuntimeException(e);
            }
          }
        };
      }
    });
  }

  private static <T> Iterator<T> wrapClose(Closeable c, Iterator<T> t) {
    return new Iterator<T>() {

      @Override
      public boolean hasNext() {
        try {
          boolean hasNext = t.hasNext();
          if (!hasNext) {
            IOUtils.closeQuietly(c);
          }
          return hasNext;
        } catch (Throwable t) {
          IOUtils.closeQuietly(c);
          if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
          } else {
            throw new RuntimeException(t);
          }
        }
      }

      @Override
      public T next() {
        return t.next();
      }
    };
  }

  private List<BlurSparkSplit> getSplits(String table, User user, String classLoaderId) {
    try {
      Map<String, String> shardServerLayout = _client.shardServerLayout(table);
      List<BlurSparkSplit> splits = new ArrayList<BlurSparkSplit>();
      for (Entry<String, String> e : shardServerLayout.entrySet()) {
        String shard = e.getKey();
        String shardServerWithThriftPort = e.getValue();
        String host = getHost(shardServerWithThriftPort);
        int port = getStreamPort(shardServerWithThriftPort);
        String username = user.getUsername();
        Map<String, String> attributes = user.getAttributes();
        splits.add(new BlurSparkSplit(host, port, _timeout, table, shard, classLoaderId, username, attributes));
      }
      return splits;
    } catch (BlurException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private String getHost(String shardServerWithThriftPort) {
    return shardServerWithThriftPort.substring(0, shardServerWithThriftPort.indexOf(':'));
  }

  private int getStreamPort(String shardServerWithThriftPort) throws BlurException, TException {
    String port = _client.configurationPerServer(shardServerWithThriftPort,
        BlurConstants.BLUR_STREAM_SERVER_RUNNING_PORT);
    return Integer.parseInt(port);
  }

  public static byte[] toBytes(Serializable s) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
      objectOutputStream.writeObject(s);
      objectOutputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return outputStream.toByteArray();
  }

}
