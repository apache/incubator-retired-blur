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
package org.apache.blur.command.stream;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;

import com.google.common.io.Closer;

public class StreamClient implements Closeable {

  private static final Log LOG = LogFactory.getLog(StreamClient.class);

  private final String _host;
  private final int _port;
  private final Socket _socket;
  private final DataInputStream _dataInputStream;
  private final DataOutputStream _dataOutputStream;
  private final Closer _closer = Closer.create();

  public StreamClient(String host, int port, int timeout) throws IOException {
    _host = host;
    _port = port;
    _socket = _closer.register(new Socket(_host, _port));
    _socket.setTcpNoDelay(true);
    _socket.setSoTimeout(timeout);
    _dataInputStream = new DataInputStream(_closer.register(_socket.getInputStream()));
    _dataOutputStream = new DataOutputStream(_closer.register(_socket.getOutputStream()));
  }

  public String getHost() {
    return _host;
  }

  public int getPort() {
    return _port;
  }

  public DataInputStream getDataInputStream() {
    return _dataInputStream;
  }

  public DataOutputStream getDataOutStream() {
    return _dataOutputStream;
  }

  @Override
  public void close() throws IOException {
    _closer.close();
  }

  public boolean isClassLoaderAvailable(String classLoaderId) throws IOException {
    _dataOutputStream.write(StreamCommand.CLASS_LOAD_CHECK.getCommand());
    StreamUtil.writeString(_dataOutputStream, classLoaderId);
    _dataOutputStream.flush();
    String message = StreamUtil.readString(_dataInputStream);
    if (message.equals("OK")) {
      return true;
    } else {
      return false;
    }
  }

  public void loadJars(String classLoaderId, Iterable<String> testJars) throws IOException {
    _dataOutputStream.write(StreamCommand.CLASS_LOAD.getCommand());
    StreamUtil.writeString(_dataOutputStream, classLoaderId);
    sendZip(testJars, _dataOutputStream);
    _dataOutputStream.flush();
    String message = StreamUtil.readString(_dataInputStream);
    if (!message.equals("LOADED")) {
      throw new IOException("Unknown error during load, check logs on server.");
    }
  }

  private void sendZip(Iterable<String> testJars, DataOutputStream outputStream) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ZipOutputStream zipOutputStream = new ZipOutputStream(out);
    for (String jar : testJars) {
      LOG.info("Sending jar [{0}]", jar);
      File file = new File(jar);
      FileInputStream in = new FileInputStream(file);
      zipOutputStream.putNextEntry(new ZipEntry(file.getName()));
      IOUtils.copy(in, zipOutputStream);
      in.close();
      zipOutputStream.closeEntry();
    }
    zipOutputStream.close();
    byte[] bs = out.toByteArray();
    outputStream.writeInt(bs.length);
    outputStream.write(bs);
  }

  public void loadJars(String classLoaderId, String... testJars) throws IOException {
    loadJars(classLoaderId, Arrays.asList(testJars));
  }

  public <T> Iterable<T> executeStream(StreamSplit streamSplit, StreamFunction<T> streamFunction) throws IOException {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator() {
        try {
          _dataOutputStream.write(StreamCommand.STREAM.getCommand());
          byte[] streamSplitBytes = StreamUtil.toBytes(streamSplit.copy());
          byte[] streamFunctionBytes = StreamUtil.toBytes(streamFunction);
          _dataOutputStream.writeInt(streamSplitBytes.length);
          _dataOutputStream.write(streamSplitBytes);
          _dataOutputStream.writeInt(streamFunctionBytes.length);
          _dataOutputStream.write(streamFunctionBytes);
          _dataOutputStream.flush();
          ObjectInputStream objectInputStream = new ObjectInputStream(_dataInputStream);
          return new Iterator<T>() {

            private boolean _more = true;
            private Object _obj;

            @Override
            public boolean hasNext() {
              if (!_more) {
                return false;
              }
              if (_obj != null) {
                return true;
              }
              Object o;
              try {
                o = objectInputStream.readObject();
              } catch (ClassNotFoundException | IOException e) {
                throw new RuntimeException(e);
              }
              if (o instanceof StreamComplete) {
                return _more = false;
              }
              _obj = o;
              return true;
            }

            @SuppressWarnings("unchecked")
            @Override
            public T next() {
              T o = (T) _obj;
              _obj = null;
              return o;
            }
          };
        } catch (IOException e) {
          throw new RuntimeException("Unknown error.", e);
        }
      }
    };
  }
}
