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

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.blur.concurrent.Executors;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.trace.Trace;
import org.apache.blur.trace.Tracer;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;
import org.apache.commons.io.IOUtils;

import com.google.common.io.Closer;

public class StreamServer implements Closeable {

  private static final Log LOG = LogFactory.getLog(StreamServer.class);

  private final int _port;
  private final int _threadCount;
  private final StreamProcessor _streamProcessor;
  private final Closer _closer = Closer.create();
  private final AtomicBoolean _running = new AtomicBoolean(true);

  private ServerSocket _serverSocket;
  private ExecutorService _service;
  private Thread _thread;
  private int _runningPort;

  public StreamServer(int port, int threadCount, StreamProcessor streamProcessor) {
    _port = port;
    _threadCount = threadCount;
    _streamProcessor = streamProcessor;
  }

  @Override
  public void close() throws IOException {
    _closer.close();
  }

  public void start() throws IOException {
    _service = Executors.newThreadPool("stream-server", _threadCount);
    _serverSocket = new ServerSocket(_port, 1000);
    _runningPort = _serverSocket.getLocalPort();
    _thread = new Thread(new Runnable() {

      @Override
      public void run() {
        while (_running.get()) {
          try {
            handleSocket(_serverSocket.accept());
          } catch (IOException e) {
            LOG.error("Unknown error.", e);
          }
        }
      }
    });
    _closer.register(new Closeable() {
      @Override
      public void close() throws IOException {
        _running.set(false);
        _thread.interrupt();
      }
    });
    _thread.setName("stream-server-main");
    _thread.setDaemon(true);
    _thread.start();
  }

  protected void handleSocket(Socket socket) {
    _service.submit(new SocketHandler(socket, _streamProcessor));
  }

  private static class SocketHandler implements Runnable {

    private final Socket _socket;
    private final Closer _closer = Closer.create();
    private final StreamProcessor _streamProcessor;

    public SocketHandler(Socket socket, StreamProcessor streamProcessor) {
      _socket = _closer.register(socket);
      _streamProcessor = streamProcessor;
    }

    @Override
    public void run() {
      InputStream inputStream;
      OutputStream outputStream;
      try {
        inputStream = _closer.register(_socket.getInputStream());
        outputStream = _closer.register(_socket.getOutputStream());
        while (true) {
          int read = inputStream.read();
          StreamCommand command = StreamCommand.find(read);
          switch (command) {
          case STREAM: {
            executeStream(_streamProcessor, inputStream, outputStream);
            break;
          }
          case CLASS_LOAD: {
            executeClassLoad(_streamProcessor, inputStream, outputStream);
            break;
          }
          case CLASS_LOAD_CHECK: {
            checkClassLoad(_streamProcessor, inputStream, outputStream);
            break;
          }
          case CLOSE: {
            return;
          }
          default:
            throw new RuntimeException("Command [" + command + "] not supported.");
          }
        }
      } catch (Throwable t) {
        if (t instanceof SocketException) {
          if (t.getMessage().trim().toLowerCase().equals("socket closed")) {
            return;
          }
        }
        LOG.error("Unknown error.", t);
      } finally {
        try {
          _closer.close();
        } catch (IOException e) {
          LOG.error("Unknown Error");
        }
      }
    }
  }

  public static void executeStream(StreamProcessor streamProcessor, InputStream in, OutputStream outputStream)
      throws IOException {
    Tracer tracer = Trace.trace("stream - executeStream");
    try {
      DataInputStream inputStream = new DataInputStream(in);
      byte[] streamSplitBytes = getObjectBytes(inputStream);
      byte[] functionBytes = getObjectBytes(inputStream);
      StreamSplit streamSplit = getStreamSplit(toInputStream(streamSplitBytes));
      String table = streamSplit.getTable();
      String shard = streamSplit.getShard();
      String classLoaderId = streamSplit.getClassLoaderId();
      User user = new User(streamSplit.getUser(), streamSplit.getUserAttributes());
      UserContext.setUser(user);
      StreamIndexContext indexContext = null;
      try {
        indexContext = streamProcessor.getIndexContext(table, shard);
        StreamFunction<?> function = streamProcessor.getStreamFunction(classLoaderId, toInputStream(functionBytes));
        streamProcessor.execute(function, outputStream, indexContext);
      } finally {
        IOUtils.closeQuietly(indexContext);
        UserContext.reset();
      }
    } finally {
      tracer.done();
    }
  }

  public static void executeClassLoad(StreamProcessor streamProcessor, InputStream inputStream,
      OutputStream outputStream) throws IOException {
    DataInputStream in = new DataInputStream(inputStream);
    DataOutputStream out = new DataOutputStream(outputStream);

    int length = in.readInt();
    byte[] buf = new byte[length];
    in.readFully(buf);
    String id = new String(buf);
    streamProcessor.loadClassLoader(id, in);
    byte[] bs = "LOADED".getBytes();
    out.writeInt(bs.length);
    out.write(bs);
  }

  public static void checkClassLoad(StreamProcessor streamProcessor, InputStream inputStream, OutputStream outputStream)
      throws IOException {
    DataInputStream in = new DataInputStream(inputStream);
    DataOutputStream out = new DataOutputStream(outputStream);

    int length = in.readInt();
    byte[] buf = new byte[length];
    in.readFully(buf);
    String id = new String(buf);
    byte[] bs;
    if (streamProcessor.isClassLoaderLoaded(id)) {
      bs = "OK".getBytes();
    } else {
      bs = "NOT FOUND".getBytes();
    }
    out.writeInt(bs.length);
    out.write(bs);
  }

  private static InputStream toInputStream(byte[] bs) {
    return new ByteArrayInputStream(bs);
  }

  private static byte[] getObjectBytes(DataInputStream inputStream) throws IOException {
    int length = inputStream.readInt();
    byte[] buf = new byte[length];
    inputStream.readFully(buf);
    return buf;
  }

  private static StreamSplit getStreamSplit(InputStream inputStream) throws IOException {
    Tracer tracer = Trace.trace("stream - getStreamSplit");
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
    try {
      return (StreamSplit) objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      objectInputStream.close();
      tracer.done();
    }
  }

  public int getPort() {
    return _runningPort;
  }

}
