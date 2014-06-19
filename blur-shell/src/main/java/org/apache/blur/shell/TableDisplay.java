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
package org.apache.blur.shell;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import jline.Terminal;
import jline.console.ConsoleReader;

public class TableDisplay implements Closeable {

  public static void main(String[] args) throws IOException, InterruptedException {

    // System.out.println("\u001B[1mfoo\u001B[0m@bar\u001B[32m@baz\u001B[0m>");

    // for (int i = 0; i < 100; i++) {
    // System.out.println("\u001B[0m" + i + "\u001B[" + i + "mfoo");
    // }

    ConsoleReader reader = new ConsoleReader();
    TableDisplay tableDisplay = new TableDisplay(reader);
    tableDisplay.setSeperator("|");
//    Random random = new Random();
    int maxX = 20;
    int maxY = 100;
    tableDisplay.setHeader(0, "");
    for (int i = 1; i < maxX; i++) {
      tableDisplay.setHeader(i, "col-" + i);
    }

    for (int i = 0; i < maxY; i++) {
      tableDisplay.set(0, i, i);
    }

    final AtomicBoolean running = new AtomicBoolean(true);

    tableDisplay.addKeyHook(new Runnable() {
      @Override
      public void run() {
        synchronized (running) {
          running.set(false);
          running.notifyAll();
        }
      }
    }, 'q');

    try {
      // int i = 0;
      // while (true) {
      // if (i >= 100000) {
      // i = 0;
      // }
      // tableDisplay.set(random.nextInt(maxX) + 1, random.nextInt(maxY),
      // random.nextLong());
      // Thread.sleep(3000);
      // i++;
      // }
      for (int x = 0; x < maxX; x++) {
        for (int y = 0; y < maxY; y++) {
          tableDisplay.set(x, y, x + "," + y);
        }
      }
      while (running.get()) {
        synchronized (running) {
          running.wait(1000);
        }
      }
    } finally {
      tableDisplay.close();
    }
  }

  public void addKeyHook(Runnable runnable, int c) {
    _keyHookMap.put(c, runnable);
  }

  private static final String SEP = "|";
  private final ConsoleReader _reader;
  private final ConcurrentMap<Key, Object> _values = new ConcurrentHashMap<TableDisplay.Key, Object>();
  private final ConcurrentMap<Integer, String> _header = new ConcurrentHashMap<Integer, String>();
  private final ConcurrentMap<Integer, Integer> _maxWidth = new ConcurrentHashMap<Integer, Integer>();
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final Timer _timer;
  private final Canvas _canvas;
  private int _maxY;
  private int _maxX;
  private String _seperator = SEP;
  private final Thread _inputReaderThread;
  private final Map<Integer, Runnable> _keyHookMap = new ConcurrentHashMap<Integer, Runnable>();

  public void setSeperator(String seperator) {
    _seperator = seperator;
  }

  public TableDisplay(ConsoleReader reader) {
    _reader = reader;
    _timer = new Timer("Render Thread", true);
    _canvas = new Canvas(_reader);
    _timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          render(_canvas);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }, 100, 250);
    _inputReaderThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          InputStream input = _reader.getInput();
          int read;
          while ((read = input.read()) != -1 && _running.get()) {
            if (read == 27) {
              if (input.read() == 91) {
                read = input.read();
                switch (read) {
                case 68:
                  // left
                  _canvas.moveLeft();
                  break;
                case 67:
                  // right
                  _canvas.moveRight();
                  break;
                case 65:
                  // up
                  _canvas.moveUp();
                  break;
                case 66:
                  // down
                  _canvas.moveDown();
                  break;
                default:
                  break;
                }
              }
            } else {
              Runnable runnable = _keyHookMap.get(read);
              if (runnable != null) {
                runnable.run();
              }
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
    _inputReaderThread.setName("Input Reader Thread");
    _inputReaderThread.setDaemon(true);
    _inputReaderThread.start();
  }

  private void render(Canvas canvas) throws IOException {
    canvas.reset();
    buildHeaderOutput(canvas);
    buildTableOutput(canvas);
    canvas.write();
  }

  private void buildHeaderOutput(Canvas canvas) {
    canvas.startLine();
    for (int x = 0; x < _maxX; x++) {
      if (x != 0) {
        canvas.append(_seperator);
      }
      Object value = _header.get(x);
      int widthForColumn = getWidthForColumn(x);
      buffer(canvas, getString(value), widthForColumn);
    }
    canvas.endLine();
  }

  private void buildTableOutput(Canvas canvas) {
    for (int y = 0; y < _maxY; y++) {
      canvas.startLine();
      for (int x = 0; x < _maxX; x++) {
        if (x != 0) {
          canvas.append(_seperator);
        }
        Key key = new Key(x, y);
        Object value = _values.get(key);
        int widthForColumn = getWidthForColumn(x);
        buffer(canvas, getString(value), widthForColumn);
      }
      canvas.endLine();
    }
  }

  private int getWidthForColumn(int x) {
    Integer width = _maxWidth.get(x);
    if (width == null) {
      return 0;
    }
    return width;
  }

  private void buffer(Canvas canvas, String value, int width) {
    canvas.append(value);
    width -= value.length();
    while (width > 0) {
      canvas.append(' ');
      width--;
    }
  }

  public void setHeader(int x, String name) {
    _header.put(x, name);
    setMaxWidth(x, name);
  }

  public void set(int x, int y, Object value) {
    if (value == null) {
      remove(x, y);
      return;
    }
    _values.put(new Key(x, y), value);
    setMaxX(x);
    setMaxY(y);
    setMaxWidth(x, value);
  }

  private void setMaxWidth(int x, Object value) {
    if (value != null) {
      String s = getString(value);
      int length = s.length();
      while (true) {
        Integer width = _maxWidth.get(x);
        if (width == null) {
          width = _maxWidth.putIfAbsent(x, length);
          if (width == null) {
            return;
          }
        }
        if (width < length) {
          if (_maxWidth.replace(x, width, length)) {
            return;
          }
        } else {
          return;
        }
      }
    }
  }

  private String getString(Object value) {
    if (value != null) {
      return value.toString();
    }
    return "";
  }

  private void setMaxY(int y) {
    if (_maxY < y + 1) {
      _maxY = y + 1;
    }
  }

  private void setMaxX(int x) {
    if (_maxX < x + 1) {
      _maxX = x + 1;
    }
  }

  public void remove(int x, int y) {
    _values.remove(new Key(x, y));
  }

  @Override
  public void close() throws IOException {
    if (_running.get()) {
      _running.set(false);
      _timer.cancel();
      _timer.purge();
      _inputReaderThread.interrupt();
    }
  }

  public static class Key implements Comparable<Key> {

    final int _x;
    final int _y;

    public Key(int x, int y) {
      _x = x;
      _y = y;
    }

    public int getX() {
      return _x;
    }

    public int getY() {
      return _y;
    }

    @Override
    public int compareTo(Key o) {
      if (_y == o._y) {
        return _x - o._x;
      }
      return _y - o._y;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + _x;
      result = prime * result + _y;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Key other = (Key) obj;
      if (_x != other._x)
        return false;
      if (_y != other._y)
        return false;
      return true;
    }
  }

  static class Canvas {

    private final StringBuilder _builder = new StringBuilder();
    private final StringBuilder _lineBuilder = new StringBuilder();
    private final ConsoleReader _reader;
    private int _height;
    private int _width;
    private int _line;
    private volatile int _posX = 0;
    private volatile int _posY = 0;
    private int _leftRightMoveSize;

    public Canvas(ConsoleReader reader) {
      _reader = reader;
    }

    public void reset() {
      _line = 0;
      _builder.setLength(0);
      Terminal terminal = _reader.getTerminal();
      _height = terminal.getHeight() - 2;
      _width = terminal.getWidth() - 2;
      _leftRightMoveSize = _width / 4;
    }

    public void endLine() {
      int pos = _line - _posY;
      if (pos >= 0 && pos < _height) {
        int end = _posX + _width;
        _builder.append(_lineBuilder.substring(_posX, Math.min(_lineBuilder.length(), end)));
        _builder.append('\n');
      }
      _line++;
      _lineBuilder.setLength(0);
    }

    public void startLine() {

    }

    public void append(char c) {
      _lineBuilder.append(c);
    }

    public void append(String s) {
      for (int i = 0; i < s.length(); i++) {
        append(s.charAt(i));
      }
    }

    public void write() throws IOException {
      _reader.clearScreen();
      Writer writer = _reader.getOutput();
      writer.write(_builder.toString());
      writer.flush();
    }

    public void moveLeft() {
      if (_posX > 0) {
        _posX -= _leftRightMoveSize;
      }
    }

    public void moveRight() {
      _posX += _leftRightMoveSize;
    }

    public void moveUp() {
      if (_posY > 0) {
        _posY--;
      }
    }

    public void moveDown() {
      _posY++;
    }

  }
}
