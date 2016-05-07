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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import jline.Terminal;
import jline.console.ConsoleReader;

public class TableDisplay implements Closeable {

  public static void main(String[] args) throws IOException, InterruptedException {

    // System.out.println("\u001B[1mfoo\u001B[0m@bar\u001B[32m@baz\u001B[0m>");

    // \u001B[0m normal
    // \u001B[33m yellow

    // System.out.println("\u001B[33mCOLUMN\u001B[0mnormal");
    //
    // for (int i = 0; i < 100; i++) {
    // System.out.println("\u001B[0m" + i + "\u001B[" + i + "mfoo");
    // }

    ConsoleReader reader = new ConsoleReader();
    TableDisplay tableDisplay = new TableDisplay(reader);
    tableDisplay.setSeperator("|");
    // Random random = new Random();
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
          if (x == 7 && y == 7) {
            tableDisplay.set(x, y, highlight(x + "," + y));
          } else {
            tableDisplay.set(x, y, x + "," + y);
          }
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

  private static String highlight(String s) {
    // return s;
    return "\u001B[33m" + s + "\u001B[0m";
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
  private final Thread _paintThread;
  private final Canvas _canvas;
  private int _maxY;
  private int _maxX;
  private String _seperator = SEP;
  private final Thread _inputReaderThread;
  private final Map<Integer, Runnable> _keyHookMap = new ConcurrentHashMap<Integer, Runnable>();
  private final AtomicBoolean _stopReadingInput = new AtomicBoolean(false);
  private String _description = "";
  private volatile long _lastPaint = 0;
  private volatile boolean _dirty = true;

  public void setDescription(String description) {
    _description = description;
  }

  public void setSeperator(String seperator) {
    _seperator = seperator;
  }

  public TableDisplay(ConsoleReader reader) {
    _reader = reader;
    _canvas = new Canvas(_reader);
    _paintThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          long now = System.currentTimeMillis();
          synchronized (_canvas) {
            if (_lastPaint + 1000 < now) {
              try {
                render(_canvas);
              } catch (IOException e) {
                e.printStackTrace();
              }
              _lastPaint = System.currentTimeMillis();
              _dirty = false;
            }
            if (_dirty) {
              try {
                _canvas.wait(1000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            } else {
              try {
                _canvas.wait();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
          }
        }
      }
    });
    _paintThread.setDaemon(true);
    _paintThread.setName("Render Thread");
    _paintThread.start();
    _inputReaderThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          InputStream input = _reader.getInput();
          int read;
          while (!_stopReadingInput.get() && _running.get() && (read = input.read()) != -1) {
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
                case 53:
                  // up
                  _canvas.pageUp();
                  break;
                case 54:
                  // down
                  _canvas.pageDown();
                  break;
                default:
                  break;
                }
                _lastPaint = 0;
              }
            } else {
              Runnable runnable = _keyHookMap.get(read);
              if (runnable != null) {
                runnable.run();
              }
            }
            paint();
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
    canvas.resetHeader();
    canvas.appendHeader(_description);
    canvas.endHeader();
    buildHeaderOutput(canvas);
    buildTableOutput(canvas);
    canvas.write();
  }

  private void buildHeaderOutput(Canvas canvas) {
    canvas.startHeader();
    for (int x = 0; x < _maxX; x++) {
      if (x != 0) {
        canvas.appendHeader(_seperator);
      }
      Object value = _header.get(x);
      int widthForColumn = getWidthForColumn(x);
      bufferHeader(canvas, getString(value), widthForColumn);
    }
    canvas.endHeader();
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

  private void bufferHeader(Canvas canvas, String value, int width) {
    canvas.appendHeader(value);
    width -= getVisibleLength(value);
    while (width > 0) {
      canvas.appendHeader(' ');
      width--;
    }
  }

  private void buffer(Canvas canvas, String value, int width) {
    canvas.append(value);
    width -= getVisibleLength(value);
    while (width > 0) {
      canvas.append(' ');
      width--;
    }
  }

  public void setHeader(int x, String name) {
    _header.put(x, name);
    setMaxWidth(x, name);
    paint();
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
    paint();
  }

  private void paint() {
    synchronized (_canvas) {
      _canvas.notify();
      _dirty = true;
    }
  }

  private void setMaxWidth(int x, Object value) {
    if (value != null) {
      String s = getString(value);
      int length = getVisibleLength(s);
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

  private int getVisibleLength(String s) {
    int length = 0;
    boolean color = false;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (color) {
        if (c == 'm') {
          color = false;
        }
      } else {
        if (c == 27) {
          color = true;
        } else {
          length++;
        }
      }
    }
    return length;
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
      _inputReaderThread.interrupt();
      try {
        _inputReaderThread.join();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
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

  public static class LineBuilder {

    static class LineBuilderPart {
      String _visible;
      String _notVisible;
    }

    private List<LineBuilderPart> _parts = new ArrayList<LineBuilderPart>();
    private boolean _readingColor;
    private StringBuilder _currentNotVisible = new StringBuilder();
    private StringBuilder _currentVisible = new StringBuilder();

    public void reset() {
      _currentNotVisible.setLength(0);
      _currentVisible.setLength(0);
      _parts.clear();
    }

    public void close() {
      startNewPart();
    }

    public int visibleLength() {
      int l = 0;
      for (LineBuilderPart p : _parts) {
        l += p._visible.length();
      }
      return l;
    }

    public String substring(int start, int end) {
      StringBuilder b = new StringBuilder();
      Iterator<LineBuilderPart> iterator = _parts.iterator();
      LineBuilderPart current = iterator.next();
      int partPos = 0;
      boolean currentPartHasEmittedNonVisible = false;
      for (int i = 0; i < end; i++) {
        if (partPos >= current._visible.length()) {
          current = iterator.next();
          partPos = 0;
          currentPartHasEmittedNonVisible = false;
        }
        if (isVisible(i, start, end)) {
          if (!currentPartHasEmittedNonVisible) {
            b.append(current._notVisible);
            currentPartHasEmittedNonVisible = true;
          }
          b.append(current._visible.charAt(partPos));
        }
        partPos++;
      }
      return b.toString();
    }

    private boolean isVisible(int i, int start, int end) {
      if (i >= start && i < end) {
        return true;
      }
      return false;
    }

    public void append(char c) {
      if (_readingColor) {
        if (c == 'm') {
          _readingColor = false;
          _currentNotVisible.append(c);
        } else {
          _currentNotVisible.append(c);
        }
      } else {
        if (c == 27) {
          startNewPart();
          _readingColor = true;
          _currentNotVisible.append(c);
        } else {
          _currentVisible.append(c);
        }
      }
    }

    private void startNewPart() {
      LineBuilderPart part = new LineBuilderPart();
      part._notVisible = _currentNotVisible.toString();
      part._visible = _currentVisible.toString();
      _parts.add(part);
      _currentNotVisible.setLength(0);
      _currentVisible.setLength(0);
    }

    public void append(String s) {
      for (int i = 0; i < s.length(); i++) {
        append(s.charAt(i));
      }
    }

  }

  static class Canvas {

    private final StringBuilder _screenBuilder = new StringBuilder();
    private final LineBuilder _lineBuilder = new LineBuilder();
    private final StringBuilder _headerScreenBuilder = new StringBuilder();
    private final LineBuilder _headerLineBuilder = new LineBuilder();
    private final ConsoleReader _reader;
    private int _height;
    private int _width;
    private int _line;
    private volatile int _posX = 0;
    private volatile int _posY = 0;
    private int _leftRightMoveSize;
    private int _headerLine;

    public Canvas(ConsoleReader reader) {
      _reader = reader;
    }

    public void resetHeader() {
      _headerLine = 0;
      _headerScreenBuilder.setLength(0);
    }

    public void reset() {
      _line = 0;
      _screenBuilder.setLength(0);
      Terminal terminal = _reader.getTerminal();
      _height = terminal.getHeight() - 3;
      _width = terminal.getWidth() - 2;
      _leftRightMoveSize = _width / 4;
    }

    public void endLine() {
      _lineBuilder.close();
      int pos = _line - _posY;
      if (pos >= 0 && pos < (_height - _headerLine)) {
        int end = _posX + _width;
        int s = _posX;
        int e = Math.min(_lineBuilder.visibleLength(), end);
        if (e > s) {
          _screenBuilder.append(_lineBuilder.substring(s, e));
        }
        _screenBuilder.append('\n');
      }
      _line++;
      _lineBuilder.reset();
    }

    public void endHeader() {
      _headerLineBuilder.close();
      int end = _posX + _width;
      int s = _posX;
      int e = Math.min(_headerLineBuilder.visibleLength(), end);
      if (e > s) {
        _headerScreenBuilder.append(_headerLineBuilder.substring(s, e));
      }
      _headerScreenBuilder.append('\n');
      _headerLineBuilder.reset();
      _headerLine++;
    }

    public void startLine() {

    }

    public void startHeader() {

    }

    public void appendHeader(char c) {
      _headerLineBuilder.append(c);
    }

    public void appendHeader(String s) {
      _headerLineBuilder.append(s);
    }

    public void append(char c) {
      _lineBuilder.append(c);
    }

    public void append(String s) {
      _lineBuilder.append(s);
    }

    public void write() throws IOException {
      _reader.clearScreen();
      Writer writer = _reader.getOutput();
      writer.write(_headerScreenBuilder.toString());
      writer.write(_screenBuilder.toString());
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

    public void pageUp() {
      if (_posY < _height) {
        _posY = 0;
      } else {
        _posY -= _height;
      }
    }

    public void pageDown() {
      _posY += _height;
    }

  }

  public void setStopReadingInput(boolean b) {
    _stopReadingInput.set(true);
  }

}
