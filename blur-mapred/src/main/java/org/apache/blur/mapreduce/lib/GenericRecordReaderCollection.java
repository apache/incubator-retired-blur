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
package org.apache.blur.mapreduce.lib;

import java.io.IOException;
import java.util.List;

import org.apache.blur.mapreduce.lib.BlurInputFormat.BlurInputSplit;
import org.apache.blur.mapreduce.lib.BlurInputFormat.BlurInputSplitColletion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class GenericRecordReaderCollection {

  private Configuration _configuration;
  private boolean _setup;
  private List<BlurInputSplit> _splits;
  private boolean _complete;
  private GenericRecordReader _reader;
  private int _currentSplit = 0;

  public void initialize(BlurInputSplitColletion split, Configuration configuration) {
    _configuration = configuration;
    if (_setup) {
      return;
    }
    _setup = true;
    _splits = split.getSplits();
  }

  public boolean nextKeyValue() throws IOException {
    if (_complete) {
      return false;
    }
    while (true) {
      if (_reader == null) {
        // setup first reader
        _reader = new GenericRecordReader();
        _reader.initialize(_splits.get(_currentSplit), _configuration);
        _currentSplit++;
      }
      boolean nextKeyValue = _reader.nextKeyValue();
      if (nextKeyValue) {
        return true;
      } else {
        _reader.close();
        if (_currentSplit < _splits.size()) {
          _reader = new GenericRecordReader();
          _reader.initialize(_splits.get(_currentSplit), _configuration);
          _currentSplit++;
        } else {
          _reader = null;
          _complete = true;
          return false;
        }
      }
    }
  }

  public Text getCurrentKey() throws IOException {
    return _reader.getCurrentKey();
  }

  public TableBlurRecord getCurrentValue() throws IOException {
    return _reader.getCurrentValue();
  }

  public float getProgress() {
    return _currentSplit / (float) _splits.size();
  }

  public void close() throws IOException {
    if (_reader != null) {
      _reader.close();
    }
  }

}
