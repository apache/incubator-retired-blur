package com.nearinfinity.blur.mapreduce.lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class BlurInputSplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit {

  private int _endingDocId;
  private int _startingDocId;
  private String _segmentName;
  private Path _path;

  public BlurInputSplit() {

  }

  public BlurInputSplit(Path path, String segmentName, int startingDocId, int endingDocId) {
    _endingDocId = endingDocId;
    _startingDocId = startingDocId;
    _segmentName = segmentName;
    _path = path;
  }

  @Override
  public long getLength() {
    return _endingDocId - _startingDocId;
  }

  @Override
  public String[] getLocations() {
    return new String[] {};
  }

  public Path getIndexPath() {
    return _path;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public int getStartingDocId() {
    return _startingDocId;
  }

  public int getEndingDocId() {
    return _endingDocId;
  }

  public void setEndingDocId(int endingDocId) {
    _endingDocId = endingDocId;
  }

  public void setStartingDocId(int startingDocId) {
    _startingDocId = startingDocId;
  }

  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  public void setPath(Path path) {
    _path = path;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(_startingDocId);
    out.writeInt(_endingDocId);
    writeString(out, _segmentName);
    writeString(out, _path.toUri().toString());
  }

  private void writeString(DataOutput out, String s) throws IOException {
    byte[] bs = s.getBytes();
    out.writeInt(bs.length);
    out.write(bs);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    _startingDocId = in.readInt();
    _endingDocId = in.readInt();
    _segmentName = readString(in);
    _path = new Path(readString(in));
  }

  private String readString(DataInput in) throws IOException {
    int length = in.readInt();
    byte[] buf = new byte[length];
    in.readFully(buf);
    return new String(buf);
  }

  @Override
  public String toString() {
    return "path=" + _path + ", segmentName=" + _segmentName + ", startingDocId=" + _startingDocId + ", endingDocId=" + _endingDocId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + _endingDocId;
    result = prime * result + ((_path == null) ? 0 : _path.hashCode());
    result = prime * result + ((_segmentName == null) ? 0 : _segmentName.hashCode());
    result = prime * result + _startingDocId;
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
    BlurInputSplit other = (BlurInputSplit) obj;
    if (_endingDocId != other._endingDocId)
      return false;
    if (_path == null) {
      if (other._path != null)
        return false;
    } else if (!_path.equals(other._path))
      return false;
    if (_segmentName == null) {
      if (other._segmentName != null)
        return false;
    } else if (!_segmentName.equals(other._segmentName))
      return false;
    if (_startingDocId != other._startingDocId)
      return false;
    return true;
  }
}
