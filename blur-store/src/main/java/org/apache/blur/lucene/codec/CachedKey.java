package org.apache.blur.lucene.codec;

import org.apache.lucene.index.SegmentInfo;

class CachedKey {

  final long _filePointer;
  final String _name;
  final SegmentInfo _si;

  public CachedKey(String name, long filePointer, SegmentInfo si) {
    _name = name;
    _filePointer = filePointer;
    _si = si;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (_filePointer ^ (_filePointer >>> 32));
    result = prime * result + ((_name == null) ? 0 : _name.hashCode());
    result = prime * result + ((_si == null) ? 0 : _si.hashCode());
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
    CachedKey other = (CachedKey) obj;
    if (_filePointer != other._filePointer)
      return false;
    if (_name == null) {
      if (other._name != null)
        return false;
    } else if (!_name.equals(other._name))
      return false;
    if (_si == null) {
      if (other._si != null)
        return false;
    } else if (!_si.equals(other._si))
      return false;
    return true;
  }

}