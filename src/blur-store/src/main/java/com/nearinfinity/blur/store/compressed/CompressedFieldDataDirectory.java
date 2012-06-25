/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.store.compressed;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import com.nearinfinity.blur.index.DirectIODirectory;
import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;

public class CompressedFieldDataDirectory extends DirectIODirectory {

  private static final Log LOG = LogFactory.getLog(CompressedFieldDataDirectory.class);

  private static final int _MIN_BUFFER_SIZE = 100;
  private static final String FDZ = ".fdz";
  private static final String FDT = ".fdt";
  private static final String Z_TMP = ".tmp";

  private static final int COMPRESSED_BUFFER_SIZE = 65536;

  public static CompressionCodec DEFAULT_COMPRESSION = new DefaultCodec();

  private CompressionCodec _compression = DEFAULT_COMPRESSION;
  private DirectIODirectory _directory;
  private int _writingBlockSize;

  public Directory getInnerDirectory() {
    return _directory;
  }

  public CompressedFieldDataDirectory(DirectIODirectory dir) {
    this(dir, DEFAULT_COMPRESSION);
  }

  public CompressedFieldDataDirectory(DirectIODirectory dir, CompressionCodec compression) {
    this(dir, compression, COMPRESSED_BUFFER_SIZE);
  }

  public CompressedFieldDataDirectory(DirectIODirectory dir, CompressionCodec compression, int blockSize) {
    _directory = dir;
    if (compression == null) {
      _compression = DEFAULT_COMPRESSION;
    } else {
      _compression = compression;
    }
    _writingBlockSize = blockSize;
  }

  private IndexInput wrapInput(String name) throws IOException {
    IndexInput indexInput = _directory.openInput(name);
    int version = getVersion(indexInput);
    switch (version) {
    case 0:
      return new CompressedIndexInput_V0(name, indexInput, _compression);
    case 1:
      return new CompressedIndexInput_V1(name, indexInput, _compression);
    default:
      throw new RuntimeException("Unknown version [" + version + "]");
    }
  }

  private int getVersion(IndexInput indexInput) throws IOException {
    long length = indexInput.length();
    indexInput.seek(length - 8);
    long l = indexInput.readLong();
    if (l < 0) {
      return (int) Math.abs(l);
    } else {
      return 0;
    }
  }

  private IndexOutput wrapOutput(String name) throws IOException {
    return new CompressedIndexOutput_V0(name, _directory, _compression, _writingBlockSize);
  }

  public IndexInput openInput(String name) throws IOException {
    if (compressedFileExists(name)) {
      return wrapInput(getCompressedName(name));
    }
    return _directory.openInput(name);
  }

  public IndexInput openInput(String name, int bufferSize) throws IOException {
    if (compressedFileExists(name)) {
      return wrapInput(getCompressedName(name));
    }
    return _directory.openInput(name, bufferSize);
  }

  private boolean compressedFileExists(String name) throws IOException {
    if (!name.endsWith(FDT)) {
      return false;
    }
    return _directory.fileExists(getCompressedName(name));
  }

  private String getCompressedName(String name) {
    int index = name.lastIndexOf('.');
    return name.substring(0, index) + FDZ;
  }

  private String getNormalName(String compressedName) {
    int index = compressedName.lastIndexOf('.');
    return compressedName.substring(0, index) + FDT;
  }

  public IndexOutput createOutput(String name) throws IOException {
    if (name.endsWith(FDT)) {
      return wrapOutput(getCompressedName(name));
    }
    return _directory.createOutput(name);
  }

  public void clearLock(String name) throws IOException {
    _directory.clearLock(name);
  }

  public void close() throws IOException {
    _directory.close();
  }

  public void deleteFile(String name) throws IOException {
    if (compressedFileExists(name)) {
      _directory.deleteFile(getCompressedName(name));
    } else {
      _directory.deleteFile(name);
    }
  }

  public boolean fileExists(String name) throws IOException {
    if (compressedFileExists(name)) {
      return true;
    }
    return _directory.fileExists(name);
  }

  public long fileLength(String name) throws IOException {
    if (compressedFileExists(name)) {
      IndexInput input = _directory.openInput(getCompressedName(name));
      try {
        long length = input.length();
        input.seek(length - 8);
        long fileLength = input.readLong();
        if (fileLength < 0) {
          input.seek(length - 16);
          return input.readLong();
        } else {
          return fileLength;
        }
      } finally {
        input.close();
      }
    }
    return _directory.fileLength(name);
  }

  public long fileModified(String name) throws IOException {
    if (compressedFileExists(name)) {
      return _directory.fileModified(getCompressedName(name));
    }
    return _directory.fileModified(name);
  }

  public String[] listAll() throws IOException {
    return fixNames(_directory.listAll());
  }

  private String[] fixNames(String[] listAll) {
    for (int i = 0; i < listAll.length; i++) {
      if (listAll[i].endsWith(FDZ)) {
        listAll[i] = getNormalName(listAll[i]);
      }
    }
    return listAll;
  }

  public void touchFile(String name) throws IOException {
    // do nothing
  }

  public LockFactory getLockFactory() {
    return _directory.getLockFactory();
  }

  public String getLockID() {
    return _directory.getLockID();
  }

  public Lock makeLock(String name) {
    return _directory.makeLock(name);
  }

  public void setLockFactory(LockFactory lockFactory) throws IOException {
    _directory.setLockFactory(lockFactory);
  }

  @SuppressWarnings("deprecation")
  public void sync(String name) throws IOException {
    if (compressedFileExists(name)) {
      _directory.sync(getCompressedName(name));
    } else {
      _directory.sync(name);
    }
  }

  public String toString() {
    return _directory.toString();
  }

  public static class CompressedIndexOutput_V1 extends IndexOutput {

    private static final long VERSION = -1L;
    private long _position = 0;
    private IndexOutput _output;
    private byte[] _buffer;
    private int _bufferPosition = 0;
    private byte[] _compressedBuffer;
    private IndexOutput _tmpOutput;
    private Directory _directory;
    private String _name;
    private int _blockCount;
    private Compressor _compressor;

    public CompressedIndexOutput_V1(String name, Directory directory, CompressionCodec codec, int blockSize)
        throws IOException {
      _compressor = codec.createCompressor();
      if (_compressor == null) {
        throw new RuntimeException("CompressionCodec [" + codec + "] does not support compressor on this platform.");
      }
      _directory = directory;
      _name = name;
      _output = directory.createOutput(name);
      _tmpOutput = directory.createOutput(name + Z_TMP);
      _buffer = new byte[blockSize];
      int dsize = blockSize * 2;
      if (dsize < _MIN_BUFFER_SIZE) {
        dsize = _MIN_BUFFER_SIZE;
      }
      _compressedBuffer = new byte[dsize];
    }

    @Override
    public void writeByte(byte b) throws IOException {
      _buffer[_bufferPosition] = b;
      _bufferPosition++;
      _position++;
      flushIfNeeded();
    }

    private void flushIfNeeded() throws IOException {
      if (_bufferPosition >= _buffer.length) {
        flushBuffer();
        _bufferPosition = 0;
      }
    }

    private void flushBuffer() throws IOException {
      if (_bufferPosition > 0) {
        _compressor.reset();
        _compressor.setInput(_buffer, 0, _bufferPosition);
        _compressor.finish();

        long filePointer = _output.getFilePointer();

        int length = _compressor.compress(_compressedBuffer, 0, _compressedBuffer.length);

        _tmpOutput.writeLong(filePointer);
        _blockCount++;
        _output.writeBytes(_compressedBuffer, 0, length);
      }
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      int len = length + offset;
      for (int i = offset; i < len; i++) {
        writeByte(b[i]);
      }
    }

    @Override
    public void close() throws IOException {
      flushBuffer();
      _tmpOutput.close();
      IndexInput input = _directory.openInput(_name + Z_TMP);
      try {
        long len = input.length();
        long readCount = 0;
        while (readCount < len) {
          int toRead = readCount + _buffer.length > len ? (int) (len - readCount) : _buffer.length;
          input.readBytes(_buffer, 0, toRead);
          _output.writeBytes(_buffer, toRead);
          readCount += toRead;
        }
        _output.writeInt(_blockCount);
        _output.writeInt(_buffer.length);
        _output.writeLong(_position);
        _output.writeLong(VERSION);
      } finally {
        try {
          _output.close();
        } finally {
          input.close();
        }
      }
      _directory.deleteFile(_name + Z_TMP);
      _compressor.end();
    }

    @Override
    public long getFilePointer() {
      return _position;
    }

    @Override
    public long length() throws IOException {
      throw new RuntimeException("not supported");
    }

    @Override
    public void seek(long pos) throws IOException {
      throw new RuntimeException("not supported");
    }

    @Override
    public void flush() throws IOException {

    }
  }

  public static class CompressedIndexOutput_V0 extends IndexOutput {

    private long _position = 0;
    private IndexOutput _output;
    private byte[] _buffer;
    private int _bufferPosition = 0;
    private byte[] _compressedBuffer;
    private IndexOutput _tmpOutput;
    private Directory _directory;
    private String _name;
    private int _blockCount;
    private Compressor _compressor;

    public CompressedIndexOutput_V0(String name, Directory directory, CompressionCodec codec, int blockSize)
        throws IOException {
      _compressor = codec.createCompressor();
      if (_compressor == null) {
        throw new RuntimeException("CompressionCodec [" + codec + "] does not support compressor on this platform.");
      }
      _directory = directory;
      _name = name;
      _output = directory.createOutput(name);
      _tmpOutput = directory.createOutput(name + Z_TMP);
      _buffer = new byte[blockSize];
      int dsize = blockSize * 2;
      if (dsize < _MIN_BUFFER_SIZE) {
        dsize = _MIN_BUFFER_SIZE;
      }
      _compressedBuffer = new byte[dsize];
    }

    @Override
    public void writeByte(byte b) throws IOException {
      _buffer[_bufferPosition] = b;
      _bufferPosition++;
      _position++;
      flushIfNeeded();
    }

    private void flushIfNeeded() throws IOException {
      if (_bufferPosition >= _buffer.length) {
        flushBuffer();
        _bufferPosition = 0;
      }
    }

    private void flushBuffer() throws IOException {
      if (_bufferPosition > 0) {
        _compressor.reset();
        _compressor.setInput(_buffer, 0, _bufferPosition);
        _compressor.finish();

        long filePointer = _output.getFilePointer();

        int length = _compressor.compress(_compressedBuffer, 0, _compressedBuffer.length);

        _tmpOutput.writeVLong(filePointer);
        _tmpOutput.writeVInt(length);
        _blockCount++;
        _output.writeBytes(_compressedBuffer, 0, length);
      }
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      int len = length + offset;
      for (int i = offset; i < len; i++) {
        writeByte(b[i]);
      }
    }

    @Override
    public void close() throws IOException {
      flushBuffer();
      _tmpOutput.close();
      IndexInput input = _directory.openInput(_name + Z_TMP);
      try {
        long len = input.length();
        long readCount = 0;
        while (readCount < len) {
          int toRead = readCount + _buffer.length > len ? (int) (len - readCount) : _buffer.length;
          input.readBytes(_buffer, 0, toRead);
          _output.writeBytes(_buffer, toRead);
          readCount += toRead;
        }
        _output.writeLong(len);
        _output.writeInt(_blockCount);
        _output.writeInt(_buffer.length);
        _output.writeLong(_position);
      } finally {
        try {
          _output.close();
        } finally {
          input.close();
        }
      }
      _directory.deleteFile(_name + Z_TMP);
      _compressor.end();
    }

    @Override
    public long getFilePointer() {
      return _position;
    }

    @Override
    public long length() throws IOException {
      throw new RuntimeException("not supported");
    }

    @Override
    public void seek(long pos) throws IOException {
      throw new RuntimeException("not supported");
    }

    @Override
    public void flush() throws IOException {

    }
  }

  public static class CompressedIndexInput_V1 extends IndexInput {

    private static final long VERSION = -1l;

    private static final int _SIZES_META_DATA = 24;

    private final AtomicLongArray _blockPositions;
    private final long _realLength;
    private final long _origLength;
    private final int _blockSize;

    private IndexInput _indexInput;
    private long _pos;
    private boolean _isClone;
    private long _currentBlockId = -1;
    private byte[] _blockBuffer;
    private byte[] _decompressionBuffer;
    private int _blockBufferLength;
    private Decompressor _decompressor;
    private int _blockCount;
    private Thread _openerThread;
    private AtomicBoolean _errorInOpener = new AtomicBoolean(false);
    private String _name;

    public CompressedIndexInput_V1(String name, IndexInput indexInput, CompressionCodec codec) throws IOException {
      super(name);
      _name = name;
      long s = System.nanoTime();
      _decompressor = codec.createDecompressor();
      if (_decompressor == null) {
        throw new RuntimeException("CompressionCodec [" + codec + "] does not support decompressor on this platform.");
      }
      _indexInput = indexInput;
      _realLength = _indexInput.length();

      // read meta data
      _indexInput.seek(_realLength - _SIZES_META_DATA); // 8 - 4 - 4 - 8
      _blockCount = _indexInput.readInt();
      _blockSize = _indexInput.readInt();
      _origLength = _indexInput.readLong();
      long version = _indexInput.readLong();
      if (version != VERSION) {
        throw new IOException("Version [" + version + "] mismatch!");
      }

      _blockPositions = new AtomicLongArray(_blockCount);
      for (int i = 0; i < _blockCount; i++) {
        _blockPositions.set(i, -1l);
      }
      readBlockPositions((IndexInput) indexInput.clone(), name);
      setupBuffers(this);
      long e = System.nanoTime();
      double total = (e - s) / 1000000.0;
      LOG.debug("Took [" + total + " ms] to open file [" + name + "].");
    }

    private void readBlockPositions(final IndexInput indexInput, final String name) throws IOException {
      _openerThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            long s = System.nanoTime();
            long metaDataLength = _blockCount * 8;
            indexInput.seek(_realLength - _SIZES_META_DATA - metaDataLength);
            for (int i = 0; i < _blockCount; i++) {
              _blockPositions.set(i, indexInput.readLong());
            }
            long e = System.nanoTime();
            double total = (e - s) / 1000000.0;
            LOG.debug("Took [{0} ms] to read block positions with blockCount of [{1}] in file [{2}].", total, _blockCount, name);
            indexInput.close();
          } catch (Exception e) {
            LOG.error("Error during the reading of block positions in file [{0}] ", e, name);
            _errorInOpener.set(true);
          }
        }
      });
      _openerThread.setName("Block-Position-Reader-" + name);
      _openerThread.start();
    }

    private int getBlockLength(int blockId) throws IOException {
      int newBlockId = blockId + 1;
      if (newBlockId == _blockCount) {
        // last block
        return (int) (_realLength - _SIZES_META_DATA - getBlockPosition(blockId));
      } else {
        return (int) (getBlockPosition(newBlockId) - getBlockPosition(blockId));
      }
    }

    public long getBlockPosition(int blockId) throws IOException {
      long position = _blockPositions.get(blockId);
      while (true) {
        if (position < 0) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        } else {
          return position;
        }
        if (_errorInOpener.get()) {
          throw new IOException("Block positions for file [" + _name + "] can not be read.");
        }
        position = _blockPositions.get(blockId);
      }
    }

    private static void setupBuffers(CompressedIndexInput_V1 input) {
      input._blockBuffer = new byte[input._blockSize];
      int dsize = input._blockSize * 2;
      if (dsize < _MIN_BUFFER_SIZE) {
        dsize = _MIN_BUFFER_SIZE;
      }
      input._decompressionBuffer = new byte[dsize];
    }

    public Object clone() {
      CompressedIndexInput_V1 clone = (CompressedIndexInput_V1) super.clone();
      clone._isClone = true;
      clone._indexInput = (IndexInput) _indexInput.clone();
      setupBuffers(clone);
      return clone;
    }

    public void close() throws IOException {
      if (!_isClone) {
        _decompressor.end();
      }
      _indexInput.close();
    }

    public long getFilePointer() {
      return _pos;
    }

    public long length() {
      return _origLength;
    }

    public byte readByte() throws IOException {
      int blockId = getBlockId();
      if (blockId != _currentBlockId) {
        fetchBlock(blockId);
      }
      int blockPosition = getBlockPosition();
      _pos++;
      return _blockBuffer[blockPosition];
    }

    public void readBytes(byte[] b, int offset, int len) throws IOException {
      while (len > 0) {
        int blockId = getBlockId();
        if (blockId != _currentBlockId) {
          fetchBlock(blockId);
        }
        int blockPosition = getBlockPosition();
        int length = Math.min(_blockBufferLength - blockPosition, len);
        System.arraycopy(_blockBuffer, blockPosition, b, offset, length);
        _pos += length;
        len -= length;
        offset += length;
      }
    }

    private int getBlockPosition() {
      return (int) (_pos % _blockSize);
    }

    private void fetchBlock(int blockId) throws IOException {
      long position = getBlockPosition(blockId);
      int length = getBlockLength(blockId);
      _indexInput.seek(position);
      _indexInput.readBytes(_decompressionBuffer, 0, length);

      synchronized (_decompressor) {
        _decompressor.reset();
        _decompressor.setInput(_decompressionBuffer, 0, length);
        _blockBufferLength = _decompressor.decompress(_blockBuffer, 0, _blockBuffer.length);
      }

      _currentBlockId = blockId;
    }

    private int getBlockId() {
      return (int) (_pos / _blockSize);
    }

    public void seek(long pos) throws IOException {
      _pos = pos;
    }
  }

  public static class CompressedIndexInput_V0 extends IndexInput {

    private static final int _SIZES_META_DATA = 24;

    private final int[] _blockLengths;
    private final long[] _blockPositions;
    private final long _realLength;
    private final long _origLength;
    private final int _blockSize;

    private IndexInput _indexInput;
    private long _pos;
    private boolean _isClone;
    private long _currentBlockId = -1;
    private byte[] _blockBuffer;
    private byte[] _decompressionBuffer;
    private int _blockBufferLength;
    private Decompressor _decompressor;

    public CompressedIndexInput_V0(String name, IndexInput indexInput, CompressionCodec codec) throws IOException {
      super(name);
      _decompressor = codec.createDecompressor();
      if (_decompressor == null) {
        throw new RuntimeException("CompressionCodec [" + codec + "] does not support decompressor on this platform.");
      }
      long s1 = System.nanoTime();
      _indexInput = indexInput;
      _realLength = _indexInput.length();

      // read meta data
      _indexInput.seek(_realLength - _SIZES_META_DATA); // 8 - 4 - 4 - 8
      long metaDataLength = _indexInput.readLong();
      int blockCount = _indexInput.readInt();
      _blockSize = _indexInput.readInt();
      _origLength = _indexInput.readLong();
      long e1 = System.nanoTime();

      _blockLengths = new int[blockCount];
      _blockPositions = new long[blockCount];

      long s2 = System.nanoTime();
      _indexInput.seek(_realLength - _SIZES_META_DATA - metaDataLength);
      for (int i = 0; i < blockCount; i++) {
        _blockPositions[i] = _indexInput.readVLong();
        _blockLengths[i] = _indexInput.readVInt();
      }
      long e2 = System.nanoTime();

      setupBuffers(this);

      double total = (e2 - s1) / 1000000.0;
      double _1st = (e1 - s1) / 1000000.0;
      double _2nd = (e2 - s2) / 1000000.0;
      LOG.debug("Took [" + total + " ms] to open [" + _1st + "] [" + _2nd + " with blockCount of " + blockCount + "].");
    }

    private static void setupBuffers(CompressedIndexInput_V0 input) {
      input._blockBuffer = new byte[input._blockSize];
      int dsize = input._blockSize * 2;
      if (dsize < _MIN_BUFFER_SIZE) {
        dsize = _MIN_BUFFER_SIZE;
      }
      input._decompressionBuffer = new byte[dsize];
    }

    public Object clone() {
      CompressedIndexInput_V0 clone = (CompressedIndexInput_V0) super.clone();
      clone._isClone = true;
      clone._indexInput = (IndexInput) _indexInput.clone();
      setupBuffers(clone);
      return clone;
    }

    public void close() throws IOException {
      if (!_isClone) {
        _decompressor.end();
      }
      _indexInput.close();
    }

    public long getFilePointer() {
      return _pos;
    }

    public long length() {
      return _origLength;
    }

    public byte readByte() throws IOException {
      int blockId = getBlockId();
      if (blockId != _currentBlockId) {
        fetchBlock(blockId);
      }
      int blockPosition = getBlockPosition();
      _pos++;
      return _blockBuffer[blockPosition];
    }

    public void readBytes(byte[] b, int offset, int len) throws IOException {
      while (len > 0) {
        int blockId = getBlockId();
        if (blockId != _currentBlockId) {
          fetchBlock(blockId);
        }
        int blockPosition = getBlockPosition();
        int length = Math.min(_blockBufferLength - blockPosition, len);
        System.arraycopy(_blockBuffer, blockPosition, b, offset, length);
        _pos += length;
        len -= length;
        offset += length;
      }
    }

    private int getBlockPosition() {
      return (int) (_pos % _blockSize);
    }

    private void fetchBlock(int blockId) throws IOException {
      long position = _blockPositions[blockId];
      int length = _blockLengths[blockId];
      _indexInput.seek(position);
      _indexInput.readBytes(_decompressionBuffer, 0, length);

      synchronized (_decompressor) {
        _decompressor.reset();
        _decompressor.setInput(_decompressionBuffer, 0, length);
        _blockBufferLength = _decompressor.decompress(_blockBuffer, 0, _blockBuffer.length);
      }

      _currentBlockId = blockId;
    }

    private int getBlockId() {
      return (int) (_pos / _blockSize);
    }

    public void seek(long pos) throws IOException {
      _pos = pos;
    }
  }

  @Override
  public IndexOutput createOutputDirectIO(String name) throws IOException {
    return _directory.createOutputDirectIO(name);
  }

  @Override
  public IndexInput openInputDirectIO(String name) throws IOException {
    return _directory.openInputDirectIO(name);
  }
}