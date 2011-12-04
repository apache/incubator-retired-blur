package com.nearinfinity.blur.store;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BufferStore {
  
  private static BlockingQueue<byte[]> _1024 = new ArrayBlockingQueue<byte[]>(1024 * 8);
  private static BlockingQueue<byte[]> _8192 = new ArrayBlockingQueue<byte[]>(1024 * 8);

  public static byte[] takeBuffer(int bufferSize) {
    switch (bufferSize) {
    case 1024:
      return newBuffer(1024,_1024.poll());
    case 8192:
      return newBuffer(8192,_8192.poll());
    default:
      return newBuffer(bufferSize,null);
    }
  }
  
  public static void putBuffer(byte[] buffer) {
    if (buffer == null) {
      return;
    }
    int bufferSize = buffer.length;
    switch (bufferSize) {
    case 1024:
      checkReturn(_1024.offer(buffer));
      return;
    case 8192:
      checkReturn(_8192.offer(buffer));
      return;
    }
  }

  private static void checkReturn(boolean offer) {
    if (!offer) {
      System.out.println("buffer lost");
    }
  }

  private static byte[] newBuffer(int size, byte[] buf) {
    if (buf != null) {
      return buf;
    }
    return new byte[size];
  }
}
