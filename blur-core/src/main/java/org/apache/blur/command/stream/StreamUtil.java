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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;

import com.google.common.base.Splitter;

public class StreamUtil {

  private static final String UTF_8 = "UTF-8";
  private static final char JAR_END = '!';
  private static final String JAR_START = "jar:";
  private static final String TMP_BLUR_JOB = "tmp-blur-job_";
  private static final String JAR = ".jar";
  private static final String SEP = "/";
  private static final String PATH_SEPARATOR = "path.separator";
  private static final String JAVA_CLASS_PATH = "java.class.path";

  public static void writeString(DataOutput output, String s) throws IOException {
    try {
      byte[] bs = s.getBytes(UTF_8);
      output.writeInt(bs.length);
      output.write(bs);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Does not supported UTF-8?", e);
    }
  }

  public static String readString(DataInput input) throws IOException {
    int length = input.readInt();
    byte[] buf = new byte[length];
    input.readFully(buf);
    return new String(buf, UTF_8);
  }

  public static byte[] toBytes(Serializable s) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(outputStream);
    out.writeObject(s);
    out.close();
    return outputStream.toByteArray();
  }

  public static String[] getJarPaths(Class<?>... clazzes) throws IOException {
    Set<String> classPathThatNeedsToBeIncluded = findJarFiles(clazzes);
    Set<String> jars = new HashSet<String>();
    for (String s : classPathThatNeedsToBeIncluded) {
      if (isJarFile(s)) {
        jars.add(s);
      } else {
        jars.add(createJar(s));
      }
    }
    return jars.toArray(new String[jars.size()]);
  }

  private static Set<String> findJarFiles(Class<?>[] clazzes) {
    Set<String> result = new HashSet<String>();
    for (Class<?> c : clazzes) {
      result.add(findJarFile(c));
    }
    return result;
  }

  private static String findJarFile(Class<?> c) {
    String resourceName = "/" + c.getName().replace('.', '/') + ".class";
    URL url = StreamUtil.class.getResource(resourceName);
    try {
      URI uri = url.toURI();
      if (uri.getScheme().equals("file")) {
        return findFileInClassFileUri(uri);
      } else if (uri.getScheme().equals("jar")) {
        return findFileInJarFileUri(uri);
      } else {
        throw new RuntimeException("Unsupported schema [" + uri.getScheme() + "] for uri [" + uri + "]");
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static String findFileInClassFileUri(URI uri) {
    String classPath = System.getProperty(JAVA_CLASS_PATH);
    String pathSeparator = System.getProperty(PATH_SEPARATOR);
    Splitter splitter = Splitter.on(pathSeparator);
    Iterable<String> split = splitter.split(classPath);
    String path = uri.getPath();
    for (String s : split) {
      if (path.startsWith(s)) {
        return new File(s).getAbsolutePath();
      }
    }
    throw new RuntimeException("Uri [" + uri + "] was not found on the classpath.");
  }

  private static String findFileInJarFileUri(URI uri) throws URISyntaxException {
    String s = uri.toString();
    int indexOf1 = s.indexOf(JAR_START);
    int indexOf2 = s.indexOf(JAR_END);
    return new File(new URI(s.substring(indexOf1 + JAR_START.length(), indexOf2))).getAbsolutePath();
  }

  private static String createJar(String s) throws IOException {
    File sourceFile = new File(s);
    if (sourceFile.isDirectory()) {
      File file = File.createTempFile(TMP_BLUR_JOB, JAR);
      OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
      JarOutputStream jarOut = new JarOutputStream(outputStream);
      for (File f : sourceFile.listFiles()) {
        pack(sourceFile, f, jarOut);
      }
      jarOut.close();
      file.deleteOnExit();
      return file.getAbsolutePath();
    }
    throw new RuntimeException("File [" + s + "] is not a directory.");
  }

  private static void pack(File rootPath, File source, JarOutputStream target) throws IOException {
    String name = getName(rootPath, source);
    if (source.isDirectory()) {
      if (!SEP.equals(name)) {
        JarEntry entry = new JarEntry(name);
        entry.setTime(source.lastModified());
        target.putNextEntry(entry);
        target.closeEntry();
      }
      for (File f : source.listFiles()) {
        pack(rootPath, f, target);
      }
    } else {
      JarEntry entry = new JarEntry(name);
      entry.setTime(source.lastModified());
      target.putNextEntry(entry);
      BufferedInputStream in = new BufferedInputStream(new FileInputStream(source));
      IOUtils.copy(in, target);
      in.close();
      target.closeEntry();
    }
  }

  private static String getName(File rootPath, File source) {
    String rootStr = rootPath.toURI().toString();
    String sourceStr = source.toURI().toString();
    if (sourceStr.startsWith(rootStr)) {
      String result = sourceStr.substring(rootStr.length());
      if (source.isDirectory() && !result.endsWith(SEP)) {
        result += SEP;
      }
      return result;
    } else {
      throw new RuntimeException("Not sure what happened.");
    }
  }

  private static boolean isJarFile(String s) {
    if (s.endsWith(JAR) || s.endsWith(".zip")) {
      return true;
    }
    return false;
  }

}
