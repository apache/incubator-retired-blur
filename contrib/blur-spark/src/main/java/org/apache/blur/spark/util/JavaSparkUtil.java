package org.apache.blur.spark.util;

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



import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;

import com.google.common.base.Splitter;

public class JavaSparkUtil {

  private static final String DOT = ".";
  private static final String TMP_SPARK_JOB = "tmp-spark-job_";
  private static final String JAR = ".jar";
  private static final String PATH_SEPARATOR = "path.separator";
  private static final String JAVA_CLASS_PATH = "java.class.path";
  private static final String SEP = "/";

  public static void packProjectJars(SparkConf conf) throws IOException {
    String classPath = System.getProperty(JAVA_CLASS_PATH);
    String pathSeparator = System.getProperty(PATH_SEPARATOR);
    Splitter splitter = Splitter.on(pathSeparator);
    Iterable<String> split = splitter.split(classPath);
    List<String> list = toList(split);
    List<String> classPathThatNeedsToBeIncluded = removeSparkLibs(list);
    List<String> jars = new ArrayList<String>();
    for (String s : classPathThatNeedsToBeIncluded) {
      if (isJarFile(s)) {
        jars.add(s);
      } else {
        jars.add(createJar(s));
      }
    }
    conf.setJars(jars.toArray(new String[jars.size()]));
  }

  private static String createJar(String s) throws IOException {
    File sourceFile = new File(s);
    if (sourceFile.isDirectory()) {
      File file = File.createTempFile(TMP_SPARK_JOB, JAR);
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

  private static List<String> removeSparkLibs(List<String> list) {
    String sparkJar = findSparkJar(list);
    String sparkLib = sparkJar.substring(0, sparkJar.lastIndexOf(SEP) + 1);
    List<String> result = new ArrayList<String>();
    for (String s : list) {
      if (!s.startsWith(sparkLib)) {
        result.add(s);
      }
    }
    return result;
  }

  private static String findSparkJar(List<String> list) {
    String name = SparkConf.class.getName();
    String resourceName = SEP + name.replace(DOT, SEP) + ".class";
    URL url = SparkConf.class.getResource(resourceName);
    String urlStr = url.toString();
    urlStr = urlStr.substring(0, urlStr.indexOf('!'));
    urlStr = urlStr.substring(urlStr.lastIndexOf(':') + 1);
    return urlStr;
  }

  private static List<String> toList(Iterable<String> split) {
    List<String> list = new ArrayList<String>();
    for (String s : split) {
      list.add(s);
    }
    return list;
  }
}