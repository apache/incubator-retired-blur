/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.blur.mapreduce.lib;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;
import org.apache.blur.lucene.security.DocumentVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

/**
 * This utility code was taken from HBase to locate classes and the jars files
 * to add to the MapReduce Job.
 */
public class BlurMapReduceUtil {

  private final static Log LOG = LogFactory.getLog(BlurMapReduceUtil.class);

  /**
   * Add the Blur dependency jars as well as jars for any of the configured job
   * classes to the job configuration, so that JobClient will ship them to the
   * cluster and add them to the DistributedCache.
   */
  public static void addDependencyJars(Job job) throws IOException {
    try {
      addDependencyJars(job.getConfiguration(), org.apache.zookeeper.ZooKeeper.class, job.getMapOutputKeyClass(),
          job.getMapOutputValueClass(), job.getInputFormatClass(), job.getOutputKeyClass(), job.getOutputValueClass(),
          job.getOutputFormatClass(), job.getPartitionerClass(), job.getCombinerClass(), DocumentVisibility.class);
      addAllJarsInBlurLib(job.getConfiguration());
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Adds all the jars in the same path as the blur jar files.
   * 
   * @param conf
   * @throws IOException
   */
  public static void addAllJarsInBlurLib(Configuration conf) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Set<String> jars = new HashSet<String>();
    jars.addAll(conf.getStringCollection("tmpjars"));

    String property = System.getProperty("java.class.path");
    String[] files = property.split("\\:");

    String blurLibPath = getPath("blur-", files);
    if (blurLibPath == null) {
      return;
    }
    List<String> pathes = getPathes(blurLibPath, files);
    for (String pathStr : pathes) {
      Path path = new Path(pathStr);
      if (!localFs.exists(path)) {
        LOG.warn("Could not validate jar file " + path);
        continue;
      }
      jars.add(path.makeQualified(localFs.getUri(), localFs.getWorkingDirectory()).toString());
    }
    if (jars.isEmpty()) {
      return;
    }
    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[0])));
  }

  private static List<String> getPathes(String path, String[] files) {
    List<String> pathes = new ArrayList<String>();
    for (String file : files) {
      if (file.startsWith(path)) {
        pathes.add(file);
      }
    }
    return pathes;
  }

  private static String getPath(String startsWith, String[] files) {
    for (String file : files) {
      int lastIndexOf = file.lastIndexOf('/');
      String fileName = file.substring(lastIndexOf + 1);
      if (fileName.startsWith(startsWith)) {
        return file.substring(0, lastIndexOf);
      }
    }
    return null;
  }

  /**
   * Add the jars containing the given classes to the job's configuration such
   * that JobClient will ship them to the cluster and add them to the
   * DistributedCache.
   */
  public static void addDependencyJars(Configuration conf, Class<?>... classes) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Set<String> jars = new HashSet<String>();
    // Add jars that are already in the tmpjars variable
    jars.addAll(conf.getStringCollection("tmpjars"));

    // Add jars containing the specified classes
    for (Class<?> clazz : classes) {
      if (clazz == null) {
        continue;
      }

      String pathStr = findOrCreateJar(clazz);
      if (pathStr == null) {
        LOG.warn("Could not find jar for class " + clazz + " in order to ship it to the cluster.");
        continue;
      }
      Path path = new Path(pathStr);
      if (!localFs.exists(path)) {
        LOG.warn("Could not validate jar file " + path + " for class " + clazz);
        continue;
      }
      jars.add(path.makeQualified(localFs.getUri(), localFs.getWorkingDirectory()).toString());
    }
    if (jars.isEmpty()) {
      return;
    }

    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[0])));
  }

  /**
   * If org.apache.hadoop.util.JarFinder is available (0.23+ hadoop), finds the
   * Jar for a class or creates it if it doesn't exist. If the class is in a
   * directory in the classpath, it creates a Jar on the fly with the contents
   * of the directory and returns the path to that Jar. If a Jar is created, it
   * is created in the system temporary directory.
   * 
   * Otherwise, returns an existing jar that contains a class of the same name.
   * 
   * @param my_class
   *          the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException
   */
  private static String findOrCreateJar(Class<?> my_class) throws IOException {
    try {
      Class<?> jarFinder = Class.forName("org.apache.hadoop.util.JarFinder");
      // hadoop-0.23 has a JarFinder class that will create the jar
      // if it doesn't exist. Note that this is needed to run the mapreduce
      // unit tests post-0.23, because mapreduce v2 requires the relevant jars
      // to be in the mr cluster to do output, split, etc. At unit test time,
      // the hbase jars do not exist, so we need to create some. Note that we
      // can safely fall back to findContainingJars for pre-0.23 mapreduce.
      Method m = jarFinder.getMethod("getJar", Class.class);
      return (String) m.invoke(null, my_class);
    } catch (InvocationTargetException ite) {
      // function was properly called, but threw it's own exception
      throw new IOException(ite.getCause());
    } catch (Exception e) {
      // ignore all other exceptions. related to reflection failure
    }

    LOG.debug("New JarFinder: org.apache.hadoop.util.JarFinder.getJar " + "not available.  Using old findContainingJar");
    return findContainingJar(my_class);
  }

  /**
   * Find a jar that contains a class of the same name, if any. It will return a
   * jar file, even if that is not the first thing on the class path that has a
   * class with the same name.
   * 
   * This is shamelessly copied from JobConf
   * 
   * @param my_class
   *          the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException
   */
  private static String findContainingJar(Class<?> my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration<URL> itr = loader.getResources(class_file); itr.hasMoreElements();) {
        URL url = itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          // URLDecoder is a misnamed class, since it actually decodes
          // x-www-form-urlencoded MIME type rather than actual
          // URL encoding (which the file path has). Therefore it would
          // decode +s to ' 's which is incorrect (spaces are actually
          // either unencoded or encoded as "%20"). Replace +s first, so
          // that they are kept sacred during the decoding process.
          toReturn = toReturn.replaceAll("\\+", "%2B");
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
