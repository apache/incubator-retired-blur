package org.apache.blur.mapreduce.lib.update;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsConfigurationNamespaceMerge {

  private static final String DFS_NAMESERVICES = "dfs.nameservices";
  private static final Log LOG = LogFactory.getLog(HdfsConfigurationNamespaceMerge.class);

  public static void main(String[] args) throws IOException {
    Path p = new Path("./src/main/scripts/conf/hdfs");

    Configuration configuration = mergeHdfsConfigs(p.getFileSystem(new Configuration()), p);

    // configuration.writeXml(System.out);

    Collection<String> nameServices = configuration.getStringCollection(DFS_NAMESERVICES);
    for (String name : nameServices) {
      Path path = new Path("hdfs://" + name + "/");
      FileSystem fileSystem = path.getFileSystem(configuration);
      FileStatus[] listStatus = fileSystem.listStatus(path);
      for (FileStatus fileStatus : listStatus) {
        System.out.println(fileStatus.getPath());
      }
    }
  }

  private static boolean checkHostName(String host) {
    try {
      InetAddress.getAllByName(host);
      return true;
    } catch (UnknownHostException e) {
      LOG.warn("Host not found [" + host + "]");
      return false;
    }
  }

  public static Configuration mergeHdfsConfigs(FileSystem fs, Path p) throws IOException {
    List<Configuration> configList = new ArrayList<Configuration>();
    gatherConfigs(fs, p, configList);
    return merge(configList);
  }

  public static Configuration merge(List<Configuration> configList) throws IOException {
    Configuration merge = new Configuration(false);
    Set<String> nameServices = new HashSet<String>();
    for (Configuration configuration : configList) {
      String nameService = configuration.get(DFS_NAMESERVICES);
      if (nameServices.contains(nameService)) {
        throw new IOException("Multiple confs define namespace [" + nameService + "]");
      }
      nameServices.add(nameService);
      if (shouldAdd(configuration, nameService)) {
        for (Entry<String, String> e : configuration) {
          String key = e.getKey();
          if (key.contains(nameService)) {
            String value = e.getValue();
            merge.set(key, value);
          }
        }
      }
    }
    merge.set(DFS_NAMESERVICES, StringUtils.join(nameServices, ","));
    return merge;
  }

  private static boolean shouldAdd(Configuration configuration, String nameService) {
    for (Entry<String, String> e : configuration) {
      String key = e.getKey();
      if (key.contains(nameService) && key.startsWith("dfs.namenode.rpc-address.")) {
        return checkHostName(getHost(e.getValue()));
      }
    }
    return false;
  }

  private static String getHost(String host) {
    return host.substring(0, host.indexOf(":"));
  }

  public static void gatherConfigs(FileSystem fs, Path p, List<Configuration> configList) throws IOException {
    if (fs.isFile(p)) {
      if (p.getName().endsWith(".xml")) {
        LOG.info("Loading file [" + p + "]");
        Configuration configuration = new Configuration(false);
        configuration.addResource(p);
        configList.add(configuration);
      } else {
        LOG.info("Skipping file [" + p + "]");
      }
    } else {
      FileStatus[] listStatus = fs.listStatus(p);
      for (FileStatus fileStatus : listStatus) {
        gatherConfigs(fs, fileStatus.getPath(), configList);
      }
    }
  }

}
