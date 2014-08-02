package org.apache.blur.console.util;

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


import org.apache.blur.BlurConfiguration;
import org.apache.blur.console.providers.AuthenticationDenied;
import org.apache.blur.console.providers.EmptyAuthorization;
import org.apache.blur.console.providers.IAuthenticationProvider;
import org.apache.blur.console.providers.IAuthorizationProvider;
import org.apache.blur.manager.clusterstatus.ZookeeperClusterStatus;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.user.User;
import org.apache.blur.user.UserContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

public class Config {

  private static final File TMPDIR = new File(System.getProperty("blur.tmp.dir", "./target/mini-cluster"));
  private static final Log log = LogFactory.getLog(Config.class);
  private static final int DEFAULT_PORT = 8080;

  private static int port;
  private static BlurConfiguration blurConfig;
  private static ZookeeperClusterStatus zk;
  private static String blurConnection;
  private static Object cluster;
  private static IAuthenticationProvider authenticationProvider;
  private static IAuthorizationProvider authorizationProvider;

  public static int getConsolePort() {
    return port;
  }

  public static BlurConfiguration getBlurConfig() {
    return blurConfig;
  }

  public static void setupConfig() throws Exception {
    if (cluster == null) {
      blurConfig = new BlurConfiguration();
    } else { // in dev mode
      blurConfig = new BlurConfiguration(false);
      setDevelopmentZookeeperConnection();
      setDevelopmentProperties();
    }
    zk = new ZookeeperClusterStatus(blurConfig.get("blur.zookeeper.connection"), blurConfig);
    blurConnection = buildConnectionString();
    port = blurConfig.getInt("blur.console.port", DEFAULT_PORT);
    setupProviders();
  }

  private static void setDevelopmentZookeeperConnection() {
    String zkConnection = "";
    try {
      Method zkMethod = cluster.getClass().getMethod("getZkConnectionString");
      zkConnection = (String) zkMethod.invoke(cluster);
    } catch (Exception e) {
      log.fatal("Unable get zookeeper connection string", e);
    }

    blurConfig.set("blur.zookeeper.connection", zkConnection);
  }

  private static void setDevelopmentProperties() {
    Properties properties = System.getProperties();
    for (String name : properties.stringPropertyNames()) {
      if (name.startsWith("blur.")) {
        blurConfig.set(name, properties.getProperty(name));
      }
    }
  }

  private static void setupProviders() throws Exception {
    String authenticationProviderClassName = blurConfig.get("blur.console.authentication.provider", "org.apache.blur.console.providers.AllAuthenticated");
    String authorizationProviderClassName = blurConfig.get("blur.console.authorization.provider");


    Class authenticationProviderClass = Class.forName(authenticationProviderClassName, false, Config.class.getClassLoader());

    if (authenticationProviderClass == null) {
      authenticationProvider = new AuthenticationDenied();
      log.error("Error in blur.console.authentication.provider: AuthenticationDenied");
    } else {
      authenticationProvider = (IAuthenticationProvider) authenticationProviderClass.newInstance();
      log.info("Using " + authenticationProviderClassName + " for authentication");
      if(authenticationProviderClassName.equals(authorizationProviderClassName)) {
        log.info("authentication and authorization providers are the same, reusing");
        authorizationProvider = (IAuthorizationProvider) authenticationProvider;
      }
    }
    authenticationProvider.setupProvider(blurConfig);

    if(authorizationProvider == null) {
      if(authorizationProviderClassName != null) {
        Class authorizationProviderClass = Class.forName(authorizationProviderClassName, false, Config.class.getClassLoader());
        if (authorizationProviderClass == null) {
          log.error("Error in blur.console.authorization.provider: EmptyAuthorization");
          authorizationProvider = new EmptyAuthorization();
        } else {
          log.info("Using " + authorizationProviderClassName + " for authorization");
          authorizationProvider = (IAuthorizationProvider) authorizationProviderClass.newInstance();
        }
      } else {
        authorizationProvider = new EmptyAuthorization();
      }
      authorizationProvider.setupProvider(blurConfig);
    }
  }

  public static String getConnectionString() throws IOException {
    return blurConnection;
  }

  public static ZookeeperClusterStatus getZookeeper() {
    return zk;
  }

  private static String buildConnectionString() {
    List<String> allControllers = new ArrayList<String>();
    allControllers = zk.getControllerServerList();
    return StringUtils.join(allControllers, ",");
  }

  public static void shutdownMiniCluster() throws IOException {
    if (cluster != null) {
      try {
        Method method = cluster.getClass().getMethod("shutdownBlurCluster");
        method.invoke(cluster);
      } catch (Exception e) {
        log.fatal("Unable to stop mini cluster through reflection.", e);
      }
      File file = new File(TMPDIR, "blur-cluster-test");
      if (file.exists()) {
        FileUtils.deleteDirectory(file);
      }
    }
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void setupMiniCluster() throws IOException {
    File testDirectory = new File(TMPDIR, "blur-cluster-test").getAbsoluteFile();
    testDirectory.mkdirs();

    testDirectory.delete();
    try {
      Class clusterClass = Class.forName("org.apache.blur.MiniCluster", false, Config.class.getClassLoader());

      if (clusterClass != null) {
        cluster = clusterClass.newInstance();
        Method startBlurCluster = clusterClass.getDeclaredMethod("startBlurCluster", String.class, int.class, int.class, boolean.class);
        startBlurCluster.invoke(cluster, new File(testDirectory, "cluster").getAbsolutePath(), 2, 3, true);
      }
    } catch (Exception e) {
      log.fatal("Unable to start in dev mode because MiniCluster isn't in classpath", e);
      cluster = null;
    }
  }

  public static Iface getClient(org.apache.blur.console.model.User user, String securityUser) throws IOException {
    Iface client = BlurClient.getClient(getConnectionString());
    Map<String, String> securityAttributes = user.getSecurityAttributes(securityUser);
    if(securityAttributes != null) {
      UserContext.setUser(new User(user.getName(), securityAttributes));
    }

    return client;
  }

  public static boolean isClusterSetup() {
    return cluster != null;
  }

  public static IAuthenticationProvider getAuthenticationProvider() {
    return authenticationProvider;
  }

  public static IAuthorizationProvider getAuthorizationProvider() {
    return authorizationProvider;
  }
}
