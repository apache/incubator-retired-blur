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
package org.apache.blur.thrift.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Method;

import org.apache.blur.thrift.ClientLock;
import org.apache.blur.thrift.generated.Blur;
import org.apache.blur.thrift.generated.Blur.Client;

public class GenerateSafeClient {

  private static final String PUBLIC_CLASS_BLUR = "public class Blur {";

  public static void main(String[] args) throws IOException {
    File file = new File("./src/main/java/" + toPath(Blur.Client.class)).getAbsoluteFile();
    File output = new File(file.getParentFile(), "SafeClientGen.java");
    if (output.exists() && !output.delete()) {
      throw new IOException("Cannot delete [" + output + "]");
    }
    System.out.println("Writing new safe client to [" + output + "]");
    PrintWriter writer = new PrintWriter(output);
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.trim().equals(PUBLIC_CLASS_BLUR)) {
        break;
      }
      writer.println(line);
    }
    reader.close();

    writer.println("public class SafeClientGen extends org.apache.blur.thrift.generated.Blur.Client {");
    writer.println();
    writer.println("private final " + ClientLock.class.getName() + " _lock = new " + ClientLock.class.getName()
        + "(\"client\");");
    writer.println();
    writer
        .println("public SafeClientGen(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol iprot, org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol oprot) {");
    writer.println("super(iprot, oprot);");
    writer.println("}");
    writer.println();
    writer.println("public SafeClientGen(org.apache.blur.thirdparty.thrift_0_9_0.protocol.TProtocol prot) {");
    writer.println("super(prot);");
    writer.println("}");
    writer.println();

    Method[] methods = Blur.Client.class.getDeclaredMethods();
    for (Method method : methods) {
      Class<?>[] exceptionTypes = method.getExceptionTypes();
      Class<?>[] parameterTypes = method.getParameterTypes();
      Class<?> returnType = method.getReturnType();

      String returnTypeStr = getReturnType(returnType);
      writer.println("@Override");
      writer.println("public " + returnTypeStr + " " + method.getName() + "(" + getParams(parameterTypes) + ") throws "
          + getExceptions(exceptionTypes) + " {");
      writer.println("  _lock.errorFailLock(); try {");
      if (returnTypeStr.equals("void")) {
        writer.println("    super." + method.getName() + "(" + getArgs(parameterTypes) + ");");
      } else {
        writer.println("    return super." + method.getName() + "(" + getArgs(parameterTypes) + ");");
      }
      writer.println("  } finally {_lock.unlock();}");
      writer.println("}");
      writer.println();
    }
    writer.println("}");
    writer.close();
  }

  private static String getArgs(Class<?>[] parameterTypes) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < parameterTypes.length; i++) {
      if (builder.length() != 0) {
        builder.append(", ");
      }
      builder.append("arg" + i);
    }
    return builder.toString();
  }

  private static String getReturnType(Class<?> returnType) {
    return returnType.getName();
  }

  private static String getParams(Class<?>[] parameterTypes) {
    StringBuilder builder = new StringBuilder();
    int i = 0;
    for (Class<?> p : parameterTypes) {
      if (builder.length() != 0) {
        builder.append(", ");
      }
      builder.append(p.getName() + " arg" + i);
      i++;
    }
    return builder.toString();
  }

  private static String getExceptions(Class<?>[] exceptionTypes) {
    StringBuilder builder = new StringBuilder();
    for (Class<?> e : exceptionTypes) {
      if (builder.length() != 0) {
        builder.append(", ");
      }
      builder.append(e.getName());
    }
    return builder.toString();
  }

  private static String toPath(Class<Client> clazz) {
    String name = clazz.getName();
    int index = name.indexOf("$");
    return name.substring(0, index).replace(".", "/") + ".java";
  }

}
