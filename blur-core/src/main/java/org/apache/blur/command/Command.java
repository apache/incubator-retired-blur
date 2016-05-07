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
package org.apache.blur.command;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

import org.apache.blur.command.annotation.OptionalArgument;
import org.apache.blur.command.annotation.RequiredArgument;
import org.apache.blur.thrift.generated.Blur.Iface;

public abstract class Command<R> implements Cloneable {

  @OptionalArgument
  private String commandExecutionId;

  public abstract String getName();

  public abstract R run() throws IOException;

  public abstract R run(String connectionStr) throws IOException;

  public abstract R run(Iface client) throws IOException;

  public abstract Set<String> routeTables(BaseContext context);

  public abstract Set<Shard> routeShards(BaseContext context, Set<String> tables);

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public Command<R> clone() {
    try {
      return (Command) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> Set<T> asSet(T t) {
    Set<T> set = new HashSet<T>();
    set.add(t);
    return set;
  }

  public Set<Argument> getRequiredArguments() {
    Set<Argument> arguments = new HashSet<Argument>();
    readArguments(getClass(), arguments, RequiredArgument.class);
    return arguments;
  }

  public Set<Argument> getOptionalArguments() {
    Set<Argument> arguments = new HashSet<Argument>();
    readArguments(getClass(), arguments, OptionalArgument.class);
    return arguments;
  }

  public abstract String getReturnType();

  private void readArguments(Class<?> clazz, Set<Argument> arguments, Class<? extends Annotation> annotationClass) {
    if (clazz.equals(Command.class)) {
      return;
    }
    readArguments(clazz.getSuperclass(), arguments, annotationClass);
    Field[] declaredFields = clazz.getDeclaredFields();
    for (Field field : declaredFields) {
      field.setAccessible(true);
      Annotation annotation = field.getAnnotation(annotationClass);
      Class<?> type = field.getType();
      Type genericType = field.getGenericType();
      Type[] actualTypeArguments = null;
      if (genericType instanceof ParameterizedType) {
        ParameterizedType parameterizedType = (ParameterizedType) genericType;
        actualTypeArguments = parameterizedType.getActualTypeArguments();
      }
      String simpleName = getTypeName(type.getSimpleName(), actualTypeArguments);
      String name = field.getName();
      if (annotation instanceof RequiredArgument) {
        RequiredArgument requiredArgument = (RequiredArgument) annotation;
        String value = requiredArgument.value();
        arguments.add(new Argument(name, simpleName, value));
      } else if (annotation instanceof OptionalArgument) {
        OptionalArgument optionalArgument = (OptionalArgument) annotation;
        String value = optionalArgument.value();
        arguments.add(new Argument(name, simpleName, value));
      }
    }
  }

  private String getTypeName(String simpleName, Type[] actualTypeArguments) {
    if (actualTypeArguments == null) {
      return simpleName;
    }
    StringBuilder builder = new StringBuilder();
    builder.append(simpleName).append('<');
    for (Type type : actualTypeArguments) {
      if (type instanceof Class) {
        builder.append(((Class<?>) type).getSimpleName());
      }
    }
    builder.append('>');
    return builder.toString();
  }

  public String getCommandExecutionId() {
    return commandExecutionId;
  }

  public void setCommandExecutionId(String commandExecutionId) {
    this.commandExecutionId = commandExecutionId;
  }

}
