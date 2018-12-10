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

import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;

import org.apache.blur.command.annotation.OptionalArgument;
import org.apache.blur.command.annotation.RequiredArgument;
import org.apache.blur.log.Log;
import org.apache.blur.log.LogFactory;

public class ArgumentOverlay {

  private static final Log LOG = LogFactory.getLog(ArgumentOverlay.class);

  private final Map<String, ? extends Object> _args;

  public ArgumentOverlay(BlurObject args, BlurObjectSerDe serDe) {
    _args = serDe.deserialize(args);
  }

  public <T> Command<T> setup(Command<T> command) {
    Class<?> clazz = command.getClass();
    setupInternal(clazz, command);
    return setupCommandExecutionId(command);
  }

  private <T> Command<T> setupCommandExecutionId(Command<T> command) {
    String commandExecutionId = command.getCommandExecutionId();
    if (commandExecutionId == null) {
      commandExecutionId = UUID.randomUUID().toString();
      LOG.info("Command execution id [{0}] has been assigned to [{1}]", commandExecutionId, command);
      command.setCommandExecutionId(commandExecutionId);
    }
    return command;
  }

  private void setupInternal(Class<?> clazz, Command<?> command) {
    if (clazz.equals(Command.class)) {
      mapValuesToFields(clazz, command);
      return;
    }
    setupInternal(clazz.getSuperclass(), command);
    mapValuesToFields(clazz, command);
  }

  private void mapValuesToFields(Class<?> clazz, Command<?> command) {
    Field[] declaredFields = clazz.getDeclaredFields();
    for (Field field : declaredFields) {
      RequiredArgument requiredArgument = field.getAnnotation(RequiredArgument.class);
      if (requiredArgument != null) {
        field.setAccessible(true);
        String name = field.getName();
        try {
          Object o = _args.get(name);
          if (o != null) {
            field.set(command, o);
          } else {
            throw new IllegalArgumentException("Field [" + name + "] is required.");
          }
        } catch (IllegalArgumentException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }

      OptionalArgument optionalArgument = field.getAnnotation(OptionalArgument.class);
      if (optionalArgument != null) {
        field.setAccessible(true);
        String name = field.getName();
        try {
          Object o = _args.get(name);
          if (o != null) {
            field.set(command, o);
          }
        } catch (IllegalArgumentException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

}
