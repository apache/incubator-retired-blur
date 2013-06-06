package org.apache.blur.thrift;

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
import org.apache.blur.thirdparty.thrift_0_9_0.TException;
import org.apache.blur.thrift.generated.BlurException;

public abstract class AbstractCommand<CLIENT, T> implements Cloneable {

  private boolean detachClient = false;

  /**
   * Reads if this command is to detach the client from the pool or not. If
   * detach is set to true, then the user of the call needs to return the client
   * to the pool by calling returnClient on the {@link BlurClientManager}.
   * 
   * @return the boolean.
   */
  public boolean isDetachClient() {
    return detachClient;
  }

  /**
   * Sets the attribute of detach client.
   * 
   * @param detachClient
   *          the boolean value.
   */
  public void setDetachClient(boolean detachClient) {
    this.detachClient = detachClient;
  }

  /**
   * If this method is implemented then the call(CLIENT client) method is not
   * called. This allows the command to gain access to the {@link Connection}
   * object that is not normally needed. Usually used in conjunction with the
   * detachClient attribute.
   * 
   * @param client
   *          the client.
   * @param connection
   *          the connection object.
   * @return object.
   * @throws BlurException
   * @throws TException
   */
  public T call(CLIENT client, Connection connection) throws BlurException, TException {
    return call(client);
  }

  /**
   * Abstract method that will be executed with a CLIENT object. And it will be
   * retried if a {@link TException} is throw (that type of exception is assumed
   * to be a problem with the connection to the remote system).
   * 
   * @param client the client.
   * @return object.
   * @throws BlurException
   * @throws TException
   */
  public abstract T call(CLIENT client) throws BlurException, TException;

  @SuppressWarnings("unchecked")
  @Override
  public AbstractCommand<CLIENT, T> clone() {
    try {
      return (AbstractCommand<CLIENT, T>) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}
