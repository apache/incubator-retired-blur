package org.apache.blur.spark;

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

import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.blur.thrift.generated.RowMutation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Time;

import scala.Tuple2;

@SuppressWarnings("serial")
public abstract class BlurBulkLoadSparkProcessor<T> extends BlurLoadSparkProcessor<T> {
  
  private static final Log LOG = LogFactory.getLog(BlurBulkLoadSparkProcessor.class);

  @Override
  protected Function2<JavaPairRDD<String, RowMutation>, Time, Void> getFunction() {
    return new Function2<JavaPairRDD<String, RowMutation>, Time, Void>() {
      // Blur Thrift Client
      @Override
      public Void call(JavaPairRDD<String, RowMutation> rdd, Time time) throws Exception {
        Iface client = getBlurClient();
        for (Tuple2<String, RowMutation> tuple : rdd.collect()) {
          if (tuple != null) {
            try {
              RowMutation rm = tuple._2;
              // Index using enqueue mutate call
              client.enqueueMutate(rm);
            } catch (Exception ex) {
              LOG.error("Unknown error while trying to call enqueueMutate.", ex);
              throw ex;
            }
          }
        }
        return null;
      }
    };
  }

}
