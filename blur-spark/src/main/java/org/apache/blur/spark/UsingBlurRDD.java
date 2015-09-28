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
package org.apache.blur.spark;

import java.io.IOException;

import org.apache.blur.command.IndexContext;
import org.apache.blur.command.stream.StreamFunction;
import org.apache.blur.command.stream.StreamWriter;
import org.apache.blur.thrift.BlurClient;
import org.apache.blur.thrift.generated.Blur.Iface;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UsingBlurRDD {

  @SuppressWarnings("serial")
  public static void main(String[] args) throws IOException {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName("test");
    sparkConf.setMaster("local[2]");
    BlurSparkUtil.packJars(sparkConf, UsingBlurRDD.class);
    JavaSparkContext context = new JavaSparkContext(sparkConf);

    Iface client = BlurClient.getClient("127.0.0.1:40020");
    BlurRDD blurRDD = new BlurRDD(client, sparkConf);
    String table = "test1234";
    final String field = "fam0.col0";

    for (int i = 0; i < 1; i++) {
      long s = System.nanoTime();
      JavaRDD<String> rdd = blurRDD.executeStream(context, table, new StreamFunction<String>() {
        @Override
        public void call(IndexContext indexContext, StreamWriter<String> writer) throws Exception {
          IndexReader indexReader = indexContext.getIndexReader();
          for (AtomicReaderContext atomicReaderContext : indexReader.leaves()) {
            AtomicReader reader = atomicReaderContext.reader();
            Terms terms = reader.fields().terms(field);
            if (terms != null) {
              TermsEnum termsEnum = terms.iterator(null);
              BytesRef ref;
              while ((ref = termsEnum.next()) != null) {
                writer.write(ref.utf8ToString());
              }
            }
          }
        }
      });
      long count = rdd.distinct().count();
      long e = System.nanoTime();

      System.out.println(count + " " + (e - s) / 1000000.0 + " ms");

    }
    // Iterator<String> iterator = rdd.distinct().toLocalIterator();
    // while (iterator.hasNext()) {
    // System.out.println(iterator.next());
    // }
    context.close();
  }

}
