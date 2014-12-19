package org.apache.blur.manager.results;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;

import org.apache.blur.thrift.generated.BlurException;
import org.apache.blur.thrift.generated.BlurResult;
import org.apache.blur.thrift.generated.SortFieldResult;
import org.apache.blur.utils.BlurIterator;
import org.apache.blur.utils.BlurUtil;
import org.apache.blur.utils.ShardUtil;
import org.junit.Test;

public class MultipleBlurResultIterableTest {

  @Test
  public void testMultipleHitsIterableNoSort() throws BlurException, IOException {
    BlurResultIterableMultiple iterable = new BlurResultIterableMultiple();
    Random random = new Random();
    iterable.addBlurResultIterable(newBlurResultIterableNoSort(0, random, 0, 0.1, 3, 2, 9, 10, 2));
    iterable.addBlurResultIterable(newBlurResultIterableNoSort(1, random, 7, 2, 9, 1, 34, 53, 12));
    iterable.addBlurResultIterable(newBlurResultIterableNoSort(2, random, 4, 3));
    iterable.addBlurResultIterable(newBlurResultIterableNoSort(3, random, 7, 2, 34, 132));
    iterable.addBlurResultIterable(newBlurResultIterableNoSort(4, random));

    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult hit = iterator.next();
      System.out.println(hit);
    }
    iterable.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMultipleHitsIterableSort() throws BlurException, IOException {
    BlurResultIterableMultiple iterable = new BlurResultIterableMultiple();
    Random random = new Random();
    iterable.addBlurResultIterable(newBlurResultIterableSort(0, random, e(0, "a"), e(0.1, "b"), e(3, "y"), e(2, "r"),
        e(9, null), e(10, "m"), e(2, "r")));
    iterable.addBlurResultIterable(newBlurResultIterableSort(1, random, e(7, "bb"), e(2, "x"), e(9, "aaaa"),
        e(1, "t-"), e(34, "erw"), e(53, "iow"), e(12, "rewt")));

    BlurIterator<BlurResult, BlurException> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      BlurResult hit = iterator.next();
      System.out.println(hit);
    }
    iterable.close();
  }

  private Entry<Double, List<SortFieldResult>> e(final double score, String s) {
    final List<SortFieldResult> sortFields = new ArrayList<SortFieldResult>();
    SortFieldResult sortField = new SortFieldResult();
    if (s == null) {
      sortField.setNullValue(true);
    } else {
      sortField.setStringValue(s);
    }
    sortFields.add(sortField);
    return new Entry<Double, List<SortFieldResult>>() {

      @Override
      public List<SortFieldResult> getValue() {
        return sortFields;
      }

      @Override
      public Double getKey() {
        return score;
      }

      @Override
      public List<SortFieldResult> setValue(List<SortFieldResult> value) {
        return null;
      }
    };
  }

  private BlurResultIterable newBlurResultIterableSort(int shard, Random random,
      Entry<Double, List<SortFieldResult>>... entries) {
    List<BlurResult> results = new ArrayList<BlurResult>();
    for (Entry<Double, List<SortFieldResult>> entry : entries) {
      String shardName = ShardUtil.getShardName(shard);
      int docId = random.nextInt(Integer.MAX_VALUE);
      Double score = entry.getKey();
      results.add(new BlurResult(shardName + "/" + docId, score, null, entry.getValue()));
    }
    Collections.sort(results, new Comparator<BlurResult>() {
      @Override
      public int compare(BlurResult o1, BlurResult o2) {
        List<SortFieldResult> sortFields1 = o1.getSortFieldResults();
        List<SortFieldResult> sortFields2 = o2.getSortFieldResults();
        if (sortFields1.size() != sortFields2.size()) {
          throw new RuntimeException("SortFields must be the same size.");
        }
        for (int i = 0; i < sortFields1.size(); i++) {
          SortFieldResult sortField1 = sortFields1.get(i);
          SortFieldResult sortField2 = sortFields2.get(i);
          int compare = BlurUtil.SORT_FIELD_COMPARATOR.compare(sortField1, sortField2);
          if (compare != 0) {
            return compare;
          }
        }
        double score1 = o1.getScore();
        double score2 = o2.getScore();
        if (score1 == score2) {
          return o1.getLocationId().compareTo(o2.getLocationId());
        }
        return (int) (score1 - score2);
      }
    });
    return new BlurResultIterableSimple(UUID.randomUUID().toString(), results);
  }

  private BlurResultIterable newBlurResultIterableNoSort(int shard, Random random, double... ds) {
    List<BlurResult> results = new ArrayList<BlurResult>();
    for (double d : ds) {
      String shardName = ShardUtil.getShardName(shard);
      int docId = random.nextInt(Integer.MAX_VALUE);
      results.add(new BlurResult(shardName + "/" + docId, d, null, null));
    }
    Collections.sort(results, new Comparator<BlurResult>() {
      @Override
      public int compare(BlurResult o1, BlurResult o2) {
        double score1 = o1.getScore();
        double score2 = o2.getScore();
        if (score1 == score2) {
          return o1.getLocationId().compareTo(o2.getLocationId());
        }
        return (int) (score1 - score2);
      }
    });
    Collections.sort(results, BlurUtil.HITS_COMPARATOR);
    return new BlurResultIterableSimple(UUID.randomUUID().toString(), results);
  }

}
