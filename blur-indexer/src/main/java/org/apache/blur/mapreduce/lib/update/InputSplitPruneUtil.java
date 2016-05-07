package org.apache.blur.mapreduce.lib.update;

import org.apache.blur.utils.ShardUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class InputSplitPruneUtil {

  private static final String BLUR_LOOKUP_ROWID_UPDATE_FROM_NEW_DATA_COUNT_PREFIX = "blur.lookup.rowid.update.from.new.data.count";
  private static final String BLUR_LOOKUP_ROWID_FROM_NEW_DATA_COUNT_PREFIX = "blur.lookup.rowid.from.new.data.count.";
  private static final String BLUR_LOOKUP_ROWID_FROM_INDEX_COUNT_PREFIX = "blur.lookup.rowid.from.index.count.";

  private static final String BLUR_LOOKUP_TABLE = "blur.lookup.table";
  private static final String BLUR_LOOKUP_RATIO_PER_SHARD = "blur.lookup.ratio.per.shard";
  private static final String BLUR_LOOKUP_MAX_TOTAL_PER_SHARD = "blur.lookup.max.total.per.shard";

  private static final double DEFAULT_LOOKUP_RATIO = 0.5;
  private static final long DEFAULT_LOOKUP_MAX_TOTAL = Long.MAX_VALUE;

  public static boolean shouldLookupExecuteOnShard(Configuration configuration, String table, int shard) {
    double lookupRatio = getLookupRatio(configuration);
    long maxLookupCount = getMaxLookupCount(configuration);
    long rowIdFromNewDataCount = getBlurLookupRowIdFromNewDataCount(configuration, table, shard);
    long rowIdUpdateFromNewDataCount = getBlurLookupRowIdUpdateFromNewDataCount(configuration, table, shard);
    long rowIdFromIndexCount = getBlurLookupRowIdFromIndexCount(configuration, table, shard);
    return shouldLookupRun(rowIdFromIndexCount, rowIdFromNewDataCount, rowIdUpdateFromNewDataCount, lookupRatio,
        maxLookupCount);
  }

  private static boolean shouldLookupRun(long rowIdFromIndexCount, long rowIdFromNewDataCount,
      long rowIdUpdateFromNewDataCount, double lookupRatio, long maxLookupCount) {
    if (rowIdUpdateFromNewDataCount > maxLookupCount) {
      return false;
    }
    double d = (double) rowIdUpdateFromNewDataCount / (double) rowIdFromIndexCount;
    if (d <= lookupRatio) {
      return true;
    }
    return false;
  }

  public static double getLookupRatio(Configuration configuration) {
    return configuration.getDouble(BLUR_LOOKUP_RATIO_PER_SHARD, DEFAULT_LOOKUP_RATIO);
  }

  private static long getMaxLookupCount(Configuration configuration) {
    return configuration.getLong(BLUR_LOOKUP_MAX_TOTAL_PER_SHARD, DEFAULT_LOOKUP_MAX_TOTAL);
  }

  public static void setTable(Job job, String table) {
    setTable(job.getConfiguration(), table);
  }

  public static void setTable(Configuration configuration, String table) {
    configuration.set(BLUR_LOOKUP_TABLE, table);
  }

  public static String getTable(Configuration configuration) {
    return configuration.get(BLUR_LOOKUP_TABLE);
  }

  public static String getBlurLookupRowIdFromIndexCountName(String table) {
    return BLUR_LOOKUP_ROWID_FROM_INDEX_COUNT_PREFIX + table;
  }

  public static String getBlurLookupRowIdFromNewDataCountName(String table) {
    return BLUR_LOOKUP_ROWID_FROM_NEW_DATA_COUNT_PREFIX + table;
  }

  public static String getBlurLookupRowIdUpdateFromNewDataCountName(String table) {
    return BLUR_LOOKUP_ROWID_UPDATE_FROM_NEW_DATA_COUNT_PREFIX + table;
  }

  public static long getBlurLookupRowIdUpdateFromNewDataCount(Configuration configuration, String table, int shard) {
    String[] strings = configuration.getStrings(getBlurLookupRowIdUpdateFromNewDataCountName(table));
    return getCount(strings, shard);
  }

  public static long getBlurLookupRowIdFromNewDataCount(Configuration configuration, String table, int shard) {
    String[] strings = configuration.getStrings(getBlurLookupRowIdFromNewDataCountName(table));
    return getCount(strings, shard);
  }

  public static long getBlurLookupRowIdFromIndexCount(Configuration configuration, String table, int shard) {
    String[] strings = configuration.getStrings(getBlurLookupRowIdFromIndexCountName(table));
    return getCount(strings, shard);
  }

  public static void setBlurLookupRowIdFromNewDataCounts(Job job, String table, long[] counts) {
    setBlurLookupRowIdFromNewDataCounts(job.getConfiguration(), table, counts);
  }

  public static void setBlurLookupRowIdFromNewDataCounts(Configuration configuration, String table, long[] counts) {
    configuration.setStrings(getBlurLookupRowIdFromNewDataCountName(table), toStrings(counts));
  }

  public static void setBlurLookupRowIdUpdateFromNewDataCounts(Job job, String table, long[] counts) {
    setBlurLookupRowIdUpdateFromNewDataCounts(job.getConfiguration(), table, counts);
  }

  public static void setBlurLookupRowIdUpdateFromNewDataCounts(Configuration configuration, String table, long[] counts) {
    configuration.setStrings(getBlurLookupRowIdUpdateFromNewDataCountName(table), toStrings(counts));
  }

  public static void setBlurLookupRowIdFromIndexCounts(Job job, String table, long[] counts) {
    setBlurLookupRowIdFromIndexCounts(job.getConfiguration(), table, counts);
  }

  public static void setBlurLookupRowIdFromIndexCounts(Configuration configuration, String table, long[] counts) {
    configuration.setStrings(getBlurLookupRowIdFromIndexCountName(table), toStrings(counts));
  }

  public static long getCount(String[] strings, int shard) {
    return Long.parseLong(strings[shard]);
  }

  public static int getShardFromDirectoryPath(Path path) {
    return ShardUtil.getShardIndex(path.getName());
  }

  public static String[] toStrings(long[] counts) {
    if (counts == null) {
      return null;
    }
    String[] strs = new String[counts.length];
    for (int i = 0; i < counts.length; i++) {
      strs[i] = Long.toString(counts[i]);
    }
    return strs;
  }

}
