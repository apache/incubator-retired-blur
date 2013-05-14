package org.apache.blur.mapreduce.lib;

import java.io.IOException;

import org.apache.blur.mapreduce.BlurMutate;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DefaultBlurReducer extends Reducer<Text, BlurMutate, Text, BlurMutate> {

  @Override
  protected void reduce(Text key, Iterable<BlurMutate> values, Context context) throws IOException,
      InterruptedException {
    for (BlurMutate value : values) {
      context.write(key, value);
    }
  }
}
