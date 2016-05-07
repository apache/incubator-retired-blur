package org.apache.blur.mapreduce.lib.update;

import java.io.IOException;

import org.apache.blur.mapreduce.lib.BlurRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LookupBuilderMapper extends Mapper<Text, BlurRecord, Text, NullWritable> {

  @Override
  protected void map(Text key, BlurRecord value, Mapper<Text, BlurRecord, Text, NullWritable>.Context context)
      throws IOException, InterruptedException {
    context.write(new Text(value.getRowId()), NullWritable.get());
  }

}
