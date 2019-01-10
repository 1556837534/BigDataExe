package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Char;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Char
 * @Author: 15568
 * @CreateTime: 2019-01-02 21:51
 * @Description: ${Description}
 */
public class StringMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String data = value.toString();
          context.write(new Text(data),NullWritable.get());
    }
}
