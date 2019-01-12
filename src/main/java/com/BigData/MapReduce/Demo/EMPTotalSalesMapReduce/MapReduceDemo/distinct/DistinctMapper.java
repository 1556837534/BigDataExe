package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.distinct;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.distinct
 * @Author: Jackson_J
 * @CreateTime: 2019-01-12 10:32
 * @Description: 去除 重复字符串  利用 Mapper 输出到 Reducer  k2的 去重特性
 */
public class DistinctMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString().trim();
        context.write(new Text(data),NullWritable.get());
    }
}
