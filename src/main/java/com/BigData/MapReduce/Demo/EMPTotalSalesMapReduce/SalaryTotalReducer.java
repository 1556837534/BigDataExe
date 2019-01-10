package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce;

import com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.Serializedable.MapReduceSerializedable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce
 * @Author: 15568
 * @CreateTime: 2018-12-31 16:25
 * @Description: Reducer 程序
 */
public class SalaryTotalReducer extends Reducer<LongWritable, MapReduceSerializedable,LongWritable,LongWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<MapReduceSerializedable> values, Context context) throws IOException, InterruptedException {
        // 获取获取到的部门上的薪水集合进行汇总
        long sumSaraly = 0l;
        for (MapReduceSerializedable s : values) {
            sumSaraly += s.getSal();
        }
        // 记录到上下文中
        context.write(key,new LongWritable(sumSaraly));
    }
}
