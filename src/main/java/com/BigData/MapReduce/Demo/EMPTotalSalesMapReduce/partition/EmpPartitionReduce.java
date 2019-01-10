package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.partition;

import com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.Serializedable.MapReduceSerializedable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.partition
 * @Author: Jackson_J
 * @CreateTime: 2019-01-10 21:40
 * @Description: 把分区后的数据写到HDFS上
 *                                                                                        员工号         员工信息
 */
public class EmpPartitionReduce extends Reducer <LongWritable, MapReduceSerializedable,LongWritable,MapReduceSerializedable>{
    @Override
    protected void reduce(LongWritable key, Iterable<MapReduceSerializedable> values, Context context) throws IOException, InterruptedException {
        for (MapReduceSerializedable s : values) {
            // 记录到上下文中
            context.write(new LongWritable(s.getEmpNo()),s);
        }

    }
}
