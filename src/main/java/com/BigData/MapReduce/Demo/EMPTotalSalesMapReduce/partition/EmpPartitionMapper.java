package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.partition;

import com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.Serializedable.MapReduceSerializedable;
import com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Object.Employee;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.partition
 * @Author: Jackson_J
 * @CreateTime: 2019-01-10 21:38
 * @Description: ${Description}
 */
public class EmpPartitionMapper extends Mapper<LongWritable, Text,LongWritable, MapReduceSerializedable> {
    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 数据  7499,ALLEN,SALESMAN,7698,1981/2/20,1600,300,30
        String data  =value.toString();
        // 分词
        String[] words = data.split(",");
        MapReduceSerializedable mapReduceSerializedable = null;
        //for (String word : words) {
        mapReduceSerializedable = new MapReduceSerializedable();
        mapReduceSerializedable.setEmpNo(Integer.valueOf(words[0]));
        mapReduceSerializedable.setEnName(words[1]);
        mapReduceSerializedable.setJob(words[2]);
        mapReduceSerializedable.setMgr(StringUtils.isBlank(words[3])?0:Integer.valueOf(words[3]));
        mapReduceSerializedable.setHireDate(words[4]);
        mapReduceSerializedable.setSal(Integer.valueOf(words[5]));
        mapReduceSerializedable.setComm(StringUtils.isBlank(words[6])?0:Integer.valueOf(words[6]));
        mapReduceSerializedable.setDeptNo(Integer.valueOf(words[7]));
        // 上下文中 存放 key 部门号  value 薪水 --》 自定义数据
        context.write(new LongWritable(mapReduceSerializedable.getDeptNo()),mapReduceSerializedable);
        //}
    }
}
