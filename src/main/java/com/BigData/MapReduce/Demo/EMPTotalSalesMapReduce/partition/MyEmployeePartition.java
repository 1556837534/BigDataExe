package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.partition;

import com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.Serializedable.MapReduceSerializedable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.partition
 * @Author: Jackson_J
 * @CreateTime: 2019-01-10 21:47
 * @Description: MR 分区逻辑(安装Map 的输出进行分区)   按照部门号进行分区   k v  是 Map 的输出
 */
public class MyEmployeePartition extends Partitioner<LongWritable, MapReduceSerializedable> {
    /**
     * 建立自己的分区规则
     * @param longWritable   Key2
     * @param mapReduceSerializedable  value2
     * @param numParts  分区个数
     * @return
     */
    @Override
    public int getPartition(LongWritable longWritable, MapReduceSerializedable mapReduceSerializedable, int numParts) {
        // numParts 需要在 主程序的Job中进行设置
        int deptNo = mapReduceSerializedable.getDeptNo();
        if (deptNo == 10) {
            // 10号部门分到一号区
            return 1 % numParts;
        } else if (deptNo == 20) { // 20号部门员工分到2号区
            return 2 % numParts;
        } else {
            return 3 % numParts;
        }
    }
}
