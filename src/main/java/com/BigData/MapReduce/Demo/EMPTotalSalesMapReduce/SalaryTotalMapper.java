package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce;

import com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.Serializedable.MapReduceSerializedable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce
 * @Author: 15568
 * @CreateTime: 2018-12-31 16:02
 * @Description:
 * MR的案例：求每个部门的工资总额
 * 	1、表：员工表emp
 * 	       SQL: select deptno,sum(sal) from emp group by deptno;
 * 			DEPTNO   SUM(SAL)
 * 		---------- ----------
 * 				30       9400
 * 				20      10875
 * 				10       8750
 *
 * 	2、开发MR实现
 * 			# hdfs dfs -cat /output/0917/s2/part-r-00000
 * 			10      8750
 * 			20      10875
 * 			30      9400
 *
 * 	开发步骤
 * 	    1. 将 Oralce 中的 emp 与dept表 导出为 csv 文件格式(可以用Excel打开，也可以用记事本打开，用记事本打开显示用逗号分隔，便于Map以逗号进行分隔)并上传到HDFS中
 *      2.
 */
public class SalaryTotalMapper extends Mapper<LongWritable,Text,LongWritable, MapReduceSerializedable> {
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
