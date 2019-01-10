package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Object;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Object
 * @Author: 15568
 * @CreateTime: 2019-01-02 22:20
 * @Description: ${Description}
 */
public class EmployeeSortMapper extends Mapper<LongWritable, Text,Employee, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 数据：7654,MARTIN,SALESMAN,7698,1981/9/28,1250,1400,30
        String data = value.toString();

        //分词
        String[] words = data.split(",");

        //创建一个员工对象
        Employee e = new Employee();

        //设置员工号
        e.setEmpNo(Integer.parseInt(words[0]));
        //设置姓名
        e.setEnName(words[1]);

        //设置职位 job
        e.setJob(words[2]);

        //设置老板号
        try{
            e.setMgr(Integer.parseInt(words[3]));
        }catch(Exception ex){
            //老板号为null
            e.setMgr(0);
        }

        //设置入职日期
        e.setHireDate(words[4]);

        //设置薪水
        e.setSal(Integer.parseInt(words[5]));

        //设置奖金
        try{
            e.setComm(Integer.parseInt(words[6]));
        }catch(Exception ex){
            //没有奖金
            e.setComm(0);
        }

        //设置部门号
        e.setDeptNo(Integer.parseInt(words[7]));


        //输出：一定要把员工对象作为key2
        context.write(e, NullWritable.get());
    }
}
