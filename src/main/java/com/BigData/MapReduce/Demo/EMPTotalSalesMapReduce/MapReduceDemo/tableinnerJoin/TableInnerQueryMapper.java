package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.tableinnerJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.tableinnerJoin
 * @Author: Jackson_J
 * @CreateTime: 2019-01-12 12:06
 * @Description:  多表的自连接
*  查询员工信息：老板姓名  员工姓名
* 	   条件：员工的老板号 === 老板的员工号
 *       select b.ename,e.ename
 * 	      from emp e,emp b
 * 	      where e.mgr=b.empno;
 *
 * 	      员工输出  老板号 与 员工名称
 * 	      老板输出  员工号    老板名称
 *
 */
public class TableInnerQueryMapper extends Mapper<LongWritable, Text,LongWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString().trim();

        //分词
        String[] words = data.split(",");

        // 输出    自表关联 一条数据输出两次
        // 作为 老板输出  员工号  名称
        context.write(new LongWritable(Long.parseLong(words[0])),new Text("*"+words[1]));
        // 作为 员工输出  老板员工号 名称  老板号可能不存在
        try {
            context.write(new LongWritable(Long.parseLong(words[3])),new Text(words[1]));
        } catch (Exception ex) {
            // 表示 产生异常  表示这条数据为 大老板
            context.write(new LongWritable(-1),new Text(words[1]));
        }


    }
}
