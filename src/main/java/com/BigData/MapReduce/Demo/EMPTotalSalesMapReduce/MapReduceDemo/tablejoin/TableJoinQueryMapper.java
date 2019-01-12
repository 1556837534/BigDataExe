package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.tablejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.tablejoin
 * @Author: Jackson_J
 * @CreateTime: 2019-01-12 11:26
 * @Description:
 * 	查询员工信息：部门名称   员工姓名
 * 	select d.dname,e.ename
 * 	from emp e,dept d
 * 	where e.deptno=d.deptno;
 *  多表等值连接 Mapper 处理类
 */
public class TableJoinQueryMapper extends Mapper<LongWritable, Text,LongWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString().trim();
        //分词
        String[] words = data.split(",");

        //判断数组长度  长度 <=3 是部门  否则是员工  利用数组长度判断数据类型是员工还是部门 前提是两个数据长度不一致 如果一致使用文件名来判断 -->见倒排索引demo
        if (words.length == 3) { //部门数据
             // 输出 部门号  部门名称  在输出 的部门号前面 添加一个特殊符号 * 用来在Reducer 中的v3中区分 是员工名称还是部门名称 前提是 原来的名称中不能含有该特殊符号
            context.write(new LongWritable(Long.parseLong(words[0])),new Text("*"+words[1]));
        } else { //员工数据
            // 输出 员工号  员工名称
            context.write(new LongWritable(Long.parseLong(words[7])),new Text(words[1]));
        }
    }
}
