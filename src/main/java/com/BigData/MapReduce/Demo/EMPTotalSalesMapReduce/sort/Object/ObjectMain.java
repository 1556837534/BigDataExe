package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Object;

import com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Char.MyStringComparator;
import com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Char.StringMain;
import com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Char.StringMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.Object
 * @Author: 15568
 * @CreateTime: 2019-01-02 22:25
 * @Description: ${Description}
 */
public class ObjectMain {
    public static void main(String[] args) throws Exception {
        // 1.创建一个 job = Mapper + Reduce
        Job job = Job.getInstance(new Configuration());

        // 指定Job 的入口
        job.setJarByClass(ObjectMain.class);

        // 指定任务的 Mapper 和输出的数据类型
        job.setMapperClass(EmployeeSortMapper.class);
        job.setMapOutputKeyClass(Employee.class);    // key2 数据类型
        job.setMapOutputValueClass(NullWritable.class); // value2 数据类型

        //指定任务的 Reducer
        //指定任务的Reduce和输出的数据类型
        job.setReducerClass(EmployeeSortReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Employee.class);

        //指定输入数据  输出数据
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 执行任务
        job.waitForCompletion(true);

        // 2. 导出为Jar 包 放到 Hadoop 的 Yarn 容器中运行
    }
}
