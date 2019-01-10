package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.number;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.sort.number
 * @Author: 15568
 * @CreateTime: 2019-01-02 21:23
 * @Description: ${Description}
 */
public class NumberMain {
    public static void main(String[] args) throws Exception {
        // 1.创建一个 job = Mapper + Reduce
        Job job = Job.getInstance(new Configuration());

        // 指定Job 的入口
        job.setJarByClass(NumberMain.class);

        // 指定任务的 Mapper 和输出的数据类型
        job.setMapperClass(NumberMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);    // key2 数据类型
        job.setMapOutputValueClass(NullWritable.class); // value2 数据类型

        // 指定自己的比较器
        job.setSortComparatorClass(MyNumberComparator.class);

        //指定任务的 Reducer
        job.setOutputKeyClass(LongWritable.class);    //key4 类型
        job.setOutputValueClass(NullWritable.class); // value4 类型

        //指定输入数据  输出数据
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 执行任务
        job.waitForCompletion(true);

        // 2. 导出为Jar 包 放到 Hadoop 的 Yarn 容器中运行
    }
}
