package com.bigData.MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.MapReduce
 * @Author: 15568
 * @CreateTime: 2018-12-28 22:23
 * @Description: 5. 主程序
 * idea 打包为 可执行Jar   打包执行的代码不能放在  test 目录下
 * https://blog.csdn.net/u011499747/article/details/78970070
 */
public class WordCountMain {
    public static void main(String[] args) throws Exception {
        // 1.创建一个 job = Mapper + Reduce
        Job job = Job.getInstance(new Configuration());

        // 指定Job 的入口
        job.setJarByClass(WordCountMain.class);

        // 指定任务的 Mapper 和输出的数据类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);    // key2 数据类型
        job.setMapOutputValueClass(LongWritable.class); // value2 数据类型

        //指定任务的 Reducer
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);    //key4 类型
        job.setOutputValueClass(LongWritable.class); // value4 类型

        //指定输入数据  输出数据
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 执行任务
        job.waitForCompletion(true);

        // 2. 导出为Jar 包 放到 Hadoop 的 Yarn 容器中运行
    }
}
