package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.revertedIndex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.myindex
 * @Author: Jackson_J
 * @CreateTime: 2019-01-12 14:08
 * @Description: ${Description}
 */
public class MyIndexMain {
    public static void main(String[] args) throws Exception {
        // 1.创建一个 job = Mapper + Reduce
        Job job = Job.getInstance(new Configuration());

        // 指定Job 的入口
        job.setJarByClass(MyIndexMain.class);

        // 指定任务的 Mapper 和输出的数据类型
        job.setMapperClass(MyIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);    // key2 数据类型
        job.setMapOutputValueClass(Text.class); // value2 数据类型

        // 设置Combiner
        job.setCombinerClass(MyIndexCombiner.class);

        //指定任务的 Reducer
        job.setReducerClass(MyIndexReducer.class);
        job.setOutputKeyClass(Text.class);    //key4 类型
        job.setOutputValueClass(Text.class); // value4 类型

        //指定输入数据  输出数据
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 执行任务
        job.waitForCompletion(true);

        // 2. 导出为Jar 包 放到 Hadoop 的 Yarn 容器中运行
    }
}
