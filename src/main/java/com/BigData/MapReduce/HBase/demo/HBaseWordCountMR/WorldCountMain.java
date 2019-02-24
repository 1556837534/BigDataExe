package com.BigData.MapReduce.HBase.demo.HBaseWordCountMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.HBase.demo.HBaseWordCountMR
 * @Author: Jackson_J
 * @CreateTime: 2019-02-24 14:36
 * @Description: 基于HBase 的单词计数MapReduce 主程序
 */
public class WorldCountMain {
    public static void main(String[] args) throws Exception{
        // 创建一个Job  之前使用HDFS 中的Job 不需要配置信息  但是读取HBase 中的数据 需要配置连接信息 连接到zookeeper
        // 配置zookeeper 连接信息
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.199.135");
        Job job = Job.getInstance(conf);
        // 设置Job 入口
        job.setJarByClass(WorldCountMain.class);
        // 指定任务Mapper  使用 HBase 提供的工具类指定Mapper 与 Reduce
        /** table 表
         *  scan  扫描器 读取想处理的数据
         *  Mapper mapper 实现类
         *  Text   输入数据类型
         *  outputValueClass 输出数据类型
         *  job  绑定到任务进程
         **/
        // 定义一个 想读取数据的扫描器
        Scan scan = new Scan();
        // 列族 列名
        scan.addColumn(Bytes.toBytes("content"),Bytes.toBytes("info"));
        TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("word"),scan,WorldCountMapper.class, Text.class, LongWritable.class,job);

        /**
         *  table 输出表
         *  Reduce  reduce 处理类
         *  job 任务
         */
        // 指定任务Reduce
        TableMapReduceUtil.initTableReducerJob("stat",WorldCountReduce.class,job);
        // 执行任务
        job.waitForCompletion(true);

        //打成可执行jar 包上传执行 需要在pom.xml 中指定入口main
        // hadoop jar b.jar
        // 如果运行 提示早不到 HBase 中的Jar 需要 将 HBase 中的jar 包含在Hadoop路径中
        // $: export HADOOP_CLASSPATH=$HBASE_HOME/lib/*;$CLASSPATH
    }
}
