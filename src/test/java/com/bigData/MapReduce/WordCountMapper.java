package com.bigData.MapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.MapReduce
 * @Author: 15568   单词计数 逻辑参考 有道笔记 的 第七篇笔记  MapReduce 简介
 * @CreateTime: 2018-12-28 22:22
 * @Description: 1. 单词拆分
 * 需要继承 Mapper 父类 传入 Hadoop 中的数据类型  即 Mapper 接收的 key-value 类型 与输出的 key-value 类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    /**
     * 2. 重写 map 方法  该方法 就是将一个大任务拆分为多个小任务  在单词计数中就是 分词
     * @param key2
     * @param value2
     * @param context  代表 Map 的上下文  上文就是 HDFS  下文就是 Reducer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key2, Text value2, Context context) throws IOException, InterruptedException {
        // 数据
        String data = value2.toString();
        // 分词
        String[] words = data.split(" " );

        // 输出 key2  value2
        for (String word : words) { //每个单词记一次数 1
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
