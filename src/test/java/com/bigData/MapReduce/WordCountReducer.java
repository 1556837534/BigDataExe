package com.bigData.MapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.MapReduce
 * @Author: 15568
 * @CreateTime: 2018-12-28 22:23
 * @Description: 3. 单词汇总
 *      需要继承 Reducer 父类 传入 Hadoop 中的数据类型  即 Reducer 接收的 key-value 类型 与输出的 key-value 类型
 */
public class WordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    /**
     * 4. 重写 reduce 方法 就是对Map 拆分后的任务进行汇总
     * @param key3
     * @param values3  汇总后存放是上文 Map 中 同一个单词的出现次数 比如 I （1,1）
     * @param context  Reducer 上下文  上文是 Mapper 下文 是HDFS
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key3, Iterable<LongWritable> values3, Context context) throws IOException, InterruptedException {
        // 进行任务循环汇总
        long total = 0l;
        for (LongWritable l : values3) {
            total += l.get();
        }

        // 输出 k4 v4
        context.write(key3,new LongWritable(total));
    }
}
