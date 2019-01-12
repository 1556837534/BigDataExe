package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.revertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.myindex
 * @Author: Jackson_J
 * @CreateTime: 2019-01-12 14:08
 * @Description: 倒排索引  合并操作  (Combiner 是特殊的Reducer 只是这个在Mapper节点执行)
 *   这个合并 在Mapper 输出后 执行 但是要谨慎 ！！
 *   Reducer 的输入数据类型是Mapper的输出类型 不是Combiner 的输出类型  所以要在Combiner 中将输出的数据类型与Mapper的输出类型一致
 */
public class MyIndexCombiner extends Reducer<Text, Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          // combiner 合并 操作的目的是 统计同一个文件中的单词进行计数
        long total = 0l;
        for (Text text : values) {
            String value = text.toString();
            total += Long.parseLong(value);
        }

        // key2' 单词:文件名  key3' 输出
        String key2 = key.toString();
        String[] keyArray = key2.split(":");
        String word = keyArray[0];
        String fileName = keyArray[1];
        // Combiner 输出是 key3' 单词  value3' 文件名+次数
        context.write(new Text(word),new Text(fileName+":"+total));
    }
}
