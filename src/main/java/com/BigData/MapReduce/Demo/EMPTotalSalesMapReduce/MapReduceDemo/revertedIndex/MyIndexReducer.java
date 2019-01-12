package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.revertedIndex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.myindex
 * @Author: Jackson_J
 * @CreateTime: 2019-01-12 14:08
 * @Description: ${Description}
 */
public class MyIndexReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // distinct group by key3  输入 key3 单词  value3 文件名:次数
        // 文件名集合
        List<String> files = new ArrayList<>();
        for (Text text : values) {
            String value = text.toString();
            files.add(value);
        }
         context.write(key,new Text("("+StringUtils.join(files,";")+")"));
    }
}
