package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.tableinnerJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.tableinnerJoin
 * @Author: Jackson_J
 * @CreateTime: 2019-01-12 12:07
 * @Description:
 */
public class TableInnerQueryReducer extends Reducer<LongWritable, Text,Text,Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 定义 一个老板 名称  一个员工名称集合
        String bossName = "";
        List<String> empNames = new ArrayList<>();
        for (Text t : values) {
             String name = t.toString();
             // 判断是否存在* 号
            if (StringUtils.startsWith(name,"*")) {
                //老板
                bossName = name.substring(1);
            } else {
                empNames.add(name);
            }
        }

        //输出
        // 如果是 大老板 或者 该老板 没有员工信息 就进行过滤不输出
        if (empNames.size() >0 && StringUtils.isNotBlank(bossName)) {
            context.write(new Text(bossName),new Text("("+StringUtils.join(empNames,";")+")"));
        }
    }
}
