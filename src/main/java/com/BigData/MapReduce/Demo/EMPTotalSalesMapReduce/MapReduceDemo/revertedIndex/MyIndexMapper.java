package com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.revertedIndex;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Demo.EMPTotalSalesMapReduce.MapReduceDemo.myindex
 * @Author: Jackson_J
 * @CreateTime: 2019-01-12 14:08
 * @Description:  实现倒排索引
 * 输入
 *             [root@BigF-master ~]# more data1.txt
 *             I love Beijing and love Shanghai
 *             [root@BigF-master ~]# more data2.txt
 *             I love China
 *             [root@BigF-master ~]# more data3.txt
 *             Beijing is the capital of China
 *
 * 输出
 *  Beijing 	(data3.txt:1)(data1.txt:1)
 * 	China   	(data2.txt:1)(data3.txt:1)
 * 	I       	(data1.txt:1)(data2.txt:1)
 * 	Shanghai    (data1.txt:1)
 * 	and     	(data1.txt:1)
 * 	capital 	(data3.txt:1)
 * 	is      	(data3.txt:1)
 * 	love    	(data2.txt:1)(data1.txt:2)
 * 	of      	(data3.txt:1)
 * 	the     	(data3.txt:1)
 *                                                     单词+文件名 次数
 */
public class MyIndexMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // I love Beijing and love Shanghai
        String data = value.toString().trim();

         //分词
        String[] words = data.split(" ");
        // 获取输入的文件名
        // 1. 获取输入的切片
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        // 2. 通过切片获取文件路径
        String path = inputSplit.getPath().toString();
        // 获取对应的文件名
        String fileName = path.substring(path.lastIndexOf("/")+1);

        // 输出
        for (String word :words) {
            // 输出 key2: 单词 + 文件名  value2: 1
            context.write(new Text(word+":"+fileName),new Text("1"));
        }
    }
}
