package com.BigData.MapReduce.HBase.demo.HBaseWordCountMR;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.HBase.demo.HBaseWordCountMR
 * @Author: Jackson_J
 * @CreateTime: 2019-02-24 14:37
 * @Description: 读取 HBase 中 word 表中的单词数据
 * 基于HBase 的MapReduce 单词计数  这里有之前的Mapper 改为HBase中的TableMapper
 */
// 从HBase 表中读取数据                            k2 v2  love  1   china 2（没有k1,v1 是因为读取进来的数据就是一条带位置的数据信息）
public class WorldCountMapper extends TableMapper<Text, LongWritable> {
    /**
     * @Author Jackson_J
     * @Description 
     * @Date 14:47 2019/2/24
     * @Param [key HBase 中的rowkey, value 输入的数据, 代表表中输入的一条数据 context]
     * @return void
     **/
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 获取输入的数据
        String data = Bytes.toString(value.getValue(Bytes.toBytes("content"),Bytes.toBytes("info")));
        // 分词
        String[] words = data.split(" ");
        // 输出数据 到Reduce 中
        for (String word :words) {
            // 一个单词 计一个数1
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
