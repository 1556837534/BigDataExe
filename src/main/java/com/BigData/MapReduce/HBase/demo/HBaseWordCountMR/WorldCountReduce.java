package com.BigData.MapReduce.HBase.demo.HBaseWordCountMR;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.HBase.demo.HBaseWordCountMR
 * @Author: Jackson_J
 * @CreateTime: 2019-02-24 14:37
 * @Description: 接收 HBase 中Mapper 传过来的数据
 */
//                                                 k3        v3       代表输出的一条记录rowkey  与Mapper 读取的rowKey 类型一样
public class WorldCountReduce extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
    /**
     * @Author Jackson_J
     * @Description 
     * @Date 14:54 2019/2/24
     * @Param [key,   k3
     *         values, v3
     *         context 上下文 输出到HBase 中
      *        ]
     * @return void
     **/
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long total = 0l;
        for (LongWritable longWritable : values) {
            total += longWritable.get();
        }
        // 输出数据 作为一条 表中记录
        // 构造一个 put 对象  将单词作为rowKey
        Put put = new Put(Bytes.toBytes(key.toString()));
        // 列族  列名（后面创建跟json key一样）  列值
        put.add(Bytes.toBytes("content"),Bytes.toBytes("result"),Bytes.toBytes(String.valueOf(total)));
        // 输出  指定rowkey | 记录
        context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
    }
}
