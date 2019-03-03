package com.BigData.MapReduce.Pig.diy.function;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.BigData.MapReduce.Pig.diy.function
 * @Author: Jackson_J
 * @CreateTime: 2019-03-03 11:49
 * @Description:  自定义加载函数
 *       原因： pig 的加载函数默认一行就是一个tuple
 *       目的:  想要按照自己的规则进行加载数据 例如 一行单词改为一个单词一个tuple  （pig 中的tuple 可以嵌套bag）
 *              实现一个自己的加载函数，每个单词作为一个tuple 然后把这些tuple 放入一个bag中，在把这个bag嵌入tuple
 *       步骤:  1. 继承 LoadFunc
 */
public class LoadFun extends LoadFunc {
    // 定义输入流变量 来保存HDFS 输入流
    private RecordReader recordReader;

    /*指定 HDFS 输入路径
    * path 输入的路径
    * job MapReduce 任务
    * */
    @Override
    public void setLocation(String path, Job job) throws IOException {
        FileInputFormat.setInputPaths(job,new Path(path));
    }

    // 输入数据的格式 ：字符串
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }
    /*
    * recordReader 表示HDFS的输入流
    * pigSplit 导入的时候默认设置的分隔符
    * */
    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        // 初始化HDFS 输入流
        this.recordReader = recordReader;
    }

    // 表示从输入流中读取的数据如何加载 加载完后返回 Tuple
    /*e
         (  // 一行 Tuple
           {  // 嵌套的一个 bag
             (I),(love),(Beijing) // bag 中的Tuple
           }
         )
    *
    * */
    @Override
    public Tuple getNext() throws IOException {
        // 加载数据 I Love Beijing  ---> 每个单词一个tuple
        Tuple tuple = null;
        try {
            // 判断是否读取了数据
            boolean b = this.recordReader.nextKeyValue();
            if (b) {//获取到了数据 false 表示没有读取到数据
                //数据 I Love Beijing
                String data = this.recordReader.getCurrentValue().toString();

                //生成结果的 tuple
                tuple = TupleFactory.getInstance().newTuple();

                // 分词操作
                String [] words = data.split(" ");


                // 生成表
                DataBag bag = BagFactory.getInstance().newDefaultBag();
                //把每个单词放入一个tuple 在把这些tuple 放到bag中
                for (String word :words) {
                    Tuple tmp = TupleFactory.getInstance().newTuple();
                    tmp.append(word);//加入单词
                    // 把tuple 加入bag
                    bag.add(tmp);
                }

                //把表放入返回的tuple 中
                tuple.append(bag);

            } else {
                return  tuple; //返回一个空值
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return tuple;
    }
}
