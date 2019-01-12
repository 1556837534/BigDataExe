package com.bigData.MRUnit;

import com.bigData.MapReduce.WordCountMapper;
import com.bigData.MapReduce.WordCountReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.MRUnit
 * @Author: Jackson_J
 * @CreateTime: 2019-01-12 14:48
 * @Description:
 *  MapReducer 进行单元测试
 *  1. 下载MRUnit jar包
 * <dependency>
 *     <groupId>org.apache.mrunit</groupId>
 *     <artifactId>mrunit</artifactId>
 *     <version>1.1.0</version>
 *     <classifier>hadoop2</classifier>
 *     <scope>test</scope>
 * </dependency>
 *
 * 2.
 *
 */
public class MapReduceUnit {

    @Test
    public void testWordCountMapper() throws Exception{
        // 测试Mapper
        // 设置一个环境变量  不设置不影响 运行结果 只是解决异常问题 在idea 下可以不用设置
        //System.setProperty("hadoop.home.dir","winutil.exe 的根目录");
        // 创建一个Mapper 对象
        WordCountMapper wordCountMapper = new WordCountMapper();

        // 创建一个 Mapper的驱动 Driver
        MapDriver<LongWritable, Text,Text,LongWritable> driver = new MapDriver<>(wordCountMapper);
        // 指定Mapper 的输入数据 k1 v1
        driver.withInput(new LongWritable(1),new Text("I love Beijing"));
        // 指定Mapper 输出的数据 k2 v2   ---> 期望得到的数据  如果期望输出数据与 执行的输出数据一样 就可以测试通过 就行对比
        driver.withOutput(new Text("I"),new LongWritable(1))
                .withOutput(new Text("love"),new LongWritable(1))
                .withOutput(new Text("Beijing"),new LongWritable(1));
        // 执行单元测试 对比期望数据与实际数据运行结果  若不一致 运行不通过
        driver.runTest();

    }

    @Test
    public void testWordCountReducer() throws Exception{
        //测试Reduce
        // 1. 创建一个Reducer
        WordCountReducer reducer = new WordCountReducer();
        // 2. 创建一个Reducer 的Driver
        ReduceDriver<Text, LongWritable,Text,LongWritable>  reduceDriver = new ReduceDriver<>(reducer);
        // 3. 指定 Reducer 的输入数据
        // 3.1 构造  v3
        List<LongWritable> v3 = new ArrayList<>();
        v3.add(new LongWritable(1));
        v3.add(new LongWritable(1));
        v3.add(new LongWritable(1));
        reduceDriver.withInput(new Text("Beijing"),v3);
        // 4. 指定 Reducer 的输出数据
        reduceDriver.withOutput(new Text("Beijing"),new LongWritable(3));
        // 执行 期望结果与实际运行结果对比  若不一致 运行不通过
        reduceDriver.runTest();
    }

    @Test
    public void testWordCountJob() throws Exception{
        // 将 Mapper 与 Reducer 合并为一个任务进行测试
        // 1. 创建测试的对象
        // 创建一个Mapper 对象
        WordCountMapper wordCountMapper = new WordCountMapper();
        // 创建一个Reducer
        WordCountReducer reducer = new WordCountReducer();
        // 2. 创建一个 Driver  没有k3 v3是因为与k2 v2数据类型是一致的
        //MapReduceDriver<k1,v1,k2,v2,k4,v3>
        MapReduceDriver<LongWritable, Text,Text,LongWritable,Text,LongWritable> mapReduceDriver = new MapReduceDriver<>(wordCountMapper,reducer);
        // 3. 指定Mapper 输入的数据
        mapReduceDriver.withInput(new LongWritable(1),new Text("I love Beijing"))
        .withInput(new LongWritable(4),new Text("I love China"))
        .withInput(new LongWritable(7),new Text("Beijing is the capital of China"));
        // 4. 指定Reducer 的输出数据  输出的数据需要注意排序 不然与实际运行结果不一致
        mapReduceDriver.withOutput(new Text("Beijing"),new LongWritable(2))
                .withOutput(new Text("China"),new LongWritable(2))
                .withOutput(new Text("I"),new LongWritable(2))
                .withOutput(new Text("capital"),new LongWritable(1))
                .withOutput(new Text("is"),new LongWritable(1))
                .withOutput(new Text("love"),new LongWritable(2))
                .withOutput(new Text("of"),new LongWritable(1))
                .withOutput(new Text("the"),new LongWritable(1));
        //5. 执行测试
        mapReduceDriver.runTest();

    }
}
