package com.bigData.HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HBase
 * @Author: Jackson_J
 * @CreateTime: 2019-02-20 22:23
 * @Description: HBase Java API
 *      启动HBase 的测试用例 需要更改文件
 *      --> 修改Windows：C:\Windows\System32\drivers\etc\hosts
 *      --> 增加三行:   Hbase 主节点 从节点
 *               192.168.199.135  BigF-master    主机IP 主机名称
 *               192.168.199.136  bigf-node1
 *               192.168.199.137  bigf-node2
 *      --> 原因: 可以使用 zooInter 工具看到 zookeeper中存的是主机名称而不是主机IP
 */
public class HBaseFirst {
    private static final String tablName = "mytable";
    private Configuration configuration;
    @Before
    public void init() throws Exception{
        // 需要通过 zookeeper 来操作 HBase
        this.configuration = new Configuration();
        //1. 配置 zookeeper 地址信息
        this.configuration.set("hbase.zookeeper.quorum","192.168.199.135");

    }

    /**
     * @Author Jackson_J
     * @Description 创建HBase表  在HBase Shell 命令 list 查看
     * @Date 22:36 2019/2/20
     * @Param
     * @return
     **/
    @Test
    public void testCreatedTable () throws Exception {
        // 2. 获取 HBase 客户端
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        // 2.1 创建表的描述符
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tablName));
        // 2.2 创建表的列族
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
        HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("grade");
        // 2.3 将 表的列族 加入表的描述符中
        hTableDescriptor.addFamily(hColumnDescriptor);
        hTableDescriptor.addFamily(hColumnDescriptor1);

        // 3. 通过客户端 创建一个表
        hBaseAdmin.createTable(hTableDescriptor);
        // 4. 关闭客户端
        hBaseAdmin.close();
    }

    /**
     * @Author Jackson_J
     * @Description  HBase 数据插入
     * @Date 22:38 2019/2/20
     * @Param []
     * @return void
     **/
    @Test
    public void testPut() throws Exception{
        //2.1 得到表的客户端
        HTable hTable = new HTable(configuration,tablName);

        // 2.2 构造一个 put对象 参数就是rowkey
        Put put = new Put(Bytes.toBytes("id001"));
        // family 列族  qualifier 列名 value 列值
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("tom"));

        //3. 插入数据
        hTable.put(put);

        // 4. 关闭客户端
        hTable.close();
    }

    /**
     * @Author Jackson_J
     * @Description  通过 get 方式查询 数据  只能通过 rowkey 来查询
     * @Date 22:52 2019/2/20
     * @Param []
     * @return void
     **/
    @Test
    public void testGetQueryData() throws Exception{
        //2.1 得到表的客户端
        HTable hTable = new HTable(configuration,tablName);

        //3. 构造get对象
        Get get = new Get(Bytes.toBytes("id001"));

        //4.查询 result 表示表中记录
        Result result = hTable.get(get);
        byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
        System.out.println("========="+Bytes.toString(value));
        //5. 关闭客户端
        hTable.close();
    }

    /**
     * @Author Jackson_J
     * @Description  通过 scan 方式查询 数据
     * @Date 22:52 2019/2/20
     * @Param []
     * @return void
     **/
    @Test
    public void testScanQueryData() throws Exception{
        //2.1 得到表的客户端
        HTable hTable = new HTable(configuration,tablName);

        //3. 构造scan 扫描对象
        Scan scan = new Scan();

        //4.查询 ResultScanner (是一个集合)表示表中记录
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
            System.out.println("========="+Bytes.toString(value));
        }

        //5. 关闭客户端
        hTable.close();
    }
}
