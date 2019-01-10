package com.bigData.HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: com.bigdata.study.bigdata.HDFS
 * @Author: Tao.jin
 * @CreateTime: 2018-11-21 21:01
 * @Description: 利用HDFS 提供的API 创建目录 主要是用这个实例来探讨权限问题
 */
public class MkdirDirectByAPI {
    @Test
    public void test() throws IOException {
        // 1. 通过设置环境变量解决 用户权限问题
        //System.setProperty("HADOOP_USER_NAME","root");
        // 相当于 core-sit.xml 文件
        Configuration configuration = new Configuration();
        // 配置NameNode 地址信息
        configuration.set("fs.defaultFS","hdfs://192.168.199.135:9000");
        //得到 HDFS的客户端
        FileSystem fileSystem = FileSystem.get(configuration);
        //创建一个目录  直接创建会抛出 权限否定的问题  HDFS直接根据本地执行的用户来判断权限问题
        //Caused by: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.AccessControlException): Permission denied: user=15568, access=WRITE, inode="/folderAPI":root:supergroup:drwxr-xr-x
        /**
         * HDFS 的权限校验比较弱，它是根据 本地执行的用户去判断有没有相应的权限，这里有四种方法来改变权限的问题
         * 1. 设置一个环境变量 HADOOP_USER_NAME  创建后 可以在服务器上 执行 hdfs -ls 查看目录
         * 2. 使用JAVA的 —D 的参数
         *     -D参数：  要求获取命令行的参数  一般通过main方法中的args 这里 可以使用—D
         *     -D参数名称=参数值   例如 —DUser=a -Dage =12 那么在程序任意方法 System.getProperty(“User”) 可以获取到对应的值
         *     -DHADOOP_USER_NAME=root 设置jvm 执行参数就可以直接运行 而不会报权限问题
         *  3. 使用 chmod 命令 来改变 /folderAPI_2 目录的权限 就可以在该目录下进行创建目录
         *  4. 最彻底的方式  在配置 hdfs-site.xml 文件 把权限检查的配置注释掉 dfs.permissions
         * */
        fileSystem.mkdirs(new Path("/folderAPI_1/folderAPI_SUB"));
    }
}
