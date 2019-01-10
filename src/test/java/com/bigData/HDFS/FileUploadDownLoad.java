package com.bigData.HDFS;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HDFS
 * @Author: 15568
 * @CreateTime: 2018-12-13 21:55
 * @Description: ${Description}
 */
public class FileUploadDownLoad {
    protected Configuration configuration;
    @Before
    public void init () {
        // 1. 通过设置环境变量解决 用户权限问题
        // 相当于 core-sit.xml 文件
        Configuration configuration = new Configuration();
        // 配置NameNode 地址信息
        configuration.set("fs.defaultFS","hdfs://192.168.199.135:9000");
        this.configuration = configuration;

    }

    @Test
    public void test() {
        // 获取 HDFS  的元信息
        try {
            // 1. 获取 HDFS的 客户端
            FileSystem fileSystem = FileSystem.get(configuration);
            // 2. 指定需要获取那个目录的元信息
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
            // 3. 变量元信息
            for (FileStatus status : fileStatuses) {
                System.out.println(status.isDirectory()?"目录":"文件");
            }
            // 获取某个文件的数据块信息
            // 1. 获取该文件的状态信息
            FileStatus fileStatus = fileSystem.getFileStatus(new Path("/folderAPI_1/LICENSE.txt"));
            // 2. 获取数据块的信息  haddop 2.9 每个数据块 128M 超过会自动分割数据块
            BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            for ( BlockLocation location : fileBlockLocations) {
                System.out.println("缓存主机:"+Arrays.toString(location.getCachedHosts())); // 有多个数据块的 有多个 主机信息
                System.out.println("主机:"+ Arrays.toString(location.getHosts()));
                System.out.println("数据块名称:"+ Arrays.toString(location.getNames()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1() { //文件上传
       // HDFS的 上传下载
        InputStream inputStream = null;
        OutputStream outputStream = null;
       try {
           System.setProperty("HADOOP_USER_NAME","root");
           // 1. 获取 HDFS的 客户端
           FileSystem fileSystem = FileSystem.get(configuration);
           // 2. 打开一个 字节输入流
           inputStream = new FileInputStream(new File("F:\\Game\\1.jpg"));

           //3. 创建一个输出流 输出到HDFS
           outputStream = fileSystem.create(new Path("/upload/1.jpg"));

           // 创建一个缓冲区
           byte[] buffer = new byte[1024];
           int len = 0;
           while ((len = inputStream.read()) >0) {
               // 写出到输出流
               outputStream.write(buffer,0,len);
           }
           outputStream.flush();
       } catch (Exception e) {
           e.printStackTrace();
       } finally {
           if (outputStream != null) {
               try {
                   outputStream.close();
               } catch (IOException e) {
                   e.printStackTrace();
               }
           }
           if (inputStream != null) {
               try {
                   inputStream.close();
               } catch (IOException e) {
                   e.printStackTrace();
               }
           }
       }
    }

    @Test
    public void test2Utils() { // 文件工具类上传
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            System.setProperty("HADOOP_USER_NAME", "root");
            // 1. 获取 HDFS的 客户端
            FileSystem fileSystem = FileSystem.get(configuration);
            // 2. 打开一个 字节输入流
            inputStream = new FileInputStream(new File("F:\\Game\\2.jpg"));

            //3. 创建一个输出流 输出到HDFS
            outputStream = fileSystem.create(new Path("/upload/2.jpg"));
            // 4 使用工具类 进行上传
            IOUtils.copyBytes(inputStream,outputStream,1024);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testDownLoad() { //文件下载
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            System.setProperty("HADOOP_USER_NAME", "root");
            // 1. 获取 HDFS的 客户端
            FileSystem fileSystem = FileSystem.get(configuration);
            // 2. 打开一个 字节输入流 从 /upload/2.jpg 读取数据
            inputStream = fileSystem.open(new Path("/upload/2.jpg"));

            //3. 创建一个输出流 输出到 本地
            outputStream = new FileOutputStream("F:\\Game\\2-1.jpg");
            // 4 使用工具类 进行下载
            IOUtils.copyBytes(inputStream,outputStream,1024);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
