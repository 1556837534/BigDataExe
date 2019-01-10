package com.bigData.HDFS.RPCClient;

import com.bigData.HDFS.RPCServer.MyHadoopRPCServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HDFS.RPCClient
 * @Author: 15568
 * @CreateTime: 2018-12-25 22:22
 * @Description: ${Description}
 */
public class MyRPCClient {
    public static void main(String[] args) throws IOException {
        // 得到 RPC 的客户端 通过 客户端调用部署在 RPCServer 上的程序

        MyHadoopRPCServer localhost = RPC.getProxy(MyHadoopRPCServer.class,  //调用接口
                MyHadoopRPCServer.versionID, // 需要与服务端的versionID 一致
                new InetSocketAddress("localhost", 8089),     //服务器地址 在server 端 是部署在本机
                new Configuration());

        // 调用服务端的方法
        System.out.println(localhost.sayHello("Jackson_J"));

    }
}
