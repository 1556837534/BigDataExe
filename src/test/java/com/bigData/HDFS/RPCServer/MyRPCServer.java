package com.bigData.HDFS.RPCServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @BelongsProject: BigDataPro
 * @BelongsPackage: com.bigData.HDFS.RPCServer
 * @Author: 15568
 * @CreateTime: 2018-12-25 22:18
 * @Description: ${Description}
 */
public class MyRPCServer {
    public static void main(String[] args) throws IOException {
        // 使用 HDFS 的 RPC Server 来部署 我们的 程序
        RPC.Builder builder = new RPC.Builder(new Configuration());
        // 创建 RPC 的 Server
        builder.setBindAddress("localhost");
        // 监听端口
        builder.setPort(8089);

        // 部署程序
        builder.setProtocol(MyHadoopRPCServer.class);// 部署的接口
        // 部署实现  客户端 调用的时候要指定 相同的 versionID
        builder.setInstance(new MyHadoopRPCServerImpl());

        // 生成 RPC Server
        RPC.Server server = builder.build();

        //启动
        server.start();
    }
}
