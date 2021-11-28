package com.cpucode.distributeTest;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author : cpucode
 * @date : 2021/11/28 16:25
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class DistributeServer {
    private static String connectString = "cpucode100:2181,cpucode101:2181,cpucode102:2181";
    private static int sessionTimeOut = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";

    public static void main(String[] args) throws Exception{
        // 1 获取 zk 连接
        DistributeServer server = new DistributeServer();
        server.getConnect();

        // 2 利用 zk 连接注册服务器信息
        server.registerServer(args[0]);
        // 3 启动业务功能
        server.business(args[0]);
    }

    /**
     * 创建到 zk 的客户端连接
     * @throws IOException
     */
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    /**
     *  注册服务器
     * @param hostname
     * @throws Exception
     */
    public void registerServer(String hostname) throws Exception {
        String create = zk.create(parentNode + "/" + hostname,
                hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname +  "is online " + create);
    }

    /**
     * 业务功能
     * @param hostname
     * @throws Exception
     */
    public void business(String hostname) throws Exception {
        System.out.println(hostname + "is working ...");

        Thread.sleep(Long.MAX_VALUE);
    }

}
