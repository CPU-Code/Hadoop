package com.cpucode.distributeTest;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : cpucode
 * @date : 2021/11/28 16:25
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class DistributeClient {
    private static String connectString = "cpucode100:2181,cpucode101:2181,cpucode102:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";

    public static void main(String[] args) throws Exception{
        // 1 获取 zk 连接
        DistributeClient client = new DistributeClient();
        client.getConnect();

        // 2 获取 servers 的子节点信息，从中获取服务器信息列表
        client.getServerList();
        // 3 业务进程启动
        client.business();
    }

    /**
     * 创建到 zk 的客户端连接
     * @throws IOException
     */
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 再次启动监听
                try {
                    getServerList();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 获取服务器列表信息
     * @throws Exception
     */
    public void getServerList() throws Exception {
        // 1 获取服务器子节点信息，并且对父节点进行监听
        List<String> children = zk.getChildren(parentNode, true);

        // 2 存储服务器信息列表
        ArrayList<String> servers = new ArrayList<>();

        // 3 遍历所有节点，获取节点中的主机名称信息
        for (String child : children) {
            byte[] data = zk.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }

        // 4 打印服务器列表信息
        System.out.println(servers);
    }

    /**
     * 业务功能
     * @throws Exception
     */
    public void business() throws Exception{
        System.out.println("client is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }
}
