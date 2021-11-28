package com.cpucode.distributeLock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author : cpucode
 * @date : 2021/11/28 17:30
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class DistributeLock {
    // zookeeper server 列表
    private final String connectString = "cpucode100:2181,cpucode101:2181,cpucode102:2181";
    // 超时时间
    private final int sessionTimeout = 2000;
    private ZooKeeper zk = null;

    private String rootNode = "locks";
    private String subNode = "seq-";

    //ZooKeeper 连接
    private CountDownLatch connectLatch = new CountDownLatch(1);
    //ZooKeeper 节点等待
    private CountDownLatch waitLatch = new CountDownLatch(1);

    // 当前 client 等待的子节点
    private String waitPath;
    // 当前 client 创建的子节点
    private String currentNode;

    /**
     *  和 zk 服务建立连接，并创建根节点
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public DistributeLock() throws IOException, InterruptedException, KeeperException {
        // 获取连接
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 连接建立时, 打开 latch, 唤醒 wait 在该 latch 上的线程
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectLatch.countDown();
                }

                // 发生了 waitPath 的删除事件
                if (watchedEvent.getType() == Event.EventType.NodeDeleted &&
                        watchedEvent.getPath().equals(waitPath)) {
                    waitLatch.countDown();
                }
            }
        });

        // 等待连接建立
        connectLatch.await();

        // 获取根节点状态
        Stat stat = zk.exists("/" + rootNode, false);

        // 如果根节点不存在，则创建根节点，根节点类型为永久节点
        if (stat == null) {
            System.out.println("根节点不存在");

            // 创建一下根节点
            zk.create("/" + rootNode,
                    new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
    }

    /**
     * 对zk加锁
     */
    public void zkLock() {
        // 创建对应的临时带序号节点
        try {
            //在根节点下创建临时顺序节点，返回值为创建的节点路径
            currentNode = zk.create("/" + rootNode + "/" + subNode,
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);

            // wait一小会, 让结果更清晰一些
            Thread.sleep(10);

            // 判断创建的节点是否是最小的序号节点，如果是获取到锁；如果不是，监听他序号前一个节点
            List<String> childrenNodes = zk.getChildren("/" + rootNode, false);

            // 如果children 只有一个值，那就直接获取锁； 如果有多个节点，需要判断，谁最小
            if (childrenNodes.size() == 1) {
                return;
            } else {
                // 对根节点下的所有临时顺序节点进行从小到大排序
                Collections.sort(childrenNodes);

                // 获取节点名称 seq-00000000
                String thisNode = currentNode.substring(("/" + rootNode + "/").length());

                // 通过seq-00000000获取该节点在children集合的位置
                int index = childrenNodes.indexOf(thisNode);

                // 判断
                if (index == -1){
                    System.out.println("数据异常");
                } else if (index == 0){
                    // index == 0, 说明 thisNode 在列表中最小, 当前client 获得锁
                    return;
                } else {
                    // 获得排名比 currentNode 前 1 位的节点
                    this.waitPath = "/" + rootNode + "/" + childrenNodes.get(index - 1);

                    // 在 waitPath 上注册监听器, 当 waitPath 被删除时, zookeeper 会回调监听器的 process 方法
                    zk.getData(waitPath, true, new Stat());

                    //进入等待锁状态
                    waitLatch.await();

                    return;
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 解锁
     */
    public void unZkLock() {
        // 删除节点
        try {
            zk.delete(this.currentNode, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
