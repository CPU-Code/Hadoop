package com.cpucode.curatorLock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author : cpucode
 * @date : 2021/11/28 21:01
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class CuratorLockTest {

    private static String rootNode = "/locks";
    // zookeeper server 列表
    private static String connectString = "cpucode100:2181,cpucode101:2181,cpucode102:2181";
    // connection 超时时间
    private static int connectionTimeout = 2000;
    // session 超时时间
    private static int sessionTimeOut = 2000;

    public static void main(String[] args) {
        new CuratorLockTest().test();
    }

    // 测试
    private void test() {
        // 创建分布式锁1
        InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(), "/locks");

        // 创建分布式锁2
        InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(), "/locks");

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    lock1.acquire();
                    System.out.println("线程 1 获取锁");

                    // 测试锁重入
                    lock1.acquire();
                    System.out.println("线程 1 再次获取锁");
                    Thread.sleep(5 * 1000);

                    lock1.release();
                    System.out.println("线程 1 释放锁");

                    lock1.release();
                    System.out.println("线程 1 再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    lock2.acquire();
                    System.out.println("线程 2 获取锁");

                    // 测试锁重入
                    lock2.acquire();
                    System.out.println("线程 2 再次获取锁");
                    Thread.sleep(5 * 1000);

                    lock2.release();
                    System.out.println("线程 2 释放锁");

                    lock2.release();
                    System.out.println("线程 2 再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    /**
     *  分布式锁初始化
     * @return
     */
    private static CuratorFramework getCuratorFramework() {
        //重试策略，初试时间 3 秒，重试 3 次
        RetryPolicy policy = new ExponentialBackoffRetry(3000, 3);

        //通过工厂创建 Curator
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                        .connectionTimeoutMs(connectionTimeout)
                        .sessionTimeoutMs(sessionTimeOut)
                        .retryPolicy(policy).build();

        //开启连接
        client.start();
        System.out.println("zookeeper 初始化完成...");
        return client;
    }
}
