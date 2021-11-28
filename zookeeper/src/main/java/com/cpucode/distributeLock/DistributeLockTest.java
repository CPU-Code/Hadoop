package com.cpucode.distributeLock;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @author : cpucode
 * @date : 2021/11/28 17:38
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class DistributeLockTest {
    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        // 创建分布式锁 1
        final DistributeLock lock1 = new DistributeLock();
        // 创建分布式锁 2
        final DistributeLock lock2 = new DistributeLock();

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    lock1.zkLock();

                    System.out.println("线程 1 获取锁");
                    Thread.sleep(5 * 1000);

                    lock1.unZkLock();
                    System.out.println("线程 1 释放锁");
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
                    lock2.zkLock();

                    System.out.println("线程 2 获取锁");
                    Thread.sleep(5 * 1000);

                    lock2.unZkLock();
                    System.out.println("线程 2 释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
