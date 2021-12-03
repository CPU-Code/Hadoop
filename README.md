# Hadoop

Hadoop 学习之旅

-----------------------------

## [HDFS](HdfsClientDemo)

- [x] [客户端操作 HDFS](HdfsClientDemo/src/main/java/com/cpucode/hdfs/HdfsClient.java)

-------------------------

## [MapReduce](MapReduceDemo)

- [x] [单词计数](MapReduceDemo/src/main/java/com/cpucode/mapreduce/wordcount/WordCountDriver.java)
- [x] [单词计数入参](MapReduceDemo/src/main/java/com/cpucode/mapreduce/wordcountargs/WordCountDriver.java)
- [x] [序列化](MapReduceDemo/src/main/java/com/cpucode/mapreduce/writable/FlowDriver.java)
- [x] [切片机制](MapReduceDemo/src/main/java/com/cpucode/mapreduce/combineTextInputformat/WordCountDriver.java)
- [x] [Partition分区](MapReduceDemo/src/main/java/com/cpucode/mapreduce/partitioner/FlowDriver.java)
- [x] [全排序](MapReduceDemo/src/main/java/com/cpucode/mapreduce/writableComparable/FlowDriver.java)
- [x] [区内排序](MapReduceDemo/src/main/java/com/cpucode/mapreduce/partitionerComparable/FlowDriver.java)
- [x] [Combiner合并](MapReduceDemo/src/main/java/com/cpucode/mapreduce/combiner/WordCountDriver.java)
- [x] [自定义OutputFormat](MapReduceDemo/src/main/java/com/cpucode/mapreduce/outputformat/LogDriver.java)
- [x] [Reduce Join](MapReduceDemo/src/main/java/com/cpucode/mapreduce/reduceJoin/TableDriver.java)

-------------------------

## [Yarn](YarnDemo)

- [ ] [Yarn调度](YarnDemo/src/main/java/com/cpucode/yarn/WordCountDriver.java)

 
-------------------------

## [Zookeeper](zookeeper)

- [x] [zk测试api](zookeeper/src/main/java/com/cpucode/zk/ZkClient.java)
- [x] [服务器动态上下线监听](zookeeper/src/main/java/com/cpucode/distributeTest/DistributeClient.java)
- [x] [分布式锁](zookeeper/src/main/java/com/cpucode/distributeLock/DistributeLockTest.java)
- [x] [Curator框架实现分布式锁](zookeeper/src/main/java/com/cpucode/curatorLock/CuratorLockTest.java)

-------------------------

## [Kafka](kafka)

- [x] [生产者_异步发送_不带回调](kafka/src/main/java/com/cpucode/kafka/producer/KafkaAsyn.java)
- [x] [生产者_异步发送_带回调](kafka/src/main/java/com/cpucode/kafka/producer/KafkaCallback.java)
- [x] [生产者_同步发送_带回调](kafka/src/main/java/com/cpucode/kafka/producer/KafkaSync.java)
- [x] [自定义分区](kafka/src/main/java/com/cpucode/kafka/partitioner/KafkaProducerPartitioner.java)
- [x] [自动提交](kafka/src/main/java/com/cpucode/kafka/consumer/KafkaAuto.java)
- [x] [重置offset](kafka/src/main/java/com/cpucode/kafka/consumer/KafkaReset.java)
- [x] [同步提交](kafka/src/main/java/com/cpucode/kafka/consumer/ConsumerSync.java)
- [x] [异步提交](kafka/src/main/java/com/cpucode/kafka/consumer/ConsumerAsync.java)
- [ ] [拦截器](kafka/src/main/java/com/cpucode/kafka/interceptor/KafkaProducerInterceptor.java)

-------------------------

