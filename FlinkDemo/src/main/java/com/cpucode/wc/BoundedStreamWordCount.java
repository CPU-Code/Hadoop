package com.cpucode.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author : cpucode
 * @date : 2022/4/19 21:18
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境 , 流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文件
        DataStreamSource<String> lineDSS = env.readTextFile("FlinkDemo/src/main/resources/input/words.txt");

        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS.flatMap((String line, Collector<String> word) -> {
            Arrays.stream(line.split(" ")).forEach(word::collect);
        }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 分组 , 匿名函数作为键选择器
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        // 6. 打印
        result.print();

        // 7. 开始执行任务
        env.execute();
    }
}
