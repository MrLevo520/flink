package org.apache.flink.streaming.examples.customasync;


import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.examples.customasync.func.AsyncImpl;
import org.apache.flink.streaming.examples.customasync.func.SourceImpl;
import org.apache.flink.streaming.examples.customasync.pojo.Elem;


/**
 * 异步AsyncDataStream的一些测试
 *
 * @author: Kylekaixu
 * @create: 2022-03-03 16:13
 **/
public class AsyncTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认是电脑能提供的核数
        env.setParallelism(1);
        //DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 18081, "\n");

        DataStreamSource<Elem> source = env.addSource(new SourceImpl("1", 20L))
                ;


        SingleOutputStreamOperator<String> result = AsyncDataStream.orderedWait(
                source,
                new AsyncImpl(2, 3, 1, 60L),
                12,
                TimeUnit.SECONDS,
                4);

        result.print().setParallelism(1);
        env.execute("async test");
    }
}
