package org.apache.flink.streaming.examples.customasync.func;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.examples.customasync.pojo.Elem;

/**
 * @description: d
 * @author: Kylekaixu
 * @create: 2022-03-08 14:31
 **/
public class AsyncImpl extends RichAsyncFunction<Elem, String> {

    private ThreadPoolExecutor threadPoolExecutor;
    private final int corePoolSize;
    private final int maxPoolSize;
    private final int blockQueue;
    private final long queueKeepAlive;
    private BlockingQueue<Runnable> workQueue;

    public AsyncImpl(int corePoolSize, int maxPoolSize, int blockQueue, long queueKeepAlive) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.blockQueue = blockQueue;
        this.queueKeepAlive = queueKeepAlive;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        workQueue = new LinkedBlockingDeque<>(this.blockQueue);
        threadPoolExecutor = new ThreadPoolExecutor(
                this.corePoolSize,
                this.maxPoolSize,
                this.queueKeepAlive,
                TimeUnit.SECONDS,
                workQueue,
                (r, executor) -> System.out.println("REJECT BY QUEUE:" + executor.toString()));

        // 实现默认拒绝策略直接抛异常
//        threadPoolExecutor = new ThreadPoolExecutor(
//                this.corePoolSize,
//                this.maxPoolSize,
//                this.queueKeepAlive,
//                TimeUnit.SECONDS,
//                workQueue);
    }

    @Override
    public void asyncInvoke(Elem input, ResultFuture<String> resultFuture) {

        Future<String> future = threadPoolExecutor.submit(() -> {
            if (null == input) {
                return null;
            }
            try {
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                int delay = input.getDelayTime();
                String processTime = df.format(new Date());
                input.setProcessTime(processTime);
                Thread.sleep(delay * 1000L + 1);
                input.setEndTime(df.format(new Date()));
                return String.format(
                        "=== Thread Name:%s; input:%s; produceTime: %s; processTime: %s; delay:%s; endTime:%s; workQueueSize: %s ===",
                        Thread.currentThread().getName(),
                        input.getElemName(),
                        input.getProduceTime(),
                        input.getProcessTime(),
                        input.getDelayTime(),
                        input.getEndTime(),
                        workQueue.size()
                );


            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        });

        CompletableFuture.supplyAsync(() -> {
            try {
                return future.get();
            } catch (Exception e) {
                return null;
            }
        }).thenAccept(result ->
                resultFuture.complete(Collections.singletonList(result)));
    }

    @Override
    public void timeout(Elem input, ResultFuture<String> resultFuture) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String processTime = df.format(new Date());
        input.setProcessTime(processTime);
        resultFuture.complete(Collections.singletonList(String.format(
                "Timeout! Thread Name:%s; input:%s; produceTime: %s; processTime: %s; delay:%s;",
                Thread.currentThread().getName(),
                input.getElemName(),
                input.getProduceTime(),
                input.getProcessTime(),
                input.getDelayTime())));
    }
}
