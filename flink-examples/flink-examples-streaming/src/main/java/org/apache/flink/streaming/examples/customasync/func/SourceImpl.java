package org.apache.flink.streaming.examples.customasync.func;

import java.text.SimpleDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import org.apache.flink.streaming.examples.customasync.pojo.Elem;


/**
 * @description: d
 * @author: Kylekaixu
 * @create: 2022-03-08 14:30
 **/
public class SourceImpl implements SourceFunction<Elem> {

    private final String mode;
    private final long gap;
    private volatile boolean isRunning = true;

    public SourceImpl(String mode, long gap) {
        this.mode = mode;
        this.gap = gap;
    }

    @Override
    public void run(SourceContext<Elem> sourceContext) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        if ("0".equals(mode)) {
            String[] elem = {"aaa:4", "bbb:13", "ccc:6", "ddd:8", "eee:3", "fff:10", "ggg:2", "hhh:11"};
            for (String e : elem) {
                Thread.sleep(this.gap);
                String produceTime = df.format(new Date());
                sourceContext.collect(new Elem(e.split(":")[0], produceTime, Integer.valueOf(e.split(":")[1])));

            }
        } else {
            while (isRunning) {
                Double random = Math.random() * 10;
                Thread.sleep(this.gap);
                String produceTime = df.format(new Date());
                //sourceContext.collect(new Elem("aaa", produceTime, random.intValue()));
                sourceContext.collect(new Elem("aaa", produceTime, 1));

            }
        }


    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
