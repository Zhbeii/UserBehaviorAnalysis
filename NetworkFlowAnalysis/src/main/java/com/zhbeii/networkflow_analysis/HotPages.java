package com.zhbeii.networkflow_analysis;/*
@author Zhbeii
@date 2022/1/4 - 20:50
*/

import com.zhbeii.networkflow_analysis.beans.ApacheLogEvent;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.text.SimpleDateFormat;

public class HotPages {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //事件时间语义
        env.setParallelism(1); //全局并行度

        //读取文件,转换成POJO
        URL resource = HotPages.class.getResource("/apache.log"); //利用了反射
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<ApacheLogEvent> dataStream = inputStream
                .map( line ->{
                    String[] fields = line.split(" ");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    //得到一个时间戳
                    Long timestamp = simpleDateFormat.parse(fields[3]).getTime(); //gettime得到的是毫秒
                    return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);

                })
                //上面的事件时间语义,分配时间戳和watermark,因为数据时间是乱序的,
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {
                    //Time.minutes(1)是有一分钟的延迟
                    @Override
                    public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                        return apacheLogEvent.getTimestamp();
                    }
                });

        //分组开窗聚合
        dataStream.filter( data -> "GET".equals(data.getMethod()) )    //过滤GET请求
        .keyBy( ApacheLogEvent::getUrl )



        //收集同一窗口count数据,排序输出


        env.execute("hot page job");


    }
}
