package com.zhbeii.hotitems_analysis;/*
@author Zhbeii
@date 2022/1/3 - 20:39
*/

import com.zhbeii.hotitems_analysis.beans.ItemViewCount;
import com.zhbeii.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class HotItems {
    public static void main(String[] args) throws Exception{
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局的并行度为  1
        env.setParallelism(1);
        // 设置事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2. 读取数据,创建DataStream
        DataStream<String> inputStream = env.readTextFile("D:\\Program\\workspace\\spark\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        //3. 转换为POJO,分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map( line ->{
                    String[] fields = line.split(",");
                    return new UserBehavior(
                            new Long(fields[0]),
                            new Long(fields[1]),
                            new Integer(fields[2]),
                            fields[3],
                            new Long(fields[4])
                    );
                })
                //watermark 的引入:assignTimestampsAndWatermarks
                //一种简单的特殊情况是，如果我们事先得知数据流的时间戳是单调递增的，也
                //就是说没有乱序，那我们可以使用 AscendingTimestampExtractor，这个类会直接使
                //用数据的时间戳生成 watermark。
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //4. 分组开窗聚合,得到每个商品内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream




    }
}
