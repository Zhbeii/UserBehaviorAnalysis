package com.zhbeii.hotitems_analysis;/*
@author Zhbeii
@date 2022/1/4 - 15:45
*/

import com.zhbeii.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class HotItemsWithSql {


    public static void main(String[] args) throws Exception {
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
                .map(line -> {
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


        //4. 创建表的执行环境,用blink版本
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //5. 将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId, behavior, timestamp.rowtime as ts");

        //6.分组开窗
        //table api
        Table windowAggTable = dataTable
                .filter("behavior = 'pv'")  //算子对每个元素进行过滤，过滤的过程使用一个filter函数进行逻辑判断。对于输入的每个元素，如果filter函数返回True，则保留，如果返回False，则丢弃。
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))    //开一个滑动窗口
                .groupBy("itemId, w")
                .select("itemId, w.end as windowEnd, itemId.count as cnt");

        //7.利用开窗函数,对count值进行排序并获取Row number ,得到Top N
        //SQL
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg", aggStream, "itemId, windowEnd, cnt");
        Table resultTable = tableEnv.sqlQuery("select * from " +
                "(select * ,ROW_NUMBER() over(partition by windowEnd order by cnt desc) as row_num" +
                " from agg)" +
                "where row_num <= 5 ");

        // 纯SQL实现
        tableEnv.createTemporaryView("data_table", dataStream, "itemId, behavior, timestamp.rowtime as ts");
        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                "  ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "  from ( " +
                "    select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd " +
                "    from data_table " +
                "    where behavior = 'pv' " +
                "    group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "    )" +
                "  ) " +
                " where row_num <= 5 ");


//        tableEnv.toRetractStream(resultTable, Row.class).print();
        tableEnv.toRetractStream(resultSqlTable, Row.class).print();
        env.execute("hot items with sql job");
    }
}
