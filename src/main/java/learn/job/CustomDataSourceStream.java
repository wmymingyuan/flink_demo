package learn.job;

import learn.source.CustomDataSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CustomDataSourceStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Map<String, String>> mapDataStream = env.addSource(new CustomDataSource());
        WatermarkStrategy<Map<String, String>> watermarkStrategy = WatermarkStrategy.<Map<String, String>>forMonotonousTimestamps()
                .withTimestampAssigner((map, timestamp) -> Long.parseLong(map.get("time")));
        WindowedStream<Map<String, String>, String, TimeWindow> window = mapDataStream.assignTimestampsAndWatermarks(watermarkStrategy).keyBy(m -> m.get("id")).window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)));

        env.execute("Custom Data Source Job");
    }
}
