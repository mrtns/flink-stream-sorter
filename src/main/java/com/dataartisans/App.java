package com.dataartisans;

import com.dataartisans.provided.Event;
import com.dataartisans.provided.EventGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class App {
    private static final String INPUT_PATH_PARAM = "inputPath";
    private static final String OUTPUT_PATH_PARAM = "outputPath";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> eventStream = getStreamFromSource(env, params);

        DataStream<Event> timestampedEventStream = AppFunctions.assignTimestampsAndWatermarks(eventStream);

        DataStream<Event> orderedEventStream = AppFunctions.orderEventStream(timestampedEventStream);

        setStreamSink(orderedEventStream, params);

        env.execute("Run Stream smoother");
    }

    private static DataStream<Event> getStreamFromSource(StreamExecutionEnvironment env, ParameterTool appConfig) {
        if (appConfig.has(INPUT_PATH_PARAM)) {
            return env.readTextFile(appConfig.get(INPUT_PATH_PARAM))
                    .map(new JsonToEventMapper());
        }
        return env.addSource(new EventGenerator());
    }

    private static void setStreamSink(DataStream<Event> stream, ParameterTool appConfig) {
        if (appConfig.has(OUTPUT_PATH_PARAM)) {
            stream.map(new EventToJsonMapper())
                    .writeAsText(appConfig.get(OUTPUT_PATH_PARAM));
        } else {
            stream.print();
        }
    }
}
