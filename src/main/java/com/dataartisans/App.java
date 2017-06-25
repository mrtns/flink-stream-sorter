package com.dataartisans;

import com.dataartisans.provided.Event;
import com.dataartisans.provided.EventGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class App {
    private static final String INPUT_PATH_PARAM = "inputPath";
    private static final String OUTPUT_PATH_PARAM = "outputPath";

    public static void main( String[] args ) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> eventStream = getStreamFromSource(env, params);

        eventStream.transform("OrderOperator", eventStream.getType(), new OrderOperator<>());

        setStreamSink(eventStream, params);

        env.execute("Run Stream smoother");
    }

    private static DataStream<Event> getStreamFromSource(StreamExecutionEnvironment env, ParameterTool appConfig) {
        if(appConfig.has(INPUT_PATH_PARAM)) {
            return env.readTextFile(appConfig.get(INPUT_PATH_PARAM)).map(new JsonToEventMapper());
        }
        return env.addSource(new EventGenerator());
    }
    
    private static void setStreamSink(DataStream<Event> stream, ParameterTool appConfig) {
        if(appConfig.has(OUTPUT_PATH_PARAM)) {
            stream.map(new EventToJsonMapper())
                    .writeAsText(appConfig.get(OUTPUT_PATH_PARAM));
        } else {
            stream.print();
        }
    }

    public static class JsonToEventMapper implements MapFunction<String, Event> {
        private transient ObjectMapper jsonMapper;

        @Override
        public Event map(String value) throws Exception {
            if (jsonMapper == null) {
                jsonMapper = new ObjectMapper();
            }
            return jsonMapper.readValue(value, Event.class);
        }
    }

    public static class EventToJsonMapper implements MapFunction<Event, String> {
        private transient ObjectMapper jsonMapper;

        @Override
        public String map(Event value) throws Exception {
            if (jsonMapper == null) {
                jsonMapper = new ObjectMapper();
            }
            return jsonMapper.writeValueAsString(value);
        }
    }

    private static class OrderOperator<T> extends AbstractStreamOperator<T> implements OneInputStreamOperator<T, T> {
        private static final long serialVersionUID = 607406801052018312L;

        @Override
        public void processElement(StreamRecord<T> streamRecord) throws Exception {
            // this method is called for each incoming stream record
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            // this method is called for each incoming watermark
        }
    }
}
