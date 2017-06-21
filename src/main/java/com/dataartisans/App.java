package com.dataartisans;

import com.dataartisans.provided.Event;
import com.dataartisans.provided.EventGenerator;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class App {
    public static void main( String[] args ) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> eventStream = env.addSource(new EventGenerator());
        eventStream.transform("OrderOperator", eventStream.getType(), new OrderOperator<>());

        env.execute("Run Stream smoother");
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
