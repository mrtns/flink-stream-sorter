package com.dataartisans;

import com.dataartisans.provided.Event;
import com.dataartisans.provided.EventGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class AppFunctions {
    public static DataStream<Event> assignTimestampsAndWatermarks(DataStream<Event> stream) {
        Time maxAllowedLatenessForEventGeneratorSource = Time.milliseconds(EventGenerator.OutOfOrderness);
        DataStream<Event> result = stream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Event>(maxAllowedLatenessForEventGeneratorSource) {
                    @Override
                    public long extractTimestamp(Event element) {
                        return element.time;
                    }
                }
        );
        return result;
    }

    public static DataStream<Event> orderEventStream(DataStream<Event> stream) {
        stream.transform("OrderOperator", stream.getType(), new OrderOperator<>());
        return stream;
    }

    private static class OrderOperator<T> extends AbstractStreamOperator<T> implements OneInputStreamOperator<T, T> {
        private static final long serialVersionUID = 607406801052018312L;

        @Override
        public void processElement(StreamRecord<T> streamRecord) throws Exception {
            // this method is called for each incoming stream record
            // System.out.println(String.format("streamRecord.hasTimeStamp? %s", streamRecord.hasTimestamp()));
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            // this method is called for each incoming watermark
        }
    }
}
