package com.dataartisans;

import com.dataartisans.provided.Event;
import com.google.common.collect.Lists;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link AppFunctions}.
 */
public class AppFunctionsTests {

   public static class AssignTimestampsAndWatermarksTests {
       @Test
       public void shouldHaveNoTimestampsBeforeAssignment() throws Exception {
           final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
           env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

           DataStream<Event> stream = env.fromCollection(
                   new ArrayList<>(Arrays.asList(
                           new Event(1498404088692L, "some data")
                   ))
           );

           DataStream<String> timestampInspectedStream = stream.keyBy("time").process(new TimestampInspector());
           Iterator<String> result = DataStreamUtils.collect(timestampInspectedStream);
           assertEquals("", Lists.newArrayList("Timestamp: null"), Lists.newArrayList(result));
       }

       @Test
       public void shouldAssignTimestamps() throws Exception {
           final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
           env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

           DataStream<Event> stream = env.fromCollection(
                   new ArrayList<>(Arrays.asList(
                           new Event(1498404088692L, "some data")
                   ))
           );
           DataStream<Event> timestampedEventStream = AppFunctions.assignTimestampsAndWatermarks(stream);

           DataStream<String> timestampInspectedStream = timestampedEventStream.keyBy("time").process(new TimestampInspector());
           Iterator<String> result = DataStreamUtils.collect(timestampInspectedStream);
           assertEquals("", Lists.newArrayList("Timestamp: 1498404088692"), Lists.newArrayList(result));
       }

       private static class TimestampInspector extends RichProcessFunction<Event, String> {
           @Override
           public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
               System.out.println(String.format("processElement: Context.timestamp is %s", ctx.timestamp()));
               String elementTimestampAsString = ctx.timestamp() == null ? "null" : ctx.timestamp().toString();
               out.collect(String.format("Timestamp: %s", elementTimestampAsString));

           }

           @Override
           public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
              // do nothing
           }
       }
   }
}
