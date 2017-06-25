package com.dataartisans;

import com.dataartisans.provided.Event;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Tests for {@link AppFunctions}.
 */
public class AppFunctionsTests {

   public static class AssignTimestampsAndWatermarksTests {
       @Test
       public void shouldAssignTimestamps() throws Exception {
           final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
           env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

           DataStream<Event> stream = env.fromCollection(new ArrayList<>(Arrays.asList(new Event())));

           // expect StreamRecord.hasTimeStamp to be false

           DataStream<Event> result = AppFunctions.assignTimestampsAndWatermarks(stream);

           // expect StreamRecord.hasTimeStamp to be true

           throw new UnsupportedOperationException("not implemented yet");

           //env.execute();
       }
   }
}
