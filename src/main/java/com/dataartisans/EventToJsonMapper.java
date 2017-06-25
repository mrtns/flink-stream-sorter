package com.dataartisans;

import com.dataartisans.provided.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

public class EventToJsonMapper implements MapFunction<Event, String> {
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public String map(Event value) throws Exception {
        return jsonMapper.writeValueAsString(value);
    }
}
