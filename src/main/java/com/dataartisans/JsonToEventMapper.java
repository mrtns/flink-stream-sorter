package com.dataartisans;

import com.dataartisans.provided.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

public class JsonToEventMapper implements MapFunction<String, Event> {
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public Event map(String value) throws Exception {
        return jsonMapper.readValue(value, Event.class);
    }
}
