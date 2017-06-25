package com.dataartisans;

import com.dataartisans.provided.Event;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JsonToEventMapperTests {
    @Test
    public void shouldParseSuccessfully() {
        String validJsonString = "{\"time\":1498404088692,\"someData\":\"some data value\"}";
        Event result = null;

        try {
            result = new App.JsonToEventMapper().map(validJsonString);
        } catch (Exception e) {
            fail(e.toString());
        }

        assertEquals("should be an Event object with matching values", new Event(1498404088692L, "some data value"), result);
    }
}
