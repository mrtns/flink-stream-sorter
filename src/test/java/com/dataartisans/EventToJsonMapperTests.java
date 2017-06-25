package com.dataartisans;

import com.dataartisans.provided.Event;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link EventToJsonMapper}.
 */
public class EventToJsonMapperTests {
    @Test
    public void shouldSerializeEventToJson() {
        Event anEvent = new Event(1498404088692L, "some data value");
        String result = null;

        try {
            result = new EventToJsonMapper().map(anEvent);
        } catch (Exception e) {
            fail(e.toString());
        }

        assertEquals("should be a JSON string with matching values", "{\"time\":1498404088692,\"someData\":\"some data value\"}", result);
    }
}


