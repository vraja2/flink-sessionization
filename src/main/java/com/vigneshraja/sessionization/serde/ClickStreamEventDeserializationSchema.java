package com.vigneshraja.sessionization.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vigneshraja.sessionization.models.ClickstreamEvent;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

/**
 * Created by vraja on 9/3/18
 */
public class ClickStreamEventDeserializationSchema extends AbstractDeserializationSchema<ClickstreamEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public ClickstreamEvent deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, ClickstreamEvent.class);
    }
}
