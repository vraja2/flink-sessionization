package com.vigneshraja.sessionization.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vigneshraja.sessionization.models.Session;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by vraja on 9/23/18
 */
public class SessionSerializationSchema implements SerializationSchema<Session> {

    private static final Logger LOG = LoggerFactory.getLogger(SessionSerializationSchema.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(Session session) {
        byte[] bytes = new byte[0];
        try {
            bytes = MAPPER.writeValueAsBytes(session);
        } catch (JsonProcessingException e) {
            LOG.error("Unable to serialize session {}", session);
        }

        return bytes;
    }
}
