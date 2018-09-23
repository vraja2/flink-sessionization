package com.vigneshraja.sessionization.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.time.Instant;

/**
 * Created by vraja on 9/3/18
 */
public abstract class Event {

    @JsonProperty
    protected long timestamp;

    public Event() {
        this.timestamp = Instant.now().toEpochMilli();
    }

    /**
     * Returns the key that will be used to build sessions
     */
    abstract String getKey();

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("timestamp", timestamp)
            .toString();
    }
}
