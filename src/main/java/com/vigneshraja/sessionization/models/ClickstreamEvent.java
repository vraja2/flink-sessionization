package com.vigneshraja.sessionization.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

/**
 * Created by vraja on 9/3/18
 */
public class ClickstreamEvent implements Event {

    public enum EventType {
        PAGE_VIEW,
        BUTTON_CLICK
    }

    @JsonProperty
    private String userId;

    @JsonProperty
    private EventType eventType;

    public ClickstreamEvent() {}

    public ClickstreamEvent(String userId, EventType eventType) {
        this.userId = userId;
        this.eventType = eventType;
    }

    @JsonIgnore
    @Override
    public String getKey() {
        return userId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("userId", userId)
            .add("eventType", eventType)
            .toString();
    }
}
