package com.vigneshraja.sessionization.models;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by vraja on 9/4/18
 */
public class Session {

    private List<Event> events;

    public Session() {
        this.events = Lists.newArrayList();
    }

    public Session(List<Event> events) {
        this.events = events;
    }

    public List<Event> getEvents() {
        return events;
    }

    public void setEvents(List<Event> events) {
        this.events = events;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("events", events)
            .toString();
    }
}
