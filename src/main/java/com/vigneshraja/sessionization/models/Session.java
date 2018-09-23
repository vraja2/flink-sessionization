package com.vigneshraja.sessionization.models;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.UUID;

/**
 * Created by vraja on 9/4/18
 */
public class Session {

    public enum Status {
        OPEN,
        CLOSED
    }

    private List<Event> events;
    private long lastEventTimestamp;
    private Status status;
    private String id;

    public Session() {
        this(Lists.newArrayList(), 0, Status.OPEN);
    }

    public Session(List<Event> events, long lastEventTimestamp, Status status) {
        this.events = events;
        this.lastEventTimestamp = lastEventTimestamp;
        this.status = status;
        this.id = UUID.randomUUID().toString();
    }

    public List<Event> getEvents() {
        return events;
    }

    public void setEvents(List<Event> events) {
        this.events = events;
    }

    public long getLastEventTimestamp() {
        return lastEventTimestamp;
    }

    public void setLastEventTimestamp(long lastEventTimestamp) {
        this.lastEventTimestamp = lastEventTimestamp;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public void addEvent(Event event) {
        events.add(event);
        if (event.getTimestamp() > lastEventTimestamp) {
            lastEventTimestamp = event.getTimestamp();
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("events", events)
            .add("lastEventTimestamp", lastEventTimestamp)
            .add("status", status)
            .add("id", id)
            .toString();
    }
}
