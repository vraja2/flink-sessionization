package com.vigneshraja.sessionization;

import com.google.common.collect.Lists;
import com.vigneshraja.sessionization.models.ClickstreamEvent;
import com.vigneshraja.sessionization.models.Session;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.vigneshraja.sessionization.models.Session.*;

/**
 * Created by vraja on 9/6/18
 */
public class ComputeSessions extends KeyedProcessFunction<String, ClickstreamEvent, Session> {

    // TODO: store in config
    private static final long SESSION_INACTIVITY_MS = TimeUnit.MINUTES.toMillis(30);

    private transient MapState<String, Session> sessionState;
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Session> descriptor =
            new MapStateDescriptor<>("session", TypeInformation.of(String.class), TypeInformation.of(Session.class));
        sessionState = getRuntimeContext().getMapState(descriptor);

        ValueStateDescriptor<Long> timerStateDescriptor =
            new ValueStateDescriptor<>("timerState", TypeInformation.of(Long.class));
        timerState = getRuntimeContext().getState(timerStateDescriptor);
    }

    @Override
    public void processElement(ClickstreamEvent event, Context ctx, Collector<Session> out) throws Exception {
        // set the first timer
        if (timerState.value() == null) {
            // TODO: put timer interval in config
            long nextTimer = Instant.now().toEpochMilli() + 60000;
            ctx.timerService().registerProcessingTimeTimer(nextTimer);
            timerState.update(nextTimer);
        }

        if (sessionState.contains(event.getKey())) {
            // if session is closed, replace with new session
            Session session = sessionState.get(event.getKey());
            if (session.getStatus() == Status.CLOSED) {
                sessionState.put(
                    event.getKey(), new Session(Lists.newArrayList(event), event.getTimestamp(), Status.OPEN)
                );
            } else {
                session.addEvent(event);
                sessionState.put(event.getKey(), session);
            }
        } else {
            sessionState.put(
                event.getKey(),
                new Session(Lists.newArrayList(event), event.getTimestamp(), Status.OPEN)
            );
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Session> out) throws Exception {
        for (Map.Entry<String, Session> entry : sessionState.entries()) {
            Session session = entry.getValue();
            // close session if it's been more than 30 minutes of inactivity
            if (Instant.now().toEpochMilli() - session.getLastEventTimestamp() >= SESSION_INACTIVITY_MS) {
                session.setStatus(Status.CLOSED);
            }
            out.collect(session);
        }

        // computing current time again since we want to wait a minute after the RocksDB iteration is performed
        long nextTimer = Instant.now().toEpochMilli() + 60000;
        timerState.update(nextTimer);
        ctx.timerService().registerProcessingTimeTimer(nextTimer);
    }
}
