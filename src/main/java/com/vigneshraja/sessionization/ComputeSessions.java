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

import java.util.Map;

/**
 * Created by vraja on 9/6/18
 */
public class ComputeSessions extends KeyedProcessFunction<String, ClickstreamEvent, Session> {

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
            long nextTimer = System.currentTimeMillis() + 60000;
            ctx.timerService().registerProcessingTimeTimer(nextTimer);
            timerState.update(nextTimer);
        }

        if (sessionState.contains(event.getKey())) {
            // TODO: if session is closed, replace with new session
            sessionState.get(event.getKey()).getEvents().add(event);
        } else {
            sessionState.put(event.getKey(), new Session(Lists.newArrayList(event)));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Session> out) throws Exception {
        for (Map.Entry<String, Session> entry : sessionState.entries()) {
            // TODO: if more than 30 minutes of inactivity, close session
            out.collect(entry.getValue());
        }

        long nextTimer = System.currentTimeMillis() + 60000;
        timerState.update(nextTimer);
        ctx.timerService().registerProcessingTimeTimer(nextTimer);
    }
}
