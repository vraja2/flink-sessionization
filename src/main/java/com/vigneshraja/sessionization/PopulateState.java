package com.vigneshraja.sessionization;

import com.google.common.collect.Lists;
import com.vigneshraja.sessionization.models.ClickstreamEvent;
import com.vigneshraja.sessionization.models.Session;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Created by vraja on 9/6/18
 */
public class PopulateState extends KeyedProcessFunction<String, ClickstreamEvent, ClickstreamEvent> {

    private transient MapState<String, Session> sessionState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Session> descriptor =
            new MapStateDescriptor<>("session", TypeInformation.of(String.class), TypeInformation.of(Session.class));
        sessionState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(ClickstreamEvent event, Context ctx, Collector<ClickstreamEvent> out) throws Exception {
        if (sessionState.contains(event.getKey())) {
            sessionState.get(event.getKey()).getEvents().add(event);
        } else {
            sessionState.put(event.getKey(), new Session(Lists.newArrayList(event)));
        }
    }
}
