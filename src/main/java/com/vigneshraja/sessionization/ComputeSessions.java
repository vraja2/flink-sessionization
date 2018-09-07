package com.vigneshraja.sessionization;

import com.google.common.collect.Lists;
import com.vigneshraja.sessionization.models.ClickstreamEvent;
import com.vigneshraja.sessionization.models.Session;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by vraja on 9/4/18
 */
public class ComputeSessions extends ProcessWindowFunction<ClickstreamEvent, Session, String, GlobalWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(ComputeSessions.class);
    private transient MapState<String, Session> sessionState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Session> descriptor =
            new MapStateDescriptor<>("session", TypeInformation.of(String.class), TypeInformation.of(Session.class));
        sessionState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void clear(Context context) throws Exception {
        super.clear(context);
    }

    @Override
    public void process(String s, Context context, Iterable<ClickstreamEvent> events, Collector<Session> out) throws Exception {
        // scan through rocksDb
        boolean sawElements = false;
        for (Map.Entry<String, Session> entry : sessionState.entries()) {
            LOG.info("ENTRY");
            LOG.info(entry.getKey());
            LOG.info(entry.getValue().toString());
            out.collect(entry.getValue());
            sawElements = true;
        }

        System.out.println("Saw elements " + sawElements);
        LOG.info("Saw elements {}", sawElements);
    }
}
