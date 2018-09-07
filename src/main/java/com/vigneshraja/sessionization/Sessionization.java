package com.vigneshraja.sessionization;

import com.vigneshraja.sessionization.models.ClickstreamEvent;
import com.vigneshraja.sessionization.models.Session;
import com.vigneshraja.sessionization.serde.ClickStreamEventDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.concurrent.TimeUnit;

/**
 * Created by vraja on 9/2/18
 */
public class Sessionization {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(
            Sessionization.class.getClassLoader().getResourceAsStream("sessionization.properties")
        );
        DataStreamSource<ClickstreamEvent> datastream = env.addSource(
            new FlinkKafkaConsumer011<>(
                parameterTool.get("input.topic"),
                new ClickStreamEventDeserializationSchema(),
                parameterTool.getProperties()
            )
        );

        env.enableCheckpointing(1000L);

        StateBackend stateBackend = new RocksDBStateBackend("file:////Users/vraja/flink-checkpoint");
        env.setStateBackend(stateBackend);

        // GlobalWindows assigner doesn't have default trigger unlike other window assigners
        // clickstream will be grouped by userId
        // process keyedStream and store in RocksDB state (use ProcessFunction)
        // trigger defines when to process elements in window
        KeyedStream<ClickstreamEvent, String> keyedClickStream = datastream.keyBy(ClickstreamEvent::getKey);

        keyedClickStream.process(new PopulateState());

        // process rocksdb state and for closed sessions push to kafka
        SingleOutputStreamOperator<Session> sessionStream = keyedClickStream
            .window(GlobalWindows.create())
            .trigger(ContinuousProcessingTimeTrigger.of(Time.of(1, TimeUnit.MINUTES)))
            .process(new ComputeSessions());

        sessionStream.map(Session::toString).print();

        env.execute();
    }
}
