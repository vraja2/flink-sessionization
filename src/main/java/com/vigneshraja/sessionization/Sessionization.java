package com.vigneshraja.sessionization;

import com.vigneshraja.sessionization.models.ClickstreamEvent;
import com.vigneshraja.sessionization.models.Session;
import com.vigneshraja.sessionization.serde.ClickStreamEventDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

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

        SingleOutputStreamOperator<Session> sessionStream = datastream
            .keyBy(ClickstreamEvent::getKey)
            .process(new ComputeSessions());

        // just write to sdout for now
        sessionStream.map(Session::toString).print();

        env.execute();
    }
}
