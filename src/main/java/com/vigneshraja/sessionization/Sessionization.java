package com.vigneshraja.sessionization;

import com.vigneshraja.sessionization.models.ClickstreamEvent;
import com.vigneshraja.sessionization.serde.ClickStreamEventDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

        // just a simple identity mapping to verify streams are configured correctly
        datastream.rebalance().map(ClickstreamEvent::toString).print();

        env.execute();
    }
}
