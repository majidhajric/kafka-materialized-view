package dev.demo.materializedview;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class OrderViewTest {

    @Test
    void shouldCreateMaterializedView() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        new OrderView().buildOrdersView(streamsBuilder);
        Properties properties = new Properties();
        properties.putAll(Map.of(StreamsConfig.APPLICATION_ID_CONFIG, "test-app",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092"));

        TopologyTestDriver testDriver = new TopologyTestDriver(streamsBuilder.build(), properties);
        TestInputTopic<Integer, String> inputTopic = testDriver.createInputTopic("orders", Serdes.Integer().serializer(), Serdes.String().serializer());

        inputTopic.pipeInput(1, "Product 1");
        inputTopic.pipeInput(2, "Product 2");
        inputTopic.pipeInput(1, "Product 1 - updated");
        inputTopic.pipeInput(2, "Product 2 - updated");

        KeyValueStore<Integer, String> ordersStore = testDriver.getKeyValueStore("orders-store");
        assertThat(ordersStore.get(1)).isEqualTo("Product 1 - updated");
        assertThat(ordersStore.get(2)).isEqualTo("Product 2 - updated");
    }
}