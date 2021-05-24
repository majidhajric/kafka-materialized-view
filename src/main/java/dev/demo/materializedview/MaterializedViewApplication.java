package dev.demo.materializedview;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@EnableKafkaStreams
@SpringBootApplication
public class MaterializedViewApplication {

	public static void main(String[] args) {
		SpringApplication.run(MaterializedViewApplication.class, args);
	}

}

@Component
class OrderView {

	@Autowired
	public void buildOrdersView(StreamsBuilder builder) {
		builder.table("orders", Consumed.with(Serdes.Integer(), Serdes.String()), Materialized.as("orders-store"));
	}
}

@RequiredArgsConstructor
@RestController
@RequestMapping("/orders")
class InteractiveQueryController {

	private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	@GetMapping(path = "/{orderId}")
	public String getOrder(@PathVariable Integer orderId) {
		KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
		ReadOnlyKeyValueStore<Integer, String> store = kafkaStreams.store(StoreQueryParameters.fromNameAndType("orders-store", QueryableStoreTypes.keyValueStore()));
		return store.get(orderId);
	}
}

@RequiredArgsConstructor
@Component
class Producer {
	private final KafkaTemplate<Integer, String> kafkaTemplate;

	@EventListener(ApplicationStartedEvent.class)
	public void produce() {
		kafkaTemplate.send("orders", 1, "Product 1");
		kafkaTemplate.send("orders", 2, "Product 2");
		kafkaTemplate.send("orders", 1, "Product 1 - updated");
		kafkaTemplate.send("orders", 2, "Product 2 - updated");
	}
}

@Configuration
class KafkaAdminConfig {
	@Value("${spring.kafka.properties.bootstrap.servers}")
	private String bootstrapServers;

	@Bean
	public KafkaAdmin kafkaAdmin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		return new KafkaAdmin(configs);
	}

	@Bean
	public NewTopic ordersTopic() {
		return TopicBuilder.name("orders").build();
	}
}