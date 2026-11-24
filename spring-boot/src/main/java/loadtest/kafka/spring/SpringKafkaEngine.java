package loadtest.kafka.spring;

import java.time.Duration;
import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.stereotype.Component;

import loadtest.kafka.core.ConsumerEvents;
import loadtest.kafka.core.Engine;
import loadtest.kafka.core.LoadTestConfig;
import loadtest.kafka.core.Stats;

/**
 * Engine auf Basis von Spring Kafka: KafkaTemplate zum Senden und ein
 * ConcurrentMessageListenerContainer mit Batch-Listener zum Empfangen, also derselbe
 * Poll- und Commit-Mechanismus wie bei einem @KafkaListener.
 */
@Component
public class SpringKafkaEngine implements Engine {

    private static final Logger LOG = LoggerFactory.getLogger("kafka-loadtest");

    @Override
    public Sender newSender(String name, LoadTestConfig cfg, Stats stats) {
        // Eigene Factory je Sender: so hat jeder Producer-Thread seinen eigenen KafkaProducer.
        DefaultKafkaProducerFactory<byte[], byte[]> factory =
                new DefaultKafkaProducerFactory<>(cfg.producer().settings());
        KafkaTemplate<byte[], byte[]> template = new KafkaTemplate<>(factory);
        return new Sender() {
            @Override
            public void send(byte[] value) {
                long start = System.nanoTime();
                template.send(cfg.record(value)).whenComplete((result, error) -> {
                    stats.sendResult(value.length, start, error);
                    if (error != null) {
                        LOG.warn("[{}] Zustellung fehlgeschlagen: {}", name, error.toString());
                    }
                });
            }

            @Override
            public void close() {
                template.flush();
                factory.destroy();
            }
        };
    }

    @Override
    public AutoCloseable startConsumers(LoadTestConfig cfg, ConsumerEvents events) {
        LoadTestConfig.ConsumerPart cc = cfg.consumer();
        ContainerProperties props = new ContainerProperties(cfg.topic());
        props.setPollTimeout(Math.max(1, (long) cc.pollTimeoutMs()));
        props.setIdleBetweenPolls((long) cc.pollIntervalMs());
        props.setShutdownTimeout(Duration.ofSeconds(30).toMillis());
        props.setMessageListener((BatchMessageListener<byte[], byte[]>) events::records);
        props.setConsumerRebalanceListener(new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                events.assigned(Thread.currentThread().getName(), partitions);
            }

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                events.revoked(Thread.currentThread().getName(), partitions);
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                events.lost(Thread.currentThread().getName(), partitions);
            }
        });

        ConcurrentMessageListenerContainer<byte[], byte[]> container = new ConcurrentMessageListenerContainer<>(
                new DefaultKafkaConsumerFactory<>(cc.settings()), props);
        container.setConcurrency(cc.instances());
        container.setBeanName("consumer");
        container.start();
        return container::stop;
    }
}
