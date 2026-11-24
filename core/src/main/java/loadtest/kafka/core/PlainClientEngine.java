package loadtest.kafka.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Engine auf Basis der reinen Kafka-Clients (KafkaProducer / KafkaConsumer). */
public class PlainClientEngine implements Engine {

    private static final Logger LOG = LoggerFactory.getLogger("kafka-loadtest");

    @Override
    public Sender newSender(String name, LoadTestConfig cfg, Stats stats) {
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(cfg.producer().settings());
        return new Sender() {
            @Override
            public void send(byte[] value) {
                long start = System.nanoTime();
                producer.send(cfg.record(value), (metadata, error) -> {
                    stats.sendResult(value.length, start, error);
                    if (error != null) {
                        LOG.warn("[{}] Zustellung fehlgeschlagen: {}", name, error.toString());
                    }
                });
            }

            @Override
            public void close() {
                producer.close(Duration.ofSeconds(30));
            }
        };
    }

    @Override
    public AutoCloseable startConsumers(LoadTestConfig cfg, ConsumerEvents events) {
        List<KafkaConsumer<byte[], byte[]>> consumers = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < cfg.consumer().instances(); i++) {
            String name = "consumer-" + i;
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(cfg.consumer().settings());
            consumers.add(consumer);
            Thread t = new Thread(() -> consume(name, consumer, cfg, events), name);
            t.start();
            threads.add(t);
        }
        return () -> {
            consumers.forEach(KafkaConsumer::wakeup);
            for (Thread t : threads) {
                t.join(40_000);
            }
        };
    }

    private void consume(String name, KafkaConsumer<byte[], byte[]> consumer, LoadTestConfig cfg,
                         ConsumerEvents events) {
        LoadTestConfig.ConsumerPart cc = cfg.consumer();
        Duration pollTimeout = Duration.ofNanos((long) (cc.pollTimeoutMs() * 1_000_000));
        try (consumer) {
            consumer.subscribe(List.of(cfg.topic()), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    events.assigned(name, partitions);
                }

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    events.revoked(name, partitions);
                }

                @Override
                public void onPartitionsLost(Collection<TopicPartition> partitions) {
                    events.lost(name, partitions);
                }
            });
            while (!events.stopped()) {
                try {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
                    events.records(records);
                } catch (WakeupException e) {
                    break;
                } catch (KafkaException e) {
                    events.error(name, e);
                }
                if (cc.pollIntervalMs() > 0) {
                    events.sleepUnlessStopped(cc.pollIntervalMs());
                }
            }
        } catch (WakeupException e) {
            // beim Schließen: ignorieren
        }
    }
}
