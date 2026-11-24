package loadtest.kafka.core;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Was die Engine aus ihren Consumer-Threads meldet: Nachrichten, Rebalances, Fehler. */
public final class ConsumerEvents {

    private static final Logger LOG = LoggerFactory.getLogger("kafka-loadtest");

    private final LoadTestConfig cfg;
    private final Stats stats;
    private final AtomicBoolean stop;
    private final Long countLimit;

    ConsumerEvents(LoadTestConfig cfg, Stats stats, AtomicBoolean stop) {
        this.cfg = cfg;
        this.stats = stats;
        this.stop = stop;
        // Nur ohne Producer begrenzt count die empfangenen Nachrichten.
        this.countLimit = cfg.producer() == null ? cfg.count() : null;
    }

    public boolean stopped() {
        return stop.get();
    }

    public void sleepUnlessStopped(double millis) {
        sleep(millis, stop);
    }

    /** Ein Batch aus einem poll; simuliert die Verarbeitungszeit je Nachricht. */
    public void records(Iterable<ConsumerRecord<byte[], byte[]>> records) {
        long nowMs = System.currentTimeMillis();
        double processMs = cfg.consumer().processTimeMs();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            int bytes = record.value() == null ? 0 : record.value().length;
            stats.received(bytes, record.timestamp(), nowMs);
            if (processMs > 0) {
                sleep(processMs, stop);
            }
        }
        if (countLimit != null && stats.totals().received() >= countLimit) {
            stop.set(true);
        }
    }

    public void assigned(String name, Collection<TopicPartition> partitions) {
        // Die erste Zuweisung ist der normale Start, erst jede weitere ein Rebalance.
        boolean first = stats.groupEvent(name);
        if (!first) {
            stats.rebalance();
        }
        LOG.info("[{}] {} assign  Partitionen {}", name, first ? "START    " : "REBALANCE", describe(partitions));
    }

    public void revoked(String name, Collection<TopicPartition> partitions) {
        stats.groupEvent(null);
        LOG.info("[{}] REBALANCE revoke  Partitionen {}", name, describe(partitions));
    }

    public void lost(String name, Collection<TopicPartition> partitions) {
        stats.groupEvent(null);
        LOG.warn("[{}] REBALANCE lost    Partitionen {} (Session/Poll-Timeout)", name, describe(partitions));
    }

    public void error(String name, Exception e) {
        stats.consumeError();
        LOG.warn("[{}] Fehler beim Lesen: {}", name, e.toString());
    }

    private static String describe(Collection<TopicPartition> partitions) {
        return partitions.isEmpty() ? "-"
                : partitions.stream().map(p -> String.valueOf(p.partition())).sorted().collect(Collectors.joining(","));
    }

    /** Schläft die angegebene Zeit, bricht aber ab, sobald der Lauf beendet wird. */
    public static void sleep(double millis, AtomicBoolean stop) {
        long end = System.nanoTime() + (long) (millis * 1_000_000);
        long remaining;
        while (!stop.get() && (remaining = end - System.nanoTime()) > 0) {
            LockSupport.parkNanos(Math.min(remaining, 100_000_000L));
        }
    }
}
