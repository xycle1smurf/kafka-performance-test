package loadtest.kafka.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/** Zähler und Latenz-Stichproben eines Laufs; von allen Client-Threads gemeinsam genutzt. */
public final class Stats {

    private static final int MAX_SAMPLES = 200_000;

    /** Stichproben in Sekunden, auf MAX_SAMPLES begrenzt. */
    static final class Samples {
        private double[] values = new double[1024];
        private int size;

        synchronized void add(double seconds) {
            if (size >= MAX_SAMPLES) {
                return;
            }
            if (size == values.length) {
                values = Arrays.copyOf(values, Math.min(MAX_SAMPLES, size * 2));
            }
            values[size++] = seconds;
        }

        synchronized double[] drain() {
            double[] out = Arrays.copyOf(values, size);
            size = 0;
            return out;
        }

        synchronized void addAll(double[] more) {
            for (double v : more) {
                add(v);
            }
        }

        synchronized double[] snapshot() {
            return Arrays.copyOf(values, size);
        }
    }

    public record Counters(long sent, long acked, long sendErrors, long sentBytes, long received,
                           long receivedBytes, long consumeErrors, long rebalances) {
        Counters minus(Counters o) {
            return new Counters(sent - o.sent, acked - o.acked, sendErrors - o.sendErrors, sentBytes - o.sentBytes,
                    received - o.received, receivedBytes - o.receivedBytes, consumeErrors - o.consumeErrors,
                    rebalances - o.rebalances);
        }
    }

    public record Interval(Counters delta, double[] ack, double[] e2e) {
    }

    private final Long countLimit;
    private final AtomicLong reserved = new AtomicLong();
    private final LongAdder sent = new LongAdder();
    private final LongAdder acked = new LongAdder();
    private final LongAdder sendErrors = new LongAdder();
    private final LongAdder sentBytes = new LongAdder();
    private final LongAdder received = new LongAdder();
    private final LongAdder receivedBytes = new LongAdder();
    private final LongAdder consumeErrors = new LongAdder();
    private final LongAdder rebalances = new LongAdder();
    private final Samples ackInterval = new Samples();
    private final Samples e2eInterval = new Samples();
    private final Samples ackAll = new Samples();
    private final Samples e2eAll = new Samples();
    private final Set<String> assigned = new HashSet<>();
    private long lastGroupEventNanos = System.nanoTime();
    private Counters last = new Counters(0, 0, 0, 0, 0, 0, 0, 0);

    public Stats(Long countLimit) {
        this.countLimit = countLimit;
    }

    /** Reserviert die nächste zu sendende Nachricht; false, wenn count erreicht ist. */
    public boolean reserve() {
        if (countLimit == null) {
            return true;
        }
        if (reserved.incrementAndGet() > countLimit) {
            reserved.decrementAndGet();
            return false;
        }
        return true;
    }

    public void sent() {
        sent.increment();
    }

    /** Ergebnis eines Sendevorgangs; error == null bedeutet vom Broker bestätigt. */
    public void sendResult(int bytes, long startNanos, Throwable error) {
        if (error != null) {
            sendErrors.increment();
            return;
        }
        acked.increment();
        sentBytes.add(bytes);
        ackInterval.add((System.nanoTime() - startNanos) / 1e9);
    }

    public void sendError() {
        sendErrors.increment();
    }

    public void received(int bytes, long recordTimestampMs, long nowMs) {
        received.increment();
        receivedBytes.add(bytes);
        if (recordTimestampMs > 0) {
            e2eInterval.add(Math.max(0, nowMs - recordTimestampMs) / 1000.0);
        }
    }

    public void consumeError() {
        consumeErrors.increment();
    }

    public void rebalance() {
        rebalances.increment();
    }

    /** Merkt sich Zeitpunkt und Consumer einer Gruppenänderung; true bei dessen erster Zuweisung. */
    public synchronized boolean groupEvent(String assignedConsumer) {
        lastGroupEventNanos = System.nanoTime();
        return assignedConsumer != null && assigned.add(assignedConsumer);
    }

    public synchronized boolean groupStable(int instances, double quietSeconds) {
        return assigned.size() >= instances && (System.nanoTime() - lastGroupEventNanos) / 1e9 >= quietSeconds;
    }

    public Counters totals() {
        return new Counters(sent.sum(), acked.sum(), sendErrors.sum(), sentBytes.sum(), received.sum(),
                receivedBytes.sum(), consumeErrors.sum(), rebalances.sum());
    }

    public synchronized Interval interval() {
        Counters now = totals();
        Counters delta = now.minus(last);
        last = now;
        double[] ack = ackInterval.drain();
        double[] e2e = e2eInterval.drain();
        ackAll.addAll(ack);
        e2eAll.addAll(e2e);
        return new Interval(delta, ack, e2e);
    }

    public double[] allAck() {
        return ackAll.snapshot();
    }

    public double[] allE2e() {
        return e2eAll.snapshot();
    }

    public static String formatLatency(double[] samples) {
        if (samples.length == 0) {
            return "-";
        }
        double[] s = samples.clone();
        Arrays.sort(s);
        double sum = 0;
        for (double v : s) {
            sum += v;
        }
        return String.format(Locale.ROOT, "avg %.1f / p95 %.1f / p99 %.1f / max %.1f ms", sum / s.length * 1000,
                pick(s, 0.95), pick(s, 0.99), s[s.length - 1] * 1000);
    }

    private static double pick(double[] sorted, double q) {
        return sorted[Math.min(sorted.length - 1, (int) (sorted.length * q))] * 1000;
    }
}
