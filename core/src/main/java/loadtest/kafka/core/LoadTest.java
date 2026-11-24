package loadtest.kafka.core;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Ablauf eines Laufs; identisch für alle Java-Varianten und fachlich gleich zur Python-Variante. */
public final class LoadTest {

    private static final Logger LOG = LoggerFactory.getLogger("kafka-loadtest");

    private LoadTest() {
    }

    /** Einstieg für die main-Methoden: liefert den Exit-Code (0 ok, 1 Fehler im Lauf, 2 Konfiguration). */
    public static int main(String[] args, Engine engine) {
        try {
            Options options = Options.parse(args, System.getenv());
            if (options.flag("help")) {
                System.out.println(Options.USAGE);
                return 0;
            }
            LoadTestConfig cfg = LoadTestConfig.load(options);
            describe("Producer", cfg, cfg.producer() == null ? null : cfg.producer().settings(),
                    cfg.producer() == null ? 0 : cfg.producer().instances());
            describe("Consumer", cfg, cfg.consumer() == null ? null : cfg.consumer().settings(),
                    cfg.consumer() == null ? 0 : cfg.consumer().instances());
            return run(cfg, engine);
        } catch (ConfigException e) {
            LOG.error(e.getMessage());
            return 2;
        }
    }

    private static void describe(String role, LoadTestConfig cfg, Map<String, Object> settings, int instances) {
        if (settings == null) {
            return;
        }
        Map<String, Object> shown = new TreeMap<>(settings);
        shown.keySet().removeIf(k -> k.contains("password") || k.contains("secret") || k.contains("jaas")
                || k.endsWith("serializer"));
        LOG.info("{} x{} -> Topic '{}', Settings: {}", role, instances, cfg.topic(), shown);
    }

    public static int run(LoadTestConfig cfg, Engine engine) {
        Stats stats = new Stats(cfg.producer() != null ? cfg.count() : null);
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch finished = new CountDownLatch(1);
        long started = System.nanoTime();

        // docker stop / Strg+C: Lauf sauber beenden und auf den Abschlussbericht warten.
        AtomicBoolean shuttingDown = new AtomicBoolean();
        Thread hook = new Thread(() -> {
            LOG.info("Abbruch angefordert, beende Clients ...");
            shuttingDown.set(true);
            stop.set(true);
            try {
                finished.await(45, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "klt-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);

        AtomicBoolean reporterStop = new AtomicBoolean();
        Thread reporter = new Thread(() -> {
            long last = System.nanoTime();
            while (!reporterStop.get()) {
                ConsumerEvents.sleep(cfg.reportIntervalSeconds() * 1000, reporterStop);
                if (reporterStop.get()) {
                    break;
                }
                long now = System.nanoTime();
                LOG.info(reportLine(cfg, stats.interval(), (now - last) / 1e9));
                last = now;
            }
        }, "klt-reporter");
        reporter.setDaemon(true);

        AutoCloseable consumers = null;
        List<Thread> producers = new ArrayList<>();
        try {
            if (cfg.consumer() != null) {
                consumers = engine.startConsumers(cfg, new ConsumerEvents(cfg, stats, stop));
                if (cfg.producer() != null) {
                    // Erst senden, wenn alle Consumer Partitionen haben und die Gruppe 3 s ruhig ist;
                    // sonst überspringt auto.offset.reset=latest die ersten Nachrichten.
                    LOG.info("Warte auf Partitionszuweisung der Consumer ...");
                    while (!stats.groupStable(cfg.consumer().instances(), 3.0) && !stop.get()) {
                        if ((System.nanoTime() - started) / 1e9 > 90) {
                            throw new ConfigException("Consumer-Gruppe nach 90 s nicht stabil "
                                    + "(Broker erreichbar? Topic vorhanden?)");
                        }
                        ConsumerEvents.sleep(200, stop);
                    }
                }
            }

            long deadline = cfg.durationSeconds() == null ? Long.MAX_VALUE
                    : System.nanoTime() + (long) (cfg.durationSeconds() * 1e9);
            reporter.start();
            if (cfg.producer() != null && !stop.get()) {
                for (int i = 0; i < cfg.producer().instances(); i++) {
                    String name = "producer-" + i;
                    Thread t = new Thread(() -> produce(name, cfg, engine, stats, stop, deadline), name);
                    t.start();
                    producers.add(t);
                }
                for (Thread t : producers) {
                    t.join();
                }
                if (consumers != null && !stop.get()) {
                    // Consumer noch lesen lassen, bis alles Zugestellte angekommen ist.
                    long drainEnd = System.nanoTime() + (long) (cfg.drainTimeoutSeconds() * 1e9);
                    while (stats.totals().received() < stats.totals().acked() && System.nanoTime() < drainEnd
                            && !stop.get()) {
                        ConsumerEvents.sleep(200, stop);
                    }
                }
            } else {
                while (!stop.get() && System.nanoTime() < deadline) {
                    ConsumerEvents.sleep(200, stop);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            stop.set(true);
            for (Thread t : producers) {
                try {
                    t.join(40_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (consumers != null) {
                try {
                    consumers.close();
                } catch (Exception e) {
                    LOG.warn("Consumer ließen sich nicht sauber beenden: {}", e.toString());
                }
            }
            reporterStop.set(true);
        }

        stats.interval();
        Stats.Counters total = stats.totals();
        boolean direct = shuttingDown.get();
        emit(direct, String.format(Locale.ROOT, "=== Ergebnis nach %.1f s ===", (System.nanoTime() - started) / 1e9));
        if (cfg.producer() != null) {
            emit(direct, String.format(Locale.ROOT,
                    "Producer: gesendet %d, bestätigt %d, Fehler %d, %.2f MB | ack %s",
                    total.sent(), total.acked(), total.sendErrors(), total.sentBytes() / 1e6,
                    Stats.formatLatency(stats.allAck())));
        }
        if (cfg.consumer() != null) {
            emit(direct, String.format(Locale.ROOT,
                    "Consumer: empfangen %d, Fehler %d, Rebalances %d, %.2f MB | e2e %s",
                    total.received(), total.consumeErrors(), total.rebalances(), total.receivedBytes() / 1e6,
                    Stats.formatLatency(stats.allE2e())));
        }
        finished.countDown();
        try {
            Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException e) {
            // JVM fährt bereits herunter
        }
        boolean failed = total.sendErrors() > 0 || total.consumeErrors() > 0 || total.sent() != total.acked();
        return failed ? 1 : 0;
    }

    /**
     * Abschlussbericht ausgeben. Während die JVM herunterfährt, haben Spring und Quarkus ihr
     * Logging meist schon beendet; dann direkt auf die Konsole schreiben.
     */
    private static void emit(boolean direct, String line) {
        if (direct) {
            System.out.println(LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + " INFO  " + line);
            System.out.flush();
        } else {
            LOG.info(line);
        }
    }

    private static void produce(String name, LoadTestConfig cfg, Engine engine, Stats stats, AtomicBoolean stop,
                                long deadline) {
        LoadTestConfig.ProducerPart pc = cfg.producer();
        byte[] payload = pc.payload();
        Engine.Sender sender;
        try {
            sender = engine.newSender(name, cfg, stats);
        } catch (RuntimeException e) {
            LOG.error("[{}] Producer-Settings ungültig: {}", name, e.toString());
            stop.set(true);
            return;
        }
        try (sender) {
            while (!stop.get() && System.nanoTime() < deadline && stats.reserve()) {
                byte[] value = pc.randomSize()
                        ? Arrays.copyOf(payload, ThreadLocalRandom.current().nextInt(1, payload.length + 1))
                        : payload;
                try {
                    sender.send(value);
                    stats.sent();
                } catch (RuntimeException e) {
                    stats.sendError();
                    LOG.warn("[{}] send abgelehnt: {}", name, e.toString());
                }
                if (pc.thinkTimeMs() > 0) {
                    ConsumerEvents.sleep(pc.thinkTimeMs(), stop);
                }
            }
        }
    }

    static String reportLine(LoadTestConfig cfg, Stats.Interval i, double seconds) {
        List<String> parts = new ArrayList<>();
        Stats.Counters d = i.delta();
        if (cfg.producer() != null) {
            parts.add(String.format(Locale.ROOT, "PROD %8.1f msg/s %7.2f MB/s err %d | ack %s",
                    d.acked() / seconds, d.sentBytes() / seconds / 1e6, d.sendErrors(),
                    Stats.formatLatency(i.ack())));
        }
        if (cfg.consumer() != null) {
            parts.add(String.format(Locale.ROOT, "CONS %8.1f msg/s %7.2f MB/s err %d rebal %d | e2e %s",
                    d.received() / seconds, d.receivedBytes() / seconds / 1e6, d.consumeErrors(), d.rebalances(),
                    Stats.formatLatency(i.e2e())));
        }
        return String.join("  ||  ", parts);
    }
}
