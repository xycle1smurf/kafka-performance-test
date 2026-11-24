package loadtest.kafka.core;

/**
 * Technologie-spezifischer Teil: wie Nachrichten gesendet und Consumer betrieben werden
 * (reine Kafka-Clients bei Quarkus, KafkaTemplate und Listener-Container bei Spring).
 */
public interface Engine {

    /** Ein eigener Producer je Producer-Thread. */
    Sender newSender(String name, LoadTestConfig cfg, Stats stats);

    /** Startet alle Consumer-Instanzen; close() beendet sie und wartet auf ihr Ende. */
    AutoCloseable startConsumers(LoadTestConfig cfg, ConsumerEvents events);

    interface Sender extends AutoCloseable {
        /** Sendet asynchron; das Ergebnis meldet die Engine über Stats.sendResult. */
        void send(byte[] value);

        /** Wartet auf ausstehende Bestätigungen und schließt den Producer. */
        @Override
        void close();
    }
}
