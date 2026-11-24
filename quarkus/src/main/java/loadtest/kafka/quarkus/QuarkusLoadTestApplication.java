package loadtest.kafka.quarkus;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import loadtest.kafka.core.LoadTest;
import loadtest.kafka.core.PlainClientEngine;

/**
 * Kafka-Lasttest mit Quarkus auf Basis der reinen Kafka-Clients
 * (quarkus-kafka-client), dadurch als Native Image lauffähig.
 */
@QuarkusMain
public class QuarkusLoadTestApplication implements QuarkusApplication {

    public static void main(String... args) {
        Quarkus.run(QuarkusLoadTestApplication.class, args);
    }

    @Override
    public int run(String... args) {
        return LoadTest.main(args, new PlainClientEngine());
    }
}
