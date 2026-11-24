package loadtest.kafka.core;

/** Fehlerhafte Optionen oder Scenario-Angaben; führt zu Exit-Code 2. */
public class ConfigException extends RuntimeException {
    public ConfigException(String message) {
        super(message);
    }
}
