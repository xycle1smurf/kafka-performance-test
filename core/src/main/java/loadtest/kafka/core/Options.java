package loadtest.kafka.core;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Kommandozeilenoptionen ("--name wert" oder "--name=wert") mit Rückfall auf
 * Umgebungsvariablen KLT_NAME (z. B. KLT_BOOTSTRAP_SERVERS).
 */
public final class Options {

    public static final String USAGE = """
            Kafka-Lasttest mit Producer und/oder Consumer.

            Scenario:
              --scenario YAML            Scenario-File; ersetzt die Einzeloptionen (diese übersteuern es aber)
            Nachricht (Producer):
              --message-file DATEI       Datei mit dem Nachrichteninhalt
              --message-size N|NK        Größe in Bytes oder mit Suffix K/M (Default 1K bzw. Dateigröße)
              --random-message-size      Größe je Nachricht zufällig zwischen 1 Byte und --message-size
              --think-time-ms MS         Wartezeit zwischen zwei Nachrichten (Default 0)
              --headers YAML             YAML-Datei mit Key/Value-Headern
            Clients:
              --producer-settings YAML   YAML-Datei mit Kafka-Producer-Settings
              --consumer-settings YAML   YAML-Datei mit Kafka-Consumer-Settings
              --bootstrap-servers H:P    übersteuert bootstrap.servers in den Settings
              --topic TOPIC              Topic für Producer und Consumer
              --group-id ID              übersteuert group.id in den Consumer-Settings
              --producers N              Anzahl Producer-Threads (Default 1)
              --consumers N              Anzahl Consumer-Threads (Default 1)
            Consumer-Verhalten:
              --poll-timeout-ms MS       maximale Blockierzeit eines poll (Default 1000)
              --poll-interval-ms MS      Pause zwischen zwei polls (Default 0)
              --process-time-ms MS       simulierte Verarbeitungszeit je Nachricht (Default 0)
            Ablauf:
              --duration ZEIT            Laufzeit, z. B. 90, 90s, 5m, 1h; 0 = unbegrenzt
              --count N                  Anzahl Nachrichten, danach Ende; 0 = unbegrenzt
              --report-interval S        Sekunden zwischen Statuszeilen (Default 5)
              --verbose                  Debug-Logging

            Ohne --scenario startet --producer-settings einen Producer und --consumer-settings
            einen Consumer; mindestens eines ist nötig. Jede Option lässt sich auch als
            Umgebungsvariable KLT_<NAME> setzen (z. B. KLT_SCENARIO, KLT_BOOTSTRAP_SERVERS).
            """;

    private static final Set<String> FLAGS = Set.of("random-message-size", "verbose", "help");
    private static final List<String> NAMES = List.of(
            "scenario", "message-file", "message-size", "random-message-size", "think-time-ms", "headers",
            "producer-settings", "consumer-settings", "bootstrap-servers", "topic", "group-id", "producers",
            "consumers", "poll-timeout-ms", "poll-interval-ms", "process-time-ms", "duration", "count",
            "report-interval", "verbose", "help");

    private final Map<String, String> cli = new HashMap<>();
    private final Map<String, String> env;

    private Options(Map<String, String> env) {
        this.env = env;
    }

    public static Options parse(String[] args, Map<String, String> env) {
        Options o = new Options(env);
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-h")) {
                arg = "--help";
            }
            if (!arg.startsWith("--")) {
                throw new ConfigException("Unerwartetes Argument '" + arg + "' (siehe --help)");
            }
            String name = arg.substring(2);
            String value = null;
            int eq = name.indexOf('=');
            if (eq >= 0) {
                value = name.substring(eq + 1);
                name = name.substring(0, eq);
            }
            if (!NAMES.contains(name)) {
                throw new ConfigException("Unbekannte Option '--" + name + "' (siehe --help)");
            }
            if (FLAGS.contains(name)) {
                value = value == null ? "true" : value;
            } else if (value == null) {
                if (++i >= args.length) {
                    throw new ConfigException("Option '--" + name + "' erwartet einen Wert");
                }
                value = args[i];
            }
            o.cli.put(name, value);
        }
        return o;
    }

    /** Wert von der Kommandozeile, sonst aus KLT_NAME, sonst null. */
    public String get(String name) {
        String value = cli.get(name);
        if (value == null) {
            value = env.get("KLT_" + name.replace('-', '_').toUpperCase(Locale.ROOT));
        }
        return value == null || value.isBlank() ? null : value;
    }

    public boolean flag(String name) {
        String value = get(name);
        return value != null && !value.equalsIgnoreCase("false");
    }
}
