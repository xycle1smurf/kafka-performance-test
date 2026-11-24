package loadtest.kafka.core;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

/**
 * Fertig aufgelöste Konfiguration eines Laufs. Entsteht aus Einzeloptionen
 * und/oder einem Scenario-File; Optionen haben Vorrang vor dem Scenario.
 */
public record LoadTestConfig(String topic, Double durationSeconds, Long count, double reportIntervalSeconds,
                             double drainTimeoutSeconds, boolean verbose, ProducerPart producer,
                             ConsumerPart consumer) {

    public record ProducerPart(Map<String, Object> settings, int instances, double thinkTimeMs, byte[] payload,
                               boolean randomSize, List<Header> headers) {
    }

    public record ConsumerPart(Map<String, Object> settings, int instances, double pollTimeoutMs,
                               double pollIntervalMs, double processTimeMs) {
    }

    private static final Logger LOG = LoggerFactory.getLogger("kafka-loadtest");
    private static final List<String> SOURCE_KEYS = List.of("filename", "content", "content-base64");
    private static final String BYTES_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
    private static final String BYTES_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    /** Settings, die nur librdkafka (Python-Variante) kennt und die der Java-Client nicht braucht. */
    private static final Set<String> LIBRDKAFKA_ONLY = Set.of("batch.num.messages", "queue.buffering.max.messages",
            "queue.buffering.max.kbytes", "queued.max.messages.kbytes", "queued.min.messages",
            "socket.keepalive.enable");
    private static final Map<String, String> ASSIGNORS = Map.of(
            "range", "org.apache.kafka.clients.consumer.RangeAssignor",
            "roundrobin", "org.apache.kafka.clients.consumer.RoundRobinAssignor",
            "sticky", "org.apache.kafka.clients.consumer.StickyAssignor",
            "cooperative-sticky", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

    public ProducerRecord<byte[], byte[]> record(byte[] value) {
        return new ProducerRecord<>(topic, null, null, null, value, producer.headers());
    }

    // ---------------------------------------------------------------- Laden

    public static LoadTestConfig load(Options o) {
        Path cwd = Path.of("").toAbsolutePath();
        Path baseDir = cwd;
        Map<String, Object> scenario = Map.of();
        Map<String, Object> producer = null;
        Map<String, Object> consumer = null;

        String scenarioFile = o.get("scenario");
        if (scenarioFile != null) {
            Path path = cwd.resolve(scenarioFile);
            if (!Files.isRegularFile(path)) {
                throw new ConfigException("Scenario-File nicht gefunden: " + scenarioFile);
            }
            baseDir = path.getParent();
            scenario = parseYamlMapping(readFile(path, "scenario"), "scenario");
            producer = section(scenario, "producer");
            consumer = section(scenario, "consumer");
        }

        // Einzeloptionen: Dateien relativ zum Arbeitsverzeichnis.
        if (o.get("producer-settings") != null) {
            producer = producer == null ? new LinkedHashMap<>() : producer;
            producer.put("settings", fileSource(cwd, o.get("producer-settings")));
        }
        if (o.get("consumer-settings") != null) {
            consumer = consumer == null ? new LinkedHashMap<>() : consumer;
            consumer.put("settings", fileSource(cwd, o.get("consumer-settings")));
        }
        if (producer == null && consumer == null) {
            throw new ConfigException("Weder Producer noch Consumer konfiguriert: --producer-settings, "
                    + "--consumer-settings oder --scenario angeben");
        }
        if (producer != null) {
            if (o.get("message-file") != null) {
                producer.put("message-file", fileSource(cwd, o.get("message-file")));
            }
            if (o.get("headers") != null) {
                producer.put("headers", fileSource(cwd, o.get("headers")));
            }
            for (String name : List.of("message-size", "random-message-size", "think-time-ms")) {
                if (o.get(name) != null) {
                    producer.put(name, o.get(name));
                }
            }
            if (o.get("producers") != null) {
                producer.put("instances", o.get("producers"));
            }
        }
        if (consumer != null) {
            for (String name : List.of("poll-timeout-ms", "poll-interval-ms", "process-time-ms")) {
                if (o.get(name) != null) {
                    consumer.put(name, o.get(name));
                }
            }
            if (o.get("consumers") != null) {
                consumer.put("instances", o.get("consumers"));
            }
        }

        String topic = top(o, scenario, "topic");
        if (topic == null) {
            throw new ConfigException("Kein Topic angegeben (--topic oder 'topic:' im Scenario)");
        }
        String bootstrap = top(o, scenario, "bootstrap-servers");
        String duration = top(o, scenario, "duration");
        String count = top(o, scenario, "count");
        // 0 heißt unbegrenzt (Daemon-Betrieb, übersteuert Werte aus dem Scenario)
        Double durationSeconds = duration == null || parseDuration(duration) == 0 ? null : parseDuration(duration);
        Long countLimit = count == null || number(count, "count") == 0 ? null : (long) number(count, "count");

        ProducerPart producerPart = null;
        if (producer != null) {
            Map<String, Object> settings = clientSettings(producer, baseDir, bootstrap, true);
            settings.put("key.serializer", BYTES_SERIALIZER);
            settings.put("value.serializer", BYTES_SERIALIZER);
            byte[] base = null;
            if (producer.get("message-file") != null) {
                base = readSource(producer.get("message-file"), baseDir, "producer.message-file");
                if (base.length == 0) {
                    throw new ConfigException("producer.message-file ist leer");
                }
            }
            Integer size = producer.get("message-size") == null ? null : parseSize(producer.get("message-size"));
            List<Header> headers = new ArrayList<>();
            readMapping(producer.get("headers"), baseDir, "producer.headers").forEach((k, v) ->
                    headers.add(new RecordHeader(k, String.valueOf(v).getBytes(StandardCharsets.UTF_8))));
            producerPart = new ProducerPart(settings,
                    Math.max(1, (int) number(producer.getOrDefault("instances", 1), "producer.instances")),
                    number(producer.getOrDefault("think-time-ms", 0), "think-time-ms"),
                    payloadBuffer(base, size),
                    bool(producer.get("random-message-size")),
                    List.copyOf(headers));
        }

        ConsumerPart consumerPart = null;
        if (consumer != null) {
            Map<String, Object> settings = clientSettings(consumer, baseDir, bootstrap, false);
            settings.put("key.deserializer", BYTES_DESERIALIZER);
            settings.put("value.deserializer", BYTES_DESERIALIZER);
            if (o.get("group-id") != null) {
                settings.put("group.id", o.get("group-id"));
            }
            settings.putIfAbsent("group.id", "kafka-loadtest");
            settings.putIfAbsent("auto.offset.reset", "latest");
            consumerPart = new ConsumerPart(settings,
                    Math.max(1, (int) number(consumer.getOrDefault("instances", 1), "consumer.instances")),
                    number(consumer.getOrDefault("poll-timeout-ms", 1000), "poll-timeout-ms"),
                    number(consumer.getOrDefault("poll-interval-ms", 0), "poll-interval-ms"),
                    number(consumer.getOrDefault("process-time-ms", 0), "process-time-ms"));
        }

        String report = top(o, scenario, "report-interval");
        String drain = top(o, scenario, "drain-timeout");
        return new LoadTestConfig(topic, durationSeconds, countLimit,
                report == null ? 5 : number(report, "report-interval"),
                drain == null ? 10 : number(drain, "drain-timeout"),
                o.flag("verbose") || bool(scenario.get("verbose")), producerPart, consumerPart);
    }

    /** Top-Level-Wert: Option/Umgebung vor Scenario. */
    private static String top(Options o, Map<String, Object> scenario, String name) {
        String value = o.get(name);
        if (value == null && scenario.get(name) != null) {
            value = String.valueOf(scenario.get(name));
        }
        return value;
    }

    private static Map<String, Object> section(Map<String, Object> scenario, String name) {
        Object value = scenario.get(name);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw new ConfigException("Scenario: '" + name + "' muss ein Mapping sein");
        }
        return stringKeys(map);
    }

    private static Map<String, Object> fileSource(Path cwd, String file) {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("filename", cwd.resolve(file).toString());
        return source;
    }

    private static Map<String, Object> clientSettings(Map<String, Object> section, Path baseDir, String bootstrap,
                                                      boolean producer) {
        String role = producer ? "producer" : "consumer";
        Map<String, Object> settings = translate(readMapping(section.get("settings"), baseDir, role + ".settings"),
                producer);
        if (bootstrap != null) {
            settings.put("bootstrap.servers", bootstrap);
        }
        if (!settings.containsKey("bootstrap.servers")) {
            throw new ConfigException(role + ": bootstrap.servers fehlt (Settings oder --bootstrap-servers)");
        }
        return settings;
    }

    /**
     * Übersetzt librdkafka-Namen in die des Java-Clients, damit dieselben Scenario-Files
     * mit der Python-Variante und den Java-Varianten laufen. Werte werden zu Strings,
     * weil der Java-Client z. B. "acks: 1" als Integer ablehnt.
     */
    static Map<String, Object> translate(Map<String, Object> in, boolean producer) {
        Map<String, Object> out = new LinkedHashMap<>();
        in.forEach((key, raw) -> {
            String value = String.valueOf(raw);
            if (LIBRDKAFKA_ONLY.contains(key) || key.endsWith(".serializer") || key.endsWith(".deserializer")) {
                LOG.info("Setting '{}' wird vom Java-Client nicht verwendet und ignoriert", key);
            } else if (key.equals("fetch.wait.max.ms")) {
                out.put("fetch.max.wait.ms", value);
            } else if (key.equals("message.max.bytes")) {
                if (producer) {
                    out.put("max.request.size", value);
                }
            } else if (key.equals("partition.assignment.strategy")) {
                List<String> classes = new ArrayList<>();
                for (String name : value.split(",")) {
                    classes.add(ASSIGNORS.getOrDefault(name.trim().toLowerCase(Locale.ROOT), name.trim()));
                }
                out.put(key, String.join(",", classes));
            } else {
                out.put(key, value);
            }
        });
        return out;
    }

    // ------------------------------------------------------ Quellen und Werte

    /** Dateiquelle: Dateiname oder {filename | content | content-base64}. */
    static byte[] readSource(Object spec, Path baseDir, String what) {
        Map<String, Object> source;
        if (spec instanceof Map<?, ?> map) {
            source = stringKeys(map);
        } else {
            source = Map.of("filename", String.valueOf(spec));
        }
        List<String> given = SOURCE_KEYS.stream().filter(k -> source.get(k) != null).toList();
        if (given.size() != 1) {
            throw new ConfigException(what + ": genau eines von " + String.join(", ", SOURCE_KEYS) + " angeben");
        }
        String value = String.valueOf(source.get(given.get(0)));
        return switch (given.get(0)) {
            case "filename" -> readFile(baseDir.resolve(value), what);
            case "content" -> value.getBytes(StandardCharsets.UTF_8);
            default -> {
                try {
                    yield Base64.getDecoder().decode(value.replaceAll("\\s", ""));
                } catch (IllegalArgumentException e) {
                    throw new ConfigException(what + ": ungültiges Base64 (" + e.getMessage() + ")");
                }
            }
        };
    }

    /** Wie readSource, Inhalt ist YAML mit Key/Value. Ein Mapping ohne Quellen-Schlüssel gilt direkt als Inhalt. */
    static Map<String, Object> readMapping(Object spec, Path baseDir, String what) {
        if (spec == null) {
            return new LinkedHashMap<>();
        }
        if (spec instanceof Map<?, ?> map && SOURCE_KEYS.stream().noneMatch(map::containsKey)) {
            return stringKeys(map);
        }
        return parseYamlMapping(readSource(spec, baseDir, what), what);
    }

    private static Map<String, Object> parseYamlMapping(byte[] content, String what) {
        Object data;
        try {
            data = new Yaml(new SafeConstructor(new LoaderOptions())).load(new String(content, StandardCharsets.UTF_8));
        } catch (YAMLException e) {
            throw new ConfigException(what + ": kein gültiges YAML (" + e.getMessage() + ")");
        }
        if (data == null) {
            return new LinkedHashMap<>();
        }
        if (!(data instanceof Map<?, ?> map)) {
            throw new ConfigException(what + ": YAML muss ein Key/Value-Mapping sein");
        }
        return stringKeys(map);
    }

    private static Map<String, Object> stringKeys(Map<?, ?> map) {
        Map<String, Object> out = new LinkedHashMap<>();
        map.forEach((k, v) -> out.put(String.valueOf(k), v));
        return out;
    }

    private static byte[] readFile(Path path, String what) {
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new ConfigException(what + ": Datei nicht lesbar: " + path);
        }
    }

    static int parseSize(Object value) {
        Matcher m = Pattern.compile("\\s*(\\d+)\\s*([kKmM]?)[bB]?\\s*").matcher(String.valueOf(value));
        if (!m.matches() || Long.parseLong(m.group(1)) < 1) {
            throw new ConfigException("Ungültige Größe '" + value + "' (erwartet z. B. 512, 4K, 1M)");
        }
        long factor = switch (m.group(2).toLowerCase(Locale.ROOT)) {
            case "k" -> 1024;
            case "m" -> 1024 * 1024;
            default -> 1;
        };
        return Math.toIntExact(Long.parseLong(m.group(1)) * factor);
    }

    static double parseDuration(Object value) {
        Matcher m = Pattern.compile("\\s*(\\d+(?:\\.\\d+)?)\\s*(ms|s|m|h)?\\s*").matcher(String.valueOf(value));
        if (!m.matches()) {
            throw new ConfigException("Ungültige Dauer '" + value + "' (erwartet z. B. 90, 90s, 5m, 1h)");
        }
        double factor = switch (m.group(2) == null ? "s" : m.group(2)) {
            case "ms" -> 0.001;
            case "m" -> 60;
            case "h" -> 3600;
            default -> 1;
        };
        return Double.parseDouble(m.group(1)) * factor;
    }

    private static double number(Object value, String what) {
        try {
            return Double.parseDouble(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            throw new ConfigException(what + ": '" + value + "' ist keine Zahl");
        }
    }

    private static boolean bool(Object value) {
        return value != null && !String.valueOf(value).equalsIgnoreCase("false");
    }

    /**
     * Puffer in Maximalgröße; jede Nachricht ist ein Präfix davon.
     * Ohne Datei: zufällige ASCII-Zeichen (schlecht komprimierbar, also Worst Case).
     * Mit Datei: Dateiinhalt, bei message-size wiederholt bzw. abgeschnitten.
     */
    static byte[] payloadBuffer(byte[] base, Integer size) {
        if (base == null) {
            int n = size == null ? 1024 : size;
            byte[] random = new byte[n * 3 / 4 + 3];
            new SecureRandom().nextBytes(random);
            byte[] out = new byte[n];
            System.arraycopy(Base64.getEncoder().encode(random), 0, out, 0, n);
            return out;
        }
        if (size == null) {
            return base;
        }
        byte[] out = new byte[size];
        for (int i = 0; i < size; i++) {
            out[i] = base[i % base.length];
        }
        return out;
    }
}
