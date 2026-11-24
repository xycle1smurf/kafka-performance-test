# kafka-loadtest

Kafka-Lasttest mit Producer und/oder Consumer in drei fachlich identischen Varianten.
Alle drei verstehen dieselben Optionen und dieselben Scenario-Files.

| Verzeichnis | Technologie | Kafka-Client | Image |
|---|---|---|---|
| `python/` | Python, vyper | confluent-kafka (librdkafka) | `docker compose build python` |
| `spring-boot/` | Spring Boot, Spring Kafka | Java-Client über KafkaTemplate und Listener-Container | JIB |
| `quarkus/` | Quarkus | Java-Client direkt | Native Image, per JIB verpackt |
| `core/` | gemeinsamer Java-Code | Optionen, Scenario-Format, Statistik, Ablauf | – |

`scenarios/` enthält Beispiel-Szenarien, `settings/` die darin referenzierten Dateien.

## Optionen

Jede Option gibt es als `--name wert`, als Umgebungsvariable `KLT_NAME` und (außer den
Dateioptionen) als Schlüssel im Scenario. Vorrang: Kommandozeile, Umgebung, Scenario.

```
--scenario YAML            Scenario-File; ersetzt die Einzeloptionen
--message-file DATEI       --message-size N|NK        --random-message-size
--think-time-ms MS         --headers YAML
--producer-settings YAML   --consumer-settings YAML
--bootstrap-servers H:P    --topic TOPIC              --group-id ID
--producers N              --consumers N
--poll-timeout-ms MS       --poll-interval-ms MS      --process-time-ms MS
--duration ZEIT            --count N                  (0 = unbegrenzt)
--report-interval S        --verbose
```

Mit `--consumer-settings` läuft zusätzlich ein Consumer; ohne `--producer-settings` nur der Consumer.
Aufbau eines Scenarios: siehe `scenarios/01-baseline.yaml`.

Die Scenario-Files verwenden die Namen der Kafka-Settings, wie sie beide Clients kennen.
Abweichungen werden übersetzt: `max.poll.records` und `fetch.max.wait.ms` für Python,
`message.max.bytes` und kurze Assignor-Namen (`cooperative-sticky`) für Java.
Settings, die nur librdkafka kennt, ignorieren die Java-Varianten.

## Bauen

```
python -m venv .venv
.venv\Scripts\python -m pip install -r python\requirements.txt

mvn package                                   # alle Java-Module als Jar
mvn -pl spring-boot -am package -Pimage       # Image kafka-loadtest-spring (JIB, lokaler Docker-Daemon)
mvn -pl quarkus -am package -Pnative          # Native Image und Image kafka-loadtest-quarkus
mvn -pl quarkus -am package -Pjvm-image       # dasselbe als JVM-Image (Tag jvm), schneller Build
docker compose build python                   # Image kafka-loadtest-python
```

Der Native-Build läuft in einem Builder-Container; lokal ist kein GraalVM nötig, aber Docker
mit etwa 6 GB RAM. Er dauert einige Minuten.

## Direkt starten

```
.venv\Scripts\python python\kafka_loadtest.py --scenario scenarios\01-baseline.yaml --bootstrap-servers 127.0.0.1:19094
java -jar spring-boot\target\kafka-loadtest-spring-1.0.0.jar --scenario scenarios\01-baseline.yaml --bootstrap-servers 127.0.0.1:19094
java -jar quarkus\target\quarkus-app\quarkus-run.jar --scenario scenarios\01-baseline.yaml --bootstrap-servers 127.0.0.1:19094
```

`127.0.0.1:19094` ist der Zugang zum Compose-Cluster vom Docker-Host aus (siehe unten).

## Docker Compose

Profile: `kafka` (alle Broker), `broker-1`, `broker-2`, `broker-3`, `clients` (alle Varianten),
`python`, `spring`, `quarkus`, `all`.

```
docker compose --profile all up -d                         # alles auf einem Rechner
docker compose --profile kafka --profile spring up -d      # Cluster und nur die Spring-Variante
docker compose --profile all logs -f spring                # Statuszeilen verfolgen
docker compose --profile all down                          # stoppen (mit -v auch die Broker-Daten löschen)
```

Die Clients laufen als Daemons: Laufzeit und Anzahl sind auf unbegrenzt gesetzt, bei einem
Fehler startet der Container neu. Scenario, Topic und Gruppe je Variante stehen in `.env`
(Vorlage: `.env.example`). Standardmäßig hat jede Variante ihr eigenes Topic.

### Verteilt auf mehrere Rechner

1. `docker-compose.yaml`, `scenarios/`, `settings/` und `.env` auf jeden Rechner kopieren.
2. In `.env` auf allen Rechnern identisch `BROKER1_HOST`, `BROKER2_HOST`, `BROKER3_HOST` setzen:
   die Adresse des Rechners, auf dem der jeweilige Broker läuft.
3. Ports freigeben: je Broker n `n9092` (Clients, Inter-Broker) und `n9093` (Controller).
4. Je Rechner das passende Profil starten, zum Beispiel
   `docker compose --profile broker-2 up -d` oder `docker compose --profile quarkus up -d`.

Der Cluster wird erst arbeitsfähig, wenn zwei der drei Broker laufen (Quorum).
Die Images der Java-Varianten müssen auf dem Zielrechner vorhanden sein: entweder über eine
Registry (`KLT_SPRING_IMAGE`, `KLT_QUARKUS_IMAGE`) oder mit `docker save` und `docker load`.

Die Listener sind unverschlüsselt und ohne Anmeldung, also nur für Testnetze geeignet.
