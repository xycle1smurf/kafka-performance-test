#!/usr/bin/env python3
"""Kafka-Lasttest: Producer und/oder Consumer mit frei wählbaren Client-Settings.

Aufruf entweder mit Einzeloptionen (--producer-settings, --consumer-settings, ...)
oder mit --scenario <yaml>, das alle Optionen in einer Datei bündelt.
Kommandozeilenoptionen haben Vorrang vor Umgebungsvariablen (KLT_TOPIC,
KLT_BOOTSTRAP_SERVERS, ...) und diese vor dem Scenario-File.

Client ist confluent-kafka (librdkafka). Die Settings heißen dort fast wie im
Java-Client; die wichtigsten Abweichungen werden automatisch übersetzt
(siehe translate_settings).
"""
import argparse
import base64
import logging
import os
import random
import re
import signal
import sys
import threading
import time
from pathlib import Path

import yaml
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from vyper import Vyper

log = logging.getLogger("kafka-loadtest")

SOURCE_KEYS = ("filename", "content", "content-base64")
MAX_LATENCY_SAMPLES = 200_000


class ConfigError(Exception):
    pass


# --------------------------------------------------------------------------
# Konfiguration
# --------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        description="Kafka-Lasttest mit Producer und/oder Consumer.",
        epilog="Ohne --scenario startet --producer-settings einen Producer und "
               "--consumer-settings einen Consumer; mindestens eines ist nötig.",
    )
    g = p.add_argument_group("Scenario")
    g.add_argument("--scenario", metavar="YAML",
                   help="Scenario-File; ersetzt die Einzeloptionen (diese übersteuern es aber)")

    g = p.add_argument_group("Nachricht (Producer)")
    g.add_argument("--message-file", metavar="DATEI", help="Datei mit dem Nachrichteninhalt")
    g.add_argument("--message-size", metavar="N|NK",
                   help="Nachrichtengröße in Bytes oder mit Suffix K/M (Default 1K bzw. Dateigröße)")
    g.add_argument("--random-message-size", action="store_true", default=None,
                   help="Größe je Nachricht zufällig zwischen 1 Byte und --message-size")
    g.add_argument("--think-time-ms", type=float, metavar="MS",
                   help="Wartezeit zwischen zwei Nachrichten (Default 0)")
    g.add_argument("--headers", metavar="YAML", help="YAML-Datei mit Key/Value-Headern")

    g = p.add_argument_group("Clients")
    g.add_argument("--producer-settings", metavar="YAML", help="YAML-Datei mit Kafka-Producer-Settings")
    g.add_argument("--consumer-settings", metavar="YAML", help="YAML-Datei mit Kafka-Consumer-Settings")
    g.add_argument("--bootstrap-servers", metavar="HOST:PORT",
                   help="übersteuert bootstrap.servers in den Settings")
    g.add_argument("--topic", help="Topic für Producer und Consumer")
    g.add_argument("--group-id", metavar="ID", help="übersteuert group.id in den Consumer-Settings")
    g.add_argument("--producers", type=int, metavar="N", help="Anzahl Producer-Threads (Default 1)")
    g.add_argument("--consumers", type=int, metavar="N", help="Anzahl Consumer-Threads (Default 1)")

    g = p.add_argument_group("Consumer-Verhalten")
    g.add_argument("--poll-timeout-ms", type=float, metavar="MS",
                   help="maximale Blockierzeit eines poll (Default 1000)")
    g.add_argument("--poll-interval-ms", type=float, metavar="MS",
                   help="Pause zwischen zwei polls (Default 0)")
    g.add_argument("--process-time-ms", type=float, metavar="MS",
                   help="simulierte Verarbeitungszeit je Nachricht (Default 0)")

    g = p.add_argument_group("Ablauf")
    g.add_argument("--duration", metavar="ZEIT",
                   help="Laufzeit, z. B. 90, 90s, 5m, 1h; 0 = unbegrenzt (Default: bis Strg+C)")
    g.add_argument("--count", type=int, metavar="N", help="Anzahl Nachrichten, danach Ende; 0 = unbegrenzt")
    g.add_argument("--report-interval", type=float, metavar="S", help="Sekunden zwischen Statuszeilen (Default 5)")
    g.add_argument("--verbose", action="store_true", default=None, help="Debug-Logging")
    return p


def parse_size(value):
    m = re.fullmatch(r"\s*(\d+)\s*([kKmM]?)[bB]?\s*", str(value))
    if not m or int(m.group(1)) < 1:
        raise ConfigError(f"Ungültige Größe '{value}' (erwartet z. B. 512, 4K, 1M)")
    return int(m.group(1)) * {"": 1, "k": 1024, "m": 1024 * 1024}[m.group(2).lower()]


def parse_duration(value):
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h)?\s*", str(value))
    if not m:
        raise ConfigError(f"Ungültige Dauer '{value}' (erwartet z. B. 90, 90s, 5m, 1h)")
    return float(m.group(1)) * {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[m.group(2) or "s"]


def read_source(spec, base_dir, what):
    """Liest eine Dateiquelle: Dateiname oder {filename|content|content-base64}."""
    if isinstance(spec, str):
        spec = {"filename": spec}
    given = [k for k in SOURCE_KEYS if spec.get(k) is not None]
    if len(given) != 1:
        raise ConfigError(f"{what}: genau eines von {', '.join(SOURCE_KEYS)} angeben")
    if given[0] == "filename":
        path = Path(spec["filename"])
        if not path.is_absolute():
            path = base_dir / path
        try:
            return path.read_bytes()
        except OSError as e:
            raise ConfigError(f"{what}: {e}") from e
    if given[0] == "content":
        return str(spec["content"]).encode("utf-8")
    try:
        return base64.b64decode("".join(str(spec["content-base64"]).split()), validate=True)
    except ValueError as e:
        raise ConfigError(f"{what}: ungültiges Base64 ({e})") from e


def read_mapping(spec, base_dir, what):
    """Wie read_source, Inhalt ist YAML mit Key/Value. Ein Mapping ohne
    filename/content/content-base64 gilt direkt als Inhalt."""
    if spec is None:
        return {}
    if isinstance(spec, dict) and not any(k in spec for k in SOURCE_KEYS):
        return dict(spec)
    try:
        data = yaml.safe_load(read_source(spec, base_dir, what)) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"{what}: kein gültiges YAML ({e})") from e
    if not isinstance(data, dict):
        raise ConfigError(f"{what}: YAML muss ein Key/Value-Mapping sein")
    return data


def translate_settings(settings, role):
    """Übersetzt Java-Client-Namen nach librdkafka. Liefert (settings, max_poll_records)."""
    out, max_poll_records = {}, None
    for key, value in settings.items():
        key = str(key)
        if key == "max.poll.records":
            max_poll_records = int(value)
        elif key == "fetch.max.wait.ms":
            out["fetch.wait.max.ms"] = value
        elif key == "partition.assignment.strategy" and "." in str(value):
            # org.apache.kafka.clients.consumer.CooperativeStickyAssignor -> cooperative-sticky
            names = []
            for cls in str(value).split(","):
                short = cls.strip().rsplit(".", 1)[-1].removesuffix("Assignor")
                names.append(re.sub(r"(?<!^)(?=[A-Z])", "-", short).lower().replace("round-robin", "roundrobin"))
            out[key] = ",".join(names)
        elif key.endswith((".serializer", ".deserializer")):
            log.warning("%s-Setting '%s' wird ignoriert (Payload sind Bytes)", role, key)
        else:
            out[key] = value
    return out, max_poll_records


def load_config(argv=None):
    v = Vyper()
    v.set_env_prefix("klt")
    v.automatic_env()
    parser = build_parser()
    if argv is None:
        v.bind_args(parser)
    else:
        v._bind_parser_values(parser, argv)

    def opt(name):
        """Top-Level-Wert: CLI/Env (name_mit_unterstrich) vor Scenario (name-mit-bindestrich)."""
        val = v.get(name.replace("-", "_"))
        return val if val is not None else v.get(name)

    cli = lambda name: v.get(name.replace("-", "_"))  # noqa: E731

    scenario = opt("scenario")
    base_dir = Path.cwd()
    producer = consumer = None
    if scenario:
        if not Path(scenario).is_file():
            raise ConfigError(f"Scenario-File nicht gefunden: {scenario}")
        v.set_config_file(str(scenario))
        v.set_config_type("yaml")
        v.read_in_config()
        base_dir = Path(scenario).resolve().parent
        producer, consumer = v.get("producer"), v.get("consumer")
        for name, section in (("producer", producer), ("consumer", consumer)):
            if section is not None and not isinstance(section, dict):
                raise ConfigError(f"Scenario: '{name}' muss ein Mapping sein")

    # Einzeloptionen: Dateien werden relativ zum Arbeitsverzeichnis gelesen.
    cwd = Path.cwd()
    absolute = lambda f: {"filename": str((cwd / f).resolve())}  # noqa: E731
    if cli("producer-settings"):
        producer = dict(producer or {}, settings=absolute(cli("producer-settings")))
    if cli("consumer-settings"):
        consumer = dict(consumer or {}, settings=absolute(cli("consumer-settings")))
    if producer is None and consumer is None:
        raise ConfigError("Weder Producer noch Consumer konfiguriert: --producer-settings, "
                          "--consumer-settings oder --scenario angeben")

    if producer is not None:
        producer = dict(producer)
        if cli("message-file"):
            producer["message-file"] = absolute(cli("message-file"))
        if cli("headers"):
            producer["headers"] = absolute(cli("headers"))
        for name in ("message-size", "random-message-size", "think-time-ms"):
            if cli(name) is not None:
                producer[name] = cli(name)
        if cli("producers") is not None:
            producer["instances"] = cli("producers")
    if consumer is not None:
        consumer = dict(consumer)
        for name in ("poll-timeout-ms", "poll-interval-ms", "process-time-ms"):
            if cli(name) is not None:
                consumer[name] = cli(name)
        if cli("consumers") is not None:
            consumer["instances"] = cli("consumers")

    topic = opt("topic")
    if not topic:
        raise ConfigError("Kein Topic angegeben (--topic oder 'topic:' im Scenario)")
    bootstrap = opt("bootstrap-servers")

    cfg = {
        "topic": str(topic),
        # 0 heißt unbegrenzt (Daemon-Betrieb, übersteuert Werte aus dem Scenario)
        "duration": (parse_duration(opt("duration")) or None) if opt("duration") is not None else None,
        "count": (int(opt("count")) or None) if opt("count") is not None else None,
        "report_interval": float(opt("report-interval") or 5),
        "drain_timeout": float(opt("drain-timeout") if opt("drain-timeout") is not None else 10),
        "verbose": bool(opt("verbose")),
        "producer": None,
        "consumer": None,
    }

    def client_settings(section, role):
        settings, mpr = translate_settings(read_mapping(section.get("settings"), base_dir, f"{role}.settings"), role)
        if bootstrap:
            settings["bootstrap.servers"] = str(bootstrap)
        if "bootstrap.servers" not in settings:
            raise ConfigError(f"{role}: bootstrap.servers fehlt (Settings oder --bootstrap-servers)")
        return settings, mpr

    if producer is not None:
        settings, _ = client_settings(producer, "producer")
        base = None
        if producer.get("message-file") is not None:
            base = read_source(producer["message-file"], base_dir, "producer.message-file")
            if not base:
                raise ConfigError("producer.message-file ist leer")
        size = parse_size(producer["message-size"]) if producer.get("message-size") is not None else None
        headers = read_mapping(producer.get("headers"), base_dir, "producer.headers")
        cfg["producer"] = {
            "settings": settings,
            "instances": max(1, int(producer.get("instances", 1))),
            "think_time": float(producer.get("think-time-ms", 0)) / 1000.0,
            "payload": make_payload_buffer(base, size),
            "random_size": bool(producer.get("random-message-size", False)),
            "headers": [(str(k), str(val).encode("utf-8")) for k, val in headers.items()],
        }

    if consumer is not None:
        settings, mpr = client_settings(consumer, "consumer")
        if opt("group-id"):
            settings["group.id"] = str(opt("group-id"))
        settings.setdefault("group.id", "kafka-loadtest")
        settings.setdefault("auto.offset.reset", "latest")
        cfg["consumer"] = {
            "settings": settings,
            "instances": max(1, int(consumer.get("instances", 1))),
            "poll_timeout": float(consumer.get("poll-timeout-ms", 1000)) / 1000.0,
            "poll_interval": float(consumer.get("poll-interval-ms", 0)) / 1000.0,
            "process_time": float(consumer.get("process-time-ms", 0)) / 1000.0,
            "batch": max(1, int(mpr or 500)),
        }
    return cfg


def make_payload_buffer(base, size):
    """Puffer in Maximalgröße; jede Nachricht ist ein Präfix davon.
    Ohne Datei: zufällige ASCII-Zeichen (schlecht komprimierbar, also Worst Case).
    Mit Datei: Dateiinhalt, bei --message-size wiederholt bzw. abgeschnitten."""
    if base is None:
        size = size or 1024
        return base64.b64encode(os.urandom(size * 3 // 4 + 3))[:size]
    if size is None:
        return base
    return (base * (size // len(base) + 1))[:size]


# --------------------------------------------------------------------------
# Statistik
# --------------------------------------------------------------------------

class Stats:
    def __init__(self, count_limit):
        self.lock = threading.Lock()
        self.count_limit = count_limit
        self.reserved = 0
        self.total = dict.fromkeys(
            ("sent", "acked", "send_errors", "sent_bytes", "received", "received_bytes",
             "consume_errors", "rebalances"), 0)
        self.last = dict(self.total)
        self.ack_lat, self.e2e_lat = [], []
        self.all_ack, self.all_e2e = [], []
        self.assigned, self.last_group_event = set(), time.monotonic()

    def reserve(self):
        """Reserviert die nächste zu sendende Nachricht; False, wenn --count erreicht ist."""
        with self.lock:
            if self.count_limit is not None and self.reserved >= self.count_limit:
                return False
            self.reserved += 1
            return True

    def group_event(self, assigned_consumer=None):
        """Merkt sich Zeitpunkt und Consumer einer Gruppenänderung; True bei dessen erster Zuweisung."""
        with self.lock:
            self.last_group_event = time.monotonic()
            if assigned_consumer is None or assigned_consumer in self.assigned:
                return False
            self.assigned.add(assigned_consumer)
            return True

    def group_stable(self, instances, quiet_seconds):
        with self.lock:
            return (len(self.assigned) >= instances
                    and time.monotonic() - self.last_group_event >= quiet_seconds)

    def add(self, **deltas):
        with self.lock:
            for key, delta in deltas.items():
                self.total[key] += delta

    def latency(self, kind, seconds):
        samples = self.ack_lat if kind == "ack" else self.e2e_lat
        with self.lock:
            if len(samples) < MAX_LATENCY_SAMPLES:
                samples.append(seconds)

    def get(self, key):
        with self.lock:
            return self.total[key]

    def interval(self):
        with self.lock:
            delta = {k: self.total[k] - self.last[k] for k in self.total}
            self.last = dict(self.total)
            ack, e2e = self.ack_lat, self.e2e_lat
            self.ack_lat, self.e2e_lat = [], []
            for target, src in ((self.all_ack, ack), (self.all_e2e, e2e)):
                if len(target) < MAX_LATENCY_SAMPLES:
                    target.extend(src)
        return delta, ack, e2e


def fmt_latency(samples):
    if not samples:
        return "-"
    s = sorted(samples)
    pick = lambda q: s[min(len(s) - 1, int(len(s) * q))] * 1000  # noqa: E731
    return f"avg {sum(s) / len(s) * 1000:.1f} / p95 {pick(0.95):.1f} / p99 {pick(0.99):.1f} / max {s[-1] * 1000:.1f} ms"


def report_line(cfg, delta, ack, e2e, seconds):
    parts = []
    if cfg["producer"]:
        parts.append(f"PROD {delta['acked'] / seconds:8.1f} msg/s {delta['sent_bytes'] / seconds / 1e6:7.2f} MB/s "
                     f"err {delta['send_errors']} | ack {fmt_latency(ack)}")
    if cfg["consumer"]:
        parts.append(f"CONS {delta['received'] / seconds:8.1f} msg/s {delta['received_bytes'] / seconds / 1e6:7.2f} MB/s "
                     f"err {delta['consume_errors']} rebal {delta['rebalances']} | e2e {fmt_latency(e2e)}")
    return "  ||  ".join(parts)


# --------------------------------------------------------------------------
# Producer / Consumer
# --------------------------------------------------------------------------

def kafka_error_cb(name):
    def on_error(err):
        if err.code() == KafkaError._TRANSPORT:
            return  # Verbindungsfehler loggt librdkafka bereits selbst als FAIL-Zeile
        log.warning("[%s] Client-Fehler: %s", name, err)
    return on_error


def run_producer(name, cfg, stats, stop, deadline):
    pc = cfg["producer"]
    topic, payload, headers = cfg["topic"], pc["payload"], pc["headers"] or None
    max_size, random_size, think = len(payload), pc["random_size"], pc["think_time"]
    try:
        producer = Producer(dict(pc["settings"], error_cb=kafka_error_cb(name)), logger=log)
    except KafkaException as e:
        log.error("[%s] Producer-Settings ungültig: %s", name, e)
        stop.set()
        return

    def on_delivery(err, msg):
        if err is not None:
            stats.add(send_errors=1)
            log.warning("[%s] Zustellung fehlgeschlagen: %s", name, err)
        else:
            stats.add(acked=1, sent_bytes=len(msg))
            lat = msg.latency()
            if lat is not None:
                stats.latency("ack", lat)

    try:
        while not stop.is_set() and (deadline is None or time.monotonic() < deadline):
            if not stats.reserve():
                break
            value = payload[:random.randint(1, max_size)] if random_size else payload
            while not stop.is_set():
                try:
                    producer.produce(topic, value=value, headers=headers, on_delivery=on_delivery)
                    stats.add(sent=1)
                    break
                except BufferError:
                    producer.poll(0.1)  # lokale Queue voll: auf Zustellungen warten
                except KafkaException as e:
                    stats.add(send_errors=1)
                    log.warning("[%s] produce abgelehnt: %s", name, e)
                    break
            producer.poll(0)
            if think > 0:
                stop.wait(think)
    finally:
        remaining = producer.flush(30)
        if remaining:
            log.warning("[%s] %d Nachrichten nach flush noch nicht zugestellt", name, remaining)


def run_consumer(name, cfg, stats, stop):
    cc = cfg["consumer"]
    try:
        consumer = Consumer(dict(cc["settings"], error_cb=kafka_error_cb(name)), logger=log)
    except KafkaException as e:
        log.error("[%s] Consumer-Settings ungültig: %s", name, e)
        stop.set()
        return

    def describe(partitions):
        return ",".join(str(p.partition) for p in partitions) or "-"

    def on_assign(_c, partitions):
        # Die erste Zuweisung ist der normale Start, erst jede weitere ein Rebalance.
        first = stats.group_event(name)
        if not first:
            stats.add(rebalances=1)
        log.info("[%s] %s assign  Partitionen %s", name, "START    " if first else "REBALANCE", describe(partitions))

    def on_revoke(_c, partitions):
        stats.group_event()
        log.info("[%s] REBALANCE revoke  Partitionen %s", name, describe(partitions))

    def on_lost(_c, partitions):
        stats.group_event()
        log.warning("[%s] REBALANCE lost    Partitionen %s (Session/Poll-Timeout)", name, describe(partitions))

    consumer.subscribe([cfg["topic"]], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
    count_limit = cfg["count"] if cfg["producer"] is None else None
    try:
        while not stop.is_set():
            try:
                messages = consumer.consume(cc["batch"], cc["poll_timeout"])
            except KafkaException as e:
                stats.add(consume_errors=1)
                log.warning("[%s] consume fehlgeschlagen: %s", name, e)
                continue
            now_ms = time.time() * 1000
            for msg in messages:
                err = msg.error()
                if err is not None:
                    if err.code() != KafkaError._PARTITION_EOF:
                        stats.add(consume_errors=1)
                        log.warning("[%s] Fehler beim Lesen: %s", name, err)
                    continue
                stats.add(received=1, received_bytes=len(msg))
                _ts_type, ts = msg.timestamp()
                if ts > 0:
                    stats.latency("e2e", max(0.0, (now_ms - ts) / 1000.0))
                if cc["process_time"] > 0:
                    stop.wait(cc["process_time"])
            if count_limit is not None and stats.get("received") >= count_limit:
                stop.set()
            if cc["poll_interval"] > 0:
                stop.wait(cc["poll_interval"])
    finally:
        consumer.close()


# --------------------------------------------------------------------------
# Ablauf
# --------------------------------------------------------------------------

CONNECTION_PREFIXES = ("bootstrap.", "security.", "sasl.", "ssl.", "socket.", "broker.", "client.")


def ensure_topic(cfg):
    """Legt das Topic mit den Broker-Defaults an, falls es fehlt. Der Java-Client löst das
    beim Abonnieren selbst aus, librdkafka nicht: ohne Topic bekäme der Consumer nie Partitionen."""
    settings = (cfg["consumer"] or cfg["producer"])["settings"]
    admin = AdminClient({k: v for k, v in settings.items() if k.startswith(CONNECTION_PREFIXES)}, logger=log)
    topic = cfg["topic"]
    try:
        meta = admin.list_topics(topic=topic, timeout=15).topics.get(topic)
        if meta is not None and meta.error is None:
            return
        admin.create_topics([NewTopic(topic, num_partitions=-1, replication_factor=-1)])[topic].result(30)
        log.info("Topic '%s' angelegt", topic)
    except KafkaException as e:
        if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            log.warning("Topic '%s' ließ sich nicht prüfen oder anlegen: %s", topic, e)

def run(cfg):
    stats = Stats(cfg["count"] if cfg["producer"] else None)
    stop, reporter_stop = threading.Event(), threading.Event()
    started = time.monotonic()

    def reporter():
        last = time.monotonic()
        while not reporter_stop.wait(cfg["report_interval"]):
            now = time.monotonic()
            delta, ack, e2e = stats.interval()
            log.info(report_line(cfg, delta, ack, e2e, now - last))
            last = now

    consumers, producers = [], []
    try:
        ensure_topic(cfg)
        if cfg["consumer"]:
            instances = cfg["consumer"]["instances"]
            for i in range(instances):
                t = threading.Thread(target=run_consumer, name=f"consumer-{i}",
                                     args=(f"consumer-{i}", cfg, stats, stop), daemon=True)
                t.start()
                consumers.append(t)
            if cfg["producer"]:
                # Erst senden, wenn alle Consumer Partitionen haben und die Gruppe 3 s ruhig ist;
                # sonst überspringt auto.offset.reset=latest die ersten Nachrichten.
                log.info("Warte auf Partitionszuweisung der Consumer ...")
                while not stats.group_stable(instances, 3.0) and not stop.is_set():
                    if time.monotonic() - started > 90:
                        raise ConfigError("Consumer-Gruppe nach 90 s nicht stabil "
                                          "(Broker erreichbar? Topic vorhanden?)")
                    time.sleep(0.2)

        deadline = time.monotonic() + cfg["duration"] if cfg["duration"] else None
        threading.Thread(target=reporter, daemon=True).start()
        if cfg["producer"] and not stop.is_set():
            for i in range(cfg["producer"]["instances"]):
                t = threading.Thread(target=run_producer, name=f"producer-{i}",
                                     args=(f"producer-{i}", cfg, stats, stop, deadline), daemon=True)
                t.start()
                producers.append(t)
            while any(t.is_alive() for t in producers):
                time.sleep(0.2)
            if consumers and not stop.is_set():
                # Consumer noch lesen lassen, bis alles Zugestellte angekommen ist.
                drain_end = time.monotonic() + cfg["drain_timeout"]
                while stats.get("received") < stats.get("acked") and time.monotonic() < drain_end:
                    time.sleep(0.2)
        else:
            while not stop.is_set() and (deadline is None or time.monotonic() < deadline):
                time.sleep(0.2)
    except KeyboardInterrupt:
        log.info("Abbruch durch Benutzer, beende Clients ...")
    finally:
        stop.set()
        for t in producers + consumers:
            t.join(40)
        reporter_stop.set()

    stats.interval()
    elapsed = time.monotonic() - started
    total = stats.total
    log.info("=== Ergebnis nach %.1f s ===", elapsed)
    if cfg["producer"]:
        log.info("Producer: gesendet %d, bestätigt %d, Fehler %d, %.2f MB | ack %s",
                 total["sent"], total["acked"], total["send_errors"], total["sent_bytes"] / 1e6,
                 fmt_latency(stats.all_ack))
    if cfg["consumer"]:
        log.info("Consumer: empfangen %d, Fehler %d, Rebalances %d, %.2f MB | e2e %s",
                 total["received"], total["consume_errors"], total["rebalances"],
                 total["received_bytes"] / 1e6, fmt_latency(stats.all_e2e))
    failed = total["send_errors"] or total["consume_errors"] or total["sent"] != total["acked"]
    return 1 if failed else 0


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")
    logging.getLogger("vyper").setLevel(logging.WARNING)

    def on_sigterm(_signum, _frame):  # docker stop: wie Strg+C sauber beenden
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, on_sigterm)
    try:
        cfg = load_config(argv)
        if cfg["verbose"]:
            log.setLevel(logging.DEBUG)
        for role in ("producer", "consumer"):
            if cfg[role]:
                log.info("%s x%d -> Topic '%s', Settings: %s", role.capitalize(), cfg[role]["instances"],
                         cfg["topic"], {k: v for k, v in cfg[role]["settings"].items()
                                        if "password" not in k and "secret" not in k})
        return run(cfg)
    except ConfigError as e:
        log.error("%s", e)
        return 2


if __name__ == "__main__":
    sys.exit(main())
