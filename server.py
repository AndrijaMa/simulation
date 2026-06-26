import json
import os
import pathlib
import threading
import time
from flask import Flask, request, jsonify, send_from_directory

BASE = pathlib.Path(__file__).parent
DATA_DIR = pathlib.Path(os.environ.get("TELEMETRY_DIR", str(BASE / "telemetry")))
DATA_DIR.mkdir(parents=True, exist_ok=True)
SETTINGS_FILE = DATA_DIR / "app_settings.json"

RETRY_BACKOFF_S = 15.0

# ---- settings: seeded from env, overlaid by a persisted JSON file in the volume ----
def _envbool(name, default="1"):
    return os.environ.get(name, default) not in ("0", "false", "False", "")

DEFAULT_SETTINGS = {
    "enabled": _envbool("KAFKA_ENABLED", "1"),
    "broker": os.environ.get("KAFKA_BROKER", "orvill.ddns.net:9092"),
    "topic": os.environ.get("KAFKA_TOPIC", "connectedCar1"),
    "security_protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
    "sasl_mechanism": os.environ.get("KAFKA_SASL_MECHANISM", "PLAIN"),
    "sasl_username": os.environ.get("KAFKA_SASL_USERNAME", "") or "",
    "sasl_password": os.environ.get("KAFKA_SASL_PASSWORD", "") or "",
    # SSL/mTLS material (optional)
    "ssl_cafile": os.environ.get("KAFKA_SSL_CAFILE", "") or "",
    "ssl_certfile": os.environ.get("KAFKA_SSL_CERTFILE", "") or "",
    "ssl_keyfile": os.environ.get("KAFKA_SSL_KEYFILE", "") or "",
    "ssl_password": os.environ.get("KAFKA_SSL_PASSWORD", "") or "",
    # how often the dashboard sends a datapoint per vehicle (seconds)
    "emit_interval_s": int(os.environ.get("EMIT_INTERVAL_S", "5") or 5),
}

SETTINGS = dict(DEFAULT_SETTINGS)
_settings_lock = threading.Lock()


def load_settings():
    """Overlay persisted settings (if any) on top of env defaults."""
    if SETTINGS_FILE.exists():
        try:
            saved = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
            for k in DEFAULT_SETTINGS:
                if k in saved and saved[k] is not None:
                    SETTINGS[k] = saved[k]
        except Exception:
            pass


def save_settings():
    try:
        SETTINGS_FILE.write_text(json.dumps(SETTINGS, indent=2), encoding="utf-8")
        try:
            os.chmod(SETTINGS_FILE, 0o600)
        except Exception:
            pass
    except Exception:
        pass


load_settings()

app = Flask(__name__)

_producer = None
_producer_lock = threading.Lock()
_kafka = {"connected": False, "error": None, "sent": 0, "next_retry": 0.0}


# ---- confluent-kafka producer ----
def _build_conf(s):
    """Translate our settings dict into a librdkafka config dict."""
    conf = {
        "bootstrap.servers": s["broker"],
        "security.protocol": s["security_protocol"],
        "socket.timeout.ms": 6000,
        "message.timeout.ms": 8000,
    }
    proto = s["security_protocol"]
    if proto.startswith("SASL"):
        conf["sasl.mechanism"] = s["sasl_mechanism"]
        conf["sasl.username"] = s["sasl_username"]
        conf["sasl.password"] = s["sasl_password"]
    if "SSL" in proto:
        if s.get("ssl_cafile"):
            conf["ssl.ca.location"] = s["ssl_cafile"]
        if s.get("ssl_certfile"):
            conf["ssl.certificate.location"] = s["ssl_certfile"]
        if s.get("ssl_keyfile"):
            conf["ssl.key.location"] = s["ssl_keyfile"]
        if s.get("ssl_password"):
            conf["ssl.key.password"] = s["ssl_password"]
    return conf


def _on_error(err):
    # librdkafka surfaces auth/connection errors here (background thread)
    _kafka["error"] = str(err)
    _kafka["connected"] = False


def _on_delivery(err, msg):
    if err is not None:
        _kafka["error"] = str(err)
        _kafka["connected"] = False
    else:
        _kafka["sent"] += 1
        _kafka["connected"] = True
        _kafka["error"] = None


def reset_producer():
    """Drop the current producer so it is rebuilt with fresh settings."""
    global _producer
    with _producer_lock:
        if _producer is not None:
            try:
                _producer.flush(2)
            except Exception:
                pass
        _producer = None
        _kafka["next_retry"] = 0.0
        _kafka["connected"] = False


def get_producer():
    """Lazily build a confluent_kafka.Producer; back off on failure."""
    global _producer
    if not SETTINGS["enabled"]:
        return None
    if _producer is not None:
        return _producer
    if time.monotonic() < _kafka["next_retry"]:
        return None
    with _producer_lock:
        if _producer is not None:
            return _producer
        try:
            from confluent_kafka import Producer
            conf = _build_conf(SETTINGS)
            conf["error_cb"] = _on_error
            _producer = Producer(conf)
            _kafka["error"] = None
        except Exception as e:
            _producer = None
            _kafka["connected"] = False
            _kafka["error"] = f"{type(e).__name__}: {e}"
            _kafka["next_retry"] = time.monotonic() + RETRY_BACKOFF_S
        return _producer


def publish_kafka(key, dp):
    prod = get_producer()
    if prod is None:
        return False
    try:
        prod.produce(
            SETTINGS["topic"],
            key=(key.encode("utf-8") if key else None),
            value=json.dumps(dp).encode("utf-8"),
            callback=_on_delivery,
        )
        prod.poll(0)
        return True
    except BufferError:
        prod.poll(0.2)
        _kafka["error"] = "local queue full"
        return False
    except Exception as e:
        _kafka["connected"] = False
        _kafka["error"] = f"{type(e).__name__}: {e}"
        _kafka["next_retry"] = time.monotonic() + RETRY_BACKOFF_S
        return False


def kafka_test(overrides=None):
    """Build a short-lived admin client from current (optionally overridden)
    settings and fetch metadata. Returns a clear ok/error result."""
    s = dict(SETTINGS)
    if overrides:
        for k in DEFAULT_SETTINGS:
            if k in overrides and overrides[k] not in (None, ""):
                s[k] = overrides[k]
        # allow explicit empty password override only if key present and non-null
    errors = []
    try:
        from confluent_kafka.admin import AdminClient
        conf = _build_conf(s)
        conf["error_cb"] = lambda e: errors.append(str(e))
        admin = AdminClient(conf)
        md = admin.list_topics(timeout=8)
        topics = list(md.topics.keys())
        return {
            "ok": True,
            "broker": s["broker"],
            "topic": s["topic"],
            "topic_exists": s["topic"] in md.topics,
            "topics_count": len(topics),
        }
    except Exception as e:
        msg = "; ".join(errors) or f"{type(e).__name__}: {e}"
        return {"ok": False, "broker": s["broker"], "error": msg}


def _public_settings():
    """Settings safe to return to the browser (password masked)."""
    out = {k: SETTINGS[k] for k in SETTINGS if k not in ("sasl_password", "ssl_password")}
    out["password_set"] = bool(SETTINGS.get("sasl_password"))
    return out


@app.after_request
def cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/")
def index():
    return send_from_directory(BASE, "telemetry_dashboard.html")


@app.route("/health")
def health():
    files = sorted(p.name for p in DATA_DIR.glob("*.ndjson"))
    return jsonify(
        status="ok",
        dir=str(DATA_DIR),
        files=files,
        kafka={
            "enabled": SETTINGS["enabled"],
            "broker": SETTINGS["broker"],
            "topic": SETTINGS["topic"],
            "security_protocol": SETTINGS["security_protocol"],
            "sasl_mechanism": SETTINGS["sasl_mechanism"],
            "connected": _kafka["connected"],
            "sent": _kafka["sent"],
            "error": _kafka["error"],
        },
        emit_interval_s=SETTINGS["emit_interval_s"],
    )


@app.route("/config", methods=["GET", "POST", "OPTIONS"])
def config():
    if request.method == "OPTIONS":
        return ("", 204)
    if request.method == "GET":
        return jsonify(_public_settings())
    body = request.get_json(force=True, silent=True) or {}
    with _settings_lock:
        for k in ("enabled", "broker", "topic", "security_protocol",
                  "sasl_mechanism", "sasl_username",
                  "ssl_cafile", "ssl_certfile", "ssl_keyfile"):
            if k in body and body[k] is not None:
                SETTINGS[k] = body[k]
        if "enabled" in body:
            SETTINGS["enabled"] = bool(body["enabled"])
        # passwords: only overwrite when a non-empty value is supplied
        if body.get("sasl_password"):
            SETTINGS["sasl_password"] = body["sasl_password"]
        if body.get("ssl_password"):
            SETTINGS["ssl_password"] = body["ssl_password"]
        if "emit_interval_s" in body:
            try:
                SETTINGS["emit_interval_s"] = max(1, int(body["emit_interval_s"]))
            except (TypeError, ValueError):
                pass
        save_settings()
    reset_producer()
    return jsonify(_public_settings())


@app.route("/kafka/test", methods=["POST", "OPTIONS"])
def kafka_test_route():
    if request.method == "OPTIONS":
        return ("", 204)
    overrides = request.get_json(force=True, silent=True) or {}
    return jsonify(kafka_test(overrides))


@app.route("/kafka/send-test", methods=["POST", "OPTIONS"])
def kafka_send_test():
    if request.method == "OPTIONS":
        return ("", 204)
    import time as _t
    msg = {
        "vehicle_id": "TEST-MESSAGE",
        "message_type": "settings_test",
        "event_timestamp": _t.strftime("%Y-%m-%dT%H:%M:%SZ", _t.gmtime()),
        "source": "settings_page",
        "note": "connectivity test from the dashboard Settings tab",
    }
    ok = publish_kafka("test-message", msg)
    prod = get_producer()
    delivered = False
    if prod is not None and ok:
        remaining = prod.flush(5)
        delivered = (remaining == 0)
    good = bool(ok and delivered)
    return jsonify(
        ok=good,
        topic=SETTINGS["topic"],
        broker=SETTINGS["broker"],
        error=None if good else (_kafka["error"] or ("disabled" if not SETTINGS["enabled"] else "not delivered")),
    )


def _safe(name):
    return "".join(c for c in str(name) if c.isalnum() or c in "-_")[:64] or "session"


@app.route("/ingest", methods=["POST", "OPTIONS"])
def ingest():
    if request.method == "OPTIONS":
        return ("", 204)
    dp = request.get_json(force=True, silent=True)
    if dp is None:
        return jsonify(error="invalid json"), 400
    nav = dp.get("navigation") or {}
    sid = nav.get("session_id") or request.args.get("session") or "session"
    # key Kafka messages by vehicle so multi-vehicle streams stay partition-stable
    key = dp.get("vehicle_id") or _safe(sid)
    fpath = DATA_DIR / f"{_safe(sid)}.ndjson"
    with open(fpath, "a", encoding="utf-8") as f:
        f.write(json.dumps(dp) + "\n")
    with open(fpath, "r", encoding="utf-8") as f:
        count = sum(1 for _ in f)
    kafka_ok = publish_kafka(_safe(str(key)), dp)
    return jsonify(
        ok=True,
        file=fpath.name,
        count=count,
        kafka=kafka_ok,
        topic=SETTINGS["topic"],
        kafka_error=None if kafka_ok else _kafka["error"],
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
