import json
import os
import pathlib
import threading
import time
from flask import Flask, request, jsonify, send_from_directory

BASE = pathlib.Path(__file__).parent
DATA_DIR = pathlib.Path(os.environ.get("TELEMETRY_DIR", str(BASE / "telemetry")))
DATA_DIR.mkdir(parents=True, exist_ok=True)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "orvill.ddns.net:9093")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ConnectedCar1")
KAFKA_ENABLED = os.environ.get("KAFKA_ENABLED", "1") not in ("0", "false", "False", "")
# transport security: PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SSL")
KAFKA_SSL_CAFILE = os.environ.get("KAFKA_SSL_CAFILE") or None
KAFKA_SSL_CERTFILE = os.environ.get("KAFKA_SSL_CERTFILE") or None
KAFKA_SSL_KEYFILE = os.environ.get("KAFKA_SSL_KEYFILE") or None
KAFKA_SSL_PASSWORD = os.environ.get("KAFKA_SSL_PASSWORD") or None
KAFKA_SSL_CHECK_HOSTNAME = os.environ.get("KAFKA_SSL_CHECK_HOSTNAME", "0") not in ("0", "false", "False", "")
KAFKA_SASL_MECHANISM = os.environ.get("KAFKA_SASL_MECHANISM") or None
KAFKA_SASL_USERNAME = os.environ.get("KAFKA_SASL_USERNAME") or None
KAFKA_SASL_PASSWORD = os.environ.get("KAFKA_SASL_PASSWORD") or None
RETRY_BACKOFF_S = 15.0

app = Flask(__name__)

_producer = None
_producer_lock = threading.Lock()
_kafka = {"connected": False, "error": None, "sent": 0, "next_retry": 0.0}


def get_producer():
    """Lazily build a KafkaProducer. On failure, back off so /ingest never
    blocks for long when the broker is down."""
    global _producer
    if not KAFKA_ENABLED:
        return None
    if _producer is not None:
        return _producer
    if time.monotonic() < _kafka["next_retry"]:
        return None
    with _producer_lock:
        if _producer is not None:
            return _producer
        try:
            from kafka import KafkaProducer
            kw = dict(
                bootstrap_servers=[b.strip() for b in KAFKA_BROKER.split(",") if b.strip()],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks=1,
                retries=2,
                linger_ms=0,
                request_timeout_ms=5000,
                max_block_ms=5000,
                api_version_auto_timeout_ms=5000,
                security_protocol=KAFKA_SECURITY_PROTOCOL,
            )
            if "SSL" in KAFKA_SECURITY_PROTOCOL:
                kw["ssl_check_hostname"] = KAFKA_SSL_CHECK_HOSTNAME
                if KAFKA_SSL_CAFILE:
                    kw["ssl_cafile"] = KAFKA_SSL_CAFILE
                if KAFKA_SSL_CERTFILE:
                    kw["ssl_certfile"] = KAFKA_SSL_CERTFILE
                if KAFKA_SSL_KEYFILE:
                    kw["ssl_keyfile"] = KAFKA_SSL_KEYFILE
                if KAFKA_SSL_PASSWORD:
                    kw["ssl_password"] = KAFKA_SSL_PASSWORD
            if KAFKA_SASL_MECHANISM:
                kw["sasl_mechanism"] = KAFKA_SASL_MECHANISM
                kw["sasl_plain_username"] = KAFKA_SASL_USERNAME
                kw["sasl_plain_password"] = KAFKA_SASL_PASSWORD
            _producer = KafkaProducer(**kw)
            _kafka["connected"] = True
            _kafka["error"] = None
        except Exception as e:  # NoBrokersAvailable, DNS, etc.
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
        prod.send(KAFKA_TOPIC, key=key, value=dp)
        _kafka["sent"] += 1
        _kafka["connected"] = True
        _kafka["error"] = None
        return True
    except Exception as e:
        _kafka["connected"] = False
        _kafka["error"] = f"{type(e).__name__}: {e}"
        _kafka["next_retry"] = time.monotonic() + RETRY_BACKOFF_S
        return False


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
            "enabled": KAFKA_ENABLED,
            "broker": KAFKA_BROKER,
            "topic": KAFKA_TOPIC,
            "security_protocol": KAFKA_SECURITY_PROTOCOL,
            "connected": _kafka["connected"],
            "sent": _kafka["sent"],
            "error": _kafka["error"],
        },
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
    fpath = DATA_DIR / f"{_safe(sid)}.ndjson"
    with open(fpath, "a", encoding="utf-8") as f:
        f.write(json.dumps(dp) + "\n")
    with open(fpath, "r", encoding="utf-8") as f:
        count = sum(1 for _ in f)
    kafka_ok = publish_kafka(_safe(sid), dp)
    return jsonify(
        ok=True,
        file=fpath.name,
        count=count,
        kafka=kafka_ok,
        topic=KAFKA_TOPIC,
        kafka_error=None if kafka_ok else _kafka["error"],
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
