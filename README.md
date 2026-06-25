# Connected Vehicle Telemetry Simulator

A self-contained, Dockerized connected-vehicle telemetry dashboard with a live
**A → B navigation simulator**. It drives a virtual electric vehicle along a real
road route, generates realistic telemetry datapoints, renders them in a dark,
modern EV-style dashboard, persists each datapoint to disk, and publishes it
to a Kafka topic.

## What it does

- **Live navigation simulator** — pick a city (geocoded via Nominatim), drop or
  randomize an **A** and **B** point, route between them with **OSRM**, and watch
  the vehicle drive the route on a **Leaflet** dark map. The map animates smoothly
  (~60 fps) and follows the vehicle, while a datapoint is emitted every 5 seconds.
- **Realistic telemetry** — each datapoint is a rich JSON document (battery,
  drivetrain, dynamics, suspension, thermal, ADAS, body, navigation, weather,
  location, system) modeled on a production EV telemetry schema.
- **Real-world data sources**
  - **Roads:** A/B endpoints are snapped to the nearest public road (OSRM
    `/nearest`), so they are always on land and drivable.
  - **Weather:** live conditions from **Open-Meteo** (temperature, condition,
    humidity, wind) shown in a map widget and saved to telemetry.
  - **Altitude:** real terrain elevation along the route from the **Open-Meteo**
    elevation API, interpolated by distance.
- **Driver-assist visuals** — turn-signal blinkers that activate as the vehicle
  approaches a turn, and a speed-limit sign. The vehicle never exceeds the posted
  limit (speed is capped per segment).
- **Battery & charging** — state of charge drains as the vehicle drives. When it
  falls into the 10–20% band the car locates the nearest **real charging station**
  (OpenStreetMap via Overpass), re-routes and drives there, charges to 80–90% with
  realistic DC fast-charge telemetry (tapering kW, charge port state, positive pack
  current), then resumes to the original destination.
- **Persistence + streaming** — the Flask server appends every datapoint to an
  NDJSON file under `telemetry/` and publishes it to a **Kafka** topic
  (`connectedcar`) with graceful degradation if the broker is unreachable.

## Architecture

| Component | Role |
|-----------|------|
| `telemetry_dashboard.html` | Single-page dashboard + navigation simulator (Leaflet, OSRM, Open-Meteo). All telemetry logic runs in the browser. |
| `server.py` | Flask server: `GET /` serves the dashboard, `POST /ingest` writes NDJSON + publishes to Kafka, `GET /health` reports file count and Kafka status. |
| `docker-compose.yml` / `Dockerfile` | Containerized runtime (`connected-vehicle`), port `8080`, telemetry volume, live-mounted HTML, Kafka env vars. |
| `requirements.txt` | `flask`, `kafka-python-ng`. |
| `telemetry/` | Persisted NDJSON datapoints (gitignored). |
| `*_telemetry_sample.*` | Reference telemetry payloads. |

## Running it

```bash
docker compose up -d --build
# open http://localhost:8080
```

Open the **Location** tab, choose a city (default Stockholm), press **Random A→B**
to plan a route, then **▶ Start** to begin driving. Datapoints appear under
`telemetry/` and are streamed to Kafka.

### Configuration (environment variables)

| Variable | Default | Purpose |
|----------|---------|---------|
| `KAFKA_BROKER` | `orvill.ddns.net:9092` | Kafka bootstrap server. |
| `KAFKA_TOPIC` | `connectedcar` | Destination topic. |
| `KAFKA_ENABLED` | `1` | Set to `0` to disable publishing. |

> Note: telemetry is always saved to disk even if Kafka is unavailable.

---

## Changelog

Changes made across the project's commits (oldest → newest):

### `6e45a5c` — Initial release: telemetry dashboard + live A→B nav simulator
- Flask server with `/ingest` (NDJSON to disk + Kafka publish to `connectedcar`).
- Dockerized via docker-compose; Leaflet / OSRM / Nominatim navigation simulator.
- Smooth ~60 fps map with a highlighted route and a datapoint emitted every 5 s.

### `59288af` — Snap A/B endpoints to public roads; add no-store cache headers
- `navRandom` snaps random points to the nearest drivable road via OSRM `/nearest`,
  guaranteeing A and B are always on land and on a public road.
- Manual **Set A / Set B** map clicks snap to the nearest road too.
- Flask responses send `Cache-Control: no-store` to prevent stale dashboards.

### `15fce52` — Turn-signal blinkers and live weather widget
- Left/right indicators blink amber as the vehicle approaches a turn (driven by the
  OSRM maneuver modifier with a speed-based lead distance); `turn_signal` recorded
  in navigation telemetry.
- Live weather via Open-Meteo (no API key): temperature, condition, humidity, and
  wind in a map widget and saved under `DATA.weather`; refreshes at trip start and
  every 10 minutes along the route.
- Moved the zoom control to bottom-right to clear the weather widget.

### `23f2c29` — Enforce speed limits
- Each route segment gets a posted limit snapped to standard values
  (30/50/70/90/110/130 km/h) from OSRM's profile speed; driving speed is capped to
  it and segment timing/ETA recomputed so the car physically drives at or under the
  limit (the public OSRM has no maxspeed annotations).
- Speed-limit sign rendered on the map; `speed_limit_kmh` and `over_limit` recorded
  in navigation telemetry.

### `9f286ef` — Real terrain elevation for altitude
- Fetch ground elevation along the route from the Open-Meteo elevation API
  (downsampled to ≤100 points, one request) and interpolate by distance.
- `gps_altitude_m` now tracks real-world terrain; `elevation_gain_m` accumulates
  positive climbs along the trip.
- Verified endpoints match Open-Meteo exactly (A = 37 m, B = 22 m).

### `4f90ac0` — Fictive manufacturer branding
- Vehicle make/model/trim now use a fictive **Voltessa** lineup (Terra GT, Ridge XT,
  Vela One, Nova S), one picked per session.
- Neutral fictive VIN prefix; the subtitle UI reflects the new brand.
- Renamed and updated the sample data files (`voltessa_telemetry_sample.*`).

### `f88f5fd` — Battery drain and automatic charging stops
- State of charge is now a stateful value that drains with distance driven across
  every leg of a trip (with a small extra cost for uphill elevation gain), updating
  derived telemetry: energy remaining, estimated range, pack voltage and current.
- When SOC drops into the 10–20% band, the vehicle queries **Overpass** for the
  nearest `amenity=charging_station`, snaps it to a road, re-routes from its current
  position and drives there (a pulsing ⚡ marker marks the station).
- On arrival it charges to a random 80–90% target with DC fast-charge telemetry
  (tapering charge rate, `charge_port_state=CHARGING`, positive pack current, gear P),
  then automatically re-routes from the station to the original destination and
  continues. Charging datapoints are persisted alongside driving datapoints.
