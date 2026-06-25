# Kafka TLS certificates

The broker on `orvill.ddns.net:9093` requires **mutual TLS** (it returns
`TLSV13_ALERT_CERTIFICATE_REQUIRED` for clients without a certificate).

Drop the following files here so the server can connect:

| File | Purpose | Maps to env var |
|------|---------|-----------------|
| `ca.pem` | Broker CA certificate (to verify the broker) | `KAFKA_SSL_CAFILE` |
| `client.pem` | Client certificate signed by that CA | `KAFKA_SSL_CERTFILE` |
| `client.key` | Client private key | `KAFKA_SSL_KEYFILE` |

If the client key is passphrase-protected, set `KAFKA_SSL_PASSWORD` in
`docker-compose.yml`. After adding the files, rebuild and restart:

```
docker compose up -d --build
curl -s localhost:8080/health   # kafka.connected should become true
```

The cert/key files themselves are git-ignored.
