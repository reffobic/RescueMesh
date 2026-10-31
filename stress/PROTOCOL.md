# Wire format assumed by stress scripts

Matches `distributed-election` intent: **JSON-encoded `Msg` over UDP**.

## Client → cloud

```json
{
  "Request": {
    "req": {
      "request_id": "uuid-or-counter",
      "incident_id": "inc-1",
      "reporter_id": "client-7",
      "category": "PowerOutage",
      "location_zone": 3,
      "severity": 5,
      "timestamp_ms": 1730000000000,
      "status": "Open",
      "ttl_secs": 3600
    },
    "reply_to": ["127.0.0.1", 41000]
  }
}
```

Serde externally tags enums as `{"Variant": { ... } }`.  
`SocketAddr` in JSON is typically `{"ip":"...","octets":[...]} ` or `"127.0.0.1:41000"` depending on serde; **`load_client.py` tries both** and also a simplified control channel.

## Cloud → client

```json
{
  "Reply": {
    "plan": {
      "request_id": "...",
      "incident_id": "...",
      "priority_score": 1.0,
      "assigned_responders": [],
      "assigned_resources": [],
      "recommended_route": [3],
      "processed_by_node": 1
    },
    "under_replicated": false
  }
}
```

## Client Ack (feeds reliability metric)

```json
{ "Ack": { "request_id": "..." } }
```

## Fault control (recommended addition)

Add to node (if missing):

```json
{ "AdminMute": { "secs": 10 } }
```

Or run: `fault_inject.py --mode process --pid ...` / SSH `kill -STOP` (document method).

## Server log lines (parse these)

```
ELECTION,strategy,req_id,attempt,winner,score,elapsed_us,complete
CATCHUP,node_id,entries,convergence_ms
STATS,node_id,queue,cpu,plans,elections,under_replicated
MUTE_START,node_id,secs
MUTE_END,node_id
RECOVERED,node_id,convergence_ms
```
