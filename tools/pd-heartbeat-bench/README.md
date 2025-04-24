# pd-heartbeat-bench

`pd-heartbeat-bench` is used to bench heartbeart.

1. You need to deploy a cluster that only contain pd firstly, like `tiup playground nightly --pd 3 --kv 0 --db 0`.
2. Then, execute `pd-heartbeart-bench` and set the pd leader as `--pd-endpoints` 

## HTTP Server
The tool starts an HTTP server based on the StatusAddr field in the configuration file. Ensure that the StatusAddr is correctly configured before starting the server.

## API Endpoints
1. Get Current Configuration

**Endpoint**: `GET /config`

**Description**: Returns the current configuration of the benchmark tool.

Response:

- Status Code: `200 OK`
- Content: JSON representation of the configuration.

Example Response:

```json
{
  "HotStoreCount": 10,
  "FlowUpdateRatio": 0.5,
  "LeaderUpdateRatio": 0.3,
  "EpochUpdateRatio": 0.2,
  "SpaceUpdateRatio": 0.1,
  "ReportRatio": 0.05
}
```

2. Update Configuration

**Endpoint**: `PUT /config`

**Description**: Updates the configuration of the benchmark tool.

Request Body: JSON representation of the new configuration.

Response:

- Status Code: `200 OK` if the update is successful.
- Status Code: `400 Bad Request` if the request body is invalid or fails validation.

Example Request:

```json
{
  "HotStoreCount": 15,
  "FlowUpdateRatio": 0.6,
  "LeaderUpdateRatio": 0.4,
  "EpochUpdateRatio": 0.3,
  "SpaceUpdateRatio": 0.2,
  "ReportRatio": 0.1
}
```