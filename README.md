# tsdb
use tsdb under goframe

As a tsdb, redis is not designed to be suitable for multiple projects,
so we don't cover this scenario when designing the key format.

Please take tdengine as the prior choice for multiple projects.

TDengine reads use `column_meta` to preserve numeric types: signed integer
columns return `int64`, unsigned integer columns return `uint64`, and `FLOAT` /
`DOUBLE` columns return `float64`. Integer columns previously returned `float64`;
callers using concrete type assertions should update them. Null values remain
`nil`. Invalid or out-of-range numeric values return an error instead of being
silently rounded. Numeric values in unknown column types remain `json.Number`.

Redis `LINEAR` fill uses exact arithmetic and rounds to the nearest integer, with
ties away from zero, preserving large integers and avoiding overflow in endpoint
differences. Existing floating-point filter fields retain their original behavior.

Redis writes reuse GoFrame's standalone or Sentinel go-redis client for ordinary,
non-transactional pipelines of up to 128 commands. `Write` waits for replies and
aggregates command errors, preserving key formats, timestamp-based Stream IDs,
GoFrame value encoding, and latest-value TTLs. Cluster and other adapters retain
synchronous writes to preserve command ordering. Values requiring custom native
encoding are sent individually so encoding failures cannot abort unrelated
commands in a pipeline.

For Redis, each latest-value hash update stores the values and their sample
times together. A partial write advances only the written points' times.
`ReadToMap` retains the device-level `_ts` and adds `_pointTs` (a
`map[string]int64` of Unix milliseconds) for returned points with valid stored
times. Legacy or malformed timestamps are omitted without dropping the point
value. Use `_pointTs` to judge point freshness. `_ts`, `_pointTs`, and
`_pointTs:`-prefixed fields are reserved for latest-value metadata.
