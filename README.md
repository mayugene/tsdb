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
