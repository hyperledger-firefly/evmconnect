

## api

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|address|Listener address for API|`string`|`127.0.0.1`
|defaultRequestTimeout|Default server-side request timeout for API calls|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|maxRequestTimeout|Maximum server-side request timeout a caller can request with a Request-Timeout header|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10m`
|passthroughHeaders|A list of HTTP request headers to pass through to dependency microservices|`[]string`|`[]`
|port|Listener port for API|`int`|`5008`
|publicURL|External address callers should access API over|`string`|`<nil>`
|readTimeout|The maximum time to wait when reading from an HTTP connection|[`time.Duration`](https://pkg.go.dev/time#Duration)|`15s`
|shutdownTimeout|The maximum amount of time to wait for any open HTTP requests to finish before shutting down the HTTP server|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10s`
|simpleQuery|Force use of original limited API query syntax, even if rich query is supported in the database|`boolean`|`<nil>`
|writeTimeout|The maximum time to wait when writing to a HTTP connection|[`time.Duration`](https://pkg.go.dev/time#Duration)|`15s`

## api.auth

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|type|The auth plugin to use for server side authentication of requests|`string`|`<nil>`

## api.auth.basic

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|passwordfile|The path to a .htpasswd file to use for authenticating requests. Passwords should be hashed with bcrypt.|`string`|`<nil>`

## api.tls

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|ca|The TLS certificate authority in PEM format (this option is ignored if caFile is also set)|`string`|`<nil>`
|caFile|The path to the CA file for TLS on this API|`string`|`<nil>`
|cert|The TLS certificate in PEM format (this option is ignored if certFile is also set)|`string`|`<nil>`
|certFile|The path to the certificate file for TLS on this API|`string`|`<nil>`
|clientAuth|Enables or disables client auth for TLS on this API|`string`|`<nil>`
|enabled|Enables or disables TLS on this API|`boolean`|`false`
|insecureSkipHostVerify|When to true in unit test development environments to disable TLS verification. Use with extreme caution|`boolean`|`<nil>`
|key|The TLS certificate key in PEM format (this option is ignored if keyFile is also set)|`string`|`<nil>`
|keyFile|The path to the private key file for TLS on this API|`string`|`<nil>`
|requiredDNAttributes|A set of required subject DN attributes. Each entry is a regular expression, and the subject certificate must have a matching attribute of the specified type (CN, C, O, OU, ST, L, STREET, POSTALCODE, SERIALNUMBER are valid attributes)|`map[string]string`|`<nil>`

## confirmations

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|blockQueueLength|Internal queue length for notifying the confirmations manager of new blocks|`int`|`50`
|fetchReceiptUponEntry|Fetch receipt of new transactions immediately when they are added to the internal queue. When set to false, fetch will only happen when a new block is received or the transaction has been queue for more than the stale receipt timeout|`boolean`|`false`
|notificationQueueLength|Internal queue length for notifying the confirmations manager of new transactions/events|`int`|`50`
|receiptWorkers|Number of workers to use to query in parallel for receipts|`int`|`10`
|required|Number of confirmations required to consider a transaction/event final|`int`|`20`
|staleReceiptTimeout|Duration after which to force a receipt check for a pending transaction|[`time.Duration`](https://pkg.go.dev/time#Duration)|`1m`

## confirmations.retry

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|factor|The retry backoff factor|`float32`|`2`
|initialDelay|The initial retry delay|[`time.Duration`](https://pkg.go.dev/time#Duration)|`100ms`
|maxDelay|The maximum retry delay|[`time.Duration`](https://pkg.go.dev/time#Duration)|`15s`

## connector

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|blockCacheSize|Maximum of blocks to hold in the block info cache|`int`|`250`
|blockPollingInterval|Interval for polling to check for new blocks|[`time.Duration`](https://pkg.go.dev/time#Duration)|`1s`
|chainTrackingMode|Tracking mode for connector block progression. light: fetches head block numbers only, does not download block details, disables block listener support, and confirmation results include only the confirmation count. full: fetches head block numbers and block details, maintains an in-memory partial chain, enables block listener support, and confirmation results include both confirmation count and block details.|`light` or `full`|`full`
|connectionTimeout|The maximum amount of time that a connection is allowed to remain with no data transmitted|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|dataFormat|Configure the JSON data format for query output and events|map,flat_array,self_describing|`map`
|expectContinueTimeout|See [ExpectContinueTimeout in the Go docs](https://pkg.go.dev/net/http#Transport)|[`time.Duration`](https://pkg.go.dev/time#Duration)|`1s`
|gasEstimationFactor|The factor to apply to the gas estimation to determine the gas limit|`float32`|`1.5`
|headers|Adds custom headers to HTTP requests|`map[string]string`|`<nil>`
|hederaCompatibilityMode|Compatibility mode for Hedera, allowing non-standard block header hashes to be processed|`boolean`|`false`
|idleTimeout|The max duration to hold a HTTP keepalive connection between calls|[`time.Duration`](https://pkg.go.dev/time#Duration)|`475ms`
|maxAsyncBlockFetchConcurrency|Maximum concurrency when using asynchronous block downloading (minium 1)|`int`|`25`
|maxConcurrentRequests|Maximum of concurrent requests to be submitted to the blockchain|`int`|`50`
|maxConnsPerHost|The max number of connections, per unique hostname. Zero means no limit|`int`|`0`
|maxIdleConns|The max number of idle connections to hold pooled|`int`|`100`
|maxIdleConnsPerHost|The max number of idle connections, per unique hostname. Zero means net/http uses the default of only 2.|`int`|`100`
|passthroughHeadersEnabled|Enable passing through the set of allowed HTTP request headers|`boolean`|`false`
|receiptCacheSize|Maximum of transaction receipts to hold in the receipt cache|`int`|`5000`
|requestTimeout|The maximum amount of time that a request is allowed to remain open|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|tlsHandshakeTimeout|The maximum amount of time to wait for a successful TLS handshake|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10s`
|traceTXForRevertReason|Enable the use of transaction trace functions (e.g. debug_traceTransaction) to obtain transaction revert reasons. This can place a high load on the EVM client.|`boolean`|`false`
|txCacheSize|Maximum of transactions to hold in the transaction info cache|`int`|`250`
|url|URL of JSON/RPC endpoint for the Ethereum node/gateway|string|`<nil>`
|useGetBlockReceipts|When true, the eth_getBlockReceipts call is available for this connector to use|`boolean`|`true`

## connector.auth

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|password|Password|`string`|`<nil>`
|username|Username|`string`|`<nil>`

## connector.dns

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|servers|An optional list of DNS server addresses (host or host:port, port defaults to 53) to use instead of the system resolver. Setting this forces use of Go's built-in DNS resolver.|`[]string`|`<nil>`
|timeout|The dial timeout when contacting a configured DNS server|[`time.Duration`](https://pkg.go.dev/time#Duration)|`<nil>`

## connector.events

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|blockTimestamps|Whether to include the block timestamps in the event information|`boolean`|`true`
|catchupDownscaleRegex|An error pattern to check for from JSON/RPC providers if they limit response sizes to eth_getLogs(). If an error is returned from eth_getLogs() and that error matches the configured pattern, the number of logs requested (catchupPageSize) will be reduced automatically.|string|`Response size is larger than.*limit`
|catchupPageSize|Number of blocks to query per poll when catching up to the head of the blockchain|`int`|`500`
|catchupThreshold|How many blocks behind the chain head an event stream or listener must be on startup, to enter catchup mode|`int`|`500`
|checkpointBlockGap|The number of blocks at the head of the chain that should be considered unstable (could be dropped from the canonical chain after a re-org). Unless events with a full set of confirmations are detected, the restart checkpoint will this many blocks behind the chain head.|`int`|`50`
|filterPollingInterval|The interval between polling calls to a filter, when checking for newly arrived events|[`time.Duration`](https://pkg.go.dev/time#Duration)|`1s`

## connector.net

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|cidrDenylist|A list of CIDR ranges to which outbound connections are blocked, as a core SSRF mitigation. Empty by default.|`[]string`|`<nil>`

## connector.proxy

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|url|Optional HTTP proxy server to connect through|`string`|`<nil>`

## connector.queryLoopRetry

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|factor|Factor to increase the delay by, between each query request retry to the RPC endpoint, applicable to all the query loops|`float32`|`2`
|initialDelay|Initial delay for retrying query requests to the RPC endpoint, applicable to all the query loops|[`time.Duration`](https://pkg.go.dev/time#Duration)|`100ms`
|maxDelay|Maximum delay for between each query request retry to the RPC endpoint, applicable to all the query loops|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`

## connector.retry

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|count|The maximum number of times to retry|`int`|`5`
|enabled|Enables retries|`boolean`|`true`
|errorStatusCodeRegex|The regex that the error response status code must match to trigger retry|`string`|`<nil>`
|factor|(Deprecated) Please refer to `connector.queryLoopRetry.factor` to understand its original purpose and use that instead|`float32`|`2`
|initWaitTime|The initial retry delay|[`time.Duration`](https://pkg.go.dev/time#Duration)|`250ms`
|initialDelay|(Deprecated) Please refer to `connector.queryLoopRetry.initialDelay` to understand its original purpose and use that instead|[`time.Duration`](https://pkg.go.dev/time#Duration)|`<nil>`
|maxDelay|(Deprecated) Please refer to `connector.queryLoopRetry.maxDelay` to understand its original purpose and use that instead|[`time.Duration`](https://pkg.go.dev/time#Duration)|`<nil>`
|maxWaitTime|The maximum retry delay|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`

## connector.throttle

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|burst|The maximum number of requests that can be made in a short period of time before the throttling kicks in.|`int`|`<nil>`
|requestsPerSecond|The average rate at which requests are allowed to pass through over time.|`int`|`<nil>`

## connector.tls

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|ca|The TLS certificate authority in PEM format (this option is ignored if caFile is also set)|`string`|`<nil>`
|caFile|The path to the CA file for TLS on this API|`string`|`<nil>`
|cert|The TLS certificate in PEM format (this option is ignored if certFile is also set)|`string`|`<nil>`
|certFile|The path to the certificate file for TLS on this API|`string`|`<nil>`
|clientAuth|Enables or disables client auth for TLS on this API|`string`|`<nil>`
|enabled|Enables or disables TLS on this API|`boolean`|`false`
|insecureSkipHostVerify|When to true in unit test development environments to disable TLS verification. Use with extreme caution|`boolean`|`<nil>`
|key|The TLS certificate key in PEM format (this option is ignored if keyFile is also set)|`string`|`<nil>`
|keyFile|The path to the private key file for TLS on this API|`string`|`<nil>`
|requiredDNAttributes|A set of required subject DN attributes. Each entry is a regular expression, and the subject certificate must have a matching attribute of the specified type (CN, C, O, OU, ST, L, STREET, POSTALCODE, SERIALNUMBER are valid attributes)|`map[string]string`|`<nil>`

## connector.ws

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|backgroundConnect|When true the connection is established in the background with infinite reconnect (makes initialConnectAttempts redundant when set)|`boolean`|`false`
|connectionTimeout|The amount of time to wait while establishing a connection (or auto-reconnection)|[`time.Duration`](https://pkg.go.dev/time#Duration)|`45s`
|enabled|When true a WebSocket is established for block listening, in addition to the HTTP RPC connections used for other functions|`boolean`|`false`
|heartbeatInterval|The amount of time to wait between heartbeat signals on the WebSocket connection|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|initialConnectAttempts|The number of attempts FireFly will make to connect to the WebSocket when starting up, before failing|`int`|`5`
|path|The WebSocket sever URL to which FireFly should connect|WebSocket URL `string`|`<nil>`
|readBufferSize|The size in bytes of the read buffer for the WebSocket connection|[`BytesSize`](https://pkg.go.dev/github.com/docker/go-units#BytesSize)|`16Kb`
|url|URL to use for WebSocket - overrides url one level up (in the HTTP config)|`string`|`<nil>`
|writeBufferSize|The size in bytes of the write buffer for the WebSocket connection|[`BytesSize`](https://pkg.go.dev/github.com/docker/go-units#BytesSize)|`16Kb`

## cors

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|credentials|CORS setting to control whether a browser allows credentials to be sent to this API|`boolean`|`true`
|debug|Whether debug is enabled for the CORS implementation|`boolean`|`false`
|enabled|Whether CORS is enabled|`boolean`|`true`
|headers|CORS setting to control the allowed headers|`[]string`|`[*]`
|maxAge|The maximum age a browser should rely on CORS checks|[`time.Duration`](https://pkg.go.dev/time#Duration)|`600`
|methods| CORS setting to control the allowed methods|`[]string`|`[GET POST PUT PATCH DELETE]`
|origins|CORS setting to control the allowed origins|`[]string`|`[*]`

## debug

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|address|Listener address|`int`|`127.0.0.1`
|enabled|Whether the debug HTTP endpoint is enabled|`boolean`|`true`
|port|An HTTP port on which to enable the go debugger|`int`|`0`
|publicURL|Externally available URL for the HTTP endpoint|`string`|`<nil>`
|readTimeout|HTTP server read timeout|[`time.Duration`](https://pkg.go.dev/time#Duration)|`15s`
|shutdownTimeout|HTTP server shutdown timeout|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10s`
|writeTimeout|HTTP server write timeout|[`time.Duration`](https://pkg.go.dev/time#Duration)|`15s`

## debug.auth

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|type|The auth plugin to use for server side authentication of requests|`string`|`<nil>`

## debug.auth.basic

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|passwordfile|The path to a .htpasswd file to use for authenticating requests. Passwords should be hashed with bcrypt.|`string`|`<nil>`

## debug.tls

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|ca|The TLS certificate authority in PEM format (this option is ignored if caFile is also set)|`string`|`<nil>`
|caFile|The path to the CA file for TLS on this API|`string`|`<nil>`
|cert|The TLS certificate in PEM format (this option is ignored if certFile is also set)|`string`|`<nil>`
|certFile|The path to the certificate file for TLS on this API|`string`|`<nil>`
|clientAuth|Enables or disables client auth for TLS on this API|`string`|`<nil>`
|enabled|Enables or disables TLS on this API|`boolean`|`false`
|insecureSkipHostVerify|When to true in unit test development environments to disable TLS verification. Use with extreme caution|`boolean`|`<nil>`
|key|The TLS certificate key in PEM format (this option is ignored if keyFile is also set)|`string`|`<nil>`
|keyFile|The path to the private key file for TLS on this API|`string`|`<nil>`
|requiredDNAttributes|A set of required subject DN attributes. Each entry is a regular expression, and the subject certificate must have a matching attribute of the specified type (CN, C, O, OU, ST, L, STREET, POSTALCODE, SERIALNUMBER are valid attributes)|`map[string]string`|`<nil>`

## eventstreams

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|checkpointInterval|Regular interval to write checkpoints for an event stream listener that is not actively detecting/delivering events|[`time.Duration`](https://pkg.go.dev/time#Duration)|`1m`

## eventstreams.defaults

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|batchSize|Default batch size for newly created event streams|`int`|`50`
|batchTimeout|Default batch timeout for newly created event streams|[`time.Duration`](https://pkg.go.dev/time#Duration)|`5s`
|blockedRetryDelay|Default blocked retry delay for newly created event streams|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|errorHandling|Default error handling for newly created event streams|'skip' or 'block'|`block`
|retryTimeout|Default retry timeout for newly created event streams|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|webhookRequestTimeout|Default WebHook request timeout for newly created event streams|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|websocketDistributionMode|Default WebSocket distribution mode for newly created event streams|'load_balance' or 'broadcast'|`load_balance`

## eventstreams.retry

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|factor|Factor to increase the delay by, between each retry|`float32`|`2`
|initialDelay|Initial retry delay|[`time.Duration`](https://pkg.go.dev/time#Duration)|`250ms`
|maxDelay|Maximum delay between retries|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`

## log

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|compress|Determines if the rotated log files should be compressed using gzip|`boolean`|`<nil>`
|filename|Filename is the file to write logs to.  Backup log files will be retained in the same directory|`string`|`<nil>`
|filesize|MaxSize is the maximum size the log file before it gets rotated|[`BytesSize`](https://pkg.go.dev/github.com/docker/go-units#BytesSize)|`100m`
|forceColor|Force color to be enabled, even when a non-TTY output is detected|`boolean`|`<nil>`
|includeCodeInfo|Enables the report caller for including the calling file and line number, and the calling function. If using text logs, it uses the logrus text format rather than the default prefix format.|`boolean`|`false`
|level|The log level - error, warn, info, debug, trace|`string`|`info`
|maxAge|The maximum time to retain old log files based on the timestamp encoded in their filename.|[`time.Duration`](https://pkg.go.dev/time#Duration)|`24h`
|maxBackups|Maximum number of old log files to retain|`int`|`2`
|noColor|Force color to be disabled, event when TTY output is detected|`boolean`|`<nil>`
|timeFormat|Custom time format for logs|[Time format](https://pkg.go.dev/time#pkg-constants) `string`|`2006-01-02T15:04:05.000Z07:00`
|utc|Use UTC timestamps for logs|`boolean`|`false`

## log.json

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|enabled|Enables JSON formatted logs rather than text. All log color settings are ignored when enabled.|`boolean`|`false`

## log.json.fields

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|file|configures the JSON key containing the calling file|`string`|`file`
|func|Configures the JSON key containing the calling function|`string`|`func`
|level|Configures the JSON key containing the log level|`string`|`level`
|message|Configures the JSON key containing the log message|`string`|`message`
|timestamp|Configures the JSON key containing the timestamp of the log|`string`|`@timestamp`

## metrics

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|address|The IP address on which the metrics HTTP API should listen|`int`|`127.0.0.1`
|enabled|Deprecated: Please use 'monitoring.enabled' instead|`boolean`|`false`
|path|Deprecated: Please use 'monitoring.metricsPath' instead|`string`|`/metrics`
|port|The port on which the metrics HTTP API should listen|`int`|`6000`
|publicURL|The fully qualified public URL for the metrics API. This is used for building URLs in HTTP responses and in OpenAPI Spec generation|URL `string`|`<nil>`
|readTimeout|The maximum time to wait when reading from an HTTP connection|[`time.Duration`](https://pkg.go.dev/time#Duration)|`15s`
|shutdownTimeout|The maximum amount of time to wait for any open HTTP requests to finish before shutting down the HTTP server|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10s`
|writeTimeout|The maximum time to wait when writing to an HTTP connection|[`time.Duration`](https://pkg.go.dev/time#Duration)|`15s`

## metrics.auth

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|type|The auth plugin to use for server side authentication of requests|`string`|`<nil>`

## metrics.auth.basic

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|passwordfile|The path to a .htpasswd file to use for authenticating requests. Passwords should be hashed with bcrypt.|`string`|`<nil>`

## metrics.tls

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|ca|The TLS certificate authority in PEM format (this option is ignored if caFile is also set)|`string`|`<nil>`
|caFile|The path to the CA file for TLS on this API|`string`|`<nil>`
|cert|The TLS certificate in PEM format (this option is ignored if certFile is also set)|`string`|`<nil>`
|certFile|The path to the certificate file for TLS on this API|`string`|`<nil>`
|clientAuth|Enables or disables client auth for TLS on this API|`string`|`<nil>`
|enabled|Enables or disables TLS on this API|`boolean`|`false`
|insecureSkipHostVerify|When to true in unit test development environments to disable TLS verification. Use with extreme caution|`boolean`|`<nil>`
|key|The TLS certificate key in PEM format (this option is ignored if keyFile is also set)|`string`|`<nil>`
|keyFile|The path to the private key file for TLS on this API|`string`|`<nil>`
|requiredDNAttributes|A set of required subject DN attributes. Each entry is a regular expression, and the subject certificate must have a matching attribute of the specified type (CN, C, O, OU, ST, L, STREET, POSTALCODE, SERIALNUMBER are valid attributes)|`map[string]string`|`<nil>`

## monitoring

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|address|Listener address|`int`|`127.0.0.1`
|enabled|Enables the monitoring APIs|`boolean`|`false`
|metricsPath|The path from which to serve the Prometheus metrics|`string`|`/metrics`
|port|Listener port|`int`|`6000`
|publicURL|Externally available URL for the HTTP endpoint|`string`|`<nil>`
|readTimeout|HTTP server read timeout|[`time.Duration`](https://pkg.go.dev/time#Duration)|`15s`
|shutdownTimeout|HTTP server shutdown timeout|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10s`
|writeTimeout|HTTP server write timeout|[`time.Duration`](https://pkg.go.dev/time#Duration)|`15s`

## monitoring.auth

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|type|The auth plugin to use for server side authentication of requests|`string`|`<nil>`

## monitoring.auth.basic

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|passwordfile|The path to a .htpasswd file to use for authenticating requests. Passwords should be hashed with bcrypt.|`string`|`<nil>`

## monitoring.tls

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|ca|The TLS certificate authority in PEM format (this option is ignored if caFile is also set)|`string`|`<nil>`
|caFile|The path to the CA file for TLS on this API|`string`|`<nil>`
|cert|The TLS certificate in PEM format (this option is ignored if certFile is also set)|`string`|`<nil>`
|certFile|The path to the certificate file for TLS on this API|`string`|`<nil>`
|clientAuth|Enables or disables client auth for TLS on this API|`string`|`<nil>`
|enabled|Enables or disables TLS on this API|`boolean`|`false`
|insecureSkipHostVerify|When to true in unit test development environments to disable TLS verification. Use with extreme caution|`boolean`|`<nil>`
|key|The TLS certificate key in PEM format (this option is ignored if keyFile is also set)|`string`|`<nil>`
|keyFile|The path to the private key file for TLS on this API|`string`|`<nil>`
|requiredDNAttributes|A set of required subject DN attributes. Each entry is a regular expression, and the subject certificate must have a matching attribute of the specified type (CN, C, O, OU, ST, L, STREET, POSTALCODE, SERIALNUMBER are valid attributes)|`map[string]string`|`<nil>`

## persistence

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|type|The type of persistence to use|`leveldb`, `postgres`(supports rich query)|`leveldb`

## persistence.leveldb

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|maxHandles|The maximum number of cached file handles LevelDB should keep open|`int`|`100`
|path|The path for the LevelDB persistence directory|`string`|`<nil>`
|syncWrites|Whether to synchronously perform writes to the storage|`boolean`|`false`

## persistence.postgres

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|maxConnIdleTime|The maximum amount of time a database connection can be idle|[`time.Duration`](https://pkg.go.dev/time#Duration)|`1m`
|maxConnLifetime|The maximum amount of time to keep a database connection open|[`time.Duration`](https://pkg.go.dev/time#Duration)|`<nil>`
|maxConns|Maximum connections to the database|`int`|`50`
|maxIdleConns|The maximum number of idle connections to the database|`int`|`<nil>`
|url|The PostgreSQL connection string for the database|`string`|`<nil>`

## persistence.postgres.migrations

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|auto|Enables automatic database migrations|`boolean`|`false`
|directory|The directory containing the numerically ordered migration DDL files to apply to the database|`string`|`./db/migrations/postgres`

## persistence.postgres.txwriter

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|batchSize|Number of persistence operations on transactions to attempt to group into a DB transaction|`int`|`100`
|batchTimeout|Duration to hold batch open for new transaction operations before flushing to the DB|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10ms`
|cacheSlots|Number of transactions to hold cached metadata for to avoid DB read operations to calculate history|`int`|`1000`
|count|Number of transactions writing routines to start|`int`|`5`
|historyCompactionInterval|Duration between cleanup activities on the DB for a transaction with a large history|[`time.Duration`](https://pkg.go.dev/time#Duration)|`0`
|historySummaryLimit|Maximum number of action entries to return embedded in the JSON response object when querying a transaction summary|`int`|`50`

## policyengine

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|name|Deprecated: Please use 'transactions.handler.name' instead|`string`|`simple`

## policyengine.simple

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|fixedGasPrice|Deprecated: Please use 'transactions.handler.simple.fixedGasPrice' instead|Raw JSON|`<nil>`
|resubmitInterval|Deprecated: Please use 'transactions.handler.simple.resubmitInterval' instead|[`time.Duration`](https://pkg.go.dev/time#Duration)|`5m`

## policyengine.simple.gasOracle

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|connectionTimeout|The maximum amount of time that a connection is allowed to remain with no data transmitted|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|expectContinueTimeout|See [ExpectContinueTimeout in the Go docs](https://pkg.go.dev/net/http#Transport)|[`time.Duration`](https://pkg.go.dev/time#Duration)|`1s`
|headers|Adds custom headers to HTTP requests|`map[string]string`|`<nil>`
|idleTimeout|The max duration to hold a HTTP keepalive connection between calls|[`time.Duration`](https://pkg.go.dev/time#Duration)|`475ms`
|maxConnsPerHost|The max number of connections, per unique hostname. Zero means no limit|`int`|`0`
|maxIdleConns|The max number of idle connections to hold pooled|`int`|`100`
|maxIdleConnsPerHost|The max number of idle connections, per unique hostname. Zero means net/http uses the default of only 2.|`int`|`100`
|method|Deprecated: Please use 'transactions.handler.simple.gasOracle.method' instead|`string`|`GET`
|mode|Deprecated: Please use 'transactions.handler.simple.gasOracle.mode' instead|'connector', 'restapi', 'fixed', or 'disabled'|`connector`
|passthroughHeadersEnabled|Enable passing through the set of allowed HTTP request headers|`boolean`|`false`
|queryInterval|Deprecated: Please use 'transactions.handler.simple.gasOracle.queryInterval' instead|[`time.Duration`](https://pkg.go.dev/time#Duration)|`5m`
|requestTimeout|The maximum amount of time that a request is allowed to remain open|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|template|Deprecated: Please use 'transactions.handler.simple.gasOracle.template' instead|[Go Template](https://pkg.go.dev/text/template) `string`|`<nil>`
|tlsHandshakeTimeout|The maximum amount of time to wait for a successful TLS handshake|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10s`
|url|Deprecated: Please use 'transactions.handler.simple.gasOracle.url' instead|`string`|`<nil>`

## policyengine.simple.gasOracle.auth

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|password|Password|`string`|`<nil>`
|username|Username|`string`|`<nil>`

## policyengine.simple.gasOracle.dns

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|servers|An optional list of DNS server addresses (host or host:port, port defaults to 53) to use instead of the system resolver. Setting this forces use of Go's built-in DNS resolver.|`[]string`|`<nil>`
|timeout|The dial timeout when contacting a configured DNS server|[`time.Duration`](https://pkg.go.dev/time#Duration)|`<nil>`

## policyengine.simple.gasOracle.net

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|cidrDenylist|A list of CIDR ranges to which outbound connections are blocked, as a core SSRF mitigation. Empty by default.|`[]string`|`<nil>`

## policyengine.simple.gasOracle.proxy

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|url|Deprecated: Please use 'transactions.handler.simple.gasOracle.proxy.url' instead|`string`|`<nil>`

## policyengine.simple.gasOracle.retry

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|count|The maximum number of times to retry|`int`|`5`
|enabled|Enables retries|`boolean`|`false`
|errorStatusCodeRegex|The regex that the error response status code must match to trigger retry|`string`|`<nil>`
|initWaitTime|The initial retry delay|[`time.Duration`](https://pkg.go.dev/time#Duration)|`250ms`
|maxWaitTime|The maximum retry delay|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`

## policyengine.simple.gasOracle.throttle

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|burst|The maximum number of requests that can be made in a short period of time before the throttling kicks in.|`int`|`<nil>`
|requestsPerSecond|The average rate at which requests are allowed to pass through over time.|`int`|`<nil>`

## policyengine.simple.gasOracle.tls

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|ca|The TLS certificate authority in PEM format (this option is ignored if caFile is also set)|`string`|`<nil>`
|caFile|The path to the CA file for TLS on this API|`string`|`<nil>`
|cert|The TLS certificate in PEM format (this option is ignored if certFile is also set)|`string`|`<nil>`
|certFile|The path to the certificate file for TLS on this API|`string`|`<nil>`
|clientAuth|Enables or disables client auth for TLS on this API|`string`|`<nil>`
|enabled|Enables or disables TLS on this API|`boolean`|`false`
|insecureSkipHostVerify|When to true in unit test development environments to disable TLS verification. Use with extreme caution|`boolean`|`<nil>`
|key|The TLS certificate key in PEM format (this option is ignored if keyFile is also set)|`string`|`<nil>`
|keyFile|The path to the private key file for TLS on this API|`string`|`<nil>`
|requiredDNAttributes|A set of required subject DN attributes. Each entry is a regular expression, and the subject certificate must have a matching attribute of the specified type (CN, C, O, OU, ST, L, STREET, POSTALCODE, SERIALNUMBER are valid attributes)|`map[string]string`|`<nil>`

## policyloop

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|interval|Deprecated: Please use 'transactions.handler.simple.interval' instead|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10s`

## policyloop.retry

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|factor|Deprecated: Please use 'transactions.handler.simple.interval' instead|[`time.Duration`](https://pkg.go.dev/time#Duration)|`2`
|initialDelay|Deprecated: Please use 'transactions.handler.simple.interval' instead|[`time.Duration`](https://pkg.go.dev/time#Duration)|`250ms`
|maxDelay|Deprecated: Please use 'transactions.handler.simple.interval' instead|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`

## transactions

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|maxHistoryCount|The number of historical status updates to retain in the operation|`int`|`50`
|maxInFlight|Deprecated: Please use 'transactions.handler.simple.maxInFlight' instead|`int`|`100`
|nonceStateTimeout|How old the most recently submitted transaction record in our local state needs to be, before we make a request to the node to query the next nonce for a signing address|[`time.Duration`](https://pkg.go.dev/time#Duration)|`1h`

## transactions.handler

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|name|The name of the transaction handler to use|`string`|`<nil>`

## transactions.handler.simple

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|fixedGasPrice|A fixed gasPrice value/structure to pass to the connector|Raw JSON|`<nil>`
|interval|Interval at which to invoke the transaction handler loop to evaluate outstanding transactions|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10s`
|maxInFlight|The maximum number of transactions to have in-flight with the transaction handler / blockchain transaction pool|`int`|`100`
|resubmitInterval|The time between warning and re-sending a transaction (same nonce) when a blockchain transaction has not been allocated a receipt|[`time.Duration`](https://pkg.go.dev/time#Duration)|`5m`

## transactions.handler.simple.gasOracle

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|connectionTimeout|The maximum amount of time that a connection is allowed to remain with no data transmitted|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|expectContinueTimeout|See [ExpectContinueTimeout in the Go docs](https://pkg.go.dev/net/http#Transport)|[`time.Duration`](https://pkg.go.dev/time#Duration)|`1s`
|headers|Adds custom headers to HTTP requests|`map[string]string`|`<nil>`
|idleTimeout|The max duration to hold a HTTP keepalive connection between calls|[`time.Duration`](https://pkg.go.dev/time#Duration)|`475ms`
|maxConnsPerHost|The max number of connections, per unique hostname. Zero means no limit|`int`|`0`
|maxIdleConns|The max number of idle connections to hold pooled|`int`|`100`
|maxIdleConnsPerHost|The max number of idle connections, per unique hostname. Zero means net/http uses the default of only 2.|`int`|`100`
|method|The HTTP Method to use when invoking the Gas Oracle REST API|`string`|`GET`
|mode|The gas oracle mode|'connector', 'restapi', 'fixed', or 'disabled'|`connector`
|passthroughHeadersEnabled|Enable passing through the set of allowed HTTP request headers|`boolean`|`false`
|queryInterval|The minimum interval between queries to the Gas Oracle|[`time.Duration`](https://pkg.go.dev/time#Duration)|`5m`
|requestTimeout|The maximum amount of time that a request is allowed to remain open|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|template|REST API Gas Oracle: A go template to execute against the result from the Gas Oracle, to create a JSON block that will be passed as the gas price to the connector|[Go Template](https://pkg.go.dev/text/template) `string`|`<nil>`
|tlsHandshakeTimeout|The maximum amount of time to wait for a successful TLS handshake|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10s`
|url|REST API Gas Oracle: The URL of a Gas Oracle REST API to call|`string`|`<nil>`

## transactions.handler.simple.gasOracle.auth

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|password|Password|`string`|`<nil>`
|username|Username|`string`|`<nil>`

## transactions.handler.simple.gasOracle.dns

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|servers|An optional list of DNS server addresses (host or host:port, port defaults to 53) to use instead of the system resolver. Setting this forces use of Go's built-in DNS resolver.|`[]string`|`<nil>`
|timeout|The dial timeout when contacting a configured DNS server|[`time.Duration`](https://pkg.go.dev/time#Duration)|`<nil>`

## transactions.handler.simple.gasOracle.net

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|cidrDenylist|A list of CIDR ranges to which outbound connections are blocked, as a core SSRF mitigation. Empty by default.|`[]string`|`<nil>`

## transactions.handler.simple.gasOracle.proxy

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|url|Optional HTTP proxy URL to use for the Gas Oracle REST API|`string`|`<nil>`

## transactions.handler.simple.gasOracle.retry

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|count|The maximum number of times to retry|`int`|`5`
|enabled|Enables retries|`boolean`|`false`
|errorStatusCodeRegex|The regex that the error response status code must match to trigger retry|`string`|`<nil>`
|initWaitTime|The initial retry delay|[`time.Duration`](https://pkg.go.dev/time#Duration)|`250ms`
|maxWaitTime|The maximum retry delay|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`

## transactions.handler.simple.gasOracle.throttle

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|burst|The maximum number of requests that can be made in a short period of time before the throttling kicks in.|`int`|`<nil>`
|requestsPerSecond|The average rate at which requests are allowed to pass through over time.|`int`|`<nil>`

## transactions.handler.simple.gasOracle.tls

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|ca|The TLS certificate authority in PEM format (this option is ignored if caFile is also set)|`string`|`<nil>`
|caFile|The path to the CA file for TLS on this API|`string`|`<nil>`
|cert|The TLS certificate in PEM format (this option is ignored if certFile is also set)|`string`|`<nil>`
|certFile|The path to the certificate file for TLS on this API|`string`|`<nil>`
|clientAuth|Enables or disables client auth for TLS on this API|`string`|`<nil>`
|enabled|Enables or disables TLS on this API|`boolean`|`false`
|insecureSkipHostVerify|When to true in unit test development environments to disable TLS verification. Use with extreme caution|`boolean`|`<nil>`
|key|The TLS certificate key in PEM format (this option is ignored if keyFile is also set)|`string`|`<nil>`
|keyFile|The path to the private key file for TLS on this API|`string`|`<nil>`
|requiredDNAttributes|A set of required subject DN attributes. Each entry is a regular expression, and the subject certificate must have a matching attribute of the specified type (CN, C, O, OU, ST, L, STREET, POSTALCODE, SERIALNUMBER are valid attributes)|`map[string]string`|`<nil>`

## transactions.handler.simple.retry

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|factor|Factor to increase the delay by, between each retry for retrieving transactions from the persistence|`float32`|`2`
|initialDelay|Initial retry delay for retrieving transactions from the persistence|[`time.Duration`](https://pkg.go.dev/time#Duration)|`250ms`
|maxDelay|Maximum delay between retries for retrieving transactions from the persistence|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`

## webhooks

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|allowPrivateIPs|Whether to allow WebHook URLs that resolve to Private IP address ranges (vs. internet addresses)|`boolean`|`true`
|connectionTimeout|The maximum amount of time that a connection is allowed to remain with no data transmitted|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|expectContinueTimeout|See [ExpectContinueTimeout in the Go docs](https://pkg.go.dev/net/http#Transport)|[`time.Duration`](https://pkg.go.dev/time#Duration)|`1s`
|headers|Adds custom headers to HTTP requests|`map[string]string`|`<nil>`
|idleTimeout|The max duration to hold a HTTP keepalive connection between calls|[`time.Duration`](https://pkg.go.dev/time#Duration)|`475ms`
|maxConnsPerHost|The max number of connections, per unique hostname. Zero means no limit|`int`|`0`
|maxIdleConns|The max number of idle connections to hold pooled|`int`|`100`
|maxIdleConnsPerHost|The max number of idle connections, per unique hostname. Zero means net/http uses the default of only 2.|`int`|`100`
|passthroughHeadersEnabled|Enable passing through the set of allowed HTTP request headers|`boolean`|`false`
|requestTimeout|The maximum amount of time that a request is allowed to remain open|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`
|tlsHandshakeTimeout|The maximum amount of time to wait for a successful TLS handshake|[`time.Duration`](https://pkg.go.dev/time#Duration)|`10s`

## webhooks.auth

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|password|Password|`string`|`<nil>`
|username|Username|`string`|`<nil>`

## webhooks.dns

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|servers|An optional list of DNS server addresses (host or host:port, port defaults to 53) to use instead of the system resolver. Setting this forces use of Go's built-in DNS resolver.|`[]string`|`<nil>`
|timeout|The dial timeout when contacting a configured DNS server|[`time.Duration`](https://pkg.go.dev/time#Duration)|`<nil>`

## webhooks.net

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|cidrDenylist|A list of CIDR ranges to which outbound connections are blocked, as a core SSRF mitigation. Empty by default.|`[]string`|`<nil>`

## webhooks.proxy

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|url|Optional HTTP proxy to use when invoking WebHooks|`string`|`<nil>`

## webhooks.retry

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|count|The maximum number of times to retry|`int`|`5`
|enabled|Enables retries|`boolean`|`false`
|errorStatusCodeRegex|The regex that the error response status code must match to trigger retry|`string`|`<nil>`
|initWaitTime|The initial retry delay|[`time.Duration`](https://pkg.go.dev/time#Duration)|`250ms`
|maxWaitTime|The maximum retry delay|[`time.Duration`](https://pkg.go.dev/time#Duration)|`30s`

## webhooks.throttle

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|burst|The maximum number of requests that can be made in a short period of time before the throttling kicks in.|`int`|`<nil>`
|requestsPerSecond|The average rate at which requests are allowed to pass through over time.|`int`|`<nil>`

## webhooks.tls

|Key|Description|Type|Default Value|
|---|-----------|----|-------------|
|ca|The TLS certificate authority in PEM format (this option is ignored if caFile is also set)|`string`|`<nil>`
|caFile|The path to the CA file for TLS on this API|`string`|`<nil>`
|cert|The TLS certificate in PEM format (this option is ignored if certFile is also set)|`string`|`<nil>`
|certFile|The path to the certificate file for TLS on this API|`string`|`<nil>`
|clientAuth|Enables or disables client auth for TLS on this API|`string`|`<nil>`
|enabled|Enables or disables TLS on this API|`boolean`|`false`
|insecureSkipHostVerify|When to true in unit test development environments to disable TLS verification. Use with extreme caution|`boolean`|`<nil>`
|key|The TLS certificate key in PEM format (this option is ignored if keyFile is also set)|`string`|`<nil>`
|keyFile|The path to the private key file for TLS on this API|`string`|`<nil>`
|requiredDNAttributes|A set of required subject DN attributes. Each entry is a regular expression, and the subject certificate must have a matching attribute of the specified type (CN, C, O, OU, ST, L, STREET, POSTALCODE, SERIALNUMBER are valid attributes)|`map[string]string`|`<nil>`