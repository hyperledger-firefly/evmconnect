[![codecov](https://codecov.io/gh/hyperledger-firefly/evmconnect/branch/main/graph/badge.svg?token=OEI8A08P0R)](https://codecov.io/gh/hyperledger-firefly/evmconnect)
[![Go Reference](https://pkg.go.dev/badge/github.com/hyperledger-firefly/evmconnect.svg)](https://pkg.go.dev/github.com/hyperledger-firefly/evmconnect)

# Hyperledger FireFly EVM Connector

This repo provides a reference implementation of the FireFly Connector API (FFCAPI)
for EVM Based blockchains.

See the [Hyperledger Firefly Documentation](https://hyperledger-firefly.github.io/firefly/latest/overview/public_vs_permissioned/#firefly-architecture-for-public-chains)
and the [FireFly Transaction Manager](https://github.com/hyperledger-firefly/transaction-manager) repository for
more information.

> Also see [firefly-ethconnect](https://github.com/hyperledger-firefly/ethconnect) for the hardened
> connector optimized for private Ethereum sidechains, optimized for finality assured consensus
> algorithms and throughput.

# License

Apache 2.0

## ABI Encoding

A key responsibility of the FFCAPI connector is to map from developer friendly JSON inputs/outputs
down to the binary encoding of the blockchain.

This repo uses the Apache 2.0 RLP encoding/decoding utilities from the
[firefly-signer](https://github.com/hyperledger-firefly/signer) repository.

## Configuration

For a full list of configuration options see [config.md](./config.md)

## Example configuration

```yaml
connectors:
  - type: ethereum
    server:
      port: 5102
    ethereum:
      url: http://localhost:8545
```

## Blockchain node compatibility

For EVM connector to function properly, you should check the blockchain node supports the following JSON-RPC Methods over HTTP:

### Event tracking

- `eth_blockNumber`
- `eth_newBlockFilter`
- `eth_getFilterLogs`
- `eth_getFilterChanges`
- `eth_getLogs`
- `eth_newFilter`
- `eth_uninstallFilter`
- `eth_getTransactionByHash`
- `eth_getTransactionReceipt`

### Query

- `eth_call`
- `eth_getBalance`
- `eth_gasPrice`

### Transaction submission

- `eth_estimateGas`
- `eth_sendTransaction` / `eth_sendRawTransaction`
- `eth_getTransactionCount`

### Optional methods

#### Transaction tracing

> Required when [connector.traceTXForRevertReason](./config.md#connector) is set to `true` (default: `false`)

- `debug_traceTransaction`

#### Block listener

> Required when [connector.blockTrackingMode](./config.md#connector) is set to `inMemoryPartialChain`

- `eth_getBlockByHash`
- `eth_getBlockByNumber`

#### Receipt fetching

> Required when [connector.useGetBlockReceipts](./config.md#connector) is set to `true`

- `eth_getBlockReceipts`
