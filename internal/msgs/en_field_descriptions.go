// Copyright © 2026 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package msgs

import (
	"github.com/hyperledger-firefly/common/pkg/i18n"
	"golang.org/x/text/language"
)

var ffm = func(key, translation string) i18n.MessageKey {
	return i18n.FFM(language.AmericanEnglish, key, translation)
}

var (
	// https://ethereum.org/developers/docs/apis/json-rpc/#eth_gettransactionreceipt
	_ = ffm("TxReceiptJSONRPC.transactionHash", "DATA, 32 Bytes - hash of the transaction.")
	_ = ffm("TxReceiptJSONRPC.transactionIndex", "QUANTITY - integer of the transactions index position in the block.")
	_ = ffm("TxReceiptJSONRPC.blockHash", "DATA, 32 Bytes - hash of the block where this transaction was in.")
	_ = ffm("TxReceiptJSONRPC.blockNumber", "QUANTITY - block number where this transaction was in.")
	_ = ffm("TxReceiptJSONRPC.from", "DATA, 20 Bytes - address of the sender.")
	_ = ffm("TxReceiptJSONRPC.to", "DATA, 20 Bytes - address of the receiver. null when its a contract creation transaction.")
	_ = ffm("TxReceiptJSONRPC.cumulativeGasUsed", "QUANTITY - The total amount of gas used when this transaction was executed in the block.")
	_ = ffm("TxReceiptJSONRPC.effectiveGasPrice", "QUANTITY - The sum of the base fee and tip paid per unit of gas.")
	_ = ffm("TxReceiptJSONRPC.gasUsed", "QUANTITY - The amount of gas used by this specific transaction alone.")
	_ = ffm("TxReceiptJSONRPC.contractAddress", "DATA, 20 Bytes - The contract address created, if the transaction was a contract creation, otherwise null.")
	_ = ffm("TxReceiptJSONRPC.logs", "Array - Array of log objects, which this transaction generated.")
	_ = ffm("TxReceiptJSONRPC.logsBloom", "DATA, 256 Bytes - Bloom filter for light clients to quickly retrieve related logs.")
	_ = ffm("TxReceiptJSONRPC.type", "QUANTITY - integer of the transaction type, 0x0 for legacy transactions, 0x1 for access list types, 0x2 for dynamic fees.")
	_ = ffm("TxReceiptJSONRPC.status", "QUANTITY either 1 (success) or 0 (failure)")
	_ = ffm("TxReceiptJSONRPC.revertReason", "Non-standard extension: The encoded revert data for the transaction")

	// https://ethereum.org/developers/docs/apis/json-rpc/#eth_gettransactionbyhash
	_ = ffm("TxInfoJSONRPC.chainId", "QUANTITY - the chain id.")
	_ = ffm("TxInfoJSONRPC.blockHash", "DATA, 32 Bytes - hash of the block where this transaction was in. null when its pending.")
	_ = ffm("TxInfoJSONRPC.blockNumber", "QUANTITY - block number where this transaction was in. null when its pending.")
	_ = ffm("TxInfoJSONRPC.from", "DATA, 20 Bytes - address of the sender.")
	_ = ffm("TxInfoJSONRPC.gas", "QUANTITY - gas provided by the sender.")
	_ = ffm("TxInfoJSONRPC.gasPrice", "QUANTITY - gas price provided by the sender in Wei.")
	_ = ffm("TxInfoJSONRPC.hash", "DATA, 32 Bytes - hash of the transaction.")
	_ = ffm("TxInfoJSONRPC.input", "DATA - the data send along with the transaction.")
	_ = ffm("TxInfoJSONRPC.nonce", "QUANTITY - the number of transactions made by the sender prior to this one.")
	_ = ffm("TxInfoJSONRPC.to", "DATA, 20 Bytes - address of the receiver. null when its a contract creation transaction.")
	_ = ffm("TxInfoJSONRPC.transactionIndex", "QUANTITY - integer of the transactions index position in the block. null when its pending.")
	_ = ffm("TxInfoJSONRPC.value", "QUANTITY - value transferred in Wei.")
	_ = ffm("TxInfoJSONRPC.v", "QUANTITY - ECDSA recovery id")
	_ = ffm("TxInfoJSONRPC.r", "QUANTITY - ECDSA signature r")
	_ = ffm("TxInfoJSONRPC.s", "QUANTITY - ECDSA signature s")
	_ = ffm("TxInfoJSONRPC.type", "QUANTITY - integer of the transaction type, 0x0 for legacy transactions, 0x1 for access list types, 0x2 for dynamic fees.")
	// https://ethereum.org/developers/docs/transactions/#whats-a-transaction
	_ = ffm("TxInfoJSONRPC.maxPriorityFeePerGas", `QUANTITY - the maximum price of the consumed gas to be included as a tip to the validator.`)
	_ = ffm("TxInfoJSONRPC.maxFeePerGas", `QUANTITY - the maximum fee per unit of gas willing to be paid for the transaction (inclusive of baseFeePerGas and maxPriorityFeePerGas).`)

	// https://ethereum.org/developers/docs/apis/json-rpc/#eth_getlogs
	_ = ffm("LogFilterJSONRPC.fromBlock", `QUANTITY|TAG - (optional, default: "latest") Integer block number, or "latest" for the last proposed block, "safe" for the latest safe block, "finalized" for the latest finalized block, or "pending", "earliest" for transactions not yet in a block.`)
	_ = ffm("LogFilterJSONRPC.toBlock", `QUANTITY|TAG - (optional, default: "latest") Integer block number, or "latest" for the last proposed block, "safe" for the latest safe block, "finalized" for the latest finalized block, or "pending", "earliest" for transactions not yet in a block.`)
	_ = ffm("LogFilterJSONRPC.address", `DATA|Array, 20 Bytes - (optional) Contract address or a list of addresses from which logs should originate.`)
	_ = ffm("LogFilterJSONRPC.topics", `Array of DATA, - (optional) Array of 32 Bytes DATA topics. Topics are order-dependent. Each topic can also be an array of DATA with "or" options.`)

	// https://ethereum.org/developers/docs/apis/json-rpc/#eth_getfilterchanges
	_ = ffm("LogJSONRPC.removed", `TAG - true when the log was removed, due to a chain reorganization. false if its a valid log.`)
	_ = ffm("LogJSONRPC.logIndex", `QUANTITY - integer of the log index position in the block. null when its pending log.`)
	_ = ffm("LogJSONRPC.transactionIndex", `QUANTITY - integer of the transactions index position log was created from. null when its pending log.`)
	_ = ffm("LogJSONRPC.transactionHash", `DATA, 32 Bytes - hash of the transactions this log was created from. null when its pending log.`)
	_ = ffm("LogJSONRPC.blockHash", `DATA, 32 Bytes - hash of the block where this log was in. null when its pending. null when its pending log.`)
	_ = ffm("LogJSONRPC.blockNumber", `QUANTITY - the block number where this log was in. null when its pending. null when its pending log.`)
	_ = ffm("LogJSONRPC.address", `DATA, 20 Bytes - address from which this log originated.`)
	_ = ffm("LogJSONRPC.data", `DATA - variable-length non-indexed log data. (In solidity: zero or more 32 Bytes non-indexed log arguments.)`)
	_ = ffm("LogJSONRPC.topics", `Array of DATA - Array of 0 to 4 32 Bytes DATA of indexed log arguments. (In solidity: The first topic is the hash of the signature of the event (e.g., Deposit(address,bytes32,uint256)), except you declared the event with the anonymous specifier.)`)

	_ = ffm("BlockInfoJSONRPC.number", `QUANTITY - the block number. null when its pending block.`)
	_ = ffm("BlockInfoJSONRPC.hash", `DATA, 32 Bytes - hash of the block. null when its pending block.`)
	_ = ffm("BlockInfoJSONRPC.parentHash", `DATA, 32 Bytes - hash of the parent block.`)
	_ = ffm("BlockInfoJSONRPC.nonce", `DATA, 8 Bytes - hash of the generated proof-of-work. null when its pending block, 0x0 for proof-of-stake blocks (since The Merge)`)
	_ = ffm("BlockInfoJSONRPC.sha3Uncles", `DATA, 32 Bytes - SHA3 of the uncles data in the block.`)
	_ = ffm("BlockInfoJSONRPC.logsBloom", `DATA, 256 Bytes - the bloom filter for the logs of the block. null when its pending block.`)
	_ = ffm("BlockInfoJSONRPC.transactionsRoot", `DATA, 32 Bytes - the root of the transaction trie of the block.`)
	_ = ffm("BlockInfoJSONRPC.stateRoot", `DATA, 32 Bytes - the root of the final state trie of the block.`)
	_ = ffm("BlockInfoJSONRPC.receiptsRoot", `DATA, 32 Bytes - the root of the receipts trie of the block.`)
	_ = ffm("BlockInfoJSONRPC.miner", `DATA, 20 Bytes - the address of the beneficiary to whom the block rewards were given.`)
	_ = ffm("BlockInfoJSONRPC.mixHash", `DATA, a 256-bit hash encoded as a hexadecimal string.`)
	_ = ffm("BlockInfoJSONRPC.difficulty", `QUANTITY - integer of the difficulty for this block.`)
	_ = ffm("BlockInfoJSONRPC.totalDifficulty", `QUANTITY - integer of the total difficulty of the chain until this block.`)
	_ = ffm("BlockInfoJSONRPC.extraData", `DATA - the "extra data" field of this block.`)
	_ = ffm("BlockInfoJSONRPC.size", `QUANTITY - integer the size of this block in bytes.`)
	_ = ffm("BlockInfoJSONRPC.gasLimit", `QUANTITY - the maximum gas allowed in this block.`)
	_ = ffm("BlockInfoJSONRPC.gasUsed", `QUANTITY - the total used gas by all transactions in this block.`)
	_ = ffm("BlockInfoJSONRPC.timestamp", `QUANTITY - the unix timestamp for when the block was collated.`)
	_ = ffm("BlockInfoJSONRPC.transactions", `Array - Array of transaction objects, or 32 Bytes transaction hashes depending on the last given parameter.`)
	_ = ffm("BlockInfoJSONRPC.uncles", `Array - Array of uncle hashes.`)
	_ = ffm("BlockInfoJSONRPC.baseFeePerGas", `QUANTITY - the market price for gas`)
)
