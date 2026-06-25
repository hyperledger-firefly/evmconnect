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

package ethrpc

import (
	"encoding/json"
	"math/big"

	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-signer/pkg/ethtypes"
)

// TxReceiptJSONRPC is the receipt obtained over JSON/RPC from the ethereum client, with gas used, logs and contract address
type TxReceiptJSONRPC struct {
	TransactionHash   ethtypes.HexBytes0xPrefix `json:"transactionHash" ffstruct:"TxReceiptJSONRPC"`
	TransactionIndex  ethtypes.HexUint64        `json:"transactionIndex" ffstruct:"TxReceiptJSONRPC"`
	BlockHash         ethtypes.HexBytes0xPrefix `json:"blockHash" ffstruct:"TxReceiptJSONRPC"`
	BlockNumber       ethtypes.HexUint64        `json:"blockNumber" ffstruct:"TxReceiptJSONRPC"`
	From              *ethtypes.Address0xHex    `json:"from" ffstruct:"TxReceiptJSONRPC"`
	To                *ethtypes.Address0xHex    `json:"to" ffstruct:"TxReceiptJSONRPC"`
	CumulativeGasUsed *ethtypes.HexInteger      `json:"cumulativeGasUsed" ffstruct:"TxReceiptJSONRPC"`
	EffectiveGasPrice *ethtypes.HexInteger      `json:"effectiveGasPrice" ffstruct:"TxReceiptJSONRPC"`
	GasUsed           *ethtypes.HexInteger      `json:"gasUsed" ffstruct:"TxReceiptJSONRPC"`
	ContractAddress   *ethtypes.Address0xHex    `json:"contractAddress" ffstruct:"TxReceiptJSONRPC"`
	Logs              []*LogJSONRPC             `json:"logs" ffstruct:"TxReceiptJSONRPC"`
	LogsBloom         ethtypes.HexBytes0xPrefix `json:"logsBloom" ffstruct:"TxReceiptJSONRPC"`
	Type              *ethtypes.HexUint64       `json:"type" ffstruct:"TxReceiptJSONRPC"`
	Status            *ethtypes.HexUint64       `json:"status" ffstruct:"TxReceiptJSONRPC"`
	RevertReason      ethtypes.HexBytes0xPrefix `json:"revertReason" ffstruct:"TxReceiptJSONRPC"`
}

func (txr *TxReceiptJSONRPC) MarshalFormat(jss *JSONSerializerSet, opts ...MarshalOption) (jb json.RawMessage, err error) {
	logsArray := make([]json.RawMessage, len(txr.Logs))
	for i, l := range txr.Logs {
		if err == nil {
			logsArray[i], err = l.MarshalFormat(jss, opts...)
		}
	}
	if err == nil {
		jb, err = jss.MarshalFormattedMap(map[string]any{
			"transactionHash":   ([]byte)(txr.TransactionHash),
			"transactionIndex":  (*uint64)(&txr.TransactionIndex),
			"blockHash":         ([]byte)(txr.BlockHash),
			"blockNumber":       (*uint64)(&txr.BlockNumber),
			"from":              (*[20]byte)(txr.From),
			"to":                (*[20]byte)(txr.To),
			"cumulativeGasUsed": (*big.Int)(txr.CumulativeGasUsed),
			"effectiveGasPrice": (*big.Int)(txr.EffectiveGasPrice),
			"gasUsed":           (*big.Int)(txr.GasUsed),
			"contractAddress":   (*[20]byte)(txr.ContractAddress),
			"logs":              logsArray,
			"logsBloom":         ([]byte)(txr.LogsBloom),
			"status":            (*uint64)(txr.Status),
			"type":              (*uint64)(txr.Type),
			"revertReason":      ([]byte)(txr.RevertReason),
		}, append(opts, MarshalOption{
			OmitNullFields: []string{"revertReason"},
		})...)
	}
	return jb, err
}

// TxInfoJSONRPC is the transaction info obtained over JSON/RPC from the ethereum client, with input data
type TxInfoJSONRPC struct {
	BlockHash            ethtypes.HexBytes0xPrefix `json:"blockHash" ffstruct:"TxInfoJSONRPC"`   // null if pending
	BlockNumber          ethtypes.HexUint64        `json:"blockNumber" ffstruct:"TxInfoJSONRPC"` // null if pending
	ChainID              *ethtypes.HexInteger      `json:"chainId" ffstruct:"TxInfoJSONRPC"`
	From                 *ethtypes.Address0xHex    `json:"from" ffstruct:"TxInfoJSONRPC"`
	Gas                  *ethtypes.HexInteger      `json:"gas" ffstruct:"TxInfoJSONRPC"`
	GasPrice             *ethtypes.HexInteger      `json:"gasPrice" ffstruct:"TxInfoJSONRPC"`
	MaxFeePerGas         *ethtypes.HexInteger      `json:"maxFeePerGas" ffstruct:"TxInfoJSONRPC"`
	MaxPriorityFeePerGas *ethtypes.HexInteger      `json:"maxPriorityFeePerGas" ffstruct:"TxInfoJSONRPC"`
	Hash                 ethtypes.HexBytes0xPrefix `json:"hash" ffstruct:"TxInfoJSONRPC"`
	Input                ethtypes.HexBytes0xPrefix `json:"input" ffstruct:"TxInfoJSONRPC"`
	Nonce                *ethtypes.HexInteger      `json:"nonce" ffstruct:"TxInfoJSONRPC"`
	To                   *ethtypes.Address0xHex    `json:"to" ffstruct:"TxInfoJSONRPC"`
	TransactionIndex     *ethtypes.HexUint64       `json:"transactionIndex" ffstruct:"TxInfoJSONRPC"` // null if pending
	Type                 *ethtypes.HexUint64       `json:"type" ffstruct:"TxInfoJSONRPC"`
	Value                *ethtypes.HexInteger      `json:"value" ffstruct:"TxInfoJSONRPC"`
	V                    *ethtypes.HexInteger      `json:"v" ffstruct:"TxInfoJSONRPC"`
	R                    *ethtypes.HexInteger      `json:"r" ffstruct:"TxInfoJSONRPC"`
	S                    *ethtypes.HexInteger      `json:"s" ffstruct:"TxInfoJSONRPC"`
}

func (txi *TxInfoJSONRPC) MarshalFormat(jss *JSONSerializerSet, opts ...MarshalOption) (_ json.RawMessage, err error) {
	optsWithNulls := make([]MarshalOption, 0, len(opts)+1)
	optsWithNulls = append(optsWithNulls, MarshalOption{OmitNullFields: []string{"maxFeePerGas", "maxPriorityFeePerGas", "gasPrice"}})
	optsWithNulls = append(optsWithNulls, opts...)
	return jss.MarshalFormattedMap(map[string]any{
		"blockHash":            ([]byte)(txi.BlockHash),
		"blockNumber":          (*uint64)(&txi.BlockNumber),
		"chainId":              (*big.Int)(txi.ChainID),
		"from":                 (*[20]byte)(txi.From),
		"gas":                  (*big.Int)(txi.Gas),
		"gasPrice":             (*big.Int)(txi.GasPrice),
		"maxFeePerGas":         (*big.Int)(txi.MaxFeePerGas),
		"maxPriorityFeePerGas": (*big.Int)(txi.MaxPriorityFeePerGas),
		"hash":                 ([]byte)(txi.Hash),
		"input":                ([]byte)(txi.Input),
		"nonce":                (*big.Int)(txi.Nonce),
		"to":                   (*[20]byte)(txi.To),
		"transactionIndex":     (*uint64)(txi.TransactionIndex),
		"type":                 (*uint64)(txi.Type),
		"value":                (*big.Int)(txi.Value),
		"v":                    (*big.Int)(txi.V),
		"r":                    (*big.Int)(txi.R),
		"s":                    (*big.Int)(txi.S),
	}, optsWithNulls...)
}

// See https://ethereum.org/hr/developers/docs/apis/json-rpc/#eth_newfilter
// The address, as well as the entries in the topic array, can be DATA|Array.
// We just use array in all cases.
type LogFilterJSONRPC struct {
	FromBlock *ethtypes.HexInteger          `json:"fromBlock,omitempty" ffstruct:"LogFilterJSONRPC"`
	ToBlock   *ethtypes.HexInteger          `json:"toBlock,omitempty" ffstruct:"LogFilterJSONRPC"`
	Address   []*ethtypes.Address0xHex      `json:"address,omitempty" ffstruct:"LogFilterJSONRPC"`
	Topics    [][]ethtypes.HexBytes0xPrefix `json:"topics,omitempty" ffstruct:"LogFilterJSONRPC"`
}

type LogJSONRPC struct {
	Removed          bool                        `json:"removed" ffstruct:"LogJSONRPC"`
	LogIndex         ethtypes.HexUint64          `json:"logIndex" ffstruct:"LogJSONRPC"`
	TransactionIndex ethtypes.HexUint64          `json:"transactionIndex" ffstruct:"LogJSONRPC"`
	BlockNumber      ethtypes.HexUint64          `json:"blockNumber" ffstruct:"LogJSONRPC"`
	TransactionHash  ethtypes.HexBytes0xPrefix   `json:"transactionHash" ffstruct:"LogJSONRPC"`
	BlockHash        ethtypes.HexBytes0xPrefix   `json:"blockHash" ffstruct:"LogJSONRPC"`
	Address          *ethtypes.Address0xHex      `json:"address" ffstruct:"LogJSONRPC"`
	Data             ethtypes.HexBytes0xPrefix   `json:"data" ffstruct:"LogJSONRPC"`
	Topics           []ethtypes.HexBytes0xPrefix `json:"topics" ffstruct:"LogJSONRPC"`
}

func (l *LogJSONRPC) MarshalFormat(jss *JSONSerializerSet, opts ...MarshalOption) (_ json.RawMessage, err error) {
	topicsArray := make([]any, len(l.Topics))
	for i, t := range l.Topics {
		topicsArray[i] = ([]byte)(t)
	}
	return jss.MarshalFormattedMap(map[string]any{
		"removed":          l.Removed,
		"logIndex":         (*uint64)(&l.LogIndex),
		"transactionIndex": (*uint64)(&l.TransactionIndex),
		"blockNumber":      (*uint64)(&l.BlockNumber),
		"transactionHash":  ([]byte)(l.TransactionHash),
		"blockHash":        ([]byte)(l.BlockHash),
		"address":          (*[20]byte)(l.Address),
		"data":             ([]byte)(l.Data),
		"topics":           topicsArray,
	}, opts...)
}

// BlockInfoJSONRPC are the info fields we parse from the JSON/RPC response, and cache
type BlockInfoJSONRPC struct {
	Number        ethtypes.HexUint64          `json:"number" ffstruct:"BlockInfoJSONRPC"`
	Hash          ethtypes.HexBytes0xPrefix   `json:"hash" ffstruct:"BlockInfoJSONRPC"`
	ParentHash    ethtypes.HexBytes0xPrefix   `json:"parentHash" ffstruct:"BlockInfoJSONRPC"`
	Timestamp     ethtypes.HexUint64          `json:"timestamp" ffstruct:"BlockInfoJSONRPC"`
	GasLimit      *ethtypes.HexInteger        `json:"gasLimit" ffstruct:"BlockInfoJSONRPC"`
	BaseFeePerGas *ethtypes.HexInteger        `json:"baseFeePerGas,omitempty" ffstruct:"BlockInfoJSONRPC"`
	LogsBloom     ethtypes.HexBytes0xPrefix   `json:"logsBloom" ffstruct:"BlockInfoJSONRPC"`
	Transactions  []ethtypes.HexBytes0xPrefix `json:"transactions" ffstruct:"BlockInfoJSONRPC"`
}

func (bi *BlockInfoJSONRPC) MarshalFormat(jss *JSONSerializerSet, opts ...MarshalOption) (_ json.RawMessage, err error) {
	txnArray := make([]any, len(bi.Transactions))
	for i, t := range bi.Transactions {
		txnArray[i] = ([]byte)(t)
	}
	return jss.MarshalFormattedMap(map[string]any{
		"number":       (*uint64)(&bi.Number),
		"hash":         ([]byte)(bi.Hash),
		"parentHash":   ([]byte)(bi.ParentHash),
		"timestamp":    (*uint64)(&bi.Timestamp),
		"logsBloom":    ([]byte)(bi.LogsBloom),
		"transactions": txnArray,
	}, opts...)
}

func (bi *BlockInfoJSONRPC) Equal(bi2 *BlockInfoJSONRPC) bool {
	return bi.Hash.Equals(bi2.Hash) &&
		bi.ParentHash.Equals(bi2.ParentHash) &&
		bi.Number == bi2.Number
}

func (bi *BlockInfoJSONRPC) IsParentOf(other *BlockInfoJSONRPC) bool {
	return bi.Hash.Equals(other.ParentHash) && (bi.Number.Uint64()+1) == other.Number.Uint64()
}

// SupportsEIP1559 returns true if the block includes baseFeePerGas (EIP-1559 / post-London).
func (bi *BlockInfoJSONRPC) SupportsEIP1559() bool {
	return bi != nil && bi.BaseFeePerGas != nil
}

type MinimalBlockInfo struct { // duplicate of apitypes.Confirmation due to circular dependency
	BlockNumber fftypes.FFuint64          `json:"blockNumber"`
	BlockHash   ethtypes.HexBytes0xPrefix `json:"blockHash"`
	ParentHash  ethtypes.HexBytes0xPrefix `json:"parentHash"`
}

func (bi *BlockInfoJSONRPC) ToMinimalBlockInfo() *MinimalBlockInfo {
	return &MinimalBlockInfo{
		BlockNumber: fftypes.FFuint64(bi.Number.Uint64()),
		BlockHash:   bi.Hash,
		ParentHash:  bi.ParentHash,
	}
}

func (c *MinimalBlockInfo) Equal(other *MinimalBlockInfo) bool {
	return c.BlockNumber == other.BlockNumber && c.BlockHash.Equals(other.BlockHash) && c.ParentHash.Equals(other.ParentHash)
}

func (c *MinimalBlockInfo) IsParentOf(other *MinimalBlockInfo) bool {
	return c.BlockHash.Equals(other.ParentHash) && c.BlockNumber+1 == other.BlockNumber
}

type BlockHeaderJSONRPC struct {
	Number           ethtypes.HexUint64          `json:"number" ffstruct:"BlockInfoJSONRPC"`
	Hash             ethtypes.HexBytes0xPrefix   `json:"hash" ffstruct:"BlockInfoJSONRPC"`
	MixHash          ethtypes.HexBytes0xPrefix   `json:"mixHash" ffstruct:"BlockInfoJSONRPC"`
	ParentHash       ethtypes.HexBytes0xPrefix   `json:"parentHash" ffstruct:"BlockInfoJSONRPC"`
	Nonce            ethtypes.HexBytes0xPrefix   `json:"nonce" ffstruct:"BlockInfoJSONRPC"`
	SHA3Uncles       ethtypes.HexBytes0xPrefix   `json:"sha3Uncles" ffstruct:"BlockInfoJSONRPC"`
	LogsBloom        ethtypes.HexBytes0xPrefix   `json:"logsBloom" ffstruct:"BlockInfoJSONRPC"`
	TransactionsRoot ethtypes.HexBytes0xPrefix   `json:"transactionsRoot" ffstruct:"BlockInfoJSONRPC"`
	StateRoot        ethtypes.HexBytes0xPrefix   `json:"stateRoot" ffstruct:"BlockInfoJSONRPC"`
	ReceiptsRoot     ethtypes.HexBytes0xPrefix   `json:"receiptsRoot" ffstruct:"BlockInfoJSONRPC"`
	Miner            *ethtypes.Address0xHex      `json:"miner" ffstruct:"BlockInfoJSONRPC"`
	Difficulty       *ethtypes.HexInteger        `json:"difficulty" ffstruct:"BlockInfoJSONRPC"`
	TotalDifficulty  *ethtypes.HexInteger        `json:"totalDifficulty" ffstruct:"BlockInfoJSONRPC"`
	ExtraData        ethtypes.HexBytes0xPrefix   `json:"extraData" ffstruct:"BlockInfoJSONRPC"`
	BaseFeePerGas    *ethtypes.HexInteger        `json:"baseFeePerGas" ffstruct:"BlockInfoJSONRPC"`
	Size             *ethtypes.HexInteger        `json:"size" ffstruct:"BlockInfoJSONRPC"`
	GasLimit         *ethtypes.HexInteger        `json:"gasLimit" ffstruct:"BlockInfoJSONRPC"`
	GasUsed          *ethtypes.HexInteger        `json:"gasUsed" ffstruct:"BlockInfoJSONRPC"`
	Timestamp        ethtypes.HexUint64          `json:"timestamp" ffstruct:"BlockInfoJSONRPC"`
	Uncles           []ethtypes.HexBytes0xPrefix `json:"uncles" ffstruct:"BlockInfoJSONRPC"`
}

func (b *BlockHeaderJSONRPC) getFormatMap() map[string]any {
	unclesArray := make([]any, len(b.Uncles))
	for i, uncle := range b.Uncles {
		unclesArray[i] = ([]byte)(uncle)
	}
	return map[string]any{
		"number":           (*uint64)(&b.Number),
		"hash":             ([]byte)(b.Hash),
		"mixHash":          ([]byte)(b.MixHash),
		"parentHash":       ([]byte)(b.ParentHash),
		"nonce":            ([]byte)(b.Nonce),
		"sha3Uncles":       ([]byte)(b.SHA3Uncles),
		"logsBloom":        ([]byte)(b.LogsBloom),
		"transactionsRoot": ([]byte)(b.TransactionsRoot),
		"stateRoot":        ([]byte)(b.StateRoot),
		"receiptsRoot":     ([]byte)(b.ReceiptsRoot),
		"miner":            (*[20]byte)(b.Miner),
		"difficulty":       (*big.Int)(b.Difficulty),
		"totalDifficulty":  (*big.Int)(b.TotalDifficulty),
		"extraData":        ([]byte)(b.ExtraData),
		"baseFeePerGas":    (*big.Int)(b.BaseFeePerGas),
		"size":             (*big.Int)(b.Size),
		"gasLimit":         (*big.Int)(b.GasLimit),
		"gasUsed":          (*big.Int)(b.GasUsed),
		"timestamp":        (*uint64)(&b.Timestamp),
		"uncles":           unclesArray,
	}
}

func (b *BlockHeaderJSONRPC) ToBlockInfo(includeLogsBloom bool) *BlockInfoJSONRPC {
	bi := &BlockInfoJSONRPC{
		Number:        b.Number,
		Hash:          b.Hash,
		ParentHash:    b.ParentHash,
		Timestamp:     b.Timestamp,
		GasLimit:      b.GasLimit,
		BaseFeePerGas: b.BaseFeePerGas,
	}
	if includeLogsBloom {
		bi.LogsBloom = b.LogsBloom
	}
	return bi
}

// EVMBlockWithTxHashesJSONRPC is the full JSON/RPC structure you get with "false" on eth_getBlockByNumber / eth_getBlockByHash
type EVMBlockWithTxHashesJSONRPC struct {
	BlockHeaderJSONRPC
	Transactions []ethtypes.HexBytes0xPrefix `json:"transactions" ffstruct:"BlockInfoJSONRPC"`
}

func (b *EVMBlockWithTxHashesJSONRPC) ToBlockInfo(includeLogsBloom bool) *BlockInfoJSONRPC {
	if b == nil {
		return nil
	}
	bi := b.BlockHeaderJSONRPC.ToBlockInfo(includeLogsBloom)
	bi.Transactions = b.Transactions
	return bi
}

// EVMBlockWithTransactionsJSONRPC is the full JSON/RPC structure you get with "true" on eth_getBlockByNumber / eth_getBlockByHash
type EVMBlockWithTransactionsJSONRPC struct {
	BlockHeaderJSONRPC
	Transactions []*TxInfoJSONRPC `json:"transactions" ffstruct:"BlockInfoJSONRPC"`
}

func (b *EVMBlockWithTransactionsJSONRPC) ToBlockInfo(includeLogsBloom bool) *BlockInfoJSONRPC {
	if b == nil {
		return nil
	}
	bi := b.BlockHeaderJSONRPC.ToBlockInfo(includeLogsBloom)
	bi.Transactions = make([]ethtypes.HexBytes0xPrefix, len(b.Transactions))
	for i, t := range b.Transactions {
		bi.Transactions[i] = t.Hash
	}
	return bi
}

func (b *EVMBlockWithTxHashesJSONRPC) MarshalFormat(jss *JSONSerializerSet, opts ...MarshalOption) (_ json.RawMessage, err error) {
	txnHashArray := make([]any, len(b.Transactions))
	for i, t := range b.Transactions {
		txnHashArray[i] = ([]byte)(t)
	}
	formatMap := b.BlockHeaderJSONRPC.getFormatMap()
	formatMap["transactions"] = txnHashArray
	return jss.MarshalFormattedMap(formatMap, opts...)
}

func (b *EVMBlockWithTransactionsJSONRPC) MarshalFormat(jss *JSONSerializerSet, opts ...MarshalOption) (jb json.RawMessage, err error) {
	txnsArray := make([]json.RawMessage, len(b.Transactions))
	for i, l := range b.Transactions {
		if err == nil {
			txnsArray[i], err = l.MarshalFormat(jss, opts...)
		}
	}
	if err == nil {
		formatMap := b.BlockHeaderJSONRPC.getFormatMap()
		formatMap["transactions"] = txnsArray
		jb, err = jss.MarshalFormattedMap(formatMap, opts...)
	}
	return jb, err
}
