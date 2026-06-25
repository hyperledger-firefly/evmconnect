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
	"fmt"
	"math/big"
	"testing"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-signer/pkg/ethtypes"
	"github.com/stretchr/testify/require"
)

const sampleBlock = `{
	"number": "0xe5f2",
	"hash": "0xd33367228e0a0e3667c910c7d92d3f6e724e2b6e2f671b28823a22f82597d023",
	"mixHash": "0x63746963616c2062797a616e74696e65206661756c7420746f6c6572616e6365",
	"parentHash": "0x78ec31452f053f75665033d4957b8b33283c55e1c7239dc5facbb684a866492e",
	"nonce": "0x0000000000000000",
	"sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
	"logsBloom": "0x00000000000000000000000000000000000000000000000000000080000100002000000000000000000000000000000000000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000010008000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020200000000000000204000000000000000000000000000002000000000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000404000000000000000",
	"transactionsRoot": "0x77c428651debcc42d181d9067386d807b204235ebb3e45dfa5f1e6e71c0b67de",
	"stateRoot": "0xaebe1ab7b242b261b1aced1a1ed729c3d6436634ff656efecad4609b38ee63a3",
	"receiptsRoot": "0x0946e4baf57a46888aa1a78dc974a01a56bb221ae8d6ee381942695596f976ec",
	"miner": "0xef5880ec6b859b6949d88ddbd6ec18e96d0f14aa",
	"difficulty": "0x1",
	"totalDifficulty": "0xe5f3",
	"extraData": "0xf87ea00000000000000000000000000000000000000000000000000000000000000000d594ef5880ec6b859b6949d88ddbd6ec18e96d0f14aac080f843b8410469b43f8d873e7b3c54c0d2a606a37af533994653f077b234a563f00723641a7ad56e6ba07d56fb38ec68bc9b6234d54fe62730f41118658a12c886fb783c3100",
	"baseFeePerGas": "0x0",
	"size": "0x3dc",
	"gasLimit": "0x2fefd800",
	"gasUsed": "0x197b8",
	"timestamp": "0x6849f937",
	"uncles": [],
	"transactions": ["0x6431a7fc56e24319bb431ed3040d77d1a7b54add9207266c19df6fc53961da99", "0xa4dd8fc1be327a13c8f5be7b74331351c419fa8b908ff7277786270ebdf2a875"]
}`

const sampleBlockHeadersOnly = `{
	"number": "0xe5f2",
	"hash": "0xd33367228e0a0e3667c910c7d92d3f6e724e2b6e2f671b28823a22f82597d023",
	"mixHash": "0x63746963616c2062797a616e74696e65206661756c7420746f6c6572616e6365",
	"parentHash": "0x78ec31452f053f75665033d4957b8b33283c55e1c7239dc5facbb684a866492e",
	"nonce": "0x0000000000000000",
	"sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
	"logsBloom": "0x00000000000000000000000000000000000000000000000000000080000100002000000000000000000000000000000000000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000010008000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020200000000000000204000000000000000000000000000002000000000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000404000000000000000",
	"transactionsRoot": "0x77c428651debcc42d181d9067386d807b204235ebb3e45dfa5f1e6e71c0b67de",
	"stateRoot": "0xaebe1ab7b242b261b1aced1a1ed729c3d6436634ff656efecad4609b38ee63a3",
	"receiptsRoot": "0x0946e4baf57a46888aa1a78dc974a01a56bb221ae8d6ee381942695596f976ec",
	"miner": "0xef5880ec6b859b6949d88ddbd6ec18e96d0f14aa",
	"difficulty": "0x1",
	"totalDifficulty": "0xe5f3",
	"extraData": "0xf87ea00000000000000000000000000000000000000000000000000000000000000000d594ef5880ec6b859b6949d88ddbd6ec18e96d0f14aac080f843b8410469b43f8d873e7b3c54c0d2a606a37af533994653f077b234a563f00723641a7ad56e6ba07d56fb38ec68bc9b6234d54fe62730f41118658a12c886fb783c3100",
	"baseFeePerGas": "0x0",
	"size": "0x3dc",
	"gasLimit": "0x2fefd800",
	"gasUsed": "0x197b8",
	"timestamp": "0x6849f937",
	"uncles": ["d07bb65030d70714e250d0917bd4e8ebecc4222b3b520d72f7229e43c2395108"]
}`

const sampleTransaction = `{
	"blockHash": "0xd33367228e0a0e3667c910c7d92d3f6e724e2b6e2f671b28823a22f82597d023",
	"blockNumber": "0xe5f2",
	"chainId": "0x3accd8b",
	"from": "0x03a85df677b2aa0f7cccc942242ee900de505ce8",
	"gas": "0x131dc",
	"gasPrice": "0x0",
	"hash": "0x6431a7fc56e24319bb431ed3040d77d1a7b54add9207266c19df6fc53961da99",
	"input": "0xa9059cbb000000000000000000000000af5ce0b6c5745e49b4292794496bf2a08b97608b00000000000000000000000000000000000000000000000246ddf97976680000",
	"nonce": "0x2",
	"to": "0xaa75b5001274491c0985ba1012b09dfc02d9675d",
	"transactionIndex": "0x0",
	"type": "0x0",
	"value": "0x0",
	"v": "0x7599b39",
	"r": "0xc98b60f2aac8ed46f7c0ae9c0f80a9aa2aac1567f19fc535326cd71ae818c825",
	"s": "0x749f803a174f2aaedd44299ad24c92a5842219ea44724928af486bdd6c6c3cc3"
}`

const sampleReceipt = `{
	"blockHash": "0xd33367228e0a0e3667c910c7d92d3f6e724e2b6e2f671b28823a22f82597d023",
	"blockNumber": "0xe5f2",
	"contractAddress": null,
	"cumulativeGasUsed": "0xcbe8",
	"from": "0x03a85df677b2aa0f7cccc942242ee900de505ce8",
	"gasUsed": "0xcbe8",
	"effectiveGasPrice": "0x0",
	"logs": [
		{
			"address": "0xaa75b5001274491c0985ba1012b09dfc02d9675d",
			"topics": [
				"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
				"0x00000000000000000000000003a85df677b2aa0f7cccc942242ee900de505ce8",
				"0x000000000000000000000000af5ce0b6c5745e49b4292794496bf2a08b97608b"
			],
			"data": "0x00000000000000000000000000000000000000000000000246ddf97976680000",
			"blockNumber": "0xe5f2",
			"transactionHash": "0x6431a7fc56e24319bb431ed3040d77d1a7b54add9207266c19df6fc53961da99",
			"transactionIndex": "0x0",
			"blockHash": "0xd33367228e0a0e3667c910c7d92d3f6e724e2b6e2f671b28823a22f82597d023",
			"logIndex": "0x0",
			"removed": false
		}
	],
	"logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020200000000000000204000000000000000000000000000002000000000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000404000000000000000",
	"status": "0x1",
	"to": "0xaa75b5001274491c0985ba1012b09dfc02d9675d",
	"transactionHash": "0x6431a7fc56e24319bb431ed3040d77d1a7b54add9207266c19df6fc53961da99",
	"transactionIndex": "0x0",
	"type": "0x0"
}`

func TestFormatTransaction(t *testing.T) {
	var txInfo TxInfoJSONRPC
	err := json.Unmarshal([]byte(sampleTransaction), &txInfo)
	require.NoError(t, err)

	jss := testJSONSerializationSet(t, "number=hex&pretty=true")

	ethSerialized, err := txInfo.MarshalFormat(jss)
	fmt.Println((string)(ethSerialized))
	require.NoError(t, err)
	require.JSONEq(t, sampleTransaction, string(ethSerialized))
}

func TestFormatReceipt(t *testing.T) {
	var receipt TxReceiptJSONRPC
	err := json.Unmarshal([]byte(sampleReceipt), &receipt)
	require.NoError(t, err)

	jss := testJSONSerializationSet(t, "number=hex&pretty=true")

	ethSerialized, err := receipt.MarshalFormat(jss)
	fmt.Println((string)(ethSerialized))
	require.NoError(t, err)
	require.JSONEq(t, sampleReceipt, string(ethSerialized))
}

func TestFormatReceiptRevertReasonAndFormatVariation(t *testing.T) {
	largeInt, _ := new(big.Int).SetString("12300000000000000000000", 10)
	receipt := &TxReceiptJSONRPC{
		BlockHash:       ethtypes.MustNewHexBytes0xPrefix("0x3ef1ef8a761284b782eb1e7db3e42bbba3fe2626e5faaadb8ae94dfda8d2f4ca"),
		BlockNumber:     10001,
		ContractAddress: ethtypes.MustNewAddress("0x4a77dbf4e2ebec9d7dbb6e44fec7b5857128969c"),
		RevertReason:    ethtypes.MustNewHexBytes0xPrefix("0xfeedbeef"),
		LogsBloom:       ethtypes.MustNewHexBytes0xPrefix("0x000011112222"), // to be redacted
		GasUsed:         (*ethtypes.HexInteger)(largeInt),
	}

	jss := testJSONSerializationSet(t, "number=json-number&bytes=base64&address=checksum")

	ethSerialized, err := receipt.MarshalFormat(jss, MarshalOption{
		RedactFields: []string{"logsBloom"},
	})
	fmt.Println((string)(ethSerialized))
	require.NoError(t, err)
	require.JSONEq(t, `{
		"blockHash": "PvHvinYShLeC6x59s+Qru6P+Jibl+qrbiulN/ajS9Mo=",
		"blockNumber": 10001,
		"contractAddress": "0x4a77dBf4e2eBeC9d7dbB6E44FeC7B5857128969C",
		"cumulativeGasUsed": null,
		"effectiveGasPrice": null,
		"from": null,
		"gasUsed": 12300000000000000000000,
		"logs": [],
		"revertReason": "/u2+7w==",
		"status": null,
		"to": null,
		"transactionHash": null,
		"transactionIndex": 0,
		"type": null
	}`, string(ethSerialized))

}

func TestFormatBlockInfo(t *testing.T) {
	var blockInfo BlockInfoJSONRPC
	err := json.Unmarshal([]byte(sampleBlock), &blockInfo)
	require.NoError(t, err)

	jss := testJSONSerializationSet(t, "pretty=true")

	ethSerialized, err := blockInfo.MarshalFormat(jss, MarshalOption{
		RedactFields: []string{"logsBloom"},
	})
	fmt.Println((string)(ethSerialized))
	require.NoError(t, err)
	require.JSONEq(t, `{
		"hash": "0xd33367228e0a0e3667c910c7d92d3f6e724e2b6e2f671b28823a22f82597d023",
		"number": "58866",
		"parentHash": "0x78ec31452f053f75665033d4957b8b33283c55e1c7239dc5facbb684a866492e",
		"timestamp": "1749678391",
		"transactions": [
			"0x6431a7fc56e24319bb431ed3040d77d1a7b54add9207266c19df6fc53961da99",
			"0xa4dd8fc1be327a13c8f5be7b74331351c419fa8b908ff7277786270ebdf2a875"
		]
	}`, string(ethSerialized))
}

func TestFormatBlockFullWithHashes(t *testing.T) {
	var block EVMBlockWithTxHashesJSONRPC
	err := json.Unmarshal([]byte(sampleBlock), &block)
	require.NoError(t, err)

	jss := testJSONSerializationSet(t, "number=hex&pretty=true")

	ethSerialized, err := block.MarshalFormat(jss)
	fmt.Println((string)(ethSerialized))
	require.NoError(t, err)
	require.JSONEq(t, sampleBlock, string(ethSerialized))

	require.NotNil(t, block.ToBlockInfo(true))
	require.NotNil(t, block.ToBlockInfo(true).ToMinimalBlockInfo())
	require.True(t, block.ToBlockInfo(true).Equal(block.ToBlockInfo(true)))
	require.True(t, block.ToBlockInfo(true).ToMinimalBlockInfo().Equal(block.ToBlockInfo(true).ToMinimalBlockInfo()))
	require.True(t, block.ToBlockInfo(true).SupportsEIP1559())
	require.Equal(t, block.BaseFeePerGas, block.ToBlockInfo(true).BaseFeePerGas)
	require.Nil(t, (*EVMBlockWithTxHashesJSONRPC)(nil).ToBlockInfo(true))
	require.False(t, block.ToBlockInfo(true).ToMinimalBlockInfo().IsParentOf(block.ToBlockInfo(true).ToMinimalBlockInfo()))
}

func TestFormatBlockFullWithTxns(t *testing.T) {
	var block EVMBlockWithTransactionsJSONRPC
	err := json.Unmarshal([]byte(sampleBlockHeadersOnly), &block)
	require.NoError(t, err)
	var txn TxInfoJSONRPC
	err = json.Unmarshal([]byte(sampleTransaction), &txn)
	require.NoError(t, err)
	block.Transactions = []*TxInfoJSONRPC{&txn}

	jss := testJSONSerializationSet(t, "pretty=true")

	ethSerialized, err := block.MarshalFormat(jss)
	fmt.Println((string)(ethSerialized))
	require.NoError(t, err)

	var genericMap map[string]any
	err = json.Unmarshal(ethSerialized, &genericMap)
	require.NoError(t, err)

	require.Equal(t, "58866", genericMap["transactions"].([]any)[0].(map[string]any)["blockNumber"])

	require.NotNil(t, block.ToBlockInfo(true))
	require.Nil(t, (*EVMBlockWithTransactionsJSONRPC)(nil).ToBlockInfo(true))

}

func TestBlockInfoSupportsEIP1559(t *testing.T) {
	require.False(t, (*BlockInfoJSONRPC)(nil).SupportsEIP1559())
	require.False(t, (&BlockInfoJSONRPC{}).SupportsEIP1559())
	require.True(t, (&BlockInfoJSONRPC{BaseFeePerGas: ethtypes.NewHexInteger64(0)}).SupportsEIP1559())
}

func TestBlockHeaderToBlockInfo(t *testing.T) {
	header := BlockHeaderJSONRPC{
		Number:        100,
		Hash:          ethtypes.MustNewHexBytes0xPrefix("0x0101010101010101010101010101010101010101010101010101010101010101"),
		ParentHash:    ethtypes.MustNewHexBytes0xPrefix("0x0202020202020202020202020202020202020202020202020202020202020202"),
		Timestamp:     12345,
		GasLimit:      ethtypes.NewHexInteger64(30000000),
		BaseFeePerGas: ethtypes.NewHexInteger64(7),
		LogsBloom:     ethtypes.MustNewHexBytes0xPrefix("0x000011112222"),
	}

	withBloom := header.ToBlockInfo(true)
	require.Equal(t, header.GasLimit, withBloom.GasLimit)
	require.Equal(t, header.BaseFeePerGas, withBloom.BaseFeePerGas)
	require.Equal(t, header.LogsBloom, withBloom.LogsBloom)
	require.True(t, withBloom.SupportsEIP1559())

	withoutBloom := header.ToBlockInfo(false)
	require.Equal(t, header.BaseFeePerGas, withoutBloom.BaseFeePerGas)
	require.Empty(t, withoutBloom.LogsBloom)

	legacyHeader := BlockHeaderJSONRPC{
		Number:   99,
		GasLimit: ethtypes.NewHexInteger64(8000000),
	}
	legacyBlock := legacyHeader.ToBlockInfo(false)
	require.Equal(t, legacyHeader.GasLimit, legacyBlock.GasLimit)
	require.Nil(t, legacyBlock.BaseFeePerGas)
	require.False(t, legacyBlock.SupportsEIP1559())

	var fullBlock EVMBlockWithTransactionsJSONRPC
	err := json.Unmarshal([]byte(sampleBlockHeadersOnly), &fullBlock)
	require.NoError(t, err)
	fullBlock.Transactions = []*TxInfoJSONRPC{}
	blockInfo := fullBlock.ToBlockInfo(false)
	require.Equal(t, fullBlock.GasLimit, blockInfo.GasLimit)
	require.Equal(t, fullBlock.BaseFeePerGas, blockInfo.BaseFeePerGas)
}

func TestBlockInfoIsParent(t *testing.T) {
	bi1 := &BlockInfoJSONRPC{
		Number:     1000,
		Hash:       ethtypes.MustNewHexBytes0xPrefix("86fc698428f38c8ac858ecef0380ee0a0b600488dc2225f88b2e91629c8e7090"),
		ParentHash: ethtypes.MustNewHexBytes0xPrefix("0229ad47aeeaac6be351dc11054a3823a2ea36ef9eda34e1560dea8573f32121"),
	}
	bi2 := &BlockInfoJSONRPC{
		Number:     1001,
		Hash:       ethtypes.MustNewHexBytes0xPrefix("85fabf1197d73685fbb8f167334affcc36ceb725a37977dc5ef6eed8a8f585b1"),
		ParentHash: ethtypes.MustNewHexBytes0xPrefix("86fc698428f38c8ac858ecef0380ee0a0b600488dc2225f88b2e91629c8e7090"),
	}
	require.True(t, bi1.IsParentOf(bi2))
	require.False(t, bi2.IsParentOf(bi1))
}

func TestABIDocumented(t *testing.T) {
	ffapi.CheckObjectDocumented(&TxReceiptJSONRPC{})
	ffapi.CheckObjectDocumented(&TxInfoJSONRPC{})
	ffapi.CheckObjectDocumented(&LogFilterJSONRPC{})
	ffapi.CheckObjectDocumented(&LogJSONRPC{})
	ffapi.CheckObjectDocumented(&BlockInfoJSONRPC{})
	ffapi.CheckObjectDocumented(&BlockHeaderJSONRPC{})
	ffapi.CheckObjectDocumented(&EVMBlockWithTxHashesJSONRPC{})
	ffapi.CheckObjectDocumented(&EVMBlockWithTransactionsJSONRPC{})
}
