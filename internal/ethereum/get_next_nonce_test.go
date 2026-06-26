// Copyright © 2022 Kaleido, Inc.
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

package ethereum

import (
	"encoding/json"
	"testing"

	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const sampleGetNextNonce = `{
	"ffcapi": {
		"version": "v1.0.0",
		"id": "904F177C-C790-4B01-BDF4-F2B4E52E607E",
		"type": "get_next_nonce"
	},
	"signer": "0x302259069aaa5b10dc6f29a9a3f72a8e52837cc3"
}`

func TestGetNextNonceOK(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionCount", "0x302259069aaa5b10dc6f29a9a3f72a8e52837cc3", "pending").
		Return(nil).
		Run(func(args mock.Arguments) {
			args[1].(*ethtypes.HexInteger).BigInt().SetString("12345", 10)
		})

	var req ffcapi.NextNonceForSignerRequest
	err := json.Unmarshal([]byte(sampleGetNextNonce), &req)
	assert.NoError(t, err)
	res, reason, err := c.NextNonceForSigner(ctx, &req)
	assert.NoError(t, err)
	assert.Empty(t, reason)

	assert.Equal(t, int64(12345), res.Nonce.Int64())

}

func TestGetNextNonceFail(t *testing.T) {

	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "eth_getTransactionCount", "0x302259069aaa5b10dc6f29a9a3f72a8e52837cc3", "pending").
		Return(&rpcbackend.RPCError{Message: "pop"})

	var req ffcapi.NextNonceForSignerRequest
	err := json.Unmarshal([]byte(sampleGetNextNonce), &req)
	assert.NoError(t, err)
	res, reason, err := c.NextNonceForSigner(ctx, &req)
	assert.Regexp(t, "pop", err)
	assert.Empty(t, reason)
	assert.Nil(t, res)

}
