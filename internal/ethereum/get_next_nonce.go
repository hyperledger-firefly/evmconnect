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

package ethereum

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

func (c *ethConnector) NextNonceForSigner(ctx context.Context, req *ffcapi.NextNonceForSignerRequest) (*ffcapi.NextNonceForSignerResponse, ffcapi.ErrorReason, error) {

	var txnCount ethtypes.HexInteger
	rpcErr := c.backend.CallRPC(ctx, &txnCount, "eth_getTransactionCount", req.Signer, "pending")
	if rpcErr != nil {
		return nil, "", rpcErr.Error()
	}

	return &ffcapi.NextNonceForSignerResponse{
		Nonce: (*fftypes.FFBigInt)(&txnCount),
	}, "", nil

}
