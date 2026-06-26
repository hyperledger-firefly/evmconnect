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

	"github.com/hyperledger-firefly/evmconnect/pkg/ethblocklistener"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

func (c *ethConnector) GetChainTrackingMode(_ context.Context) ffcapi.ChainTrackingMode {
	return c.chainTrackingMode
}

func (c *ethConnector) NewBlockListener(_ context.Context, req *ffcapi.NewBlockListenerRequest) (*ffcapi.NewBlockListenerResponse, ffcapi.ErrorReason, error) {
	// Add the block consumer
	c.blockListener.AddConsumer(req.ListenerContext, &ethblocklistener.BlockUpdateConsumer{
		ID:      req.ID,
		Ctx:     req.ListenerContext,
		Updates: req.BlockListener,
	})

	return &ffcapi.NewBlockListenerResponse{}, "", nil
}
