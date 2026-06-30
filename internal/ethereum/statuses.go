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
	"github.com/hyperledger-firefly/evmconnect/pkg/etherrors"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

func (c *ethConnector) IsLive(_ context.Context) (*ffcapi.LiveResponse, ffcapi.ErrorReason, error) {
	return &ffcapi.LiveResponse{
		Up: true,
	}, "", nil
}

func (c *ethConnector) IsReady(ctx context.Context) (*ffcapi.ReadyResponse, ffcapi.ErrorReason, error) {
	err := c.backend.CallRPC(ctx, &c.chainID, "net_version")
	if err != nil {
		return &ffcapi.ReadyResponse{
			Ready: false,
		}, etherrors.MapError(etherrors.NetVersionRPCMethods, err.Error()), err.Error()
	}

	details := &fftypes.JSONObject{
		"chainID": c.chainID,
	}

	return &ffcapi.ReadyResponse{
		Ready:             true,
		DownstreamDetails: fftypes.JSONAnyPtr(details.String()),
	}, "", nil
}
