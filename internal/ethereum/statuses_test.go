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
	"testing"

	"github.com/hyperledger-firefly/signer/pkg/rpcbackend"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestIsLive(t *testing.T) {
	ctx, c, _, done := newTestConnector(t)
	defer done()

	status, _, err := c.IsLive(ctx)
	assert.NoError(t, err)
	assert.True(t, status.Up)
}

func TestIsReady(t *testing.T) {
	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "net_version").
		Run(func(args mock.Arguments) {
			*(args[1].(*string)) = "80001"
		}).
		Return(nil)

	status, _, err := c.IsReady(ctx)
	assert.NoError(t, err)
	assert.True(t, status.Ready)
	assert.NotNil(t, status.DownstreamDetails)

	details := status.DownstreamDetails.JSONObject()
	assert.Equal(t, details.GetString("chainID"), "80001")
}

func TestIsReadyError(t *testing.T) {
	ctx, c, mRPC, done := newTestConnector(t)
	defer done()

	mRPC.On("CallRPC", mock.Anything, mock.Anything, "net_version").
		Return(&rpcbackend.RPCError{Message: "the method net_version does not exist/is not available"})

	status, reason, err := c.IsReady(ctx)
	assert.Error(t, err)
	assert.Equal(t, reason, ffcapi.ErrorReasonNotFound)
	assert.False(t, status.Ready)
	assert.Nil(t, status.DownstreamDetails)
}
