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

package etherrors

import (
	"errors"
	"testing"

	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
	"github.com/stretchr/testify/require"
)

func TestFilterRPCMethods(t *testing.T) {
	require.Equal(t, ffcapi.ErrorReasonNotFound, MapError(FilterRPCMethods, errors.New("FILTER NOT FOUND")))
	require.Equal(t, ffcapi.ErrorReasonNotFound, MapError(FilterRPCMethods, errors.New("filter not found")))
	require.Equal(t, ffcapi.ErrorReason(""), MapError(FilterRPCMethods, errors.New("another")))
}

func TestSendRPCMethods(t *testing.T) {
	require.Equal(t, ffcapi.ErrorReasonNonceTooLow, MapError(SendRPCMethods, errors.New("nonce too low")))
	require.Equal(t, ffcapi.ErrorReasonInsufficientFunds, MapError(SendRPCMethods, errors.New("insufficient funds")))
	require.Equal(t, ffcapi.ErrorReasonTransactionUnderpriced, MapError(SendRPCMethods, errors.New("transaction underpriced")))
	require.Equal(t, ffcapi.ErrorKnownTransaction, MapError(SendRPCMethods, errors.New("known transaction")))
	require.Equal(t, ffcapi.ErrorKnownTransaction, MapError(SendRPCMethods, errors.New("already known")))
	require.Equal(t, ffcapi.ErrorReason(""), MapError(SendRPCMethods, errors.New("another")))
}

func TestCallRPCMethods(t *testing.T) {
	require.Equal(t, ffcapi.ErrorReasonTransactionReverted, MapError(CallRPCMethods, errors.New("execution reverted")))
	require.Equal(t, ffcapi.ErrorReason(""), MapError(CallRPCMethods, errors.New("another")))
}

func TestBlockRPCMethods(t *testing.T) {
	require.Equal(t, ffcapi.ErrorReasonNotFound, MapError(BlockRPCMethods, errors.New("cannot query unfinalized data")))
	require.Equal(t, ffcapi.ErrorReason(""), MapError(BlockRPCMethods, errors.New("another")))
}

func TestNetVersionRPCMethods(t *testing.T) {
	require.Equal(t, ffcapi.ErrorReasonNotFound, MapError(NetVersionRPCMethods, errors.New("the method net_version does not exist/is not available")))
	require.Equal(t, ffcapi.ErrorReason(""), MapError(NetVersionRPCMethods, errors.New("another")))
}
