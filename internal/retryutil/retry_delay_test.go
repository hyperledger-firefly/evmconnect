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

package retryutil

import (
	"context"
	"testing"
	"time"

	"github.com/hyperledger-firefly/common/pkg/retry"
	"github.com/stretchr/testify/require"
)

func TestRetryDelay(t *testing.T) {

	rw := &RetryWrapper{
		Retry: &retry.Retry{
			Factor:       2.0,
			MaximumDelay: 100 * time.Microsecond,
			InitialDelay: 10 * time.Microsecond,
		},
	}

	require.Equal(t, 0*time.Microsecond, rw.CalcFailureDelay(0))
	require.Equal(t, 10*time.Microsecond, rw.CalcFailureDelay(1))
	require.Equal(t, 20*time.Microsecond, rw.CalcFailureDelay(2))
	require.Equal(t, 40*time.Microsecond, rw.CalcFailureDelay(3))
	require.Equal(t, 80*time.Microsecond, rw.CalcFailureDelay(4))
	require.Equal(t, 100*time.Microsecond, rw.CalcFailureDelay(5))
	require.Equal(t, 100*time.Microsecond, rw.CalcFailureDelay(6))

	require.False(t, rw.DoFailureDelay(context.Background(), 0))
	require.False(t, rw.DoFailureDelay(context.Background(), 1))
	require.False(t, rw.DoFailureDelay(context.Background(), 10))

}

func TestRetryDelayContextTimeout(t *testing.T) {

	rw := &RetryWrapper{
		Retry: &retry.Retry{
			Factor:       2.0,
			MaximumDelay: 100 * time.Second,
			InitialDelay: 100 * time.Second,
		},
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	cancelCtx()

	require.True(t, rw.DoFailureDelay(ctx, 10))

}
