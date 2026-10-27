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
	"fmt"
	"strings"
	"testing"

	"github.com/hyperledger-firefly/common/pkg/fftypes"
	"github.com/hyperledger-firefly/common/pkg/metric"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestListenerMetrics(t *testing.T) {
	esID := fftypes.NewUUID()
	l1ID := fftypes.NewUUID()
	l2ID := fftypes.NewUUID()
	c := &ethConnector{eventStreams: map[fftypes.UUID]*eventStream{}}
	es := &eventStream{id: esID, c: c, ctx: context.Background(), listeners: map[fftypes.UUID]*listener{}}
	es.listeners[*l1ID] = &listener{id: l1ID, es: es, hwmBlock: 12345, config: listenerConfig{name: "listener1"}}
	es.listeners[*l2ID] = &listener{id: l2ID, es: es, hwmBlock: 100, catchup: true, config: listenerConfig{name: "listener2"}}
	c.eventStreams[*esID] = es

	registry := metric.NewPrometheusMetricsRegistry("evmconnect")
	c.RegisterListenerMetrics(registry)

	expected := fmt.Sprintf(`
# HELP ff_eventlistener_catchup Whether the listener is in catchup mode (1) because it is more than events.catchupThreshold blocks behind the chain head, or not (0)
# TYPE ff_eventlistener_catchup gauge
ff_eventlistener_catchup{component="evmconnect",eventstream="%[1]s",listener="%[2]s",listener_name="listener1"} 0
ff_eventlistener_catchup{component="evmconnect",eventstream="%[1]s",listener="%[3]s",listener_name="listener2"} 1
# HELP ff_eventlistener_checkpoint_block The block the listener has scanned up to - the block number of its checkpoint, which trails the chain head by events.checkpointBlockGap when the listener is up to date
# TYPE ff_eventlistener_checkpoint_block gauge
ff_eventlistener_checkpoint_block{component="evmconnect",eventstream="%[1]s",listener="%[2]s",listener_name="listener1"} 12345
ff_eventlistener_checkpoint_block{component="evmconnect",eventstream="%[1]s",listener="%[3]s",listener_name="listener2"} 100
`, esID, l1ID, l2ID)
	err := testutil.GatherAndCompare(registry.GetGatherer(), strings.NewReader(expected),
		metricListenerCheckpointBlock, metricListenerCatchup)
	assert.NoError(t, err)

	// The whole stream being in catchup marks all of its listeners as in catchup
	es.catchup = true
	for _, s := range newListenerCollector(c).snapshot() {
		assert.True(t, s.catchup)
	}

	// A removed listener is no longer reported
	es.removeEventListener(l2ID)
	count, err := testutil.GatherAndCount(registry.GetGatherer(), metricListenerCheckpointBlock)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// A stopped stream is no longer reported
	delete(c.eventStreams, *esID)
	count, err = testutil.GatherAndCount(registry.GetGatherer(), metricListenerCheckpointBlock, metricListenerCatchup)
	assert.NoError(t, err)
	assert.Zero(t, count)
}
