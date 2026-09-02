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
	"bytes"
	"context"
	"time"

	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

// State required when doing all management of polling position client-side
type getLogsPollState struct {
	fromBlock   int64                      // the next block to poll
	polledChain []*ethrpc.BlockInfoJSONRPC // sparse ascending (number, hash) records of polled blocks in the unstable window
}

// reset (re-)establishes the poll position, discarding any recorded hash continuity
func (ps *getLogsPollState) reset(fromBlock int64) {
	ps.fromBlock = fromBlock
	ps.polledChain = nil
}

// checkReorgRewind compares the hashes recorded when we polled blocks, against the block listener's
// current canonical chain view. On a mismatch the chain has re-organized behind our poll position,
// so we rewind to the earliest diverging block to re-poll from there.
func (ps *getLogsPollState) checkReorgRewind(ctx context.Context, headChain []*ethrpc.BlockInfoJSONRPC) {
	if len(headChain) == 0 || len(ps.polledChain) == 0 {
		return
	}
	// Prune records that have aged out below the base of the monitored window - those blocks are
	// now considered stable, and we have nothing to compare them against
	baseBlock := blockNumberToInt64(headChain[0].Number.Uint64())
	firstInWindow := 0
	for firstInWindow < len(ps.polledChain) && blockNumberToInt64(ps.polledChain[firstInWindow].Number.Uint64()) < baseBlock {
		firstInWindow++
	}
	ps.polledChain = ps.polledChain[firstInWindow:]
	// Find the earliest block we polled whose hash is no longer canonical (lists are short and ordered, so linear scan is efficient)
	for i, polled := range ps.polledChain {
		polledNumber := blockNumberToInt64(polled.Number.Uint64())
		canonicalHash := blockHashInHeadChain(headChain, polledNumber)
		if canonicalHash == nil {
			continue // above the top of the current window - nothing to compare against
		}
		if !bytes.Equal(canonicalHash, polled.Hash) {
			log.L(ctx).Infof("Re-org detected at block %d (polled hash %s, now %s) - rewinding poll position from %d to %d", polledNumber, polled.Hash, canonicalHash, ps.fromBlock, polledNumber)
			ps.fromBlock = polledNumber
			ps.polledChain = ps.polledChain[:i] // records at/after the divergence are no longer valid
			return
		}
	}
}

// advance moves the poll position forwards after successfully processing blocks up to toBlock,
// recording the canonical hashes we hold for the polled range so a re-org behind the new position
// can be detected by checkReorgRewind on a later cycle.
//
// Note the hashes come from the headChain snapshot taken before the eth_getLogs query - if the
// chain re-organizes in between, the recorded hash and the queried logs can disagree, but the next
// cycle's continuity check then mismatches the new canonical view and rewinds us to re-poll.
func (ps *getLogsPollState) advance(headChain []*ethrpc.BlockInfoJSONRPC, toBlock int64) {
	for _, bi := range headChain {
		if n := blockNumberToInt64(bi.Number.Uint64()); n >= ps.fromBlock && n <= toBlock {
			ps.polledChain = append(ps.polledChain, bi)
		}
	}
	ps.fromBlock = toBlock + 1
}

// blockHashInHeadChain returns the hash of the given block number in the supplied canonical chain
// snapshot, or nil if that block number is not within the snapshot
func blockHashInHeadChain(headChain []*ethrpc.BlockInfoJSONRPC, blockNumber int64) ethtypes.HexBytes0xPrefix {
	for _, bi := range headChain {
		if blockNumberToInt64(bi.Number.Uint64()) == blockNumber {
			return bi.Hash
		}
	}
	return nil
}

// leadGroupSteadyStateGetLogs is the alternative steady state to leadGroupSteadyState, selected with
// events.filterPollingMode: client. Instead of establishing a node-side filter, we track our own
// in-memory poll position and page forwards with stateless eth_getLogs range queries.
//
// Re-org behavior depends on the chainTrackingMode:
//   - full: we poll all the way to the head of the chain. Because a re-org behind the poll position
//     would otherwise go unnoticed until restart (a node-side filter re-notifies logs on the new
//     branch, a forwards poll position does not), we record the hashes of the blocks we poll and
//     check them each cycle against the block listener's canonical chain view - see getLogsPollState.
//   - light: no block hashes are available to track, so a re-org behind the poll position would
//     permanently miss the events on the replacement blocks (nothing ever re-scans a passed
//     range). Instead a block is only polled once it is checkpointBlockGap blocks behind the
//     head: the connector's own assertion of when a block is stable, exactly as the checkpoint
//     uses it. Event confirmation remains entirely the responsibility of the FireFly Transaction
//     Manager - this gap only defines what we are safe to scan-and-forget, so operators should
//     set it to the stability depth of their chain (it bounds delivery latency in this mode).
//
// The listener HWM (scan position used for the restart checkpoint) is min(scan position, stability
// horizon) - in full mode the scan runs to the head so the checkpoint winds back to the horizon
// (checkpointBlockGap behind the head, accepting redelivery after restart in exchange for
// protection against re-orgs that happen while we are down), while in light mode the scan position
// never passes the horizon, so the checkpoint follows it exactly and restarts redeliver nothing.
func (es *eventStream) leadGroupSteadyStateGetLogs() bool {
	var ag *aggregatedListener
	lastUpdate := -1
	failCount := 0
	poll := &getLogsPollState{fromBlock: -1}
	for {
		if es.c.retry.DoFailureDelay(es.ctx, failCount) {
			log.L(es.ctx).Debugf("Stream loop exiting")
			return true
		}

		// Build the aggregated listener list if it has changed
		listenerChanged := es.buildReuseLeadGroupListener(&lastUpdate, &ag)

		caughtUpToHead := true

		// No need to poll for events, if we don't have any listeners
		if len(ag.signatureSet) > 0 {

			chainHeadBlock, ok := es.c.blockListener.GetHighestBlock(es.ctx) /* note we know we're initialized here and will not block */
			if !ok {
				log.L(es.ctx).Debugf("Stream loop exiting (closed checking block height)")
				return true
			}
			chainHead := blockNumberToInt64(chainHeadBlock)

			// (Re-)establish the poll position from the earliest listener HWM if we need to,
			// just as filter mode (re-)establishes the fromBlock of its filter
			if poll.fromBlock < 0 || listenerChanged {
				fromBlock := int64(-1)
				for _, l := range ag.listeners {
					if lHWM := l.getHWMBlock(); fromBlock < 0 || lHWM < fromBlock {
						fromBlock = lHWM
					}
				}
				poll.reset(fromBlock)
			}

			// In full chain tracking mode we poll all the way to the head, checking the blocks we
			// already polled are still canonical and rewinding our position if not.
			// In light chain tracking mode there is no canonical chain view to check against, so
			// instead we never poll a block still inside the unstable window - checkpointBlockGap
			// behind the head (see function comment).
			deliveryHead := chainHead
			var headChain []*ethrpc.BlockInfoJSONRPC
			if es.c.chainTrackingMode == ffcapi.ChainTrackingModeLight {
				deliveryHead = chainHead - es.c.checkpointBlockGap
				if deliveryHead < 0 {
					deliveryHead = 0
				}
			} else {
				headChain = es.c.blockListener.SnapshotMonitoredHeadChain()
				poll.checkReorgRewind(es.ctx, headChain)
			}

			// Check we're not outside of the steady state window, and need to fall back to catchup
			// mode. Measured against the blocks we may poll, so a checkpointBlockGap larger than
			// the threshold cannot bounce us between steady state and catchup.
			if (deliveryHead - poll.fromBlock) > es.c.catchupThreshold {
				log.L(es.ctx).Warnf("Block gap reached %d (above threshold of %d) - reverting to catchup mode", deliveryHead-poll.fromBlock, es.c.catchupThreshold)
				return false
			}

			// Poll the next page of blocks, if there are any we haven't polled yet
			toBlock := deliveryHead
			if maxToBlock := poll.fromBlock + es.c.catchupPageSize - 1; toBlock > maxToBlock {
				toBlock = maxToBlock
				caughtUpToHead = false // page again immediately, rather than waiting the polling interval
			}
			if toBlock >= poll.fromBlock {
				events, err := es.getBlockRangeEvents(es.ctx, ag, poll.fromBlock, toBlock)
				if err != nil {
					log.L(es.ctx).Errorf("Failed to query block range fromBlock=%d toBlock=%d headBlock=%d: %s", poll.fromBlock, toBlock, chainHead, err)
					failCount++
					continue
				}

				// High water mark for the restart checkpoint is min(scan position, stability horizon).
				// In full mode the scan runs to the head, so the checkpoint winds back to the horizon
				// (checkpointBlockGap behind the head, where re-orgs are not expected). In light mode
				// the poll position never passes the horizon, so the scan position is used directly.
				hwmBlock := toBlock + 1
				if es.c.chainTrackingMode != ffcapi.ChainTrackingModeLight {
					if horizon := chainHead - es.c.checkpointBlockGap; horizon < hwmBlock {
						hwmBlock = horizon
					}
					if hwmBlock < 0 {
						hwmBlock = 0
					}
				}

				// Dispatch the events
				if es.dispatchSetHWMCheckExit(ag, events, hwmBlock) {
					log.L(es.ctx).Debugf("Stream loop exiting")
					return true
				}

				// Update the head block to be the hwm block
				es.headBlock.Store(hwmBlock)

				// Advance our poll position, recording the hashes of the blocks we polled so we
				// can detect a re-org behind us on a later cycle
				poll.advance(headChain, toBlock)
			}
		}

		// Reset failure count if we reach here
		failCount = 0

		// Sleep for the polling interval, unless we are paging through a backlog
		if caughtUpToHead {
			select {
			case <-time.After(es.c.eventFilterPollingInterval):
			case <-es.ctx.Done():
				log.L(es.ctx).Debugf("Stream loop stopping")
				return true
			}
		}
	}
}
