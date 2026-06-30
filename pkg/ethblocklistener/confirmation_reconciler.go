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

package ethblocklistener

import (
	"context"

	"github.com/hyperledger-firefly/common/pkg/i18n"
	"github.com/hyperledger-firefly/common/pkg/log"
	"github.com/hyperledger-firefly/evmconnect/internal/msgs"
	"github.com/hyperledger-firefly/evmconnect/pkg/ethrpc"
	"github.com/hyperledger-firefly/signer/pkg/ethtypes"
	"github.com/hyperledger-firefly/transaction-manager/pkg/ffcapi"
)

func toBlockInfoList(ffcapiBlocks []*ethrpc.MinimalBlockInfo) (blocks []*ethrpc.BlockInfoJSONRPC) {
	blocks = make([]*ethrpc.BlockInfoJSONRPC, len(ffcapiBlocks))
	for i, b := range ffcapiBlocks {
		blocks[i] = &ethrpc.BlockInfoJSONRPC{
			Number:     ethtypes.HexUint64(b.BlockNumber),
			Hash:       b.BlockHash,
			ParentHash: b.ParentHash,
		}
	}
	return blocks
}

func (bl *blockListener) ReconcileConfirmationsForTransaction(ctx context.Context, txHash string, existingConfirmations []*ethrpc.MinimalBlockInfo, targetConfirmationCount uint64) (*ConfirmationUpdateResult, *ethrpc.TxReceiptJSONRPC, error) {

	if bl.BlockListenerConfig.ChainTrackingMode == ffcapi.ChainTrackingModeLight {
		// when chain head is the only thing that's being tracked, we only need to calculate the confirmation list based on the head block number
		// get the transaction receipt only
		txReceipt, err := bl.GetTransactionReceipt(ctx, txHash)
		if err != nil {
			log.L(ctx).Errorf("Failed to fetch transaction receipt using tx hash %s: %v", txHash, err)
			return nil, nil, err
		}
		// compare it against the chain head
		chainHead := bl.GetHeadBlockNumber(ctx)
		if chainHead < txReceipt.BlockNumber.Uint64() {
			// the transaction is not yet included in the chain head
			return nil, nil, i18n.NewError(ctx, msgs.MsgTransactionNotIncludedInChainHead, txHash, chainHead, txReceipt.BlockNumber.String())
		}
		currentConfirmationCount := chainHead - txReceipt.BlockNumber.Uint64()
		if currentConfirmationCount > targetConfirmationCount {
			currentConfirmationCount = targetConfirmationCount
		}
		return &ConfirmationUpdateResult{
			Confirmed: currentConfirmationCount == targetConfirmationCount,
			// no support on fork detection in this mode from the connector although firefly transaction manager will inject `NewFork: true` if it detects a reduction in confirmationCount.
			CurrentConfirmationCount: currentConfirmationCount,
			TargetConfirmationCount:  targetConfirmationCount,
		}, txReceipt, nil
	}

	blockInfoExistingConfirmations := toBlockInfoList(existingConfirmations)

	// Fetch the block containing the transaction first so that we can use it to build the confirmation list
	txBlockInfo, txReceipt, err := bl.getReceiptAndBlock(ctx, txHash)
	if err != nil {
		log.L(ctx).Errorf("Failed to fetch block info using tx hash %s: %v", txHash, err)
		return nil, nil, err
	}

	if txBlockInfo == nil {
		log.L(ctx).Debugf("Transaction %s not found in any block", txHash)
		return nil, nil, i18n.NewError(ctx, msgs.MsgTransactionNotFound, txHash)
	}
	confirmationUpdateResult, err := bl.buildConfirmationList(ctx, blockInfoExistingConfirmations, txBlockInfo, targetConfirmationCount)
	if confirmationUpdateResult != nil {
		confirmationUpdateResult.TargetConfirmationCount = targetConfirmationCount
		confirmationUpdateResult.CurrentConfirmationCount = uint64(len(confirmationUpdateResult.Confirmations)) - 1
		// NOTE: This function does not do the full receipt decoding, for which there is a complex function for.
		// The "Receipt" object is left empty (but the JSON/RPC receipt is return to the caller for enrichment)
	}
	return confirmationUpdateResult, txReceipt, err
}

func (bl *blockListener) buildConfirmationList(ctx context.Context, existingConfirmations []*ethrpc.BlockInfoJSONRPC, txBlockInfo *ethrpc.BlockInfoJSONRPC, targetConfirmationCount uint64) (*ConfirmationUpdateResult, error) {
	// Primary objective of this algorithm is to build a contiguous, linked list of `MinimalBlockInfo` structs, starting from the transaction block and ending as far as our current knowledge of the in-memory partial canonical chain allows.
	// Secondary objective is to report whether any fork was detected (and corrected) during this analysis

	// handle confirmation count of 0 as a special case to reduce complexity of the main algorithm
	if targetConfirmationCount == 0 {
		reconcileResult, err := bl.handleZeroTargetConfirmationCount(ctx, txBlockInfo)
		if reconcileResult != nil || err != nil {
			return reconcileResult, err
		}
	}

	// Initialize the result with the target confirmation count
	reconcileResult := &ConfirmationUpdateResult{}

	// We start by constructing 2 lists of blocks:
	// - The `earlyList`.  This is the set of earliest blocks we are interested in.  At the least, it starts with the transaction block
	//    and may also contain some number of existing confirmations i.e. the output from previous call to this function
	//    Other than the `transactionBlock`, we don't yet know whether any of the early list is still correct as per the current state of the canonical chain.
	//    The chain may have been re-organized since we discovered the blocks in that list.
	// - The `lateList`. This is the most recent set of blocks that we are interesting in and we believe are accurate for the current state of the chain

	earlyList := createPreSpliceEarlyList(existingConfirmations, txBlockInfo, reconcileResult) // ensured to have at least one entry

	// if early list is sufficient to meet the target confirmation count, we handle this as a special case as well
	if earlyList[len(earlyList)-1].Number.Uint64() >= txBlockInfo.Number.Uint64()+targetConfirmationCount {
		reconcileResult := bl.handleTargetCountMetWithEarlyList(earlyList, targetConfirmationCount)
		if reconcileResult != nil {
			return reconcileResult, nil
		}
	}

	lateList, err := createLateList(ctx, txBlockInfo, targetConfirmationCount, bl)
	if err != nil {
		return nil, err
	}

	// These 2 lists may overlap so we splice them together which will remove any overlapping blocks
	splicedList, detectedFork := newSplice(earlyList, lateList)
	if detectedFork {
		reconcileResult.NewFork = true
	}
	for {
		// now loop until we can form a contiguous linked list (by block number) from the spliced list
		if splicedList.isEarlyListEmpty() {
			// the first block in the early list is transaction block
			// if that block is removed, it means the chain is not stable enough for the logic
			// to generate a valid confirmation list
			// throw an error to the caller
			return nil, i18n.NewError(ctx, msgs.MsgFailedToBuildConfirmationQueue)
		}

		// inner loop to fill any gaps between the early list and the late list
		for splicedList.hasGap() {
			err = splicedList.fillOneGap(ctx, bl)
			if err != nil {
				return nil, err
			}
		}

		confirmations := splicedList.toSingleLinkedList()
		if confirmations != nil {
			// we have a contiguous list that starts with the transaction block and ends with the last block in the canonical chain
			// so we can return the result
			reconcileResult.Confirmations = toMinimalBlockInfoList(confirmations)
			break
		}

		// we filled all gaps and still cannot link the 2 lists, must be a fork.  Create a gap of one and try again
		reconcileResult.NewFork = true
		splicedList.removeBrokenLink()
	}

	reconcileResult.Confirmed = uint64(len(reconcileResult.Confirmations)) > targetConfirmationCount // do this maths here as a utility so that the consumer doesn't have to do it
	return reconcileResult, nil
}

// splice is the data structure that brings together 2 lists of block info with functions to remove redundant overlaps, to fill gaps and to validate linkability of the 2 lists
type splice struct {
	earlyList []*ethrpc.BlockInfoJSONRPC // beginning of the early list is the earliest block that we are interested in
	lateList  []*ethrpc.BlockInfoJSONRPC // late list is assumed to be the most recent view of the network's canonical chain
}

func newSplice(earlyList []*ethrpc.BlockInfoJSONRPC, lateList []*ethrpc.BlockInfoJSONRPC) (*splice, bool) {
	// remove any redundant overlaps between the 2 lists
	// for now, we are simply looking at block numbers to see if there is any block number for which both lists have a block info
	// if there is, we prefer to keep the block info from the late list because in the event that the 2 lists diverge, then the divergence point will be somewhere in the early list ( because we fetched the late list more recently)
	// and we will have fewer links to validate if we trim the overlap from the early list
	s := &splice{
		earlyList: earlyList,
		lateList:  lateList,
	}
	detectedFork := false
	// if the early list is bigger than the gap between the transaction block number and the first block in the late list, then we have an overlap
	txBlockNumber := s.earlyList[0].Number.Uint64()
	firstLateBlockNumber := s.lateList[0].Number.Uint64()
	if uint64(len(s.earlyList))+txBlockNumber > firstLateBlockNumber {
		// there is an overlap so we need to discard the end of the early list but before we do, lets check whether it is equivalent to the equivalent blocks from the late
		// list so that we can report whether or not a fork was detected
		discardedEarlyListBlocks := s.earlyList[firstLateBlockNumber-txBlockNumber:]
		for i := range discardedEarlyListBlocks {
			if i >= len(s.lateList) {
				break
			}
			if !discardedEarlyListBlocks[i].Equal(s.lateList[i]) {
				detectedFork = true
				break
			}
		}

		// We're in a if-block that confirms this will be at least one entry (no we never trim early list to zero)
		s.earlyList = s.earlyList[:firstLateBlockNumber-txBlockNumber]
	}
	return s, detectedFork
}

func (s *splice) hasGap() bool {
	return len(s.earlyList) > 0 &&
		len(s.lateList) > 0 &&
		s.earlyList[len(s.earlyList)-1].Number.Uint64()+1 < s.lateList[0].Number.Uint64()
}

func (s *splice) isEarlyListEmpty() bool {
	// we haven't removed the first block from the early list
	return len(s.earlyList) == 0
}

func (s *splice) fillOneGap(ctx context.Context, blockListener *blockListener) error {
	// fill one slot in the gap between the late list and the early list
	// always fill from the end of the gap ( i.e. the block before the start of the late list) because
	// the late list is our best view of the current canonical chain so working backwards from there will increase the number of blocks that we have a high confidence in

	fetchedBlock, err := blockListener.GetBlockInfoByNumber(ctx, s.lateList[0].Number.Uint64()-1, false, "", "")
	if err != nil {
		return err
	}

	// Validate parent-child relationship
	if !fetchedBlock.IsParentOf(s.lateList[0]) {
		// most likely explanation of this is an unstable chain
		// throw an error to the caller
		return i18n.NewError(ctx, msgs.MsgFailedToBuildConfirmationQueue)
	}

	// Prepend fetched block to the late list
	s.lateList = append([]*ethrpc.BlockInfoJSONRPC{fetchedBlock}, s.lateList...)
	return nil
}

func (s *splice) removeBrokenLink() {
	// remove the last block from the early list because it is not the parent of the first block in the late list and we have higher confidence in the late list
	s.earlyList = s.earlyList[:len(s.earlyList)-1]

}

func (s *splice) toSingleLinkedList() []*ethrpc.BlockInfoJSONRPC {
	if s.earlyList[len(s.earlyList)-1].IsParentOf(s.lateList[0]) {
		return append(s.earlyList, s.lateList...)
	}
	// cannot be linked because the last block in the early list is not the parent of the first block in the late list
	return nil

}

// createPreSpliceEarlyList will return a list of blocks that starts with the latest transaction block and followed by any blocks in the existing confirmations list that are still valid
// any blocks that are not contiguous will be discarded
func createPreSpliceEarlyList(existingConfirmations []*ethrpc.BlockInfoJSONRPC, txBlockInfo *ethrpc.BlockInfoJSONRPC, reconcileResult *ConfirmationUpdateResult) (earlyList []*ethrpc.BlockInfoJSONRPC) {
	if len(existingConfirmations) > 0 {
		if !existingConfirmations[0].Equal(txBlockInfo) {
			// we discard the existing confirmations list if the transaction block doesn't match
			reconcileResult.NewFork = true
		} else {
			// validate and trim the confirmations list to only include linked blocks

			earlyList = []*ethrpc.BlockInfoJSONRPC{txBlockInfo}
			for i := 1; i < len(existingConfirmations); i++ {
				if !earlyList[i-1].IsParentOf(existingConfirmations[i]) {
					// set rebuilt flag to true to indicate the existing confirmations list is not contiguous
					reconcileResult.Rebuilt = true
					break
				}
				earlyList = append(earlyList, existingConfirmations[i])
			}
		}

	}

	if len(earlyList) == 0 {
		// either because this is the first time we are reconciling this transaction or because we just discarded the existing confirmations queue
		earlyList = []*ethrpc.BlockInfoJSONRPC{txBlockInfo}
	}
	return earlyList
}

func createLateList(ctx context.Context, txBlockInfo *ethrpc.BlockInfoJSONRPC, targetConfirmationCount uint64, blockListener *blockListener) (lateList []*ethrpc.BlockInfoJSONRPC, err error) {
	lateList, err = blockListener.buildConfirmationQueueUsingInMemoryPartialChain(ctx, txBlockInfo, targetConfirmationCount)
	if err != nil {
		return nil, err
	}

	// If the late list is empty, it may be because the chain has moved on so far and the transaction is so old that
	// we no longer have the target block in memory. Lets try to grab the target block from the blockchain and work backwards from there.
	if len(lateList) == 0 {
		targetBlockInfo, err := blockListener.GetBlockInfoByNumber(ctx, txBlockInfo.Number.Uint64()+targetConfirmationCount, false, "", "")
		if err != nil {
			return nil, err
		}
		if targetBlockInfo == nil {
			return nil, i18n.NewError(ctx, msgs.MsgBlockNotAvailable)
		}
		lateList = []*ethrpc.BlockInfoJSONRPC{targetBlockInfo}
	}
	return lateList, nil
}

// validateChainCaughtUp checks if the in-memory partial chain has caught up to the transaction block.
// Returns an error if the chain is not initialized or if the chain tail is behind the transaction block.
func (bl *blockListener) validateChainCaughtUp(ctx context.Context, txBlockInfo *ethrpc.BlockInfoJSONRPC, txBlockNumber uint64) error {
	chainTailElement := bl.canonicalChain.Back()
	if chainTailElement == nil {
		return i18n.NewError(ctx, msgs.MsgInMemoryPartialChainNotCaughtUp, txBlockNumber, txBlockInfo.Hash)
	}
	chainTail := chainTailElement.Value.(*ethrpc.BlockInfoJSONRPC)
	if chainTail == nil || chainTail.Number.Uint64() < txBlockNumber {
		log.L(ctx).Debugf("in-memory partial chain is waiting for the transaction block %d (%s) to be indexed", txBlockNumber, txBlockInfo.Hash)
		return i18n.NewError(ctx, msgs.MsgInMemoryPartialChainNotCaughtUp, txBlockNumber, txBlockInfo.Hash)
	}
	return nil
}

// buildConfirmationQueueUsingInMemoryPartialChain builds the late list using the in-memory partial chain.
// It does not modify the in-memory partial chain itself, only reads from it.
// This function holds a read lock on the in-memory partial chain, so it should not make long-running queries.
func (bl *blockListener) buildConfirmationQueueUsingInMemoryPartialChain(ctx context.Context, txBlockInfo *ethrpc.BlockInfoJSONRPC, targetConfirmationCount uint64) (newConfirmationsWithoutTxBlock []*ethrpc.BlockInfoJSONRPC, err error) {
	bl.canonicalChainLock.RLock()
	defer bl.canonicalChainLock.RUnlock()
	txBlockNumber := txBlockInfo.Number.Uint64()
	targetBlockNumber := txBlockInfo.Number.Uint64() + targetConfirmationCount

	// Check if the in-memory partial chain has caught up to the transaction block
	err = bl.validateChainCaughtUp(ctx, txBlockInfo, txBlockNumber)
	if err != nil {
		return nil, err
	}

	// Build new confirmations from blocks after the transaction block

	newConfirmationsWithoutTxBlock = []*ethrpc.BlockInfoJSONRPC{}
	nextInMemoryBlock := bl.canonicalChain.Front()
	for nextInMemoryBlock != nil && nextInMemoryBlock.Value != nil {
		nextInMemoryBlockInfo := nextInMemoryBlock.Value.(*ethrpc.BlockInfoJSONRPC)

		// If we've reached the target confirmation count, mark as confirmed
		if nextInMemoryBlockInfo.Number.Uint64() > targetBlockNumber {
			break
		}

		// Skip blocks at or before the transaction block
		if nextInMemoryBlockInfo.Number.Uint64() <= txBlockNumber {
			nextInMemoryBlock = nextInMemoryBlock.Next()
			continue
		}

		// Add blocks after the transaction block to confirmations
		newConfirmationsWithoutTxBlock = append(newConfirmationsWithoutTxBlock, nextInMemoryBlockInfo)
		nextInMemoryBlock = nextInMemoryBlock.Next()
	}
	return newConfirmationsWithoutTxBlock, nil
}

func (bl *blockListener) handleZeroTargetConfirmationCount(ctx context.Context, txBlockInfo *ethrpc.BlockInfoJSONRPC) (*ConfirmationUpdateResult, error) {
	bl.canonicalChainLock.RLock()
	defer bl.canonicalChainLock.RUnlock()
	// if the target confirmation count is 0, and the transaction blocks is before the last block in the in-memory partial chain,
	// we can immediately return a confirmed result
	txBlockNumber := txBlockInfo.Number.Uint64()
	err := bl.validateChainCaughtUp(ctx, txBlockInfo, txBlockNumber)
	if err != nil {
		return nil, err
	}

	return &ConfirmationUpdateResult{
		Confirmed:     true,
		Confirmations: []*ethrpc.MinimalBlockInfo{txBlockInfo.ToMinimalBlockInfo()},
	}, nil

}

func (bl *blockListener) handleTargetCountMetWithEarlyList(existingConfirmations []*ethrpc.BlockInfoJSONRPC, targetConfirmationCount uint64) *ConfirmationUpdateResult {
	bl.canonicalChainLock.RLock()
	defer bl.canonicalChainLock.RUnlock()
	nextInMemoryBlock := bl.canonicalChain.Front()
	var nextInMemoryBlockInfo *ethrpc.BlockInfoJSONRPC
	lastExistingConfirmation := existingConfirmations[len(existingConfirmations)-1]
	// iterates to the block that immediately after the last existing confirmation
	for nextInMemoryBlock != nil && nextInMemoryBlock.Value != nil {
		nextInMemoryBlockInfo = nextInMemoryBlock.Value.(*ethrpc.BlockInfoJSONRPC)
		if nextInMemoryBlockInfo.Number.Uint64() >= lastExistingConfirmation.Number.Uint64()+1 {
			break
		}
		nextInMemoryBlock = nextInMemoryBlock.Next()
	}

	if nextInMemoryBlockInfo != nil && lastExistingConfirmation.IsParentOf(nextInMemoryBlockInfo) {
		// the existing confirmation are connected to the in memory partial chain so we can return them without fetching any more blocks
		return &ConfirmationUpdateResult{
			Confirmed:     true,
			Confirmations: toMinimalBlockInfoList(existingConfirmations[:targetConfirmationCount+1]),
		}
	}
	return nil
}
