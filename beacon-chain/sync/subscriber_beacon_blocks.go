package sync

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/transition/interop"
	"github.com/OffchainLabs/prysm/v7/config/features"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/interfaces"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/io/file"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

func (s *Service) beaconBlockSubscriber(ctx context.Context, msg proto.Message) error {
	signed, err := blocks.NewSignedBeaconBlock(msg)
	if err != nil {
		return err
	}
	if err := blocks.BeaconBlockIsNil(signed); err != nil {
		return err
	}

	s.setSeenBlockIndexSlot(signed.Block().Slot(), signed.Block().ProposerIndex())

	block := signed.Block()

	root, err := block.HashTreeRoot()
	if err != nil {
		return err
	}

	// Blob reconstruction from EL is disabled
	//roBlock, err := blocks.NewROBlockWithRoot(signed, root)
	//if err != nil {
	//	return errors.Wrap(err, "new ro block with root")
	//}

	//go func() {
	//	if err := s.processSidecarsFromExecutionFromBlock(ctx, roBlock); err != nil {
	//		log.WithError(err).WithFields(logrus.Fields{
	//			"root": fmt.Sprintf("%#x", root),
	//			"slot": block.Slot(),
	//		}).Error("Failed to process sidecars from execution from block")
	//	}
	//}()

	if err := s.cfg.chain.ReceiveBlock(ctx, signed, root, nil); err != nil {
		if blockchain.IsInvalidBlock(err) {
			r := blockchain.InvalidBlockRoot(err)
			if r != [32]byte{} {
				s.setBadBlock(ctx, r) // Setting head block as bad.
			} else {
				// TODO(13721): Remove this once we can deprecate the flag.
				interop.WriteBlockToDisk(signed, true /*failed*/)

				saveInvalidBlockToTemp(signed)
				s.setBadBlock(ctx, root)
			}
		}
		// Set the returned invalid ancestors as bad.
		for _, root := range blockchain.InvalidAncestorRoots(err) {
			s.setBadBlock(ctx, root)
		}
		return err
	}

	if err := s.processPendingAttsForBlock(ctx, root); err != nil {
		return errors.Wrap(err, "process pending atts for block")
	}

	return nil
}

// processSidecarsFromExecutionFromBlock retrieves (if available) sidecars data from the execution client,
// builds corresponding sidecars, save them to the storage, and broadcasts them over P2P if necessary.
// This function is disabled to prevent blob reconstruction from EL.
func (s *Service) processSidecarsFromExecutionFromBlock(ctx context.Context, roBlock blocks.ROBlock) error {
	// Blob reconstruction from EL is disabled
	return nil
}

// processBlobSidecarsFromExecution retrieves (if available) blob sidecars data from the execution client,
// builds corresponding sidecars, save them to the storage, and broadcasts them over P2P if necessary.
// This function is disabled to prevent blob reconstruction from EL.
func (s *Service) processBlobSidecarsFromExecution(ctx context.Context, block interfaces.ReadOnlySignedBeaconBlock) {
	// Blob reconstruction from EL is disabled
	return
}

// processDataColumnSidecarsFromExecution retrieves (if available) data column sidecars data from the execution client,
// builds corresponding sidecars, save them to the storage, and broadcasts them over P2P if necessary.
// This function is disabled to prevent blob reconstruction from EL.
func (s *Service) processDataColumnSidecarsFromExecution(ctx context.Context, source peerdas.ConstructionPopulator) error {
	// Data column reconstruction from EL is disabled
	return nil
}

// broadcastAndReceiveUnseenDataColumnSidecars broadcasts and receives unseen data column sidecars.
func (s *Service) broadcastAndReceiveUnseenDataColumnSidecars(
	ctx context.Context,
	slot primitives.Slot,
	proposerIndex primitives.ValidatorIndex,
	neededIndices map[uint64]bool,
	sidecars []blocks.VerifiedRODataColumn,
) (map[uint64]bool, error) {
	// Compute sidecars we need to broadcast and receive.
	unseenSidecars := make([]blocks.VerifiedRODataColumn, 0, len(sidecars))
	unseenIndices := make(map[uint64]bool, len(sidecars))
	for _, sidecar := range sidecars {
		// Skip data column sidecars we don't need.
		if !neededIndices[sidecar.Index] {
			continue
		}

		// Skip already seen data column sidecars.
		if s.hasSeenDataColumnIndex(slot, proposerIndex, sidecar.Index) {
			continue
		}

		unseenSidecars = append(unseenSidecars, sidecar)
		unseenIndices[sidecar.Index] = true
	}

	// Exit early if there are no nothing to broadcast or receive.
	if len(unseenSidecars) == 0 {
		return nil, nil
	}

	// Broadcast all the data column sidecars we reconstructed but did not see via gossip (non blocking).
	if err := s.cfg.p2p.BroadcastDataColumnSidecars(ctx, unseenSidecars); err != nil {
		return nil, errors.Wrap(err, "broadcast data column sidecars")
	}

	// Receive data column sidecars.
	if err := s.receiveDataColumnSidecars(ctx, unseenSidecars); err != nil {
		return nil, errors.Wrap(err, "receive data column sidecars")
	}

	return unseenIndices, nil
}

// haveAllSidecarsBeenSeen checks if all sidecars for the given slot, proposer index, and data column indices have been seen.
func (s *Service) haveAllSidecarsBeenSeen(slot primitives.Slot, proposerIndex primitives.ValidatorIndex, indices map[uint64]bool) bool {
	for index := range indices {
		if !s.hasSeenDataColumnIndex(slot, proposerIndex, index) {
			return false
		}
	}
	return true
}

// columnIndicesToSample returns the data column indices we should sample for the node.
func (s *Service) columnIndicesToSample() (map[uint64]bool, error) {
	// Retrieve our node ID.
	nodeID := s.cfg.p2p.NodeID()

	// Get the custody group sampling size for the node.
	custodyGroupCount, err := s.cfg.p2p.CustodyGroupCount(s.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "custody group count")
	}

	// Compute the sampling size.
	// https://github.com/ethereum/consensus-specs/blob/master/specs/fulu/das-core.md#custody-sampling
	samplesPerSlot := params.BeaconConfig().SamplesPerSlot
	samplingSize := max(samplesPerSlot, custodyGroupCount)

	// Get the peer info for the node.
	peerInfo, _, err := peerdas.Info(nodeID, samplingSize)
	if err != nil {
		return nil, errors.Wrap(err, "peer info")
	}

	return peerInfo.CustodyColumns, nil
}

// WriteInvalidBlockToDisk as a block ssz. Writes to temp directory.
func saveInvalidBlockToTemp(block interfaces.ReadOnlySignedBeaconBlock) {
	if !features.Get().SaveInvalidBlock {
		return
	}
	filename := fmt.Sprintf("beacon_block_%d.ssz", block.Block().Slot())
	fp := path.Join(os.TempDir(), filename)
	log.Warnf("Writing invalid block to disk at %s", fp)
	enc, err := block.MarshalSSZ()
	if err != nil {
		log.WithError(err).Error("Failed to ssz encode block")
		return
	}
	if err := file.WriteFile(fp, enc); err != nil {
		log.WithError(err).Error("Failed to write to disk")
	}
}
