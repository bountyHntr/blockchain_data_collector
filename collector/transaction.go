package collector

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	log "github.com/sirupsen/logrus"
)

type TransactionInfo struct {
	TxHash      string `csv:"tx_hash"`
	Nonce       uint64 `csv:"nonce"`
	Sender      string `csv:"sender"`
	Receiver    string `csv:"receiver"`
	BlockNumber uint64 `csv:"block_number"`
	Timestamp   uint64 `csv:"timestamp"`
}

func (c *collectorService) collectAllTxs() error {
	blockNum := new(big.Int).Set(c.fromBlock)
	for blockNum.Cmp(c.toBlock) <= 0 {
		block, err := c.cli.BlockByNumber(context.Background(), blockNum)
		if err != nil {
			return fmt.Errorf("get block %d: %w", blockNum, err)
		}

		for _, tx := range block.Transactions() {
			sender := c.txSender(tx)
			if (tx.To() != nil && *tx.To() == c.address) || sender == c.address {
				c.msgChan <- &TxWrapper{
					Tx:          tx,
					Sender:      sender,
					BlockNumber: block.NumberU64(),
					Timestamp:   block.Time(),
				}
			}
		}

		blockNum.Add(blockNum, common.Big1)
	}

	return nil
}

type TxWrapper struct {
	Tx          *types.Transaction
	Sender      common.Address
	BlockNumber uint64
	Timestamp   uint64
}

func (c *collectorService) convertToTxInfo(w *TxWrapper) TransactionInfo {
	return TransactionInfo{
		TxHash:      w.Tx.Hash().Hex(),
		Nonce:       w.Tx.Nonce(),
		Sender:      w.Sender.Hex(),
		Receiver:    w.Tx.To().Hex(),
		BlockNumber: w.BlockNumber,
		Timestamp:   w.Timestamp,
	}
}

func (c *collectorService) txSender(tx *types.Transaction) common.Address {
	signer := types.LatestSignerForChainID(tx.ChainId())
	sender, err := signer.Sender(tx)
	if err != nil {
		log.WithError(err).
			WithField("tx_hash", tx.Hash().Hex()).
			Error("failed to get tx sender")
	}

	return sender
}
