package collector

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type TransactionInfo struct {
	TxHash       string `csv:"tx_hash"`
	Nonce        uint64 `csv:"nonce"`
	Receiver     string `csv:"receiver"`
	BlockNumber  string `csv:"block_number"`
	Timestamp    string `csv:"timestamp"`
	Counterparty string `csv:"counterparty"`
}

func (c *collectorService) collectAllTxs() error {

	blockNum := new(big.Int).Set(c.fromBlock)
	for blockNum.Cmp(c.toBlock) <= 0 {
		block, err := c.cli.BlockByNumber(context.Background(), blockNum)
		if err != nil {
			return fmt.Errorf("get block %d: %w", blockNum, err)
		}

		for _, tx := range block.Transactions() {
			c.msgChan <- tx
		}

		blockNum.Add(blockNum, common.Big1)
	}

	return nil
}

func (c *collectorService) convertToTxInfo(tx *types.Transaction) TransactionInfo {
	return TransactionInfo{
		TxHash: tx.Hash().Hex(),
	}
}
