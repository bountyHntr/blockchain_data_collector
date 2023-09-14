package collector

import (
	"collector/smartcontract/token"
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	log "github.com/sirupsen/logrus"
)

type TransferInfo struct {
	Token           string  `csv:"token"`
	Symbol          string  `csv:"symbol"`
	From            string  `csv:"from"`
	To              string  `csv:"to"`
	Value           uint64  `csv:"value"`
	NormalizedValue float64 `csv:"normalized_value"`
	UsdPrice        float64 `csv:"usd_price"`
	TxHash          string  `csv:"tx_hash"`
	BlockNumber     uint64  `csv:"block_number"`
	EventID         uint64  `csv:"event_id"`
	Counterparty    string  `csv:"counterparty"`
}

func (c *collectorService) collectTransfers() error {
	var transferTopic = c.abi.Events["Transfer"].ID

	q := ethereum.FilterQuery{
		ToBlock: new(big.Int).Set(c.fromBlock),
		Topics:  [][]common.Hash{{transferTopic}},
	}
	updateQuery(&q, c.toBlock)

	for q.FromBlock.Cmp(c.toBlock) < 0 {

		events, err := c.cli.FilterLogs(context.Background(), q)
		if err != nil {
			return fmt.Errorf("filter logs from %d to %d: %w", q.FromBlock, q.ToBlock, err)
		}

		for _, event := range events {
			c.msgChan <- event
		}

		updateQuery(&q, c.toBlock)
	}

	return nil
}

func (c *collectorService) convertToTransferInfo(eventRaw types.Log) TransferInfo {
	event, err := parseTransferEvent(c.abi, &eventRaw)
	if err != nil {
		log.WithError(err).Error("parse transfer event")
		// return fmt.Errorf("parse event %d from tx %s: %w", eventRaw.Index, eventRaw.TxHash, err)
	}

	return TransferInfo{
		Token: eventRaw.Address.Hex(),
		Value: event.Value.Uint64(),
	}
}

func parseTransferEvent(ABI *abi.ABI, event *types.Log) (*token.TokenTransfer, error) {
	const eventName = "Transfer"

	var transfer token.TokenTransfer
	if err := ABI.UnpackIntoInterface(&transfer, eventName, event.Data); err != nil {
		return nil, fmt.Errorf("unpack event: %w", err)
	}

	var indexed abi.Arguments
	for _, arg := range ABI.Events[eventName].Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}

	if err := abi.ParseTopics(&transfer, indexed, event.Topics[1:]); err != nil {
		return nil, fmt.Errorf("parse topics: %w", err)
	}

	return &transfer, nil
}

func updateQuery(q *ethereum.FilterQuery, maxBlock *big.Int) {
	var batchSize = big.NewInt(1000) // const

	q.FromBlock = q.ToBlock
	q.ToBlock = minBigInt(new(big.Int).Add(q.ToBlock, batchSize), maxBlock)
}

func minBigInt(a, b *big.Int) *big.Int {
	if a.Cmp(b) < 0 {
		return a
	}
	return b
}
