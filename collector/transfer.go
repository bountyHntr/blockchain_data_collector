package collector

import (
	"context"
	"fmt"
	"math/big"

	"collector/smartcontract/erc20"

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
	TxHash          string  `csv:"tx_hash"`
	BlockNumber     uint64  `csv:"block_number"`
	EventID         uint16  `csv:"event_id"`
}

func (c *collectorService) collectTransfers() error {
	log.WithField("from_block", c.fromBlock).
		WithField("to_block", c.toBlock).
		WithField("address", c.address).
		Info("collect transfers")

	var (
		transferTopic = c.abi.Events["Transfer"].ID
		topics        = [2][][]common.Hash{
			{{transferTopic}, {c.address.Hash()}},     // from "c.address"
			{{transferTopic}, {}, {c.address.Hash()}}, // to "c.address"
		}
	)

	q := ethereum.FilterQuery{ToBlock: new(big.Int).Set(c.fromBlock)}
	updateQuery(&q, c.toBlock)

	for q.FromBlock.Cmp(c.toBlock) < 0 {
		for _, tops := range topics {

			select {
			case <-c.exitChan:
				return nil
			default:
			}

			q.Topics = tops
			events, err := c.cli.FilterLogs(context.Background(), q)
			if err != nil {
				return fmt.Errorf("filter logs from %d to %d: %w", q.FromBlock, q.ToBlock, err)
			}

			for _, event := range events {
				c.msgChan <- event
			}
		}

		updateQuery(&q, c.toBlock)
	}

	return nil
}

func (c *collectorService) convertToTransferInfo(eventRaw types.Log) (TransferInfo, bool) {
	event, err := parseTransferEvent(c.abi, &eventRaw)
	if err != nil {
		log.WithError(err).
			WithField("tx_hash", eventRaw.TxHash.Hex()).
			WithField("event_id", eventRaw.Index).
			Error("parse transfer event")
		return TransferInfo{}, false
	}

	token, err := c.getTokenInfo(eventRaw.Address.Hex())
	if err != nil {
		log.WithError(err).
			WithField("address", eventRaw.Address.Hex()).
			Error("get token info")
		return TransferInfo{}, false
	}

	return TransferInfo{
		Token:           token.Address,
		Symbol:          token.Symbol,
		From:            event.Src.Hex(),
		To:              event.Dst.Hex(),
		Value:           event.Wad.Uint64(),
		NormalizedValue: Normalize(event.Wad, token.Multiplier),
		TxHash:          eventRaw.TxHash.Hex(),
		BlockNumber:     eventRaw.BlockNumber,
		EventID:         uint16(eventRaw.Index),
	}, true
}

func parseTransferEvent(ABI *abi.ABI, event *types.Log) (*erc20.Erc20Transfer, error) {
	const eventName = "Transfer"

	var transfer erc20.Erc20Transfer
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
	batchSize := big.NewInt(1000) // const

	q.FromBlock = q.ToBlock
	q.ToBlock = minBigInt(new(big.Int).Add(q.ToBlock, batchSize), maxBlock)
}

func minBigInt(a, b *big.Int) *big.Int {
	if a.Cmp(b) < 0 {
		return a
	}
	return b
}
