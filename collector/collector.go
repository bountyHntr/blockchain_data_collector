package collector

import (
	"collector/smartcontract/token"
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Config struct {
	Url           string
	FromBlock     int64
	ToBlock       int64
	FilePath      string
	TransfersOnly bool
}

type collectorService struct {
	cli       *ethclient.Client
	abi       *abi.ABI
	fromBlock *big.Int
	toBlock   *big.Int
	msgChan   chan<- any
}

func Run(cfg Config) (err error) {
	var c collectorService
	c.cli, err = ethclient.Dial(cfg.Url)
	if err != nil {
		return fmt.Errorf("dial eth client %s: %w", cfg.Url, err)
	}
	defer c.stop()

	if err := c.initBlockRange(cfg.FromBlock, cfg.ToBlock); err != nil {
		return fmt.Errorf("init block range: %w", err)
	}

	c.msgChan, err = runCsvService(c.newCsvConfig(cfg.FilePath, cfg.TransfersOnly))
	if err != nil {
		return fmt.Errorf("run csv service: %w", err)
	}

	c.abi, err = token.TokenMetaData.GetAbi()
	if err != nil {
		return fmt.Errorf("parse token abi: %w", err)
	}

	if cfg.TransfersOnly {
		return c.collectTransfers()
	}
	return c.collectAllTxs()
}

func (c *collectorService) stop() {
	const waitBeforeExit = 5 * time.Second

	if c.cli != nil {
		c.cli.Close()
	}
	if c.msgChan != nil {
		close(c.msgChan)
	}
	time.Sleep(waitBeforeExit)
}

func (c *collectorService) initBlockRange(fromBlock, toBlock int64) error {
	if fromBlock >= 0 {
		c.fromBlock = big.NewInt(fromBlock)
	} else {
		c.fromBlock = common.Big0
	}

	if toBlock >= 0 {
		c.toBlock = big.NewInt(toBlock)
	} else {
		blockNum, err := c.cli.BlockNumber(context.Background())
		if err != nil {
			return fmt.Errorf("get last block: %w", err)
		}
		c.toBlock = big.NewInt(int64(blockNum))
	}

	return nil
}

func (c *collectorService) newCsvConfig(filePath string, transfersOnly bool) CsvConfig {
	csvConfig := CsvConfig{
		FilePath:     filePath,
		FlushOnWrite: true,
	}

	if transfersOnly {
		csvConfig.InType = &types.Log{}
		csvConfig.OutType = TransferInfo{}
		csvConfig.Converter = func(val any) any {
			return c.convertToTransferInfo(val.(types.Log))
		}
	} else {
		csvConfig.InType = &types.Transaction{}
		csvConfig.OutType = TransactionInfo{}
		csvConfig.Converter = func(val any) any {
			return c.convertToTxInfo(val.(*types.Transaction))
		}
	}

	return csvConfig
}
