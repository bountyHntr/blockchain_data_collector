package collector

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"collector/smartcontract/erc20"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	Url            string `yaml:"URL"`
	Address        string `yaml:"Address"`
	FromBlock      int64  `yaml:"FromBlock"`
	ToBlock        int64  `yaml:"ToBlock"`
	Transfers      bool   `yaml:"Transfers"`
	OutputFilePath string `yaml:"OutputFilePath"`
}

type tokenInfo struct {
	Address    string `json:"Address"`
	Symbol     string `json:"Symbol"`
	Multiplier uint64 `json:"Multiplier"`
}

type collectorService struct {
	cli       *ethclient.Client
	address   common.Address
	abi       *abi.ABI
	fromBlock *big.Int
	toBlock   *big.Int
	msgChan   chan<- any
	done      chan struct{}
	exitChan  chan os.Signal

	mu     sync.RWMutex
	tokens map[string]tokenInfo
}

func Run(cfg Config) (err error) {
	c := collectorService{address: common.HexToAddress(cfg.Address)}
	c.cli, err = ethclient.Dial(cfg.Url)
	if err != nil {
		return fmt.Errorf("dial eth client %s: %w", cfg.Url, err)
	}
	defer c.stop()

	if err := c.initBlockRange(cfg.FromBlock, cfg.ToBlock); err != nil {
		return fmt.Errorf("init block range: %w", err)
	}

	c.msgChan, err = runCsvService(c.newCsvConfig(cfg.OutputFilePath, cfg.Transfers))
	if err != nil {
		return fmt.Errorf("run csv service: %w", err)
	}

	c.abi, err = erc20.Erc20MetaData.GetAbi()
	if err != nil {
		return fmt.Errorf("parse token abi: %w", err)
	}

	if err := c.initTokensInfo(); err != nil {
		return fmt.Errorf("init tokens info: %w", err)
	}

	c.exitChan = make(chan os.Signal, 10)
	signal.Notify(c.exitChan, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

	log.Info("init collector")

	if cfg.Transfers {
		return c.collectTransfers()
	}
	return c.collectAllTxs()
}

func (c *collectorService) stop() {
	const wait = time.Second

	if c.cli != nil {
		c.cli.Close()
	}
	if c.msgChan != nil {
		close(c.msgChan)
	}

	<-c.done

	if err := c.saveTokensInfo(); err != nil {
		log.WithError(err).Error("failed to save tokens info")
	}
	time.Sleep(wait)
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

func (c *collectorService) newCsvConfig(filePath string, transfers bool) CsvConfig {
	c.done = make(chan struct{})

	csvConfig := CsvConfig{
		FilePath:     filePath,
		FlushOnWrite: true,
		Done:         c.done,
	}

	if transfers {
		csvConfig.InType = &types.Log{}
		csvConfig.OutType = TransferInfo{}
		csvConfig.Converter = func(val any) (any, bool) {
			return c.convertToTransferInfo(val.(types.Log))
		}
	} else {
		csvConfig.InType = &TxWrapper{}
		csvConfig.OutType = TransactionInfo{}
		csvConfig.Converter = func(val any) (any, bool) {
			return c.convertToTxInfo(val.(*TxWrapper))
		}
	}

	return csvConfig
}
