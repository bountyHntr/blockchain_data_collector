package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"collector/smartcontract/erc20"

	"github.com/ethereum/go-ethereum/common"
	log "github.com/sirupsen/logrus"
)

const tokensFilePath = "./.data/tokens.json"

func (c *collectorService) initTokensInfo() error {
	c.tokens = make(map[string]tokenInfo)

	data, err := os.ReadFile(tokensFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read data file %s: %w", tokensFilePath, err)
	}

	var info []tokenInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return fmt.Errorf("unmarshal json: %w", err)
	}

	c.mu.Lock()
	for _, i := range info {
		c.tokens[i.Address] = i
	}
	c.mu.Unlock()

	return nil
}

func (c *collectorService) getTokenInfo(address string) (tokenInfo, error) {
	c.mu.RLock()
	info, ok := c.tokens[address]
	c.mu.RUnlock()

	if ok {
		return info, nil
	}

	token, err := erc20.NewErc20(common.HexToAddress(address), c.cli)
	if err != nil {
		return tokenInfo{}, fmt.Errorf("bint token %s: %w", address, err)
	}

	symbol, err := token.Symbol(nil)
	if err != nil {
		return tokenInfo{}, fmt.Errorf("get token symbol %s: %w", address, err)
	}

	decimals, err := token.Decimals(nil)
	if err != nil {
		log.WithError(err).WithField("token", address).Error("get token decimals")
		decimals = 18
	}

	info = tokenInfo{
		Address:    address,
		Symbol:     symbol,
		Multiplier: uint64(Pow(10, decimals)),
	}

	c.mu.Lock()
	c.tokens[address] = info
	c.mu.Unlock()

	return info, nil
}

func (c *collectorService) saveTokensInfo() error {
	var info []tokenInfo
	c.mu.RLock()
	for _, i := range c.tokens {
		info = append(info, i)
	}
	c.mu.RUnlock()

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal json: %w", err)
	}

	if err := os.WriteFile(tokensFilePath, data, 0o644); err != nil {
		return fmt.Errorf("write file %s: %w", tokensFilePath, err)
	}

	return nil
}
