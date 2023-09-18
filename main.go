package main

import (
	"flag"
	"os"

	"collector/collector"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

func main() {
	cfg := buildConfig()
	if err := collector.Run(cfg); err != nil {
		log.WithError(err).Panic("program failed")
	}
}

func buildConfig() collector.Config {
	cfgPath := flag.String("config", "./config.yaml", "config path")

	var cfg collector.Config
	if err := parseConfigFromFile(*cfgPath, &cfg); err != nil {
		log.WithError(err).WithField("path", *cfgPath).Panic("invalid config")
	}

	return cfg
}

func parseConfigFromFile(fileName string, cfg interface{}) error {
	rawCfg, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}
	return parseConfigRaw(rawCfg, cfg)
}

func parseConfigRaw(rawCfg []byte, cfg interface{}) error {
	err := yaml.Unmarshal(rawCfg, cfg)
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal config file")
	}
	return nil
}
