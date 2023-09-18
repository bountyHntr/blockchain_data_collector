package collector

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/jszwec/csvutil"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const bufferSize = 256

type CsvConfig struct {
	FilePath     string
	FlushOnWrite bool
	InType       any
	OutType      any
	Converter    func(in any) any
	Done         chan<- struct{}
}

func runCsvService(cfg CsvConfig) (chan<- any, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.FilePath), os.ModePerm); err != nil {
		return nil, errors.Wrap(err, "cannot create statistics directory")
	}

	msgType := reflect.TypeOf(cfg.InType)
	w, err := newCsvWriter(msgType, &cfg)
	if err != nil {
		return nil, fmt.Errorf("new writer: %w", err)
	}

	msgChan := make(chan any, bufferSize)
	go w.run(msgChan)

	return msgChan, nil
}

type csvWriter struct {
	encoder      *csvutil.Encoder
	writer       *bufio.Writer
	outType      reflect.Type
	converter    func(in interface{}) interface{}
	file         *os.File
	flushOnWrite bool
	done         chan<- struct{}
}

func newCsvWriter(msgType reflect.Type, cfg *CsvConfig) (w csvWriter, err error) {
	w.flushOnWrite = cfg.FlushOnWrite

	if cfg.OutType != nil {
		w.outType = reflect.TypeOf(cfg.OutType)
		if !msgType.ConvertibleTo(w.outType) {
			if cfg.Converter != nil {
				w.converter = cfg.Converter
			} else {
				return w, fmt.Errorf("type %s is not convertible to %s", msgType, w.outType)
			}
		}
	}

	w.file, err = os.Create(cfg.FilePath)
	if err != nil {
		return w, fmt.Errorf("create file %s: %w", cfg.FilePath, err)
	}

	w.writer = bufio.NewWriter(w.file)
	w.encoder = csvutil.NewEncoder(csv.NewWriter(w.writer))
	w.done = cfg.Done

	return w, nil
}

func (w *csvWriter) run(dataChan <-chan interface{}) {
	for msg := range dataChan {
		if w.converter != nil {
			msg = w.converter(msg)
		} else if w.outType != nil {
			msg = reflect.ValueOf(msg).Convert(w.outType).Interface()
		}

		if err := w.encoder.Encode(msg); err != nil {
			log.WithError(err).Error("encode msg")
		}

		if w.flushOnWrite {
			if err := w.writer.Flush(); err != nil {
				log.WithError(err).Error("flush")
			}
		}
	}

	w.stop()
}

func (w *csvWriter) stop() {
	if err := w.writer.Flush(); err != nil {
		log.WithError(err).Error("flush")
	}

	if err := w.file.Close(); err != nil {
		log.WithError(err).Error("close file")
	}
	w.done <- struct{}{}
}
