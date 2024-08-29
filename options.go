// SPDX-License-Identifier: Apache-2.0

package dbleader

import (
	"errors"
	"github.com/shivanshvij/dblock"
	"time"

	"github.com/loopholelabs/logging/loggers/noop"
	"github.com/loopholelabs/logging/types"
)

var (
	ErrInvalidName      = errors.New("invalid name")
	ErrInvalidNamespace = errors.New("invalid namespace")
)

type DBLockOptions struct {
	DBType                dblock.DBType
	DatabaseURL           string
	LeaseDuration         time.Duration
	LeaseRefreshFrequency time.Duration
}

type Options struct {
	Logger        types.SubLogger
	Name          string
	Namespace     string
	DBLockOptions DBLockOptions
}

func (o *Options) Validate() error {
	if o.Logger == nil {
		o.Logger = noop.New(types.InfoLevel)
	}

	if o.Name == "" {
		return ErrInvalidName
	}

	if o.Namespace == "" {
		return ErrInvalidNamespace
	}

	return nil
}

func (o *Options) getDBLockOptions(logger types.SubLogger) (*dblock.Options, error) {
	options := &dblock.Options{
		Logger:                logger,
		Owner:                 o.Name,
		DBType:                o.DBLockOptions.DBType,
		DatabaseURL:           o.DBLockOptions.DatabaseURL,
		LeaseDuration:         o.DBLockOptions.LeaseDuration,
		LeaseRefreshFrequency: o.DBLockOptions.LeaseRefreshFrequency,
	}
	return options, options.Validate()
}
