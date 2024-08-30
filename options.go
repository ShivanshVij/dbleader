// SPDX-License-Identifier: Apache-2.0

package dbleader

import (
	"errors"
	"time"

	"github.com/loopholelabs/logging/loggers/noop"
	"github.com/loopholelabs/logging/types"
	"github.com/shivanshvij/dblock"
)

var (
	ErrInvalidName                  = errors.New("invalid name")
	ErrInvalidNamespace             = errors.New("invalid namespace")
	ErrInvalidDBType                = errors.New("invalid database type")
	ErrInvalidDatabaseURL           = errors.New("invalid database URL")
	ErrInvalidLeaseDuration         = errors.New("invalid lease duration")
	ErrInvalidLeaseRefreshFrequency = errors.New("invalid lease refresh frequency")
)

type DBType int

const (
	Undefined DBType = iota
	Postgres
	SQLite
)

type Options struct {
	Logger                types.Logger
	Name                  string
	Namespace             string
	DBType                DBType
	DatabaseURL           string
	LeaseDuration         time.Duration
	LeaseRefreshFrequency time.Duration
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

	if o.DBType != Postgres && o.DBType != SQLite {
		return ErrInvalidDBType
	}

	if o.DatabaseURL == "" {
		return ErrInvalidDatabaseURL
	}

	if o.LeaseDuration == 0 {
		return ErrInvalidLeaseDuration
	}

	if o.LeaseRefreshFrequency >= o.LeaseDuration {
		return ErrInvalidLeaseRefreshFrequency
	}

	if o.LeaseRefreshFrequency == 0 {
		o.LeaseRefreshFrequency = o.LeaseDuration / 2
	}

	return nil
}

func (o *Options) getDBLockOptions(logger types.Logger) (*dblock.Options, error) {
	dbType := dblock.Undefined
	switch o.DBType {
	case Postgres:
		dbType = dblock.Postgres
	case SQLite:
		dbType = dblock.SQLite
	default:
		return nil, dblock.ErrInvalidDBType
	}
	options := &dblock.Options{
		Logger:                logger,
		Owner:                 o.Name,
		DBType:                dbType,
		DatabaseURL:           o.DatabaseURL,
		LeaseDuration:         o.LeaseDuration,
		LeaseRefreshFrequency: o.LeaseRefreshFrequency,
	}
	return options, options.Validate()
}
