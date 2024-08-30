// SPDX-License-Identifier: Apache-2.0

package dbleader

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/loopholelabs/logging/types"
	"github.com/shivanshvij/dblock"

	"github.com/shivanshvij/dbleader/pkg/broadcast"
)

var (
	ErrInvalidOptions       = errors.New("invalid options")
	ErrInvalidDBLockOptions = errors.New("invalid dblock options")
	ErrCreatingDBLock       = errors.New("error creating dblock")
	ErrStarting             = errors.New("error starting dbleader")
	ErrAlreadyStopped       = errors.New("dbleader already stopped")
	ErrStopping             = errors.New("error stopping dbleader")
)

const (
	stateUninitialized int32 = iota
	stateStarted
	stateStopped
)

type IsLeaderKind bool

const (
	IsLeader    IsLeaderKind = true
	IsNotLeader IsLeaderKind = false
)

type DBLeader struct {
	logger  types.Logger
	options *Options

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	dblock    *dblock.DBLock
	lock      *dblock.Lock
	broadcast *broadcast.Broadcast[IsLeaderKind]

	state atomic.Int32
}

func New(options *Options) (*DBLeader, error) {
	var err error
	if err = options.Validate(); err != nil {
		return nil, errors.Join(ErrInvalidOptions, err)
	}

	logger := options.Logger.SubLogger("dbleader").With().Str("name", options.Name).Logger()

	var dblockOptions *dblock.Options
	if dblockOptions, err = options.getDBLockOptions(logger); err != nil {
		return nil, errors.Join(ErrInvalidDBLockOptions, err)
	}

	_dblock, err := dblock.New(dblockOptions)
	if err != nil {
		return nil, errors.Join(ErrCreatingDBLock, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &DBLeader{
		logger:  logger,
		options: options,

		ctx:    ctx,
		cancel: cancel,

		dblock:    _dblock,
		lock:      _dblock.Lock(options.Namespace),
		broadcast: broadcast.New[IsLeaderKind](),
	}, nil
}

func (db *DBLeader) Start() error {
	if db.state.CompareAndSwap(stateUninitialized, stateStarted) {
		db.wg.Add(1)
		go db.do()
		return nil
	}
	return ErrStarting
}

func (db *DBLeader) Stop() error {
	if db.state.CompareAndSwap(stateStarted, stateStopped) {
		db.cancel()
		err := db.lock.Release()
		if err != nil && !errors.Is(err, dblock.ErrLockAlreadyReleased) {
			return errors.Join(ErrStopping, err)
		}
		err = db.dblock.Stop()
		if err != nil {
			return errors.Join(ErrStopping, err)
		}
		db.wg.Wait()
		return nil
	}
	return errors.Join(ErrStopping, ErrAlreadyStopped)
}

func (db *DBLeader) Subscribe() <-chan IsLeaderKind {
	return db.broadcast.Subscribe()
}

func (db *DBLeader) Unsubscribe(ch <-chan IsLeaderKind) {
	db.broadcast.Unsubscribe(ch)
}

func (db *DBLeader) Leader() (string, error) {
	return db.dblock.Owner(db.options.Namespace)
}

func (db *DBLeader) do() {
	defer db.wg.Done()
	for {
		db.logger.Info().Msg("attempting to acquire leadership")
		err := db.lock.Acquire()
		if err != nil {
			db.logger.Error().Err(err).Msg("error acquiring leadership")
			return
		}
		db.logger.Info().Msg("acquired leadership")
		db.broadcast.Publish(IsLeader)
		select {
		case <-db.ctx.Done():
			db.logger.Info().Msg("context cancelled, releasing leadership")
			err = db.lock.Release()
			if err != nil {
				db.logger.Error().Err(err).Msg("error releasing leadership")
			}
			db.broadcast.Publish(IsNotLeader)
			return
		case <-db.lock.Notify():
			db.logger.Info().Msg("leadership lost, attempting to reacquire")
			db.broadcast.Publish(IsNotLeader)
			db.lock = db.dblock.Lock(db.options.Namespace)
			continue
		}
	}
}
