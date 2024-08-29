package dbleader

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/loopholelabs/logging"
	"github.com/shivanshvij/dblock"
	"github.com/stretchr/testify/require"
)

const leaseDuration = time.Millisecond * 200

func TestLeadership(t *testing.T) {
	connStr := fmt.Sprintf("file:%s/%s?cache=private&_fk=1", t.TempDir(), t.Name())

	leader0, err := New(&Options{
		Logger:    logging.Test(t, logging.Zerolog, t.Name()),
		Name:      "leader0",
		Namespace: t.Name(),
		DBLockOptions: DBLockOptions{
			DBType:        dblock.SQLite,
			DatabaseURL:   connStr,
			LeaseDuration: leaseDuration,
		},
	})
	require.NoError(t, err)

	leader1, err := New(&Options{
		Logger:    logging.Test(t, logging.Zerolog, t.Name()),
		Name:      "leader1",
		Namespace: t.Name(),
		DBLockOptions: DBLockOptions{
			DBType:        dblock.SQLite,
			DatabaseURL:   connStr,
			LeaseDuration: leaseDuration,
		},
	})
	require.NoError(t, err)

	err = leader0.Start()
	require.NoError(t, err)

	err = leader1.Start()
	require.NoError(t, err)

	sub0 := leader0.Subscribe()
	sub1 := leader1.Subscribe()

	var leaderName string
	var isLeader IsLeaderKind
	select {
	case isLeader = <-sub0:
		if isLeader {
			t.Log("leader0 is the leader")
			leaderName = leader0.options.Name
		} else {
			t.Fatal("received leader0 event before initial leader0 election")
		}
	case isLeader = <-sub1:
		if isLeader {
			t.Log("leader1 is the leader")
			leaderName = leader1.options.Name
		} else {
			t.Fatal("received leader1 event before initial leader1 election")
		}
	case <-time.After(leaseDuration * 2):
		t.Fatal("no leader elected")
	}

	_leaderName, err := leader0.Leader()
	require.NoError(t, err)
	assert.Equal(t, leaderName, _leaderName)

	_leaderName, err = leader1.Leader()
	require.NoError(t, err)
	assert.Equal(t, leaderName, _leaderName)

	switch leaderName {
	case leader0.options.Name:
		t.Log("stopping leader0")
		err = leader0.Stop()
		require.NoError(t, err)
	case leader1.options.Name:
		t.Log("stopping leader1")
		err = leader1.Stop()
		require.NoError(t, err)
	}

	var ok bool
	select {
	case isLeader, ok = <-sub0:
		require.True(t, ok)
		if isLeader {
			if leaderName == leader0.options.Name {
				t.Fatal("leader0: leader0 is still the leader")
			} else {
				t.Log("leader0: leader0 is now the leader")
				err = leader0.Stop()
				require.NoError(t, err)
			}
		} else {
			if leaderName == leader1.options.Name {
				t.Fatal("leader0: leader1 is still the leader")
			} else {
				t.Log("leader0: leader1 is now the leader")
			}
		}
	case <-time.After(leaseDuration * 2):
		t.Fatal("no leader elected")
	}

	select {
	case isLeader, ok = <-sub1:
		require.True(t, ok)
		if isLeader {
			if leaderName == leader1.options.Name {
				t.Fatal("leader1: leader1 is still the leader")
			} else {
				t.Log("leader1: leader1 is now the leader")
				err = leader1.Stop()
				require.NoError(t, err)
			}
		} else {
			if leaderName == leader0.options.Name {
				t.Fatal("leader1: leader0 is still the leader")
			} else {
				t.Log("leader1: leader0 is now the leader")
			}
		}
	case <-time.After(leaseDuration * 2):
		t.Fatal("no leader elected")
	}

}
