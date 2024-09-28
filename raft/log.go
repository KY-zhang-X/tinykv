// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	first uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	return &RaftLog{
		storage:   storage,
		first:     firstIndex,
		applied:   firstIndex - 1,
		committed: hardState.GetCommit(),
		stabled:   lastIndex,
		entries:   entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	allEnts := make([]pb.Entry, 0, len(l.entries))
	for _, ent := range l.entries {
		allEnts = append(allEnts, ent)
	}
	return allEnts
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[l.stabled+1-l.first:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ents = l.entries[l.applied+1-l.first : l.committed+1-l.first]
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.first + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.first {
		// FIXME: i == l.first-1的情况应该从Snapshot中取
		if i == 0 {
			return 0, nil
		}
		return 0, ErrCompacted
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	return l.entries[i-l.first].Term, nil
}

func (l *RaftLog) AppendEntry(ent *pb.Entry) {
	l.entries = append(l.entries, pb.Entry{
		Term:      ent.Term,
		Index:     l.LastIndex() + 1,
		EntryType: ent.EntryType,
		Data:      ent.Data,
	})
}

func (l *RaftLog) DeleteEntries(i uint64) error {
	if i < l.first || i > l.LastIndex()+1 {
		return ErrUnavailable
	}
	if i-1 < l.stabled {
		l.stabled = i - 1
		// FIXME: 重新持久化
	}
	l.entries = l.entries[:i-l.first]
	return nil
}

func (l *RaftLog) AppendEntries(i uint64, ents []*pb.Entry) error {
	if i < l.first || i > l.LastIndex()+1 {
		return ErrUnavailable
	}
	// 尝试覆盖原来冲突的条目, 如果不冲突则不覆盖
	// 找到第一个冲突的条目
	ci := i
	for ; ci-i < uint64(len(ents)) && ci-l.first < uint64(len(l.entries)); ci++ {
		if l.entries[ci-l.first].Term != ents[ci-i].Term {
			break
		}
	}
	// 如果没有冲突则不需要复制后续内容
	if ci-i == uint64(len(ents)) {
		return nil
	}
	// 如果有冲突依次复制
	// 删除ci之后的内容
	if err := l.DeleteEntries(ci); err != nil {
		return err
	}
	for ; ci-i < uint64(len(ents)); ci++ {
		l.entries = append(l.entries, *ents[ci-i])
	}
	return nil
}

func (l *RaftLog) Entries(lo uint64) ([]*pb.Entry, error) {
	if lo < l.first {
		return nil, ErrCompacted
	}

	ents := make([]*pb.Entry, 0, len(l.entries)-int(lo-l.first))
	for _, entry := range l.entries[lo-l.first:] {
		entryCopy := entry
		ents = append(ents, &entryCopy)
	}
	return ents, nil
}
