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
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomized election interval, should change after
	// electionElapsed reached electionTimeout
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A)
	votes := make(map[uint64]bool, len(c.peers))
	prs := make(map[uint64]*Progress, len(c.peers))
	raftLog := newLog(c.Storage)
	for _, peer := range c.peers {
		if peer == c.ID {
			prs[peer] = &Progress{
				Next:  raftLog.LastIndex() + 1,
				Match: raftLog.LastIndex(),
			}
		} else {
			prs[peer] = &Progress{
				Next:  raftLog.LastIndex() + 1,
				Match: 0,
			}
		}
	}
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	return &Raft{
		id:                    c.ID,
		Term:                  hardState.Term,
		Vote:                  hardState.Vote,
		RaftLog:               raftLog,
		Prs:                   prs,
		State:                 StateFollower,
		votes:                 votes,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		panic(err)
	}
	// 检查需要发送什么entry
	entries, err := r.RaftLog.Entries(prevLogIndex + 1)
	if err != nil {
		panic(err)
	}
	// 构造Message并发送
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		panic(err)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogIndex,
		Index:   prevLogTerm,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed += 1
	if r.electionElapsed < r.randomElectionTimeout {
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHup,
		Term:    r.Term,
	}
	r.Step(msg)
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed += 1
	if r.heartbeatElapsed < r.heartbeatTimeout {
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgBeat,
		Term:    r.Term,
	}
	r.Step(msg)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	// 推进Term
	r.Term += 1
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	// 清空votes
	for k := range r.votes {
		delete(r.votes, k)
	}
	// 投票给自己
	r.Vote = r.id
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// 更新状态
	r.State = StateLeader
	r.heartbeatElapsed = 0
	// 更新Prs
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer] = &Progress{
				Next:  r.RaftLog.LastIndex() + 1,
				Match: r.RaftLog.LastIndex(),
			}
		} else {
			r.Prs[peer] = &Progress{
				Next:  r.RaftLog.LastIndex() + 1,
				Match: 0,
			}
		}
	}
	// 产生一个noop的条目
	noop := pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Data:      nil,
	}
	propose := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&noop},
	}
	r.Step(propose)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgAppend:
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
		//TODO
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	}
	return nil
}

func (r *Raft) updateElectionState() {
	nPeers := len(r.Prs)
	nVotes := 0
	nRejects := 0
	for _, voteForMe := range r.votes {
		if voteForMe {
			nVotes += 1
		} else {
			nRejects += 1
		}
		// 获得半数以上节点的支持, 自动成为leader; 获得半数以上节点的拒绝, 自动降级为follower
		if nVotes >= nPeers/2+1 {
			r.becomeLeader()
			break
		} else if nRejects >= nPeers/2+1 {
			r.becomeFollower(r.Term, None)
			break
		}
	}
}

func (r *Raft) handleHup(m pb.Message) {
	// 成为candidate
	r.becomeCandidate()
	// 向其他节点发送RequestVote RPC
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		panic(err)
	}
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      peer,
			From:    r.id,
			Term:    r.Term,
			LogTerm: lastLogTerm,
			Index:   lastIndex,
		}
		r.msgs = append(r.msgs, msg)
	}
	// 更新节点状态, 如果只有一个节点, 此时就可以成为Leader
	r.updateElectionState()
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	if r.State == StateFollower {
		m.To = r.Lead
		m.From = r.id
		r.msgs = append(r.msgs, m)
		return
	}
	// hardState, _, err := r.RaftLog.storage.InitialState()
	// if err != nil {
	// 	panic(err)
	// }
	// 添加Entry到本地
	for _, ent := range m.Entries {
		// ent.Term = hardState.Term
		if ent.Term == 0 {
			ent.Term = r.Term
		}
		r.RaftLog.AppendEntry(ent)
	}
	// 更新本地的Prs
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	// 尝试更新Committed
	r.updateCommitted(r.RaftLog.LastIndex())
	// 广播添加Entry
	r.bcastAppend()
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// 检查是否可以投票给这个节点
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		panic(err)
	}
	// 投票的标准是: Term一致, 当前Term没有投票给其他节点, 目标节点的日志比当前节点更新
	reject := false
	if m.From != r.Vote {
		reject = m.Term < r.Term || r.Vote != None || m.LogTerm < lastLogTerm || (m.LogTerm == lastLogTerm && m.Index < lastIndex)
		if !reject {
			r.Vote = m.From
		}
	}
	// 发送RequestVote RPC的响应
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// 防止过去请求的响应干扰本轮选举
	if m.Term != r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	r.updateElectionState()
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	reject := false
	if m.Term < r.Term {
		reject = true
	} else if m.Index > r.RaftLog.LastIndex() {
		reject = true
	} else {
		term, err := r.RaftLog.Term(m.Index)
		if err == ErrCompacted {
			reject = true
		} else if err != nil {
			panic(err)
		} else if term != m.LogTerm {
			reject = true
		}
	}
	if !reject {
		err := r.RaftLog.AppendEntries(m.Index+1, m.Entries)
		if err != nil {
			panic(err)
		}
		if m.Commit > r.RaftLog.committed {
			// 根据m.Entries中最后一个项的Index更新committed
			r.RaftLog.committed = min(m.Commit, m.Index+1+uint64(len(m.Entries))-1)
		}
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) updateCommitted(index uint64) bool {
	// 尝试统计数量并推进提交
	if index <= r.RaftLog.committed {
		return false
	}
	// 不对过去term的log进行提交
	if term, err := r.RaftLog.Term(index); err != nil {
		panic(err)
	} else if term != r.Term {
		return false
	}

	update := false
	count := 0
	for _, prs := range r.Prs {
		if prs.Match >= index {
			count += 1
		}
		if count >= len(r.Prs)/2+1 {
			update = true
			break
		}
	}
	// 半数以上节点达到index, 则将committed推进到index
	if update {
		r.RaftLog.committed = index
	}
	return update
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// 防止过去请求的响应干扰本轮选举
	if m.Term != r.Term {
		return
	}
	if m.Reject {
		// 如果请求被拒绝了, 则将Next向前倒并重新发送AppendEntries // TODO: 待优化
		r.Prs[m.From].Next -= 1
		r.sendAppend(m.From)
	} else {
		// 如果请求没有被拒绝, 则更新Match和Committed
		// 尝试推进Match和Next
		if m.Index <= r.Prs[m.From].Match {
			return
		}
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		// 尝试更新Committed
		if r.updateCommitted(m.Index) {
			r.bcastAppend()
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// reject := false
	// if m.Term < r.Term {
	// 	reject = true
	// }
	// if !reject {
	// 	if m.Commit > r.RaftLog.committed {
	// 		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	// 	}
	// }

	reject := false
	if m.Term < r.Term {
		reject = true
	} else if m.Index > r.RaftLog.LastIndex() {
		reject = true
	} else {
		term, err := r.RaftLog.Term(m.Index)
		if err == ErrCompacted {
			reject = true
		} else if err != nil {
			panic(err)
		} else if term != m.LogTerm {
			reject = true
		}
	}
	if !reject {
		if m.Commit > r.RaftLog.committed {
			r.RaftLog.committed = min(m.Commit, m.Index)
		}
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// 防止过去请求的响应干扰本轮选举
	if m.Term != r.Term {
		return
	}
	// 如果尚未同步完成, 发送AppendEntry
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
