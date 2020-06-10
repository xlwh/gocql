package gocql

import (
	"context"
	"encoding/binary"
	"fmt"
)

var (
	TokenAwareBlkLoadInCorrectTokenPolicy = fmt.Errorf("tokenAwareHostPolicy should be used as HostSelectionPolicy for current session")
)

// TokenAwareBlkLoad 不是线程安全的
// 应当使用线程池，使用一个线程调用不断 `Load()`
// 当某个 host 上的 blk size 大于 serPerHost 时便会发送一个 Batch
type TokenAwareBlkLoad struct {
	session *Session

	batchTyp    BatchType
	policy      *tokenAwareHostPolicy
	consistency Consistency
	sizePerHost int

	batches map[uint64]*Batch
}

func (s *Session) NewTokenAwareBlkLoad(typo BatchType, consistency Consistency, sizePerHost int) (*TokenAwareBlkLoad, error) {
	p, ok := s.policy.(*tokenAwareHostPolicy)
	if !ok {
		return nil, TokenAwareBlkLoadInCorrectTokenPolicy
	}

	return &TokenAwareBlkLoad{
		session: s,

		batchTyp:    typo,
		policy:      p,
		consistency: consistency,
		sizePerHost: sizePerHost,

		batches: make(map[uint64]*Batch, len(p.hosts.get())),
	}, nil
}

func (b *TokenAwareBlkLoad) getRoutingKey(ctx context.Context, statement string, args ...interface{}) ([]byte, error) {
	routingKeyInfo, err := b.session.routingKeyInfo(ctx, statement)
	if err != nil {
		return nil, err
	}

	if routingKeyInfo == nil {
		return nil, nil
	}

	var buf = make([]byte, 0, 128)
	for i := range routingKeyInfo.indexes {
		encoded, err := Marshal(
			routingKeyInfo.types[i],
			args[routingKeyInfo.indexes[i]],
		)
		if err != nil {
			return nil, err
		}
		lenBuf := []byte{0x00, 0x00}
		binary.BigEndian.PutUint16(lenBuf, uint16(len(encoded)))
		buf = append(buf, lenBuf...)
		buf = append(buf, encoded...)
		buf = append(buf, 0x00)
	}
	return buf, nil
}

func (b *TokenAwareBlkLoad) Load(statement string, args ...interface{}) error {
	routingKey, err := b.getRoutingKey(context.Background(), statement, args...)
	if err != nil {
		return err
	}

	var meta = b.policy.getMetadataReadOnly()
	if meta == nil || meta.tokenRing == nil {
		return TokenAwareBlkLoadInCorrectTokenPolicy
	}

	var token = meta.tokenRing.partitioner.Hash(routingKey)
	var host, _ = meta.tokenRing.GetHostForToken(token)

	var hostID uint64 = 0
	if host != nil {
		var b = host.HostID()
		hostID = binary.LittleEndian.Uint64(b[8:])
	}

	batch, ok := b.batches[hostID]
	if !ok {
		batch = b.session.NewBatch(b.batchTyp)
		batch.Cons = b.consistency
		batch.Entries = make([]BatchEntry, 0, b.sizePerHost)

		b.batches[hostID] = batch
	}

	batch.Query(statement, args...)
	batch.Entries[len(batch.Entries)-1].Idempotent = true
	batch.routingKey = routingKey

	if batch.Size() > b.sizePerHost {
		_ = b.exec0(batch)

		// reset this batch, fuck gocql API
		var n = b.session.NewBatch(b.batchTyp)
		n.Cons = b.consistency
		n.Entries = batch.Entries[:0]

		b.batches[hostID] = n
	}

	return nil
}

func (b *TokenAwareBlkLoad) exec0(batch *Batch) error {
	return b.session.ExecuteBatch(batch)
}

func (b *TokenAwareBlkLoad) Reset() {
	for k, v := range b.batches {
		// reset this batch, and fuck gocql API
		var n = b.session.NewBatch(b.batchTyp)
		n.Cons = b.consistency
		n.Entries = v.Entries[:0]

		b.batches[k] = n
	}
}

func (b *TokenAwareBlkLoad) ExecuteALl() error {
	var e error
	for _, v := range b.batches {
		if len(v.Entries) == 0 {
			continue
		}

		if err := b.exec0(v); err != nil {
			e = err
		}
	}
	return e
}
