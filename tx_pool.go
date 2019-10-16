package bolt

import (
	"errors"
	"sync/atomic"
	"time"
)

// TxPool represents a pool of read transactions. All Tx objects from the pool
// must be put back into the pool after use.
type TxPool struct {
	output chan *Tx
	input  chan *Tx
	side   chan *Tx

	size    int
	timeout time.Duration

	putCount int64
	closed   int32
}

// TxPool initializes a new transaction pool. Currently only 1 TxPool per DB
// is supported. A TxPool can also not be closed.
func (db *DB) TxPool(size int, timeout time.Duration) (*TxPool, error) {
	if db.txPool != nil {
		return nil, errors.New("only one TxPool per DB is allowed")
	}
	pool := &TxPool{
		output:  make(chan *Tx, size),
		input:   make(chan *Tx, size),
		side:    make(chan *Tx, size),
		size:    size,
		timeout: timeout,
	}
	err := pool.populateChan(db)
	if err != nil {
		return nil, err
	}
	go pool.returner(db)
	go pool.refresher()
	db.txPool = pool
	return pool, nil
}

// Get fetches an open read transaction from the pool.
func (p *TxPool) Get() *Tx {
	return <-p.output
}

// Put returns a transaction that is not used anymore to the pool.
func (p *TxPool) Put(tx *Tx) {
	p.input <- tx
}

func (p *TxPool) populateChan(db *DB) error {
	for i := 0; i < p.size; i++ {
		tx, err := db.Begin(false)
		if err != nil {
			return err
		}
		tx.opened = time.Now()
		p.output <- tx
	}
	return nil
}

func (p *TxPool) mmapLock() {
	if p == nil {
		return
	}
	atomic.StoreInt32(&p.closed, 1)
	for len(p.output) > 0 {
		select {
		case tx := <-p.output:
			p.input <- tx
		default:
		}
	}
}

func (p *TxPool) mmapUnlock() {
	if p == nil {
		return
	}
	atomic.StoreInt32(&p.closed, 0)
	for len(p.side) > 0 {
		tx := <-p.side
		p.input <- tx
	}
}

func (p *TxPool) returner(db *DB) {
	for {
		tx := <-p.input

		if atomic.LoadInt32(&p.closed) == 1 {
			if tx != nil {
				tx.Rollback()
			}
			tx = nil
			p.side <- tx
			continue
		}

		atomic.AddInt64(&p.putCount, 1)
		var err error
		if tx == nil {
			tx, err = db.Begin(false)
			if err != nil {
				tx = nil
			} else {
				tx.opened = time.Now()
			}
		} else if time.Since(tx.opened) > p.timeout {
			if tx != nil {
				tx.Rollback()
			}
			tx, err = db.Begin(false)
			if err != nil {
				tx = nil
			} else {
				tx.opened = time.Now()
			}
		}
		p.output <- tx
	}
}

func (p *TxPool) refresher() {
	tick := time.NewTicker(p.timeout)
	for {
		<-tick.C
		if atomic.LoadInt32(&p.closed) == 1 {
			continue
		}
		puts := atomic.SwapInt64(&p.putCount, 0)
		if int(puts) > p.size {
			continue
		}
		for i := 0; i < p.size; i++ {
			tx := <-p.output
			p.input <- tx
		}
	}
}
