package db

import (
	"math/big"
	"time"
)

type Account []byte // public key of account

// each transaction is stored in one Block
type Transaction struct {
	time   time.Time
	memo   Addr // pointer to source document
	legs   []*Leg
	author Account
	sig    []byte
}

type Leg struct {
	id      int64
	account Account
	amount  *big.Int // XXX if we do this, we'll need custom msgpack (de)marshal
	symbol  Addr     // pointer to description of asset or currency
	serial  string   // XXX maybe instead e.g. Addr of asset's description
}

// XXX we either need version numbers or some other extensibility on
// the above structs -- maybe they are interfaces instead?
