// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissionKeys and
// limitations under the License.
//

package kv

import "github.com/pingcap/errors"

var (
	// ErrTransactionFailed is error info for failed transactions.
	ErrTransactionFailed = errors.Errorf("kv transaction failed")
)

// Txn wraps kv transaction.
type Txn interface {
	If(cs ...Cmp) Txn
	Then(ops ...Op) Txn
	Else(ops ...Op) Txn
	Commit() (interface{}, error)
}

// TxnBase is an abstract interface for load/save pd cluster data.
type TxnBase interface {
	Base
	NewTxn() Txn
}

type cmpTarget int64

const (
	cmpVersion cmpTarget = iota
	cmpCreate
	cmpMod
	cmpValue
)

type cmpRelation string

const (
	cmpEq  cmpRelation = "="
	cmpGt  cmpRelation = ">"
	cmpLt  cmpRelation = "<"
	cmpNeq cmpRelation = "!="
)

// Cmp wraps transaction condition.
type Cmp struct {
	target   cmpTarget
	key      string
	relation cmpRelation
	value    interface{}
}

// Version is a transaction condition based on version of a key.
func Version(key string) Cmp {
	return Cmp{target: cmpVersion, key: key}
}

// Create is a transaction condition based on creation revision of a key.
func Create(key string) Cmp {
	return Cmp{target: cmpCreate, key: key}
}

// Mod is a transaction condition based on modification revision of a key.
func Mod(key string) Cmp {
	return Cmp{target: cmpMod, key: key}
}

// Value is a transaction condition based on value of a key.
func Value(key string) Cmp {
	return Cmp{target: cmpValue, key: key}
}

// Eq is a transaction condition based on equality of target and given value.
func Eq(cmp Cmp, value interface{}) Cmp {
	cmp.relation = cmpEq
	cmp.value = value
	return cmp
}

// Gt is a transaction condition that given value is greater than target.
func Gt(cmp Cmp, value interface{}) Cmp {
	cmp.relation = cmpGt
	cmp.value = value
	return cmp
}

// Lt is a transaction condition that given value is less than target.
func Lt(cmp Cmp, value interface{}) Cmp {
	cmp.relation = cmpLt
	cmp.value = value
	return cmp
}

// Neq is a transaction condition based on inequality of target and given value.
func Neq(cmp Cmp, value interface{}) Cmp {
	cmp.relation = cmpNeq
	cmp.value = value
	return cmp
}

type opType int64

const (
	opSave opType = iota
	opRemove
	opRemoveRange
)

// Op wraps transaction operator.
type Op struct {
	t        opType
	key      string
	rangeEnd string
	value    string
	limit    int64
}

// OpSave is a save operator.
func OpSave(key string, value string) Op {
	return Op{t: opSave, key: key, value: value}
}

// OpRemove is a remove operator.
func OpRemove(key string) Op {
	return Op{t: opRemove, key: key}
}

// OpRemoveRange is a range remove operator.
func OpRemoveRange(key string, rangeEnd string, limit int64) Op {
	return Op{t: opRemoveRange, key: key, rangeEnd: rangeEnd, limit: limit}
}
