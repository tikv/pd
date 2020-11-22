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
	ErrorTransactionFailed = errors.Errorf("kv transaction failed")
)

type Txn interface {
	If(cs ...Cmp) Txn
	Then(ops ...Op) Txn
	Else(ops ...Op) Txn
	Commit() (interface{}, error)
}

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

type Cmp struct {
	target   cmpTarget
	key      string
	relation cmpRelation
	value    interface{}
}

func Version(key string) Cmp {
	return Cmp{target: cmpVersion, key: key}
}
func Create(key string) Cmp {
	return Cmp{target: cmpCreate, key: key}
}
func Mod(key string) Cmp {
	return Cmp{target: cmpMod, key: key}
}
func Value(key string) Cmp {
	return Cmp{target: cmpValue, key: key}
}

func Eq(cmp Cmp, value interface{}) Cmp {
	cmp.relation = cmpEq
	cmp.value = value
	return cmp
}
func Gt(cmp Cmp, value interface{}) Cmp {
	cmp.relation = cmpGt
	cmp.value = value
	return cmp
}
func Lt(cmp Cmp, value interface{}) Cmp {
	cmp.relation = cmpLt
	cmp.value = value
	return cmp
}
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

type Op struct {
	t        opType
	key      string
	rangeEnd string
	value    string
	limit    int64
}

func OpSave(key string, value string) Op {
	return Op{t: opSave, key: key, value: value}
}

func OpRemove(key string) Op {
	return Op{t: opRemove, key: key}
}

func OpRemoveRange(key string, rangeEnd string, limit int64) Op {
	return Op{t: opRemoveRange, key: key, rangeEnd: rangeEnd, limit: limit}
}
