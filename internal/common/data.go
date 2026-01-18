package common

import "iter"

type KVPair[T any] struct {
	Key   []byte
	Value T
}

type KVIterator[T any] iter.Seq[KVPair[T]]
