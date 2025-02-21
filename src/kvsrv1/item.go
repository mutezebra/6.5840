package kvsrv

import "6.5840/kvsrv1/rpc"

type Item struct {
	K, V    string
	Version rpc.Tversion
}

func NewItem(k, v string) *Item {
	return &Item{
		K:       k,
		V:       v,
		Version: rpc.ZERO + 1,
	}
}
