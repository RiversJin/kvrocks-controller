package util

import "sync"

type RWLock[T any] struct {
	mu   sync.RWMutex
	data T
}

func NewRWLock[T any](data T) *RWLock[T] {
	return &RWLock[T]{data: data}
}

func (r *RWLock[T]) Get() T {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.data
}

func (r *RWLock[T]) Set(data T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = data
}

func (r *RWLock[T]) Update(f func(data T) T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = f(r.data)
}

func (r *RWLock[T]) Read(f func(data T)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f(r.data)
}
