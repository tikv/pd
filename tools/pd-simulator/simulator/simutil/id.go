package simutil

// IDAllocator is used to alloc unique ID.
type idAllocator struct {
	id uint64
}

// NextID gets the next unique ID.
func (a *idAllocator) NextID() uint64 {
	a.id++
	return a.id
}

// ResetID resets the IDAllocator.
func (a *idAllocator) ResetID() {
	a.id = 0
}

// GetID gets the current ID.
func (a *idAllocator) GetID() uint64 {
	return a.id
}

// IDAllocator is used to alloc unique ID.
var IDAllocator idAllocator
