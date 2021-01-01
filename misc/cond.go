package misc

import "sync"

type Cond struct {
	sync.Cond
}

func NewCond() *Cond {
	return &Cond{
		Cond: sync.Cond{
			L: &sync.Mutex{},
		},
	}
}

func (c *Cond) WaitAndUnlock() {
	c.Wait()
	c.L.Unlock()
}

func (c *Cond) SignalLocked() {
	c.L.Lock()
	c.Signal()
	c.L.Unlock()
}
