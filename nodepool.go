package dcron

import (
	"context"
	"sync"
	"time"

	"github.com/ivanlebron/dcron/driver"
	"github.com/ivanlebron/dcron/logger"
)

// NodePool is a node pool
type NodePool struct {
	serviceName string
	nodeID      string

	rwMut sync.RWMutex
	nodes *Map

	driver         driver.Driver
	hashReplicas   int
	hashFn         Hash
	updateDuration time.Duration

	logger   logger.Logger
	stopChan chan int
	preNodes []string // sorted
}

func NewNodePool(serviceName string, drv driver.Driver, updateDuration time.Duration, hashReplicas int, logger logger.Logger) INodePool {
	np := &NodePool{
		serviceName:    serviceName,
		driver:         drv,
		hashReplicas:   hashReplicas,
		updateDuration: updateDuration,
		stopChan:       make(chan int, 1),
	}
	if logger != nil {
		np.logger = logger
	}
	np.driver.Init(serviceName,
		driver.NewTimeoutOption(updateDuration),
		driver.NewLoggerOption(np.logger))
	return np
}

func (np *NodePool) Start(ctx context.Context) (err error) {
	err = np.driver.Start(ctx)
	if err != nil {
		np.logger.Errorf("start pool error: %v", err)
		return
	}
	np.nodeID = np.driver.NodeID()
	nowNodes, err := np.driver.GetNodes(ctx)
	if err != nil {
		np.logger.Errorf("get nodes error: %v", err)
		return
	}
	np.updateHashRing(nowNodes)
	go np.waitingForHashRing()
	return
}

// CheckJobAvailable Check if this job can be run in this node.
func (np *NodePool) CheckJobAvailable(jobName string) bool {
	np.rwMut.RLock()
	defer np.rwMut.RUnlock()
	if np.nodes == nil {
		np.logger.Errorf("nodeID=%s, np.nodes is nil", np.nodeID)
	}
	if np.nodes.IsEmpty() {
		return false
	}
	targetNode := np.nodes.Get(jobName)
	if np.nodeID == targetNode {
		np.logger.Infof("job %s, running in node: %s", jobName, targetNode)
	}
	return np.nodeID == targetNode
}

func (np *NodePool) Stop(ctx context.Context) error {
	np.stopChan <- 1
	np.driver.Stop(ctx)
	np.preNodes = make([]string, 0)
	return nil
}

func (np *NodePool) GetNodeID() string {
	return np.nodeID
}

func (np *NodePool) waitingForHashRing() {
	tick := time.NewTicker(np.updateDuration)
	for {
		select {
		case <-tick.C:
			nowNodes, err := np.driver.GetNodes(context.Background())
			if err != nil {
				np.logger.Errorf("get nodes error %v", err)
				continue
			}
			np.updateHashRing(nowNodes)
		case <-np.stopChan:
			return
		}
	}
}

func (np *NodePool) updateHashRing(nodes []string) {
	np.rwMut.Lock()
	defer np.rwMut.Unlock()
	if np.equalRing(nodes) {
		np.logger.Infof("nowNodes=%v, preNodes=%v", nodes, np.preNodes)
		return
	}
	np.logger.Infof("update hashRing nodes=%+v", nodes)
	np.preNodes = make([]string, len(nodes))
	copy(np.preNodes, nodes)
	np.nodes = New(np.hashReplicas, np.hashFn)
	for _, v := range nodes {
		np.nodes.Add(v)
	}
}

func (np *NodePool) equalRing(a []string) bool {
	if len(a) == len(np.preNodes) {
		la := len(a)
		for i := 0; i < la; i++ {
			if a[i] != np.preNodes[i] {
				return false
			}
		}
		return true
	}
	return false
}
