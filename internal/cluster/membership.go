package cluster

type Cluster struct {
	NodeID string
	Peers  []string
}

func NewCluster(id string, peers []string) *Cluster {
	return &Cluster{
		NodeID: id,
		Peers:  peers,
	}
}

func (c *Cluster) GetPeers() []string {
	return c.Peers
}