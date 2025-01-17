package domain

type Cluster struct {
	Name           string
	Address        string
	SASLUser       string
	SASLPassword   string
	SASLMechanism  string
	CACertPath     string
	ClientCertPath string
	ClientKeyPath  string
}

func (c *Cluster) IsValid() bool {
	return c.Name != "" && c.Address != ""
}
