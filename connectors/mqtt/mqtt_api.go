package mqtt

func (c *Connector) Name() string {
	return c.name
}

// ID returns the client ID
func (c *Connector) ID() string {
	return c.clientOptions.ClientID
}
