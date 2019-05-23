package sqs

import (
	"github.com/jllopis/arcadia/connectors"
)

func (c *Connector) SetDefaultPublishOptions(opts *connectors.PublishOptions) *Connector {
	c.defaultPutOpts = opts
	return c
}

func (c *Connector) SetDefaultSubscribeOptions(opts *connectors.SubscribeOptions) *Connector {
	c.defaultConsumeOpts = opts
	return c
}

func (c *Connector) SetUser(p string) *Connector {
	c.user(p)
	return c
}

func (c *Connector) SetPassword(p string) *Connector {
	c.password(p)
	return c
}

func (c *Connector) SetId(p string) *Connector {
	c.connID(p)
	return c
}

func (c *Connector) SetAccessKeyID(p string) *Connector {
	c.accessKeyID(p)
	return c
}

func (c *Connector) SetSecretAccessKey(p string) *Connector {
	c.secretAccessKey(p)
	return c
}
