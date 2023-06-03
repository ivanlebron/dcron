package driver

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Driver There is only one driver for one dcron.
// Tips for write a user-defined Driver by yourself.
//  1. Confirm that `Stop` and `Start` can be called for more times.
//  2. Must make `GetNodes` will return error when timeout.
type Driver interface {
	Init(serviceName string, opts ...Option)
	NodeID() string
	GetNodes(ctx context.Context) (nodes []string, err error)
	Start(ctx context.Context) (err error)
	Stop(ctx context.Context) (err error)

	withOption(opt Option) (err error)
}

func NewRedisDriver(redisClient *redis.Client) Driver {
	return newRedisDriver(redisClient)
}

func NewRedisZSetDriver(redisClient *redis.Client) Driver {
	return newRedisZSetDriver(redisClient)
}
