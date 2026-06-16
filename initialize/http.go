package initialize

import "context"

type HttpWorker struct {
	WebhookServer func(ctx context.Context) error
	GrpcServer    func(ctx context.Context) error
}
