package worker

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

// ContextWithSignals returns a context cancelled on SIGTERM/SIGINT, plus
// the stop function (call via defer to release signal.Notify resources).
func ContextWithSignals(parent context.Context, logger *zap.Logger) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		select {
		case sig := <-sigCh:
			logger.Info("shutdown signal received", zap.String("signal", sig.String()))
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, func() {
		signal.Stop(sigCh)
		cancel()
	}
}