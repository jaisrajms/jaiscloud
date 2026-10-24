package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type countingMonitoringTicker struct{ n atomic.Int32 }

func (c *countingMonitoringTicker) TickNow(context.Context) { c.n.Add(1) }

func TestMonitoringTickHandler_DelegatesAndReturns204(t *testing.T) {
	h := NewHandler()
	ticker := &countingMonitoringTicker{}
	h.RegisterMonitoringTicker(ticker)

	rec := httptest.NewRecorder()
	h.MonitoringTickHandler(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/monitoring-tick", nil))

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, int32(1), ticker.n.Load())
}

func TestMonitoringTickHandler_NoTickerIsNoop(t *testing.T) {
	h := NewHandler()

	rec := httptest.NewRecorder()
	h.MonitoringTickHandler(rec, httptest.NewRequest(http.MethodPost, "/_jaiscloud/monitoring-tick", nil))

	require.Equal(t, http.StatusNoContent, rec.Code)
}
