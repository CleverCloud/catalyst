package catalyser

import (
	"bytes"
	"errors"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/ovh/catalyst/core"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	log "github.com/sirupsen/logrus"
)

// HandleRemoteWrite support remote_write protocol
// https://github.com/prometheus/prometheus/tree/0e0fc5a7f45ce28632f43f1ead0183ee82c7afca/documentation/examples/remote_storage/remote_storage_adapter
func HandleRemoteWrite(url *url.URL, headers *http.Header, r io.Reader, send func([]byte) error, dpCounter prometheus.Counter) (int, int, error) {
	var err error
	dps := 0

	compressed, err := io.ReadAll(r)
	if err != nil {
		log.WithError(err).Error("Cannot read body")
		return 0, http.StatusBadRequest, err
	}

	format := expfmt.ResponseFormat(*headers)
	if format == expfmt.FmtUnknown {
		return 0, http.StatusBadRequest, errors.New("unknown format")
	}

	reqBuf := compressed
	switch headers.Get("Content-Encoding") {
	case "snappy":
		reqBuf, err = snappy.Decode(nil, compressed)
		if err != nil {
			log.Error("msg", "Decode error", "err", err.Error())
			return 0, http.StatusInternalServerError, err
		}
	}

	dec := expfmt.NewDecoder(bytes.NewReader(reqBuf), format)
	for {
		var mf dto.MetricFamily
		if err := dec.Decode(&mf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return dps, http.StatusBadRequest, err
		}

		for _, promGts := range mf.GetMetric() {
			for _, gts := range formatPromGts(mf.GetName(), mf.GetType(), promGts) {
				if err := send(gts.Encode()); err != nil {
					return dps, http.StatusInternalServerError, err
				}
				dpCounter.Inc()
				dps++
			}
		}
	}

	// dps processed, response status code, error
	return dps, http.StatusOK, nil
}

func formatPromGts(familyName string, familyType dto.MetricType, ts *dto.Metric) []*core.GTS {
	// Base labels from the metric (no __name__ in dto metrics)
	baseLabels := map[string]string{}
	for _, label := range ts.GetLabel() {
		baseLabels[label.GetName()] = label.GetValue()
	}

	// Helper to clone labels and set one
	cloneWith := func(extra map[string]string) map[string]string {
		out := make(map[string]string, len(baseLabels)+len(extra))
		maps.Copy(out, baseLabels)
		maps.Copy(out, extra)
		return out
	}

	tsMs := ts.GetTimestampMs()
	if tsMs == 0 {
		tsMs = time.Now().UnixMilli()
	}

	var out []*core.GTS

	switch familyType {
	case dto.MetricType_GAUGE:
		if ts.GetGauge() != nil {
			out = append(out, makePoint(familyName, cloneWith(nil), tsMs, ts.GetGauge().GetValue()))
		}
	case dto.MetricType_COUNTER:
		if ts.GetCounter() != nil {
			out = append(out, makePoint(familyName, cloneWith(nil), tsMs, ts.GetCounter().GetValue()))
		}
	case dto.MetricType_UNTYPED:
		if ts.GetUntyped() != nil {
			out = append(out, makePoint(familyName, cloneWith(nil), tsMs, ts.GetUntyped().GetValue()))
		}
	case dto.MetricType_SUMMARY:
		if s := ts.GetSummary(); s != nil {
			// Quantiles under base name with quantile label
			for _, q := range s.GetQuantile() {
				qLabel := map[string]string{"quantile": strconv.FormatFloat(q.GetQuantile(), 'g', -1, 64)}
				out = append(out, makePoint(familyName, cloneWith(qLabel), tsMs, q.GetValue()))
			}
			// _count and _sum
			out = append(out, makePoint(familyName+"_count", cloneWith(nil), tsMs, float64(s.GetSampleCount())))
			out = append(out, makePoint(familyName+"_sum", cloneWith(nil), tsMs, s.GetSampleSum()))
		}
	case dto.MetricType_HISTOGRAM:
		if h := ts.GetHistogram(); h != nil {
			// Buckets under name_bucket with le label (upper bound)
			for _, b := range h.GetBucket() {
				ub := b.GetUpperBound()
				le := strconv.FormatFloat(ub, 'g', -1, 64)
				if math.IsInf(ub, 1) {
					le = "+Inf"
				}
				leLabel := map[string]string{"le": le}
				out = append(out, makePoint(familyName+"_bucket", cloneWith(leLabel), tsMs, float64(b.GetCumulativeCount())))
			}
			// _count and _sum
			out = append(out, makePoint(familyName+"_count", cloneWith(nil), tsMs, float64(h.GetSampleCount())))
			out = append(out, makePoint(familyName+"_sum", cloneWith(nil), tsMs, h.GetSampleSum()))
		}
	default:
		// Fallback: try untyped/counter/gauge if present
		if ts.GetGauge() != nil {
			out = append(out, makePoint(familyName, cloneWith(nil), tsMs, ts.GetGauge().GetValue()))
		} else if ts.GetCounter() != nil {
			out = append(out, makePoint(familyName, cloneWith(nil), tsMs, ts.GetCounter().GetValue()))
		} else if ts.GetUntyped() != nil {
			out = append(out, makePoint(familyName, cloneWith(nil), tsMs, ts.GetUntyped().GetValue()))
		}
	}

	log.Debugf("gts len=%d", len(out))
	return out
}

func makePoint(name string, labels map[string]string, tsMs int64, v float64) *core.GTS {
	// Normalize invalid floats
	if v == math.Inf(1) || v == math.Inf(-1) || math.IsNaN(v) {
		v = 0
	}
	return &core.GTS{
		Name:   name,
		Labels: labels,
		Ts:     float64(tsMs * 1000), // ms -> μs
		Value:  v,
	}
}
