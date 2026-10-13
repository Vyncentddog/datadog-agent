// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build kubeapiserver

package orchestrator

// retrier schedules the retries of a failing operation with an exponential backoff counted in check runs.
type retrier struct {
	baseRuns int
	delay    int
	failures int
	lastErr  error
	maxRuns  int
	runsLeft int
}

// newRetrier returns a retrier whose delay starts at baseRuns and doubles on every failure up to maxRuns. A baseRuns
// lower than 1 is raised to 1 and a maxRuns lower than baseRuns is raised to baseRuns.
func newRetrier(baseRuns, maxRuns int) *retrier {
	baseRuns = max(baseRuns, 1)
	return &retrier{
		baseRuns: baseRuns,
		maxRuns:  max(maxRuns, baseRuns),
	}
}

// Fail records a failure and schedules the next retry.
func (r *retrier) Fail(err error) {
	r.lastErr = err
	r.failures++
	r.delay = r.delayFor(r.failures)
	r.runsLeft = r.delay
}

// IsDelayMaxed reports whether the scheduled retry waits the maximum delay.
func (r *retrier) IsDelayMaxed() bool {
	return r.delay >= r.maxRuns
}

// LastError returns the error of the last failure.
func (r *retrier) LastError() error {
	return r.lastErr
}

// Reset restarts the backoff from the base delay.
func (r *retrier) Reset() {
	r.delay = 0
	r.failures = 0
	r.lastErr = nil
	r.runsLeft = 0
}

// RunsLeft returns the number of runs before the scheduled retry is due.
func (r *retrier) RunsLeft() int {
	return r.runsLeft
}

// Tick counts a run and reports whether the scheduled retry is due.
func (r *retrier) Tick() bool {
	r.runsLeft--
	return r.runsLeft <= 0
}

// delayFor returns the number of runs to wait after failures failures in a row.
func (r *retrier) delayFor(failures int) int {
	delay := r.baseRuns
	for i := 1; i < failures && delay < r.maxRuns; i++ {
		delay *= 2
	}
	return min(delay, r.maxRuns)
}
