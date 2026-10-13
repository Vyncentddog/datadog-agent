// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-present Datadog, Inc.

//go:build kubeapiserver && test

package orchestrator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetrier(t *testing.T) {
	t.Run("Success - Delay maxed", func(t *testing.T) {
		r := newRetrier(1, 4)

		maxed := make([]bool, 0, 4)
		for range 4 {
			r.Fail(errors.New("discovery failed"))
			maxed = append(maxed, r.IsDelayMaxed())
		}

		require.Equal(t, []bool{false, false, true, true}, maxed)
	})
	t.Run("Success - Delays back off up to the maximum", func(t *testing.T) {
		r := newRetrier(1, 32)

		delays := make([]int, 0, 8)
		for range 8 {
			r.Fail(errors.New("discovery failed"))
			delays = append(delays, r.RunsLeft())
		}

		require.Equal(t, []int{1, 2, 4, 8, 16, 32, 32, 32}, delays)
	})
	t.Run("Success - Delays start from the base", func(t *testing.T) {
		r := newRetrier(3, 20)

		delays := make([]int, 0, 4)
		for range 4 {
			r.Fail(errors.New("discovery failed"))
			delays = append(delays, r.RunsLeft())
		}

		require.Equal(t, []int{3, 6, 12, 20}, delays)
	})
	t.Run("Success - Invalid bounds raised", func(t *testing.T) {
		r := newRetrier(0, -5)

		r.Fail(errors.New("discovery failed"))
		r.Fail(errors.New("discovery failed"))

		require.Equal(t, 1, r.RunsLeft())
		require.True(t, r.IsDelayMaxed())
	})
	t.Run("Success - Last error recorded", func(t *testing.T) {
		r := newRetrier(1, 32)
		firstErr := errors.New("client rate limiter Wait returned an error")
		lastErr := errors.New("unable to retrieve the complete list of server APIs")

		r.Fail(firstErr)
		r.Fail(lastErr)

		require.Equal(t, lastErr, r.LastError())
	})
	t.Run("Success - Reset restarts the backoff", func(t *testing.T) {
		r := newRetrier(1, 32)
		r.Fail(errors.New("discovery failed"))
		r.Fail(errors.New("discovery failed"))
		r.Fail(errors.New("discovery failed"))

		r.Reset()
		resetState := []any{r.RunsLeft(), r.IsDelayMaxed(), r.LastError()}
		r.Fail(errors.New("discovery failed again"))

		require.Equal(t, []any{0, false, nil}, resetState)
		require.Equal(t, 1, r.RunsLeft())
	})
	t.Run("Success - Retry due after the scheduled runs", func(t *testing.T) {
		r := newRetrier(4, 32)
		r.Fail(errors.New("discovery failed"))

		due := make([]bool, 0, 4)
		for range 4 {
			due = append(due, r.Tick())
		}

		require.Equal(t, []bool{false, false, false, true}, due)
		require.Equal(t, 0, r.RunsLeft())
	})
	t.Run("Success - Retry due without failure", func(t *testing.T) {
		r := newRetrier(1, 32)

		require.True(t, r.Tick())
	})
}
