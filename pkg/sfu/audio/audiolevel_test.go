package audio

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	sampleDuration     = 20 * time.Millisecond
	samplesPerBatch    = 25
	defaultActiveLevel = 30
	// requires two noisy samples to count
	defaultPercentile      = 10
	defaultObserveDuration = 500 // ms
)

func TestAudioLevel(t *testing.T) {
	t.Run("initially to return not noisy, within a few samples", func(t *testing.T) {
		a := createAudioLevel(defaultActiveLevel, defaultPercentile, defaultObserveDuration)
		_, noisy := a.GetLevel(0)
		require.False(t, noisy)

		now := observeSamples(a, 0, 28, 5)
		_, noisy = a.GetLevel(now)
		require.False(t, noisy)
	})

	t.Run("not noisy when all samples are below threshold", func(t *testing.T) {
		a := createAudioLevel(defaultActiveLevel, defaultPercentile, defaultObserveDuration)

		now := observeSamples(a, 0, 35, 100)
		_, noisy := a.GetLevel(now)
		require.False(t, noisy)
	})

	t.Run("not noisy when less than percentile samples are above threshold", func(t *testing.T) {
		a := createAudioLevel(defaultActiveLevel, defaultPercentile, defaultObserveDuration)

		now := observeSamples(a, 0, 35, samplesPerBatch-2)
		now = observeSamples(a, now, 25, 1)
		now = observeSamples(a, now, 35, 1)

		_, noisy := a.GetLevel(now)
		require.False(t, noisy)
	})

	t.Run("noisy when higher than percentile samples are above threshold", func(t *testing.T) {
		a := createAudioLevel(defaultActiveLevel, defaultPercentile, defaultObserveDuration)

		now := observeSamples(a, 0, 35, samplesPerBatch-16)
		now = observeSamples(a, now, 25, 8)
		now = observeSamples(a, now, 29, 8)

		level, noisy := a.GetLevel(now)
		require.True(t, noisy)
		require.Greater(t, level, ConvertAudioLevel(float64(defaultActiveLevel)))
		require.Less(t, level, ConvertAudioLevel(float64(25)))

		staleAt := now + int64(2*defaultObserveDuration*time.Millisecond)
		_, noisy = a.GetLevel(staleAt - 1)
		require.True(t, noisy)
		level, noisy = a.GetLevel(staleAt)
		require.False(t, noisy)
		require.Zero(t, level)
	})
}

func createAudioLevel(activeLevel uint8, minPercentile uint8, observeDuration uint32) *AudioLevel {
	return NewAudioLevel(AudioLevelParams{
		ActiveLevel:     activeLevel,
		MinPercentile:   minPercentile,
		ObserveDuration: observeDuration,
	})
}

func observeSamples(a *AudioLevel, now int64, level uint8, count int) int64 {
	for i := 0; i < count; i++ {
		now += int64(sampleDuration)
		a.Observe(level, uint32(sampleDuration.Milliseconds()), now)
	}
	return now
}
