package com.ashutosh.scheduler.worker.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffCalculatorTest {

    @Test
    @DisplayName("Delay must always be >= 1 second and <= min(maxBackoff, baseBackoff * 2^attempt)")
    void testDelayWithinBounds() {
        long base = 5;
        long max = 3600;

        for (int attempt = 1; attempt <= 10; attempt++) {
            long maxExpectedCeiling = Math.min(max, base * (1L << attempt));
            for (int i = 0; i < 50; i++) {
                long delay = BackoffCalculator.calculateDelaySeconds(attempt, base, max);
                assertThat(delay)
                    .as("Delay for attempt %d must be between 1 and %d", attempt, maxExpectedCeiling)
                    .isGreaterThanOrEqualTo(1)
                    .isLessThanOrEqualTo(maxExpectedCeiling);
            }
        }
    }

    @Test
    @DisplayName("Large attempt numbers must cap at maxBackoff without overflow")
    void testLargeAttemptNoOverflow() {
        long base = 5;
        long max = 3600;

        for (int attempt = 30; attempt <= 100; attempt += 10) {
            long delay = BackoffCalculator.calculateDelaySeconds(attempt, base, max);
            assertThat(delay)
                .as("Delay for attempt %d must not overflow and stay <= maxBackoff", attempt)
                .isGreaterThanOrEqualTo(1)
                .isLessThanOrEqualTo(max);
        }
    }

    @Test
    @DisplayName("Full jitter must produce variable randomized delays for the same attempt")
    void testFullJitterProducesVariation() {
        long base = 10;
        long max = 1000;
        int attempt = 5; // ceiling = 320

        Set<Long> generatedDelays = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            generatedDelays.add(BackoffCalculator.calculateDelaySeconds(attempt, base, max));
        }

        // Over 100 iterations, with ceiling 320, we should see substantial randomness
        assertThat(generatedDelays.size())
            .as("Full jitter must generate multiple distinct delays to avoid thundering herds")
            .isGreaterThan(20);
    }
}
