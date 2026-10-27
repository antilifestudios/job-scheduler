package com.ashutosh.scheduler.common.util;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * RFC 9562 compliant UUIDv7 generator.
 *
 * Provides monotonic time-ordered UUIDs with a 48-bit millisecond timestamp
 * prefix. In PostgreSQL, this guarantees right-edge B-Tree index append locality,
 * eliminating random page splits, preserving buffer cache efficiency, and
 * matching the sequential insertion performance of a BIGINT while retaining
 * 128-bit global uniqueness.
 */
public final class UuidV7 {

    private static final SecureRandom RANDOM = new SecureRandom();

    private UuidV7() {
        // Utility class
    }

    /**
     * Generates a new time-ordered UUIDv7 based on current system time.
     *
     * @return monotonic UUIDv7
     */
    public static UUID generate() {
        return generate(System.currentTimeMillis());
    }

    /**
     * Generates a time-ordered UUIDv7 for a specific timestamp in milliseconds.
     *
     * @param timestampMs millisecond unix timestamp
     * @return monotonic UUIDv7
     */
    public static UUID generate(long timestampMs) {
        long mostSigBits = (timestampMs & 0xFFFFFFFFFFFFL) << 16;
        // 4-bit version 7 (0111)
        mostSigBits |= 0x7000L;
        // 12-bit random / sequence
        mostSigBits |= (RANDOM.nextLong() & 0x0FFFL);

        long leastSigBits = RANDOM.nextLong();
        // 2-bit RFC 4122/9562 variant (10xx)
        leastSigBits = (leastSigBits & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;

        return new UUID(mostSigBits, leastSigBits);
    }
}
