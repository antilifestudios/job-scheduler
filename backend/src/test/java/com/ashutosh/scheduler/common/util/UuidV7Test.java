package com.ashutosh.scheduler.common.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class UuidV7Test {

    @Test
    @DisplayName("Generated UUID should have version 7 and RFC 9562 variant")
    void testUuidV7Structure() {
        UUID uuid = UuidV7.generate();
        assertThat(uuid).isNotNull();
        assertThat(uuid.version()).isEqualTo(7);
        assertThat(uuid.variant()).isEqualTo(2); // RFC 4122/9562 variant is 2
    }

    @Test
    @DisplayName("Successive UUIDv7s should maintain monotonic ordering")
    void testMonotonicOrdering() throws InterruptedException {
        UUID first = UuidV7.generate();
        Thread.sleep(2); // Ensure timestamp advances
        UUID second = UuidV7.generate();

        assertThat(first.compareTo(second))
            .as("Earlier UUIDv7 must sort before later UUIDv7")
            .isLessThan(0);
    }
}
