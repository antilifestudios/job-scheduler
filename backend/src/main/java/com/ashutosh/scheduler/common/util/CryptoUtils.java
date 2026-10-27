package com.ashutosh.scheduler.common.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;

/**
 * Cryptographic and hashing utilities for API keys and request deduplication.
 */
public final class CryptoUtils {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String API_KEY_PREFIX = "sk_live_";

    private CryptoUtils() {}

    /**
     * Generates a new cryptographically secure API key with >= 32 bytes of entropy.
     *
     * @return raw API key string (e.g., "sk_live_...")
     */
    public static String generateApiKey() {
        byte[] entropy = new byte[32];
        SECURE_RANDOM.nextBytes(entropy);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(entropy);
        return API_KEY_PREFIX + token;
    }

    /**
     * Extracts display prefix (first 14 chars) for UI and identification.
     */
    public static String extractPrefix(String apiKey) {
        if (apiKey == null || apiKey.length() < 14) {
            return "sk_live_unknown";
        }
        return apiKey.substring(0, Math.min(apiKey.length(), 16));
    }

    /**
     * Computes the SHA-256 hex digest of a string.
     */
    public static String sha256Hex(String input) {
        if (input == null) {
            return null;
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }

    /**
     * Computes a canonical SHA-256 digest of an HTTP request for idempotency validation.
     *
     * Format: METHOD\nURL\n[sorted header_k:header_v\n]\nBODY
     */
    public static String computeRequestHash(String method, String url, Map<String, String> headers, String body) {
        StringBuilder canonical = new StringBuilder();
        canonical.append(method == null ? "" : method.toUpperCase()).append("\n");
        canonical.append(url == null ? "" : url.trim()).append("\n");

        if (headers != null && !headers.isEmpty()) {
            Map<String, String> sorted = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            sorted.putAll(headers);
            for (Map.Entry<String, String> entry : sorted.entrySet()) {
                canonical.append(entry.getKey().toLowerCase()).append(":").append(entry.getValue().trim()).append("\n");
            }
        }
        canonical.append("\n"); // separator between headers and body
        canonical.append(body == null ? "" : body);

        return sha256Hex(canonical.toString());
    }
}
