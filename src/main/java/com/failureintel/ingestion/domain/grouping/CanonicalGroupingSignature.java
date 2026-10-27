package com.failureintel.ingestion.domain.grouping;

import com.failureintel.ingestion.domain.model.NormalizedFailureEvent;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Complete canonical grouping identity and its PostgreSQL advisory-lock key. */
public final class CanonicalGroupingSignature {

    private static final int ENCODING_VERSION = 1;
    private static final byte SERVICE_NAME_TAG = 1;
    private static final byte ENVIRONMENT_TAG = 2;
    private static final byte ERROR_TYPE_TAG = 3;
    private static final byte DEPENDENCY_TARGET_TAG = 4;
    private static final byte NO_DEPENDENCY = 0;
    private static final byte DEPENDENCY_VALUE = 1;

    private final String serviceName;
    private final String environment;
    private final String errorType;
    private final String dependencyTarget;
    private final byte[] encodedSignature;
    private final long advisoryLockKey;

    private CanonicalGroupingSignature(
            String serviceName,
            String environment,
            String errorType,
            String dependencyTarget) {
        this.serviceName = serviceName;
        this.environment = environment;
        this.errorType = errorType;
        this.dependencyTarget = dependencyTarget;
        this.encodedSignature = encode();
        this.advisoryLockKey = deriveLockKey(encodedSignature);
    }

    /** Returns empty when the required canonical identity is unavailable. */
    public static Optional<CanonicalGroupingSignature> from(NormalizedFailureEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        Map<String, Object> metadata = event.getNormalizationMetadata();
        if (isTrue(metadata, "serviceNameDefaulted")
                || isTrue(metadata, "environmentDefaulted")
                || isTrue(metadata, "unknownEnvironmentValue")
                || isTrue(metadata, "errorTypeDefaulted")) {
            return Optional.empty();
        }

        return fromFields(event.getServiceName(), event.getEnvironment(), event.getErrorType(),
                event.getDependencyTarget());
    }

    /** Builds the same canonical signature from persisted incident identity fields. */
    public static Optional<CanonicalGroupingSignature> fromFields(
            String serviceName, String environment, String errorType, String dependencyTarget) {
        serviceName = trimToNull(serviceName);
        environment = trimToNull(environment);
        errorType = trimToNull(errorType);
        if (serviceName == null || environment == null || errorType == null) {
            return Optional.empty();
        }

        return Optional.of(new CanonicalGroupingSignature(
                serviceName,
                environment,
                errorType,
                trimToNull(dependencyTarget)));
    }

    public String serviceName() {
        return serviceName;
    }

    public String environment() {
        return environment;
    }

    public String errorType() {
        return errorType;
    }

    /** Empty represents logical NO_DEPENDENCY, distinct from that literal value. */
    public Optional<String> dependencyTarget() {
        return Optional.ofNullable(dependencyTarget);
    }

    /** Versioned, tagged, length-prefixed UTF-8 identity bytes. */
    public byte[] encodedSignature() {
        return encodedSignature.clone();
    }

    /** Signed PostgreSQL bigint advisory key; never use it as grouping identity. */
    public long advisoryLockKey() {
        return advisoryLockKey;
    }

    private byte[] encode() {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(bytes);
            output.writeByte(ENCODING_VERSION);
            writeRequired(output, SERVICE_NAME_TAG, serviceName);
            writeRequired(output, ENVIRONMENT_TAG, environment);
            writeRequired(output, ERROR_TYPE_TAG, errorType);
            output.writeByte(DEPENDENCY_TARGET_TAG);
            if (dependencyTarget == null) {
                output.writeByte(NO_DEPENDENCY);
            } else {
                output.writeByte(DEPENDENCY_VALUE);
                writeString(output, dependencyTarget);
            }
            output.flush();
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("In-memory signature encoding failed", impossible);
        }
    }

    private static void writeRequired(DataOutputStream output, byte tag, String value) throws IOException {
        output.writeByte(tag);
        writeString(output, value);
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(utf8.length);
        output.write(utf8);
    }

    private static long deriveLockKey(byte[] signature) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(signature);
            long key = 0;
            for (int index = 0; index < Long.BYTES; index++) {
                key = (key << Byte.SIZE) | (digest[index] & 0xffL);
            }
            return key;
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static boolean isTrue(Map<String, Object> metadata, String key) {
        return Boolean.TRUE.equals(metadata.get(key));
    }

    private static String trimToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CanonicalGroupingSignature that)) {
            return false;
        }
        return serviceName.equals(that.serviceName)
                && environment.equals(that.environment)
                && errorType.equals(that.errorType)
                && Objects.equals(dependencyTarget, that.dependencyTarget);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, environment, errorType, dependencyTarget);
    }
}
