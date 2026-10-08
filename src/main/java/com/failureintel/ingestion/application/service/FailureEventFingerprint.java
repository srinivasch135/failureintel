package com.failureintel.ingestion.application.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.failureintel.ingestion.domain.model.RawFailureEvent;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.TreeMap;

final class FailureEventFingerprint {

    private static final String VERSION = "v1:";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private FailureEventFingerprint() {
    }

    static String calculate(RawFailureEvent rawEvent) {
        ObjectNode content = JsonNodeFactory.instance.objectNode();
        put(content, "sourceSystem", rawEvent.getSourceSystem());
        put(content, "serviceName", rawEvent.getServiceName());
        put(content, "environment", rawEvent.getEnvironment());
        put(content, "eventType", rawEvent.getEventType());
        put(content, "errorType", rawEvent.getErrorType());
        put(content, "errorMessage", rawEvent.getErrorMessage());
        put(content, "dependencyTarget", rawEvent.getDependencyTarget());
        put(content, "severityHint", rawEvent.getSeverityHint());
        put(content, "occurredAt", rawEvent.getOccurredAt() == null
                ? null
                : rawEvent.getOccurredAt().toString());
        content.set("rawPayload", canonicalize(OBJECT_MAPPER.valueToTree(rawEvent.getRawPayload())));
        content.set("sourceMetadata", canonicalize(OBJECT_MAPPER.valueToTree(rawEvent.getMetaData())));

        try {
            byte[] serialized = OBJECT_MAPPER.writeValueAsBytes(canonicalize(content));
            return VERSION + toHex(MessageDigest.getInstance("SHA-256").digest(serialized));
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Unable to serialize event for idempotency comparison", exception);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }

    private static void put(ObjectNode node, String field, String value) {
        if (value == null) {
            node.putNull(field);
        } else {
            node.put(field, value);
        }
    }

    private static JsonNode canonicalize(JsonNode node) {
        if (node == null || node.isValueNode() || node.isMissingNode()) {
            return node == null ? JsonNodeFactory.instance.nullNode() : node;
        }
        if (node.isArray()) {
            ArrayNode array = JsonNodeFactory.instance.arrayNode();
            node.forEach(child -> array.add(canonicalize(child)));
            return array;
        }

        TreeMap<String, JsonNode> orderedFields = new TreeMap<>();
        node.fields().forEachRemaining(field -> orderedFields.put(field.getKey(), canonicalize(field.getValue())));
        ObjectNode object = JsonNodeFactory.instance.objectNode();
        orderedFields.forEach(object::set);
        return object;
    }

    private static String toHex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }
}
