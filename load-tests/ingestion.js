
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

// 1. Runtime configuration
const BASE_URL = (
    __ENV.FAILUREINTEL_LOADTEST_BASE_URL || 'http://ingestion-api:8081'
).replace(/\/+$/, '');

const RATE = Number(__ENV.FAILUREINTEL_LOADTEST_RATE || '5');
const DURATION = __ENV.FAILUREINTEL_LOADTEST_DURATION || '30s';
const RUN_ID = __ENV.FAILUREINTEL_LOADTEST_RUN_ID;

if (!RUN_ID || !/^[a-zA-Z0-9_-]+$/.test(RUN_ID)) {
    throw new Error('Provide a nonempty alphanumeric RUN_ID.');
}

if (!Number.isSafeInteger(RATE) || RATE <= 0) {
    throw new Error('RATE must be a positive integer.');
}

// 2. Custom metrics
const accepted = new Counter('ingestion_accepted');
const failures = new Counter('ingestion_failures');
const successRate = new Rate('ingestion_success_rate');

// 3. One constant-arrival-rate scenario
export const options = {
    scenarios: {
        ingestion: {
            executor: 'constant-arrival-rate',
            rate: RATE,
            timeUnit: '1s',
            duration: DURATION,
            preAllocatedVUs: 20,
            maxVUs: 100,
        },
    },

    thresholds: {
        ingestion_success_rate: ['rate==1'],
        dropped_iterations: ['count==0'],
    },
};

// 4. One iteration = one HTTP POST
export default function () {
    const eventKey = `k6-${RUN_ID}-${__VU}-${__ITER}`;

    const payload = {
        occurredAt: new Date().toISOString(),
        serviceName: 'failureintel-loadtest',
        serverName: 'k6-generator',
        environment: 'loadtest',
        eventType: 'APPLICATION_ERROR',
        errorType: 'LoadTestException',
        errorMessage: `Synthetic failure event ${eventKey}`,
        severityHint: 'ERROR',
        rawPayload: {
            source: 'k6',
            runId: RUN_ID,
            eventKey: eventKey,
            synthetic: true,
        },
    };

    const response = http.post(
        `${BASE_URL}/api/v1/failure-events`,
        JSON.stringify(payload),
        {
            headers: {
                'Content-Type': 'application/json',
                'Idempotency-Key': eventKey,
            },
            timeout: '10s',
        }
    );

    // 5. Validate the API response
    let body = null;

    try {
        body = response.json();
    } catch (_) {
        // Invalid JSON is counted as a failed request.
    }

    const valid = check(response, {
        'HTTP 202': (r) => r.status === 202,
        'eventId present': () =>
            typeof body?.eventId === 'string' &&
            body.eventId.length > 0,
        'status RECEIVED': () =>
            body?.status === 'RECEIVED',
    });

    successRate.add(valid);

    if (valid) {
        accepted.add(1);
    } else {
        failures.add(1);
    }
}
