// k6 write-throughput benchmark for POST /orders.
// Each iteration uses a fresh Idempotency-Key so every request creates a
// genuinely new order (measuring the write path, not the idempotency
// replay fast path).
//
// Usage (max-throughput mode): k6 run -e VUS=50 -e DURATION=20s bench/write_throughput.js
// Usage (fixed-rate mode, for e2e-lag measurement well under drain
//   capacity): k6 run -e RATE=20 -e DURATION=10s bench/write_throughput.js
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const VUS = parseInt(__ENV.VUS || '10', 10);
const DURATION = __ENV.DURATION || '20s';
const RATE = __ENV.RATE ? parseInt(__ENV.RATE, 10) : null;

const scenario = RATE
    ? {
        executor: 'constant-arrival-rate',
        rate: RATE,
        timeUnit: '1s',
        duration: DURATION,
        preAllocatedVUs: Math.max(RATE, 10),
        maxVUs: Math.max(RATE * 2, 20),
    }
    : {
        executor: 'constant-vus',
        vus: VUS,
        duration: DURATION,
    };

export const options = {
    scenarios: { write: scenario },
    summaryTrendStats: ['avg', 'min', 'med', 'p(50)', 'p(95)', 'p(99)', 'max'],
    thresholds: {
        http_req_failed: ['rate<0.01'],
    },
};

export default function () {
    const key = `bench-${__VU}-${__ITER}-${Date.now()}-${Math.random()}`;
    const body = JSON.stringify({
        customerId: '8f14e45f-ceea-467a-9e4d-3e2a1b7c9d01',
        currency: 'INR',
        items: [{ sku: 'BENCH-SKU', quantity: 1, unitPriceCents: 500 }],
    });
    const res = http.post(`${BASE_URL}/orders`, body, {
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    });
    check(res, { 'status is 201': (r) => r.status === 201 });
}
