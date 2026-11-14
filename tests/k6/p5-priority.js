import http from 'k6/http';
import { check, fail } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const apiBaseURL = (__ENV.BASE_URL || __ENV.API_BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
const authBaseURL = (__ENV.AUTH_BASE_URL || 'http://localhost:8091').replace(/\/$/, '');
const testDuration = __ENV.DURATION || '45s';

const fieldSuccess = new Rate('field_success_rate');
const fieldRejected = new Rate('field_429_rate');
const fieldLatency = new Trend('field_latency', true);
const mediaControlledRejection = new Rate('media_controlled_429_rate');
const unexpectedResponse = new Rate('unexpected_response_rate');

export const options = {
  scenarios: {
    media_overload: {
      executor: 'constant-vus',
      exec: 'mediaTraffic',
      vus: Number(__ENV.MEDIA_VUS || 1000),
      duration: testDuration,
      gracefulStop: '5s',
    },
    field_priority: {
      executor: 'constant-vus',
      exec: 'fieldTraffic',
      vus: Number(__ENV.FIELD_VUS || 50),
      duration: testDuration,
      gracefulStop: '5s',
    },
  },
  thresholds: {
    field_success_rate: ['rate>0.99'],
    field_429_rate: ['rate<0.01'],
    field_latency: ['p(95)<5000'],
    media_controlled_429_rate: ['rate>0.01'],
    unexpected_response_rate: ['rate<0.01'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

function login(clientID, clientSecret) {
  if (!clientID || !clientSecret) {
    fail('field and media client credentials must be provided');
  }

  const response = http.post(`${authBaseURL}/token`, {
    grant_type: 'client_credentials',
    client_id: clientID,
    client_secret: clientSecret,
  });
  if (response.status !== 200) {
    fail(`login for ${clientID} failed with status ${response.status}`);
  }
  return {
    accessToken: response.json('access_token'),
    expiresIn: Number(response.json('expires_in')),
  };
}

function durationSeconds(value) {
  const units = { h: 3600, m: 60, s: 1 };
  const parts = /(\d+)(h|m|s)/g;
  let total = 0;
  let consumed = '';
  let match;

  while ((match = parts.exec(value)) !== null) {
    total += Number(match[1]) * units[match[2]];
    consumed += match[0];
  }
  if (total === 0 || consumed !== value) {
    fail(`unsupported DURATION ${value}; use values such as 45s or 1m30s`);
  }
  return total;
}

export function setup() {
  const field = login(__ENV.FIELD_CLIENT_ID, __ENV.FIELD_CLIENT_SECRET);
  const media = login(__ENV.MEDIA_CLIENT_ID, __ENV.MEDIA_CLIENT_SECRET);
  const shortestTokenTTL = Math.min(field.expiresIn, media.expiresIn);

  if (durationSeconds(testDuration) >= shortestTokenTTL - 5) {
    fail(`DURATION must be below ${shortestTokenTTL - 5}s to keep setup tokens valid`);
  }
  return {
    fieldToken: field.accessToken,
    mediaToken: media.accessToken,
  };
}

function request(token, traffic) {
  return http.get(`${apiBaseURL}/hazard-events?hazard_type=COASTAL`, {
    headers: {
      Authorization: `Bearer ${token}`,
      'X-Correlation-ID': `k6-p5-${traffic}-${__VU}-${__ITER}-${Date.now()}`,
    },
    tags: { traffic },
    timeout: '6s',
  });
}

export function mediaTraffic(data) {
  const response = request(data.mediaToken, 'media');
  const controlled = response.status === 429 && response.headers['Retry-After'] === '1';

  mediaControlledRejection.add(controlled);
  unexpectedResponse.add(response.status !== 200 && response.status !== 429);
  check(response, {
    'media is served or rejected in a controlled way': (r) => r.status === 200 || controlled,
  });
}

export function fieldTraffic(data) {
  const response = request(data.fieldToken, 'field');
  const success = response.status === 200;

  fieldSuccess.add(success);
  fieldRejected.add(response.status === 429);
  fieldLatency.add(response.timings.duration);
  unexpectedResponse.add(!success && response.status !== 429);
  check(response, {
    'field request stays available': () => success,
  });
}
