import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { getAPI, seismicPath, volcanicPath } from './common.js';

const pvmbgBaseURL = (__ENV.PVMBG_BASE_URL || 'http://localhost:8082').replace(/\/$/, '');
const pvmbgToken = __ENV.PVMBG_TOKEN;
const propagationWaitSeconds = Number(__ENV.OUTAGE_PROPAGATION_WAIT_SECONDS || 6);

export const options = {
  vus: Number(__ENV.VUS || 10),
  duration: __ENV.TEST_DURATION || '30s',
  thresholds: {
    'checks{traffic:seismic}': ['rate>0.99'],
    'http_req_duration{traffic:seismic}': ['p(95)<300'],
  },
};

function setOutage(enabled) {
  return http.post(
    `${pvmbgBaseURL}/admin/outage`,
    JSON.stringify({ enabled }),
    {
      headers: {
        Authorization: `Bearer ${pvmbgToken}`,
        'Content-Type': 'application/json',
        'X-Correlation-ID': `k6-outage-${Date.now()}`,
      },
      tags: { operation: 'outage-control' },
    },
  );
}

export function setup() {
  if (!pvmbgToken) {
    fail('PVMBG_TOKEN must be provided');
  }
  const response = setOutage(true);
  if (response.status !== 200) {
    fail(`could not enable PVMBG outage: ${response.status} ${response.body}`);
  }

  sleep(propagationWaitSeconds);
}

export default function () {
  const seismic = getAPI(seismicPath, { traffic: 'seismic' });
  check(seismic, { 'seismic remains available': (r) => r.status === 200 }, { traffic: 'seismic' });

  const volcanic = getAPI(volcanicPath, { traffic: 'volcanic' });
  check(volcanic, {
    'volcanic response is not 500': (r) => r.status !== 500,
    'volcanic response is explicit': (r) => {
      if (r.status === 503) return true;
      const body = String(r.body || '').toLowerCase();
      return r.status === 200 && (body.includes('degraded') || body.includes('stale_since'));
    },
  }, { traffic: 'volcanic' });
}

export function teardown() {
  const response = setOutage(false);
  if (response.status !== 200) {
    console.error(`could not disable PVMBG outage: ${response.status} ${response.body}`);
  }
}
