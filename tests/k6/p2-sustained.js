import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import { getAPI, seismicPath, volcanicPath } from './common.js';

const non429Failures = new Rate('non_429_failures');
const controlled429s = new Counter('controlled_429s');

export const options = {
  scenarios: {
    sustained_parallel_load: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 50),
      duration: __ENV.TEST_DURATION || '60s',
    },
  },
  thresholds: {
    non_429_failures: ['rate<0.01'],
    checks: ['rate>0.99'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

// Each iteration alternates between seismic and volcanic traffic under sustained load.
export default function () {
  const path = (__ITER + __VU) % 2 === 0 ? seismicPath : volcanicPath;
  const response = getAPI(path, { traffic: path === seismicPath ? 'seismic' : 'volcanic' });
  const controlled = response.status === 429;
  const failed = !controlled && (response.status < 200 || response.status >= 300);
  controlled429s.add(controlled ? 1 : 0);
  non429Failures.add(failed);
  check(response, {
    'request succeeded or was rejected with 429': (r) => (r.status >= 200 && r.status < 300) || r.status === 429,
  });
}
