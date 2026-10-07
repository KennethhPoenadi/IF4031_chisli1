import { check } from 'k6';
import { getAPI, seismicPath, volcanicPath } from './common.js';

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

export default function () {
  const path = (__ITER + __VU) % 2 === 0 ? seismicPath : volcanicPath;
  const traffic = path === seismicPath ? 'seismic' : 'volcanic';
  const response = getAPI(path, { traffic });
  check(response, {
    'request succeeded or was rejected with 429': (r) => (r.status >= 200 && r.status < 300) || r.status === 429,
  }, { traffic });
}
