import { check } from 'k6';
import { getAPI, seismicPath, volcanicPath } from './common.js';

const duration = __ENV.TEST_DURATION || '60s';

export const options = {
  scenarios: {
    seismic_only: {
      executor: 'constant-vus',
      exec: 'seismicTraffic',
      vus: Number(__ENV.SEISMIC_VUS || 10),
      duration,
    },
    volcanic: {
      executor: 'constant-vus',
      exec: 'volcanicTraffic',
      vus: Number(__ENV.VOLCANIC_VUS || 10),
      duration,
    },
  },
  thresholds: {
    'http_req_duration{traffic:seismic}': ['p(95)<300'],
    'checks{traffic:seismic}': ['rate>0.99'],
  },
};

// seismicTraffic measures whether BMKG-backed requests stay fast and available.
export function seismicTraffic() {
  const response = getAPI(seismicPath, { traffic: 'seismic' });
  check(response, { 'seismic request succeeds': (r) => r.status === 200 }, { traffic: 'seismic' });
}

// volcanicTraffic creates slow PVMBG-backed traffic at the same time.
export function volcanicTraffic() {
  const response = getAPI(volcanicPath, { traffic: 'volcanic' });
  check(response, { 'volcanic request is controlled': (r) => [200, 429, 503].includes(r.status) }, { traffic: 'volcanic' });
}
