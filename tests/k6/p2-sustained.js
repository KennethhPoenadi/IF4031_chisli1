import { check } from 'k6';
import { apiBaseURL, authBaseURL, getAPI, seismicPath, volcanicPath } from './common.js';

// STAGES="200:20s,1000:20s,..." (VU target:durasi) memakai ramping-vus untuk mencari batas sebelum beban penuh.
// Tanpa STAGES: constant-vus sebanyak VUS selama DURATION.
const stages = (__ENV.STAGES || '').split(',').filter(Boolean).map((s) => {
  const [target, duration] = s.split(':');
  return { target: Number(target), duration };
});
const scenario = stages.length
  ? { executor: 'ramping-vus', startVUs: 0, stages, gracefulRampDown: '5s', gracefulStop: '10s' }
  : {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 50),
      duration: __ENV.DURATION || __ENV.TEST_DURATION || '120s',
      gracefulStop: '10s',
    };

// TARGET_IP memetakan nama host ingress ke alamat tertentu (mis. ingress di node lain) tanpa mengubah DNS mesin penguji.
const hostOf = (u) => u.replace(/^https?:\/\//, '').replace(/[:/].*$/, '');
const hosts = __ENV.TARGET_IP
  ? Object.fromEntries([hostOf(apiBaseURL), hostOf(authBaseURL)].map((h) => [h, __ENV.TARGET_IP]))
  : undefined;

export const options = {
  scenarios: { sustained_parallel_load: scenario },
  hosts,
  noConnectionReuse: false,
  // Skrip hanya memeriksa kode status. Tanpa menyimpan isi respons (10-19 KB per permintaan) memori per VU turun jauh,
  // yang menentukan seberapa banyak VU yang muat pada satu mesin penguji.
  discardResponseBodies: true,
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
