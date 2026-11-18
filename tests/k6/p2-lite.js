// Versi ringan p2-sustained.js untuk mesin penguji dengan memori terbatas. Perilaku sama: baca /hazard-events lewat
// client-facing-api dengan token yang diperbarui tiap VU (umur token 60 detik), tetapi tanpa metrik kustom per jenis lalu lintas
// dan tanpa memuat common.js. Penolakan 429 dihitung sebagai respons yang diharapkan (penolakan terkontrol), sehingga metrik
// bawaan http_req_failed adalah tepat laju kegagalan bukan-429 yang dibatasi spek (< 1%).
//
// Lingkungan: API_BASE_URL, AUTH_BASE_URL, CLIENT_ID, CLIENT_SECRET, VUS, DURATION (atau STAGES), TARGET_IP (opsional).
import http from 'k6/http';

const api = (__ENV.API_BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
const auth = (__ENV.AUTH_BASE_URL || 'http://localhost:8091').replace(/\/$/, '');
const hostOf = (u) => u.replace(/^https?:\/\//, '').replace(/[:/].*$/, '');

const stages = (__ENV.STAGES || '').split(',').filter(Boolean).map((s) => {
  const [target, duration] = s.split(':');
  return { target: Number(target), duration };
});

export const options = {
  scenarios: {
    load: stages.length
      ? { executor: 'ramping-vus', startVUs: 0, stages, gracefulRampDown: '5s', gracefulStop: '10s' }
      : { executor: 'constant-vus', vus: Number(__ENV.VUS || 50), duration: __ENV.DURATION || '120s', gracefulStop: '10s' },
  },
  hosts: __ENV.TARGET_IP ? { [hostOf(api)]: __ENV.TARGET_IP, [hostOf(auth)]: __ENV.TARGET_IP } : undefined,
  discardResponseBodies: true,
  thresholds: { http_req_failed: ['rate<0.01'] },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  noUsageReport: true,
};

http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 429));

const paths = ['/hazard-events?hazard_type=SEISMIC', '/hazard-events?hazard_type=VOLCANIC'];
let token = '';
let expiresAt = 0;

function bearer() {
  const now = Date.now();
  if (token && now < expiresAt - 15000) return token;
  const r = http.post(`${auth}/token`, {
    grant_type: 'client_credentials', client_id: __ENV.CLIENT_ID, client_secret: __ENV.CLIENT_SECRET || '',
  }, { responseType: 'text', tags: { name: 'token' }, responseCallback: http.expectedStatuses(200) });
  if (r.status === 200) {
    const b = r.json();
    token = b.access_token;
    expiresAt = now + Number(b.expires_in || 60) * 1000;
  }
  return token;
}

export default function () {
  http.get(`${api}${paths[(__ITER + __VU) % 2]}`, { headers: { Authorization: `Bearer ${bearer()}` }, tags: { name: 'hazard-events' } });
}
