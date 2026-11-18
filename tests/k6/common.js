import http from 'k6/http';
import { Counter, Rate, Trend } from 'k6/metrics';

export const apiBaseURL = (__ENV.API_BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
export const seismicPath = __ENV.SEISMIC_PATH || '/hazard-events?hazard_type=SEISMIC';
export const volcanicPath = __ENV.VOLCANIC_PATH || '/hazard-events?hazard_type=VOLCANIC';

export const non429Failures = new Rate('non_429_failures');
export const non429RequestFailures = new Rate('non_429_request_failures');
export const controlled429s = new Counter('controlled_429s');

const dataRequests = new Counter('data_requests');

function trafficMetrics(prefix) {
  return {
    requests: new Counter(`${prefix}_requests`),
    duration: new Trend(`${prefix}_duration`, true),
    status2xx: new Counter(`${prefix}_status_2xx`),
    status429: new Counter(`${prefix}_status_429`),
    status401: new Counter(`${prefix}_status_401`),
    status503: new Counter(`${prefix}_status_503`),
    status500: new Counter(`${prefix}_status_500`),
    statusOther: new Counter(`${prefix}_status_other`),
    networkErrors: new Counter(`${prefix}_network_errors`),
  };
}

const measuredTraffic = {
  seismic: trafficMetrics('seismic'),
  volcanic: trafficMetrics('volcanic'),
  admin: trafficMetrics('admin'),
};

// Token akses berumur pendek (bawaan 60 detik), padahal beban berlangsung >= 120 detik. Bila CLIENT_ID dan CLIENT_SECRET
// diberikan, tiap VU mengambil token client_credentials sendiri dan memperbaruinya 15 detik sebelum kedaluwarsa.
// Modul k6 dimuat per VU, jadi variabel di bawah ini milik masing-masing VU. API_TOKEN statis tetap didukung (durasi pendek).
export const authBaseURL = (__ENV.AUTH_BASE_URL || 'http://localhost:8091').replace(/\/$/, '');
export const authRequests = new Counter('auth_token_requests');
export const authFailures = new Counter('auth_token_failures');
let vuToken = '';
let vuTokenExpiresAt = 0;

function accessToken() {
  if (__ENV.API_TOKEN) return __ENV.API_TOKEN;
  if (!__ENV.CLIENT_ID) return '';
  const now = Date.now();
  if (vuToken && now < vuTokenExpiresAt - 15000) return vuToken;
  authRequests.add(1);
  const res = http.post(`${authBaseURL}/token`, {
    grant_type: 'client_credentials',
    client_id: __ENV.CLIENT_ID,
    client_secret: __ENV.CLIENT_SECRET || '',
  }, { tags: { traffic: 'auth' }, responseType: 'text' }); // isi respons token wajib disimpan walau discardResponseBodies aktif
  if (res.status === 200) {
    const body = res.json();
    vuToken = body.access_token;
    vuTokenExpiresAt = now + Number(body.expires_in || 60) * 1000;
  } else {
    authFailures.add(1);
  }
  return vuToken;
}

export function apiHeaders() {
  const vu = typeof __VU === 'undefined' ? 0 : __VU;
  const iteration = typeof __ITER === 'undefined' ? 0 : __ITER;
  const headers = {
    'X-Correlation-ID': `k6-${vu}-${iteration}-${Date.now()}`,
  };
  const token = accessToken();
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  return { headers };
}

export function getAPI(path, tags = {}) {
  const response = http.get(`${apiBaseURL}${path}`, { ...apiHeaders(), tags });
  recordResponse(response, tags.traffic);
  return response;
}

export function recordAdminResponse(response) {
  recordResponse(response, 'admin');
}

function recordResponse(response, traffic) {
  const metrics = measuredTraffic[traffic];
  if (!metrics) return;

  metrics.requests.add(1);
  metrics.duration.add(response.timings.duration);

  if (traffic !== 'admin') {
    dataRequests.add(1);
    const controlled = response.status === 429;
    const failed = !controlled && (response.status < 200 || response.status >= 300);

    non429Failures.add(failed);
    controlled429s.add(controlled ? 1 : 0);

    if (!controlled) non429RequestFailures.add(failed);
  }

  metrics.status2xx.add(0);
  metrics.status429.add(0);
  metrics.status401.add(0);
  metrics.status503.add(0);
  metrics.status500.add(0);
  metrics.statusOther.add(0);
  metrics.networkErrors.add(0);

  if (response.status === 0) metrics.networkErrors.add(1);
  else if (response.status >= 200 && response.status < 300) metrics.status2xx.add(1);
  else if (response.status === 429) metrics.status429.add(1);
  else if (response.status === 401) metrics.status401.add(1);
  else if (response.status === 503) metrics.status503.add(1);
  else if (response.status === 500) metrics.status500.add(1);
  else metrics.statusOther.add(1);
}
