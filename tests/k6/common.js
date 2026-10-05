import http from 'k6/http';

export const apiBaseURL = (__ENV.API_BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
export const seismicPath = __ENV.SEISMIC_PATH || '/hazard-events?hazard_type=SEISMIC';
export const volcanicPath = __ENV.VOLCANIC_PATH || '/hazard-events?hazard_type=VOLCANIC';

// apiHeaders adds a unique tracing ID and the optional API login token.
export function apiHeaders() {
  // setup/teardown run outside a VU iteration, where __VU and __ITER are not defined, use zero for those lifecycle phases while keeping unique IDs via the timestamp.
  const vu = typeof __VU === 'undefined' ? 0 : __VU;
  const iteration = typeof __ITER === 'undefined' ? 0 : __ITER;
  const headers = {
    'X-Correlation-ID': `k6-${vu}-${iteration}-${Date.now()}`,
  };
  if (__ENV.API_TOKEN) {
    headers.Authorization = `Bearer ${__ENV.API_TOKEN}`;
  }
  return { headers };
}

// getAPI sends a GET request using the shared base URL, headers, and metric tags.
export function getAPI(path, tags = {}) {
  return http.get(`${apiBaseURL}${path}`, { ...apiHeaders(), tags });
}
