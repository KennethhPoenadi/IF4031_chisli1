import http from 'k6/http';

export const apiBaseURL = (__ENV.API_BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
export const seismicPath = __ENV.SEISMIC_PATH || '/hazard-events?hazard_type=SEISMIC';
export const volcanicPath = __ENV.VOLCANIC_PATH || '/hazard-events?hazard_type=VOLCANIC';

// apiHeaders adds a unique tracing ID and the optional API login token.
export function apiHeaders() {
  const headers = {
    'X-Correlation-ID': `k6-${__VU}-${__ITER}-${Date.now()}`,
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
