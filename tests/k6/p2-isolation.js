import { check, fail, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';
import { getAPI, seismicPath, volcanicPath } from './common.js';

const duration = __ENV.TEST_DURATION || '60s';
const freshnessPath = __ENV.SEISMIC_FRESHNESS_PATH || '/hazard-events?hazard_type=SEISMIC&limit=1';
const freshnessPollSeconds = Number(__ENV.FRESHNESS_POLL_SECONDS || 1);
const minimumSeismicUpdates = Number(__ENV.MIN_SEISMIC_UPDATES || 4);

if (!Number.isFinite(freshnessPollSeconds) || freshnessPollSeconds <= 0) {
  throw new Error('FRESHNESS_POLL_SECONDS must be a positive number');
}
if (!Number.isInteger(minimumSeismicUpdates) || minimumSeismicUpdates < 1) {
  throw new Error('MIN_SEISMIC_UPDATES must be a positive integer');
}

const seismicUpdates = new Counter('seismic_updates');
const seismicMonitorSuccess = new Rate('seismic_monitor_success');

let previousSeismicID;

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
    seismic_freshness_monitor: {
      executor: 'constant-vus',
      exec: 'monitorSeismicFreshness',
      vus: 1,
      duration,
    },
  },
  thresholds: {
    'http_req_duration{traffic:seismic}': ['p(95)<300'],
    'checks{traffic:seismic}': ['rate>0.99'],
    seismic_monitor_success: ['rate>0.99'],
    seismic_updates: [`count>=${minimumSeismicUpdates}`],
  },
};

export function setup() {
  const response = getAPI(freshnessPath, { traffic: 'freshness-setup' });
  if (response.status !== 200) {
    fail(`could not read initial seismic event: HTTP ${response.status}`);
  }

  const initialID = newestHazardID(response);
  if (!initialID) {
    fail('initial seismic response did not contain data[0].hazard_id');
  }
  return { initialSeismicID: initialID };
}

export function seismicTraffic() {
  const response = getAPI(seismicPath, { traffic: 'seismic' });
  check(response, { 'seismic request succeeds': (r) => r.status === 200 }, { traffic: 'seismic' });
}

export function volcanicTraffic() {
  const response = getAPI(volcanicPath, { traffic: 'volcanic' });
  check(response, { 'volcanic request is controlled': (r) => [200, 429, 503].includes(r.status) }, { traffic: 'volcanic' });
}

export function monitorSeismicFreshness(setupData) {
  if (previousSeismicID === undefined) {
    previousSeismicID = setupData.initialSeismicID;
  }

  const response = getAPI(freshnessPath, { traffic: 'freshness' });
  const currentID = response.status === 200 ? newestHazardID(response) : '';
  const successful = response.status === 200 && Boolean(currentID);

  check(response, {
    'freshness monitor reads latest seismic event': () => successful,
  }, { traffic: 'freshness' });
  seismicMonitorSuccess.add(successful);

  if (successful && currentID !== previousSeismicID) {
    seismicUpdates.add(1);
    previousSeismicID = currentID;
  }

  sleep(freshnessPollSeconds);
}

function newestHazardID(response) {
  try {
    return response.json('data.0.hazard_id') || '';
  } catch (_) {
    return '';
  }
}
