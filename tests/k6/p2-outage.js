import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { Rate } from 'k6/metrics';
import { getAPI, recordAdminResponse, seismicPath, volcanicPath } from './common.js';

const pvmbgBaseURL = (__ENV.PVMBG_BASE_URL || 'http://localhost:8082').replace(/\/$/, '');
const pvmbgToken = __ENV.PVMBG_TOKEN;
const statePollSeconds = positiveNumber('OUTAGE_STATE_POLL_SECONDS', 1);
const stateTimeoutSeconds = positiveNumber('OUTAGE_STATE_TIMEOUT_SECONDS', 30);
const staleAfterSeconds = positiveNumber('OUTAGE_STALE_AFTER_SECONDS', 30);
const recoveryTimeoutSeconds = positiveNumber('OUTAGE_RECOVERY_TIMEOUT_SECONDS', 60);

const volcanicOutageResponseValid = new Rate('volcanic_outage_response_valid');
const volcanicOutageMetadataValid = new Rate('volcanic_outage_metadata_valid');
const volcanicCachedRecordPreserved = new Rate('volcanic_cached_record_preserved');
const outageStaleObserved = new Rate('outage_stale_observed');
const outageSeismicAdvanced = new Rate('outage_seismic_advanced');
const outageCleanupSuccess = new Rate('outage_cleanup_success');
const outageRecoverySuccess = new Rate('outage_recovery_success');

export const options = {
  vus: Number(__ENV.VUS || 10),
  duration: __ENV.TEST_DURATION || '30s',
  setupTimeout: __ENV.OUTAGE_SETUP_TIMEOUT || '70s',
  teardownTimeout: __ENV.OUTAGE_TEARDOWN_TIMEOUT || '2m',
  thresholds: {
    'checks{traffic:seismic}': ['rate>0.99'],
    'http_req_duration{traffic:seismic}': ['p(95)<300'],
    volcanic_outage_response_valid: ['rate>0.99'],
    volcanic_outage_metadata_valid: ['rate>0.99'],
    volcanic_cached_record_preserved: ['rate>0.99'],
    outage_stale_observed: ['rate==1'],
    outage_seismic_advanced: ['rate==1'],
    outage_cleanup_success: ['rate==1'],
    outage_recovery_success: ['rate==1'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

function setOutage(enabled) {
  const response = http.post(`${pvmbgBaseURL}/admin/outage`, JSON.stringify({ enabled }), {
    headers: {
      Authorization: `Bearer ${pvmbgToken}`,
      'Content-Type': 'application/json',
      'X-Correlation-ID': `k6-outage-${Date.now()}`,
    },
    tags: { traffic: 'admin', operation: 'outage-control' },
  });
  recordAdminResponse(response);
  return response;
}

export function setup() {
  if (!pvmbgToken) fail('PVMBG_TOKEN must be provided');

  let outageEnabled = false;
  try {
    const baseline = waitForAPIState(
      (snapshot) => snapshot.response.status === 200 && isHealthy(snapshot.pvmbg),
      stateTimeoutSeconds, 'healthy PVMBG baseline', 'setup',
    );
    const baselineVolcanicID = newestHazardID(baseline.response);
    if (!baselineVolcanicID) throw new Error('healthy baseline did not contain data[0].hazard_id');

    const seismic = getAPI(withLimit(seismicPath), { traffic: 'setup' });
    const baselineSeismicID = seismic.status === 200 ? newestHazardID(seismic) : '';
    if (!baselineSeismicID) throw new Error(`seismic baseline was unavailable: HTTP ${seismic.status}`);

    outageEnabled = true;
    const enabled = setOutage(true);
    if (enabled.status !== 200) throw new Error(`could not enable PVMBG outage: HTTP ${enabled.status}`);

    const degraded = waitForAPIState(
      (snapshot) => snapshot.response.status === 200 && isDegraded(snapshot.pvmbg),
      stateTimeoutSeconds, 'DEGRADED PVMBG metadata', 'setup',
    );
    console.log(JSON.stringify({
      event: 'outage_detected', observed_at: new Date().toISOString(),
      source_status: degraded.pvmbg.source_status,
      last_success_at: degraded.pvmbg.last_success_at,
      stale_since: degraded.pvmbg.stale_since, stale: degraded.pvmbg.stale,
      baseline_seismic_id: baselineSeismicID, baseline_volcanic_id: baselineVolcanicID,
    }));
    return {
      baselineSeismicID,
      baselineVolcanicID,
      baselineLastSuccessAt: baseline.pvmbg.last_success_at || '',
      staleSince: degraded.pvmbg.stale_since,
    };
  } catch (error) {
    let cleanupFailure = '';
    if (outageEnabled) {
      const cleanup = setOutage(false);
      const cleaned = cleanup.status === 200;
      outageCleanupSuccess.add(cleaned);
      if (!cleaned) cleanupFailure = `; cleanup also failed with HTTP ${cleanup.status}`;
    }
    fail(`outage setup failed: ${error.message || error}${cleanupFailure}`);
  }
}

export default function (setupData) {
  const seismic = getAPI(seismicPath, { traffic: 'seismic' });
  check(seismic, { 'seismic remains available': (r) => r.status === 200 }, { traffic: 'seismic' });

  const volcanic = getAPI(volcanicPath, { traffic: 'volcanic' });
  const snapshot = responseSnapshot(volcanic);
  const responseValid = volcanic.status === 200;
  const metadataValid = responseValid && isDegraded(snapshot.pvmbg);
  const cachedRecordPreserved = responseValid && newestHazardID(volcanic) === setupData.baselineVolcanicID;
  volcanicOutageResponseValid.add(responseValid);
  volcanicOutageMetadataValid.add(metadataValid);
  volcanicCachedRecordPreserved.add(cachedRecordPreserved);
  check(volcanic, {
    'volcanic cached response remains available': () => responseValid,
    'volcanic response has exact degraded metadata': () => metadataValid,
    'volcanic cached record is preserved': () => cachedRecordPreserved,
  }, { traffic: 'volcanic' });

  if (metadataValid && staleDeadlineReached(snapshot.pvmbg.stale_since)) {
    check(volcanic, {
      'volcanic metadata becomes stale after threshold': () => snapshot.pvmbg.stale === true,
    }, { traffic: 'volcanic' });
  }
}

export function teardown(setupData) {
  const failures = [];
  try {
    const stale = waitForAPIState(
      (snapshot) => snapshot.response.status === 200 && isDegraded(snapshot.pvmbg) && snapshot.pvmbg.stale === true,
      staleObservationTimeout(setupData.staleSince), 'stale PVMBG metadata', 'recovery',
    );
    outageStaleObserved.add(true);
    console.log(JSON.stringify({
      event: 'outage_stale', observed_at: new Date().toISOString(),
      source_status: stale.pvmbg.source_status, last_success_at: stale.pvmbg.last_success_at,
      stale_since: stale.pvmbg.stale_since, stale: stale.pvmbg.stale,
    }));

    const seismic = getAPI(withLimit(seismicPath), { traffic: 'recovery' });
    const advanced = seismic.status === 200 && newestHazardID(seismic) !== setupData.baselineSeismicID;
    outageSeismicAdvanced.add(advanced);
    if (!advanced) failures.push('latest seismic hazard_id did not advance during outage');
  } catch (error) {
    outageStaleObserved.add(false);
    outageSeismicAdvanced.add(false);
    failures.push(error.message || String(error));
  } finally {
    const disabled = setOutage(false);
    const cleaned = disabled.status === 200;
    outageCleanupSuccess.add(cleaned);
    if (!cleaned) failures.push(`could not disable PVMBG outage: HTTP ${disabled.status}`);
  }

  if (!failures.some((message) => message.startsWith('could not disable'))) {
    try {
      const recovered = waitForAPIState((snapshot) => {
        const latestID = newestHazardID(snapshot.response);
        const lastSuccessAdvanced = Boolean(snapshot.pvmbg.last_success_at)
          && (!setupData.baselineLastSuccessAt || snapshot.pvmbg.last_success_at > setupData.baselineLastSuccessAt);
        return snapshot.response.status === 200 && isHealthy(snapshot.pvmbg)
          && lastSuccessAdvanced && Boolean(latestID) && latestID !== setupData.baselineVolcanicID;
      }, recoveryTimeoutSeconds, 'healthy PVMBG recovery with a new volcanic report', 'recovery');
      outageRecoverySuccess.add(true);
      console.log(JSON.stringify({
        event: 'outage_recovered', observed_at: new Date().toISOString(),
        hazard_id: newestHazardID(recovered.response), source_status: recovered.pvmbg.source_status,
        last_success_at: recovered.pvmbg.last_success_at, stale: recovered.pvmbg.stale,
      }));
    } catch (error) {
      outageRecoverySuccess.add(false);
      failures.push(error.message || String(error));
    }
  } else {
    outageRecoverySuccess.add(false);
  }

  if (failures.length > 0) fail(`outage teardown/recovery failed: ${failures.join('; ')}`);
}

function waitForAPIState(predicate, timeoutSeconds, description, traffic) {
  const deadline = Date.now() + timeoutSeconds * 1000;
  let lastStatus = 0;
  do {
    const response = getAPI(withLimit(volcanicPath), { traffic });
    lastStatus = response.status;
    const snapshot = responseSnapshot(response);
    if (predicate(snapshot)) return snapshot;
    sleep(statePollSeconds);
  } while (Date.now() < deadline);
  throw new Error(`timed out waiting for ${description}; last HTTP status ${lastStatus}`);
}

function responseSnapshot(response) {
  try {
    const body = response.json();
    const pvmbg = body && body.meta && body.meta.sources && body.meta.sources.pvmbg;
    return { response, pvmbg: pvmbg && typeof pvmbg === 'object' ? pvmbg : {} };
  } catch (_) {
    return { response, pvmbg: {} };
  }
}

function isHealthy(pvmbg) {
  return pvmbg.source_status === 'HEALTHY' && pvmbg.stale === false
    && !Object.prototype.hasOwnProperty.call(pvmbg, 'stale_since');
}

function isDegraded(pvmbg) {
  return pvmbg.source_status === 'DEGRADED'
    && typeof pvmbg.last_success_at === 'string'
    && Number.isFinite(Date.parse(pvmbg.last_success_at))
    && typeof pvmbg.stale_since === 'string'
    && Number.isFinite(Date.parse(pvmbg.stale_since))
    && typeof pvmbg.stale === 'boolean';
}

function newestHazardID(response) {
  try { return response.json('data.0.hazard_id') || ''; } catch (_) { return ''; }
}

function withLimit(path) {
  return `${path}${path.includes('?') ? '&' : '?'}limit=1`;
}

function staleDeadlineReached(staleSince) {
  const timestamp = Date.parse(staleSince || '');
  return Number.isFinite(timestamp)
    && Date.now() >= timestamp + (staleAfterSeconds + statePollSeconds) * 1000;
}

function staleObservationTimeout(staleSince) {
  const timestamp = Date.parse(staleSince || '');
  if (!Number.isFinite(timestamp)) return stateTimeoutSeconds;
  const remaining = Math.max(0, (timestamp + staleAfterSeconds * 1000 - Date.now()) / 1000);
  return Math.max(stateTimeoutSeconds, remaining + statePollSeconds + 2);
}

function positiveNumber(name, fallback) {
  const value = Number(__ENV[name] || fallback);
  if (!Number.isFinite(value) || value <= 0) throw new Error(`${name} must be a positive number`);
  return value;
}
