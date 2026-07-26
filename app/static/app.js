let scans = [];
let researchJobs = [];
let activeScanId = null;
let activeResearchId = null;
let activeResults = [];
let loadedScanResultsId = null;
let pollTimer = null;
let loadedResearchDetailKey = null;
let controlJobs = [];
let activeControlId = null;
let loadedControlDetailKey = null;
let entryJobs = [];
let activeEntryId = null;
let loadedEntryDetailKey = null;
let backtestJobs = [];
let activeBacktestId = null;
let loadedBacktestDetailKey = null;

const el = id => document.getElementById(id);
const fmtPct = value => value === null || value === undefined ? '—' : `${Number(value).toFixed(1)}%`;
const fmtNum = value => new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 }).format(Number(value || 0));
const fmtMoney = value => value === null || value === undefined ? '—' : new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(Number(value || 0));
const fmtBytes = value => {
  let n = Number(value || 0), i = 0;
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i += 1; }
  return `${n.toFixed(i ? 1 : 0)} ${units[i]}`;
};
const fmtDateTime = value => value ? new Date(value).toLocaleString() : '—';
const median = values => {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
};
const escapeHtml = value => String(value ?? '').replace(/[&<>'"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[c]));

async function api(url, options = {}) {
  const response = await fetch(url, {
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    ...options,
  });
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try { message = (await response.json()).detail || message; } catch (_) { /* no JSON body */ }
    throw new Error(message);
  }
  return response.json();
}

function badge(status) {
  return `<span class="badge ${['failed', 'queued'].includes(status) ? status : ''}">${escapeHtml(status)}</span>`;
}

function renderLatest(scan) {
  if (!scan) {
    el('latest-scan').innerHTML = '<div class="empty-state">No scans found.</div>';
    return;
  }
  const total = Number(scan.progress_total || 0);
  const current = Number(scan.progress_current || 0);
  const pct = total ? Math.min(100, current / total * 100) : (scan.status === 'completed' ? 100 : 0);
  const notes = (scan.coverage_notes || []).map(note => `<div class="flag">${escapeHtml(note)}</div>`).join('');
  el('latest-scan').innerHTML = `
    <div class="scan-status">
      <div class="status-line"><strong>${escapeHtml(scan.progress_stage || scan.status)}</strong>${badge(scan.status)}</div>
      <div class="progress"><span style="width:${pct}%"></span></div>
      <div class="meta">
        <div><strong>${fmtNum(scan.universe_count)}</strong><small>universe</small></div>
        <div><strong>${fmtNum(scan.candidate_day_count)}</strong><small>candidates</small></div>
        <div><strong>${fmtNum(scan.result_count)}</strong><small>events</small></div>
      </div>
      <small>${fmtDateTime(scan.created_at)} · ${scan.lookback_start || '—'} to ${scan.lookback_end || '—'}</small>
      ${scan.error_message ? `<div class="negative">${escapeHtml(scan.error_message)}</div>` : ''}${notes}
    </div>`;
}

function renderScanList() {
  const previouslySelectedSource = el('source-scan').value;
  el('scan-list').innerHTML = scans.map(scan => `
    <div class="scan-row" data-scan="${scan.id}">
      <div><strong>${scan.lookback_start || 'Pending'} → ${scan.lookback_end || ''}</strong><br><small>${fmtDateTime(scan.created_at)}</small></div>
      <div>${badge(scan.status)}</div>
      <div><strong>${scan.parameters?.threshold_pct ?? 25}%</strong><br><small>threshold</small></div>
      <div><strong>${fmtNum(scan.result_count)}</strong><br><small>events</small></div>
      <button class="ghost">Open</button>
    </div>`).join('') || '<div class="empty-state">No scans found.</div>';
  document.querySelectorAll('[data-scan]').forEach(row => row.addEventListener('click', () => selectScan(row.dataset.scan)));

  const completed = scans.filter(scan => scan.status === 'completed');
  el('source-scan').innerHTML = completed.map(scan => `
    <option value="${scan.id}">${scan.lookback_start} → ${scan.lookback_end} · ${fmtNum(scan.result_count)} events · ${scan.id.slice(0, 8)}</option>`).join('') || '<option value="">No completed scans</option>';
  if (completed.some(scan => scan.id === previouslySelectedSource)) {
    el('source-scan').value = previouslySelectedSource;
  }
}

function renderResults(rows) {
  const query = el('search').value.trim().toLowerCase();
  const filtered = query ? rows.filter(row => `${row.symbol} ${row.company_name || ''}`.toLowerCase().includes(query)) : rows;
  activeResults = rows;
  el('result-count').textContent = filtered.length;
  el('symbol-count').textContent = new Set(filtered.map(row => row.symbol)).size;
  el('median-gain').textContent = fmtPct(median(filtered.map(row => Number(row.high_vs_prior_close_pct))));
  el('median-postopen').textContent = fmtPct(median(filtered.map(row => Number(row.open_to_peak_pct))));
  if (!filtered.length) {
    el('results-body').innerHTML = '<tr><td colspan="10" class="empty-cell">No qualifying events found.</td></tr>';
    return;
  }
  el('results-body').innerHTML = filtered.map(row => {
    const flags = (row.quality_flags || []).slice(0, 3).map(flag => `<span class="flag">${escapeHtml(flag.replaceAll('_', ' '))}</span>`).join('');
    const cross = row.threshold_cross_bar_start
      ? `${new Date(row.threshold_cross_bar_start).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', timeZone: 'America/New_York' })} ET`
      : '—';
    return `<tr>
      <td>${row.event_date}</td>
      <td><strong>${escapeHtml(row.symbol)}</strong><br><small>${escapeHtml(row.exchange || '')}</small></td>
      <td class="company">${escapeHtml(row.company_name || '—')}</td>
      <td class="gain">${fmtPct(row.high_vs_prior_close_pct)}</td>
      <td>${fmtPct(row.opening_gap_pct)}</td>
      <td>${fmtPct(row.open_to_peak_pct)}</td>
      <td>${fmtPct(row.first_minute_entry_to_peak_pct)}</td>
      <td>${cross}<br><small>${row.minutes_from_open_to_cross ?? '—'} min after open</small></td>
      <td>${fmtNum(row.session_volume)}</td>
      <td>${flags || '—'}</td>
    </tr>`;
  }).join('');
}

async function loadScans() {
  scans = await api('/api/scans');
  renderScanList();
  if (!activeScanId && scans.length) activeScanId = scans[0].id;
  const active = scans.find(scan => scan.id === activeScanId) || scans[0];
  if (active) {
    activeScanId = active.id;
    renderLatest(active);
    el('export').disabled = active.status !== 'completed';
    if (active.status === 'completed' && loadedScanResultsId !== active.id) await loadResults(active.id);
  } else {
    renderLatest(null);
  }
}

async function loadResults(id) {
  renderResults(await api(`/api/results?scan_id=${encodeURIComponent(id)}&limit=5000`));
  loadedScanResultsId = id;
}

async function selectScan(id) {
  activeScanId = id;
  if (loadedScanResultsId !== id) activeResults = [];
  const scan = scans.find(item => item.id === id);
  renderLatest(scan);
  el('export').disabled = !scan || scan.status !== 'completed';
  if (scan?.status === 'completed') await loadResults(id); else renderResults([]);
}

function bindRetryButton() {
  const button = document.querySelector('[data-retry-research]');
  if (!button) return;
  button.addEventListener('click', async () => {
    button.disabled = true;
    try {
      const job = await api(`/api/research-jobs/${encodeURIComponent(button.dataset.retryResearch)}/retry`, { method: 'POST', body: '{}' });
      const index = researchJobs.findIndex(item => item.id === job.id);
      if (index >= 0) researchJobs[index] = job;
      renderResearch(job);
      renderResearchList();
    } catch (error) {
      el('research-message').textContent = error.message;
      button.disabled = false;
    }
  });
}

function renderResearch(job) {
  if (!job) {
    el('latest-research').innerHTML = '<div class="empty-state">No research jobs found.</div>';
    el('research-events').innerHTML = '';
    el('research-files').innerHTML = '';
    return;
  }
  const total = Number(job.progress_total || 0);
  const current = Number(job.progress_current || 0);
  const pct = total ? Math.min(100, current / total * 100) : (job.status === 'completed' ? 100 : 0);
  el('latest-research').innerHTML = `
    <div class="scan-status">
      <div class="status-line"><strong>${escapeHtml(job.progress_stage || job.status)}</strong>${badge(job.status)}</div>
      <div class="progress"><span style="width:${pct}%"></span></div>
      <div class="meta">
        <div><strong>${fmtNum(job.source_event_count)}</strong><small>source events</small></div>
        <div><strong>${fmtNum(job.eligible_event_count)}</strong><small>sellable</small></div>
        <div><strong>${fmtNum(job.completed_event_count)}</strong><small>processed</small></div>
      </div>
      <small>${fmtDateTime(job.created_at)}</small>
      ${job.error_message ? `<div class="negative">${escapeHtml(job.error_message)}</div>` : ''}
      ${job.status === 'failed' ? `<button class="ghost" data-retry-research="${job.id}">Retry and resume</button>` : ''}
    </div>`;
  bindRetryButton();
}

function renderResearchList() {
  const previousControlSource = el('source-research') ? el('source-research').value : '';
  el('research-list').innerHTML = researchJobs.map(job => `
    <div class="scan-row" data-research="${job.id}">
      <div><strong>${job.id.slice(0, 8)}</strong><br><small>${fmtDateTime(job.created_at)}</small></div>
      <div>${badge(job.status)}</div>
      <div><strong>${fmtNum(job.eligible_event_count)}</strong><br><small>sellable</small></div>
      <div><strong>${fmtNum(job.completed_event_count)}/${fmtNum(job.source_event_count)}</strong><br><small>processed</small></div>
      <button class="ghost">Open</button>
    </div>`).join('') || '<div class="empty-state">No research packages yet.</div>';
  document.querySelectorAll('[data-research]').forEach(row => row.addEventListener('click', () => selectResearch(row.dataset.research)));
  if (el('source-research')) {
    const completed = researchJobs.filter(job => job.status === 'completed');
    el('source-research').innerHTML = completed.map(job => `<option value="${job.id}">${job.id.slice(0, 8)} · ${fmtNum(job.eligible_event_count)} sellable events</option>`).join('') || '<option value="">No completed research jobs</option>';
    if (completed.some(job => job.id === previousControlSource)) el('source-research').value = previousControlSource;
  }
}

function renderFileList(files, title, note = '') {
  const shown = files.slice(0, 1000);
  el('research-files').innerHTML = `
    <h3>${escapeHtml(title)}</h3>
    ${note ? `<p class="file-note">${escapeHtml(note)}</p>` : ''}
    ${files.length > shown.length ? `<p class="file-note">Showing the first ${shown.length.toLocaleString()} of ${files.length.toLocaleString()} files. The event manifest lists every stored object.</p>` : ''}
    ${shown.length ? shown.map(file => `
      <a class="file-row" href="/api/research-files/${file.id}/download">
        <span><strong>${escapeHtml(file.filename)}</strong><small>${escapeHtml(file.file_kind)} · SHA-256 ${escapeHtml(file.sha256.slice(0, 12))}…</small></span>
        <span>${fmtBytes(file.size_bytes)}</span>
      </a>`).join('') : '<div class="empty-state">No downloadable files are available yet.</div>'}`;
}

async function loadResearchFiles(jobId, eventId = null, label = null) {
  const params = new URLSearchParams({ job_id: jobId, limit: '5000' });
  if (eventId) {
    params.set('research_event_id', eventId);
    params.set('include_raw', 'true');
  }
  const files = await api(`/api/research-files?${params.toString()}`);
  renderFileList(
    files,
    eventId ? `${label || 'Event'} files` : 'Job downloads',
    eventId
      ? 'The compact ZIP is the convenient summary package. Raw trade and quote Parquet chunks preserve the complete tick history.'
      : 'Download the research index after the job completes. Select an eligible event to access its compact package and raw Parquet chunks.',
  );
}

function renderResearchEvents(rows) {
  if (!rows.length) {
    el('research-events').innerHTML = '<h3>Sellable events</h3><div class="empty-state">No confirmed-sellable events are available yet.</div>';
    return;
  }
  el('research-events').innerHTML = `
    <h3>Sellable events</h3>
    <p class="file-note">Select an event to obtain fresh signed download links for its complete file set.</p>
    <div class="event-list">${rows.map(row => `
      <div class="event-row">
        <div><strong>${escapeHtml(row.symbol)}</strong><small>${row.event_date}</small></div>
        <div><strong>${fmtMoney(row.max_bid_notional)}</strong><small>max displayed bid</small></div>
        <div><strong>${row.seconds_to_first_confirmed_exit === null || row.seconds_to_first_confirmed_exit === undefined ? '—' : `${Number(row.seconds_to_first_confirmed_exit).toFixed(3)}s`}</strong><small>${Number(row.displayed_seconds_at_or_above_threshold || 0).toFixed(1)}s bid support · ${escapeHtml(row.sellability_status || '—')}</small></div>
        <button class="ghost" data-event-files="${row.id}" data-event-label="${escapeHtml(`${row.symbol} · ${row.event_date}`)}">Files</button>
      </div>`).join('')}</div>`;
  document.querySelectorAll('[data-event-files]').forEach(button => button.addEventListener('click', () => {
    loadResearchFiles(activeResearchId, button.dataset.eventFiles, button.dataset.eventLabel).catch(error => {
      el('research-files').innerHTML = `<div class="negative">${escapeHtml(error.message)}</div>`;
    });
  }));
}

async function loadResearchEvents(jobId) {
  const rows = await api(`/api/research-events?job_id=${encodeURIComponent(jobId)}&eligible=true&event_status=completed&limit=5000`);
  renderResearchEvents(rows);
}

async function loadResearch(forceDetails = false) {
  researchJobs = await api('/api/research-jobs');
  renderResearchList();
  if (!activeResearchId && researchJobs.length) activeResearchId = researchJobs[0].id;
  const active = researchJobs.find(job => job.id === activeResearchId) || researchJobs[0];
  if (!active) {
    renderResearch(null);
    return;
  }
  activeResearchId = active.id;
  renderResearch(active);
  const detailKey = `${active.id}:${active.status}:${active.completed_event_count}:${active.eligible_event_count}`;
  const terminal = ['completed', 'failed'].includes(active.status);
  if (forceDetails || (terminal && detailKey !== loadedResearchDetailKey)) {
    await Promise.all([loadResearchEvents(active.id), loadResearchFiles(active.id)]);
    loadedResearchDetailKey = detailKey;
  } else if (!terminal && loadedResearchDetailKey?.split(':')[0] !== active.id) {
    el('research-events').innerHTML = '<h3>Sellable events</h3><div class="empty-state">Events will appear when processing completes. Use Refresh to inspect interim results.</div>';
    el('research-files').innerHTML = '<h3>Job downloads</h3><div class="empty-state">The index package is created after all source events are processed.</div>';
  }
}

async function selectResearch(id) {
  activeResearchId = id;
  loadedResearchDetailKey = null;
  const job = researchJobs.find(item => item.id === id);
  renderResearch(job);
  await Promise.all([loadResearchEvents(id), loadResearchFiles(id)]);
  loadedResearchDetailKey = `${job.id}:${job.status}:${job.completed_event_count}:${job.eligible_event_count}`;
}


function bindRetryControlButton() {
  const button = document.querySelector('[data-retry-control]');
  if (!button) return;
  button.addEventListener('click', async () => {
    button.disabled = true;
    try {
      const job = await api(`/api/control-jobs/${encodeURIComponent(button.dataset.retryControl)}/retry`, { method: 'POST', body: '{}' });
      const index = controlJobs.findIndex(item => item.id === job.id);
      if (index >= 0) controlJobs[index] = job;
      renderControl(job);
      renderControlList();
    } catch (error) {
      el('control-message').textContent = error.message;
      button.disabled = false;
    }
  });
}

function renderControl(job) {
  if (!job) {
    el('latest-control').innerHTML = '<div class="empty-state">No matched-control jobs found.</div>';
    el('control-files').innerHTML = '';
    return;
  }
  const total = Number(job.progress_total || 0);
  const current = Number(job.progress_current || 0);
  const pct = total ? Math.min(100, current / total * 100) : (job.status === 'completed' ? 100 : 0);
  el('latest-control').innerHTML = `
    <div class="scan-status">
      <div class="status-line"><strong>${escapeHtml(job.progress_stage || job.status)}</strong>${badge(job.status)}</div>
      <div class="progress"><span style="width:${pct}%"></span></div>
      <div class="meta">
        <div><strong>${fmtNum(job.positive_event_count)}</strong><small>positive events</small></div>
        <div><strong>${fmtNum(job.matched_pair_count)}</strong><small>matched pairs</small></div>
        <div><strong>${fmtNum(job.completed_control_count)}/${fmtNum(job.unique_control_count)}</strong><small>controls complete</small></div>
      </div>
      <small>${fmtDateTime(job.created_at)}</small>
      ${job.balance_gate_status ? `<div class="flag">Pre-download balance gate: ${escapeHtml(job.balance_gate_status)} · ${fmtNum(job.excellent_pair_count)} excellent + ${fmtNum(job.good_pair_count)} good</div>` : ''}
      ${job.unmatched_positive_count ? `<div class="flag">${fmtNum(job.unmatched_positive_count)} positives had fewer than the requested strong controls</div>` : ''}
      ${job.error_message ? `<div class="negative">${escapeHtml(job.error_message)}</div>` : ''}
      ${job.status === 'failed' ? `<button class="ghost" data-retry-control="${job.id}">Retry and resume</button>` : ''}
    </div>`;
  bindRetryControlButton();
}

function renderControlList() {
  const previousEntrySource = el('source-control') ? el('source-control').value : '';
  el('control-list').innerHTML = controlJobs.map(job => `
    <div class="scan-row" data-control="${job.id}">
      <div><strong>${job.id.slice(0, 8)}</strong><br><small>${fmtDateTime(job.created_at)}</small></div>
      <div>${badge(job.status)}</div>
      <div><strong>${fmtNum(job.matched_pair_count)}</strong><br><small>pairs</small></div>
      <div><strong>${fmtNum(job.completed_control_count)}/${fmtNum(job.unique_control_count)}</strong><br><small>controls</small></div>
      <button class="ghost">Open</button>
    </div>`).join('') || '<div class="empty-state">No matched-control jobs yet.</div>';
  document.querySelectorAll('[data-control]').forEach(row => row.addEventListener('click', () => selectControl(row.dataset.control)));
  if (el('source-control')) {
    const completed = controlJobs.filter(job => job.status === 'completed');
    el('source-control').innerHTML = completed.map(job => `<option value="${job.id}">${job.id.slice(0, 8)} · ${fmtNum(job.positive_event_count)} positives · ${fmtNum(job.matched_pair_count)} controls</option>`).join('') || '<option value="">No completed matched-control jobs</option>';
    if (completed.some(job => job.id === previousEntrySource)) el('source-control').value = previousEntrySource;
  }
}

async function loadControlFiles(jobId) {
  const files = await api(`/api/control-files?job_id=${encodeURIComponent(jobId)}&limit=5000`);
  el('control-files').innerHTML = `
    <h3>Analysis export</h3>
    <p class="file-note">The compact index contains matched labels, features and complete storage manifests. Raw data stays normalized to avoid duplicating overlapping sessions.</p>
    ${files.length ? files.map(file => `
      <a class="file-row" href="/api/control-files/${file.id}/download">
        <span><strong>${escapeHtml(file.filename)}</strong><small>${escapeHtml(file.file_kind)} · SHA-256 ${escapeHtml(file.sha256.slice(0, 12))}…</small></span>
        <span>${fmtBytes(file.size_bytes)}</span>
      </a>`).join('') : '<div class="empty-state">The analysis export appears after all controls complete.</div>'}`;
}

async function loadControls(forceDetails = false) {
  controlJobs = await api('/api/control-jobs');
  renderControlList();
  if (!activeControlId && controlJobs.length) activeControlId = controlJobs[0].id;
  const active = controlJobs.find(job => job.id === activeControlId) || controlJobs[0];
  if (!active) { renderControl(null); return; }
  activeControlId = active.id;
  renderControl(active);
  const detailKey = `${active.id}:${active.status}:${active.completed_control_count}:${active.failed_control_count}`;
  if (forceDetails || (['completed', 'failed'].includes(active.status) && detailKey !== loadedControlDetailKey)) {
    await loadControlFiles(active.id);
    loadedControlDetailKey = detailKey;
  }
}

async function selectControl(id) {
  activeControlId = id;
  loadedControlDetailKey = null;
  const job = controlJobs.find(item => item.id === id);
  renderControl(job);
  await loadControlFiles(id);
  loadedControlDetailKey = `${job.id}:${job.status}:${job.completed_control_count}:${job.failed_control_count}`;
}

function bindRetryEntryButton() {
  const button = document.querySelector('[data-retry-entry]');
  if (!button) return;
  button.addEventListener('click', async () => {
    button.disabled = true;
    try {
      const job = await api(`/api/entry-jobs/${encodeURIComponent(button.dataset.retryEntry)}/retry`, { method: 'POST', body: '{}' });
      const index = entryJobs.findIndex(item => item.id === job.id);
      if (index >= 0) entryJobs[index] = job;
      renderEntry(job);
      renderEntryList();
    } catch (error) {
      el('entry-message').textContent = error.message;
      button.disabled = false;
    }
  });
}

function renderEntry(job) {
  if (!job) {
    el('latest-entry').innerHTML = '<div class="empty-state">No entry-feasibility jobs found.</div>';
    el('entry-files').innerHTML = '';
    return;
  }
  const total = Number(job.progress_total || 0);
  const current = Number(job.progress_current || 0);
  const pct = total ? Math.min(100, current / total * 100) : (job.status === 'completed' ? 100 : 0);
  el('latest-entry').innerHTML = `
    <div class="scan-status">
      <div class="status-line"><strong>${escapeHtml(job.progress_stage || job.status)}</strong>${badge(job.status)}</div>
      <div class="progress"><span style="width:${pct}%"></span></div>
      <div class="meta">
        <div><strong>${fmtNum(job.positive_event_count)}</strong><small>assessed</small></div>
        <div><strong>${fmtNum(job.primary_actionable_count)}</strong><small>actionable</small></div>
        <div><strong>${fmtNum(job.retained_control_pair_count)}</strong><small>retained pairs</small></div>
      </div>
      <small>${fmtDateTime(job.created_at)}</small>
      ${job.failed_assessment_count ? `<div class="flag">${fmtNum(job.failed_assessment_count)} assessments could not be completed</div>` : ''}
      ${job.error_message ? `<div class="negative">${escapeHtml(job.error_message)}</div>` : ''}
      ${job.status === 'failed' ? `<button class="ghost" data-retry-entry="${job.id}">Retry and resume</button>` : ''}
    </div>`;
  bindRetryEntryButton();
}

function renderEntryList() {
  const previousBacktestSource = el('source-entry') ? el('source-entry').value : '';
  el('entry-list').innerHTML = entryJobs.map(job => `
    <div class="scan-row" data-entry="${job.id}">
      <div><strong>${job.id.slice(0, 8)}</strong><br><small>${fmtDateTime(job.created_at)}</small></div>
      <div>${badge(job.status)}</div>
      <div><strong>${fmtNum(job.primary_actionable_count)}</strong><br><small>actionable</small></div>
      <div><strong>${fmtNum(job.retained_control_pair_count)}</strong><br><small>pairs</small></div>
      <button class="ghost">Open</button>
    </div>`).join('') || '<div class="empty-state">No entry-feasibility exports yet.</div>';
  document.querySelectorAll('[data-entry]').forEach(row => row.addEventListener('click', () => selectEntry(row.dataset.entry)));
  if (el('source-entry')) {
    const completed = entryJobs.filter(job => job.status === 'completed');
    el('source-entry').innerHTML = completed.map(job => `<option value="${job.id}">${job.id.slice(0, 8)} · ${fmtNum(job.primary_actionable_count)} actionable</option>`).join('') || '<option value="">No completed entry jobs</option>';
    if (completed.some(job => job.id === previousBacktestSource)) el('source-entry').value = previousBacktestSource;
  }
}

async function loadEntryFiles(jobId) {
  const files = await api(`/api/entry-files?job_id=${encodeURIComponent(jobId)}`);
  const order = { entry_feasibility_index: 0, actionable_discovery: 1, actionable_validation: 2, actionable_sealed_test: 3 };
  files.sort((a, b) => (order[a.file_kind] ?? 99) - (order[b.file_kind] ?? 99));
  el('entry-files').innerHTML = `
    <h3>Entry-feasibility and frozen split exports</h3>
    <p class="file-note">Keep the sealed-test archive unopened until fixed rules survive validation.</p>
    ${files.length ? files.map(file => `
      <a class="file-row" href="/api/entry-files/${file.id}/download">
        <span><strong>${escapeHtml(file.filename)}</strong><small>${escapeHtml(file.file_kind)} · SHA-256 ${escapeHtml(file.sha256.slice(0, 12))}…</small></span>
        <span>${fmtBytes(file.size_bytes)}</span>
      </a>`).join('') : '<div class="empty-state">Exports appear after entry assessment and snapshot construction complete.</div>'}`;
}

async function loadEntries(forceDetails = false) {
  entryJobs = await api('/api/entry-jobs');
  renderEntryList();
  if (!activeEntryId && entryJobs.length) activeEntryId = entryJobs[0].id;
  const active = entryJobs.find(job => job.id === activeEntryId) || entryJobs[0];
  if (!active) { renderEntry(null); return; }
  activeEntryId = active.id;
  renderEntry(active);
  const detailKey = `${active.id}:${active.status}:${active.primary_actionable_count}:${active.retained_control_pair_count}`;
  if (forceDetails || (['completed', 'failed'].includes(active.status) && detailKey !== loadedEntryDetailKey)) {
    await loadEntryFiles(active.id);
    loadedEntryDetailKey = detailKey;
  }
}

async function selectEntry(id) {
  activeEntryId = id;
  loadedEntryDetailKey = null;
  const job = entryJobs.find(item => item.id === id);
  renderEntry(job);
  await loadEntryFiles(id);
  loadedEntryDetailKey = `${job.id}:${job.status}:${job.primary_actionable_count}:${job.retained_control_pair_count}`;
}

function bindRetryBacktestButton() {
  const button = document.querySelector('[data-retry-backtest]');
  if (!button) return;
  button.addEventListener('click', async () => {
    button.disabled = true;
    try {
      const job = await api(`/api/backtest-jobs/${encodeURIComponent(button.dataset.retryBacktest)}/retry`, { method: 'POST', body: '{}' });
      const index = backtestJobs.findIndex(item => item.id === job.id);
      if (index >= 0) backtestJobs[index] = job;
      renderBacktest(job); renderBacktestList();
    } catch (error) { el('backtest-message').textContent = error.message; button.disabled = false; }
  });
}

function renderBacktest(job) {
  if (!job) { el('latest-backtest').innerHTML = '<div class="empty-state">No execution backtests found.</div>'; el('backtest-files').innerHTML = ''; return; }
  const total = Number(job.progress_total || 0), current = Number(job.progress_current || 0);
  const pct = total ? Math.min(100, current / total * 100) : (job.status === 'completed' ? 100 : 0);
  el('latest-backtest').innerHTML = `<div class="scan-status">
    <div class="status-line"><strong>${escapeHtml(job.progress_stage || job.status)}</strong>${badge(job.status)}</div>
    <div class="progress"><span style="width:${pct}%"></span></div>
    <div class="meta"><div><strong>${fmtNum(job.universe_symbol_count)}</strong><small>universe</small></div><div><strong>${fmtNum(job.trigger_count)}</strong><small>triggers</small></div><div><strong>${fmtNum(job.filled_trade_count)}</strong><small>filled trades</small></div></div>
    <small>${job.window_start || '—'} → ${job.window_end || '—'} · separate-strategy P&amp;L sum ${fmtMoney(job.total_pnl_usd)}</small>
    ${job.error_message ? `<div class="negative">${escapeHtml(job.error_message)}</div>` : ''}
    ${job.status === 'failed' ? `<button class="ghost" data-retry-backtest="${job.id}">Retry and resume</button>` : ''}</div>`;
  bindRetryBacktestButton();
}

function renderBacktestList() {
  el('backtest-list').innerHTML = backtestJobs.map(job => `<div class="scan-row" data-backtest="${job.id}">
    <div><strong>${job.id.slice(0,8)}</strong><br><small>${fmtDateTime(job.created_at)}</small></div><div>${badge(job.status)}</div>
    <div><strong>${fmtNum(job.filled_trade_count)}</strong><br><small>fills</small></div><div><strong>${fmtMoney(job.total_pnl_usd)}</strong><br><small>strategy P&amp;L sum</small></div><button class="ghost">Open</button></div>`).join('') || '<div class="empty-state">No execution backtests yet.</div>';
  document.querySelectorAll('[data-backtest]').forEach(row => row.addEventListener('click', () => selectBacktest(row.dataset.backtest)));
}

async function loadBacktestFiles(jobId) {
  const files = await api(`/api/backtest-files?job_id=${encodeURIComponent(jobId)}`);
  el('backtest-files').innerHTML = `<h3>Execution backtest export</h3><p class="file-note">Contains every trigger, trade, failed fill, daily result, frozen specification and strategy summary.</p>${files.length ? files.map(file => `<a class="file-row" href="/api/backtest-files/${file.id}/download"><span><strong>${escapeHtml(file.filename)}</strong><small>${escapeHtml(file.file_kind)} · SHA-256 ${escapeHtml(file.sha256.slice(0,12))}…</small></span><span>${fmtBytes(file.size_bytes)}</span></a>`).join('') : '<div class="empty-state">The export appears after the backtest completes.</div>'}`;
}

async function loadBacktests(forceDetails = false) {
  backtestJobs = await api('/api/backtest-jobs'); renderBacktestList();
  if (!activeBacktestId && backtestJobs.length) activeBacktestId = backtestJobs[0].id;
  const active = backtestJobs.find(job => job.id === activeBacktestId) || backtestJobs[0];
  if (!active) { renderBacktest(null); return; }
  activeBacktestId = active.id; renderBacktest(active);
  const key = `${active.id}:${active.status}:${active.filled_trade_count}:${active.total_pnl_usd}`;
  if (forceDetails || (['completed','failed'].includes(active.status) && key !== loadedBacktestDetailKey)) { await loadBacktestFiles(active.id); loadedBacktestDetailKey = key; }
}

async function selectBacktest(id) { activeBacktestId=id; loadedBacktestDetailKey=null; const job=backtestJobs.find(x=>x.id===id); renderBacktest(job); await loadBacktestFiles(id); }

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(async () => {
    try { await Promise.all([loadScans(), loadResearch(false), loadControls(false), loadEntries(false), loadBacktests(false)]); } catch (error) { console.error(error); }
  }, 5000);
}

el('scan-form').addEventListener('submit', async event => {
  event.preventDefault();
  el('form-message').textContent = 'Queuing scan…';
  try {
    const scan = await api('/api/scans', {
      method: 'POST',
      body: JSON.stringify({
        lookback_days: Number(el('lookback').value),
        threshold_pct: Number(el('threshold').value),
        universe_mode: el('universe').value,
        feed: el('feed').value,
        include_partial_current_day: false,
        save_event_bars: el('save-bars').checked,
      }),
    });
    scans.unshift(scan);
    activeScanId = scan.id;
    loadedScanResultsId = null;
    renderScanList();
    renderLatest(scan);
    renderResults([]);
    el('form-message').textContent = 'Scan queued.';
  } catch (error) {
    el('form-message').textContent = error.message;
  }
});

el('research-form').addEventListener('submit', async event => {
  event.preventDefault();
  el('research-message').textContent = 'Queuing research package…';
  try {
    const job = await api('/api/research-jobs', {
      method: 'POST',
      body: JSON.stringify({
        source_scan_id: el('source-scan').value,
        prior_sessions: Number(el('prior-sessions').value),
        minimum_sellable_notional: Number(el('sellable-notional').value),
        sellability_window_seconds: Number(el('sellable-window').value),
        require_subsequent_trade: el('require-trade').checked,
        include_raw_trades: el('raw-trades').checked,
        include_raw_quotes: el('raw-quotes').checked,
        derive_one_second: el('derive-seconds').checked,
        include_news: el('include-news').checked,
        include_auctions: el('include-auctions').checked,
        include_corporate_actions: el('include-ca').checked,
        max_events: Number(el('max-events').value),
      }),
    });
    researchJobs.unshift(job);
    activeResearchId = job.id;
    loadedResearchDetailKey = null;
    renderResearchList();
    renderResearch(job);
    el('research-events').innerHTML = '<h3>Sellable events</h3><div class="empty-state">Waiting for the worker.</div>';
    el('research-files').innerHTML = '<h3>Job downloads</h3><div class="empty-state">The index package will be created after processing.</div>';
    el('research-message').textContent = 'Research package queued. Full raw-tick collection can take many hours and consume substantial storage.';
  } catch (error) {
    el('research-message').textContent = error.message;
  }
});


el('control-form').addEventListener('submit', async event => {
  event.preventDefault();
  el('control-message').textContent = 'Queuing matched-control collection…';
  try {
    const job = await api('/api/control-jobs', {
      method: 'POST',
      body: JSON.stringify({
        source_research_job_id: el('source-research').value,
        controls_per_event: Number(el('controls-per-event').value),
        feature_sessions: Number(el('control-feature-sessions').value),
        history_calendar_days: Number(el('control-history-days').value),
        max_control_symbol_uses: Number(el('max-symbol-uses').value),
        prior_sessions: Number(el('control-prior-sessions').value),
        feed: el('control-feed').value,
        exact_exchange_first: false,
        allow_exchange_fallback: true,
        require_corporate_action_match: el('require-ca-match').checked,
        include_raw_trades: el('control-raw-trades').checked,
        include_raw_quotes: el('control-raw-quotes').checked,
        derive_one_second: el('control-seconds').checked,
        include_news: el('control-news').checked,
        include_auctions: el('control-auctions').checked,
        include_corporate_actions: el('control-ca').checked,
        build_analysis_export: true,
        max_positive_events: Number(el('control-max-events').value),
      }),
    });
    controlJobs.unshift(job);
    activeControlId = job.id;
    loadedControlDetailKey = null;
    renderControlList();
    renderControl(job);
    el('control-files').innerHTML = '<h3>Analysis export</h3><div class="empty-state">The worker will match controls, collect data and build the export.</div>';
    el('control-message').textContent = 'Queued. V3.0.2 builds and uploads a balance report before any detailed download. Validate with Maximum positive events = 5 before running all events.';
  } catch (error) {
    el('control-message').textContent = error.message;
  }
});

el('entry-form').addEventListener('submit', async event => {
  event.preventDefault();
  el('entry-message').textContent = 'Queuing entry-feasibility assessment…';
  try {
    const job = await api('/api/entry-jobs', {
      method: 'POST',
      body: JSON.stringify({
        source_control_job_id: el('source-control').value,
        minimum_entry_notional: Number(el('entry-notional').value),
        reaction_delay_seconds: Number(el('entry-reaction-delay').value),
        minimum_opportunity_seconds: Number(el('entry-opportunity-seconds').value),
        minimum_gross_edge_pct: Number(el('entry-min-edge').value),
        require_subsequent_trade: el('entry-require-trade').checked,
        build_fixed_time_snapshots: true,
        fail_on_assessment_error: false,
        max_positive_events: Number(el('entry-max-events').value),
      }),
    });
    entryJobs.unshift(job);
    activeEntryId = job.id;
    loadedEntryDetailKey = null;
    renderEntryList();
    renderEntry(job);
    el('entry-files').innerHTML = '<h3>Entry-feasibility and frozen split exports</h3><div class="empty-state">The worker will classify entries and build the exports.</div>';
    el('entry-message').textContent = 'Queued. Use Maximum positive events = 5 for the pilot, then 0 for the full export.';
  } catch (error) {
    el('entry-message').textContent = error.message;
  }
});

el('backtest-form').addEventListener('submit', async event => {
  event.preventDefault(); el('backtest-message').textContent = 'Queuing full-universe execution backtest…';
  try {
    const job = await api('/api/backtest-jobs', { method:'POST', body:JSON.stringify({
      source_entry_job_id: el('source-entry').value, window_mode: el('backtest-window').value, feed: el('backtest-feed').value,
      enable_preopen: el('backtest-preopen').checked, enable_midday: el('backtest-midday').checked,
      position_notional: Number(el('backtest-notional').value), reaction_delay_seconds:Number(el('backtest-reaction').value),
      stop_loss_pct:Number(el('backtest-stop').value), slippage_bps:Number(el('backtest-slippage').value),
      max_trades_per_day:Number(el('backtest-max-trades').value), close_exit_minutes_before:Number(el('backtest-close-minutes').value),
      maximum_dates:Number(el('backtest-max-dates').value)
    })});
    backtestJobs.unshift(job); activeBacktestId=job.id; loadedBacktestDetailKey=null; renderBacktestList(); renderBacktest(job);
    el('backtest-files').innerHTML='<h3>Execution backtest export</h3><div class="empty-state">The worker is scanning the full universe.</div>';
    el('backtest-message').textContent='Queued. Use Maximum market dates = 3 for the pilot; after it succeeds, create a new job with 0 for the complete window.';
  } catch(error) { el('backtest-message').textContent=error.message; }
});

el('refresh-scans').addEventListener('click', loadScans);
el('refresh-research').addEventListener('click', () => loadResearch(true));
el('refresh-controls').addEventListener('click', () => loadControls(true));
el('refresh-entry').addEventListener('click', () => loadEntries(true));
el('refresh-backtest').addEventListener('click', () => loadBacktests(true));
el('search').addEventListener('input', () => renderResults(activeResults));
el('export').addEventListener('click', () => {
  if (activeScanId) window.location = `/api/export.csv?scan_id=${encodeURIComponent(activeScanId)}`;
});

Promise.all([loadScans(), loadResearch(true), loadControls(true), loadEntries(true), loadBacktests(true)]).then(startPolling).catch(error => {
  el('latest-scan').innerHTML = `<div class="negative">${escapeHtml(error.message)}</div>`;
});
