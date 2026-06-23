const state = {
  ticks: [],
  trades: [],
  signals: [],
  portfolio: [],
  sessions: [],
  seriesKey: "",
  timeframeSec: 5,
  selectedTrade: null,
  selectedFeatures: new Set(),
  viewStart: 0,
  viewEnd: 1,
  autoFollow: true,
  eventSource: null,
  liveCount: 0,
  lastLiveTs: 0,
  currentSession: null,
  health: null,
  healthTimer: 0,
  dragging: null,
};

const els = {
  ticksFile: document.getElementById("ticksFile"),
  tradesFile: document.getElementById("tradesFile"),
  signalsFile: document.getElementById("signalsFile"),
  sessionSelect: document.getElementById("sessionSelect"),
  timeframeSelect: document.getElementById("timeframeSelect"),
  showVolume: document.getElementById("showVolume"),
  showWave: document.getElementById("showWave"),
  showTrades: document.getElementById("showTrades"),
  autoFollow: document.getElementById("autoFollow"),
  featureList: document.getElementById("featureList"),
  statsPanel: document.getElementById("statsPanel"),
  tradeList: document.getElementById("tradeList"),
  statusText: document.getElementById("statusText"),
  inspectText: document.getElementById("inspectText"),
  liveBadge: document.getElementById("liveBadge"),
  botStatus: document.getElementById("botStatus"),
  feedStatus: document.getElementById("feedStatus"),
  healthUpdated: document.getElementById("healthUpdated"),
  healthCounts: document.getElementById("healthCounts"),
  feedHealthStatus: document.getElementById("feedHealthStatus"),
  feedHealthTable: document.getElementById("feedHealthTable"),
  errorStatus: document.getElementById("errorStatus"),
  errorList: document.getElementById("errorList"),
  instrumentTabs: document.getElementById("instrumentTabs"),
  portfolioCards: document.getElementById("portfolioCards"),
  portfolioUpdated: document.getElementById("portfolioUpdated"),
  positionCount: document.getElementById("positionCount"),
  openPositionsTable: document.getElementById("openPositionsTable"),
  tradeSummaryStatus: document.getElementById("tradeSummaryStatus"),
  tradeSummaryTable: document.getElementById("tradeSummaryTable"),
  chartStack: document.getElementById("chartStack"),
  priceCanvas: document.getElementById("priceCanvas"),
  volumeCanvas: document.getElementById("volumeCanvas"),
  waveCanvas: document.getElementById("waveCanvas"),
  downloadTrades: document.getElementById("downloadTrades"),
  downloadPortfolio: document.getElementById("downloadPortfolio"),
  downloadSignals: document.getElementById("downloadSignals"),
  downloadTicks: document.getElementById("downloadTicks"),
};

const NUMERIC_SKIP = new Set([
  "ts", "secid", "security_id", "ltp", "open", "high", "low", "close",
  "bid_price", "ask_price", "bid_qty", "ask_qty", "raw", "depth"
]);

const WAVE_COLORS = ["--wave-a", "--wave-b", "--wave-c", "--entry", "--exit"];
const viewerToken = new URLSearchParams(window.location.search).get("token") || "";

function readJsonlFile(file, callback) {
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => callback(parseJsonl(reader.result));
  reader.readAsText(file);
}

function parseJsonl(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

async function loadCloudSessions() {
  try {
    const response = await fetch(apiUrl("/api/sessions"), { cache: "no-store" });
    if (!response.ok) return;
    const payload = await response.json();
    state.sessions = payload.sessions || [];
    renderSessionSelect();
    if (state.sessions.length) await loadCloudSession(state.sessions[0]);
  } catch {
    renderSessionSelect();
  }
}

function renderSessionSelect() {
  els.sessionSelect.innerHTML = state.sessions.length
    ? state.sessions.map((session, index) => `<option value="${index}">${escapeHtml(session.date)} / ${escapeHtml(session.expiry)}</option>`).join("")
    : `<option value="">Local files</option>`;
}

async function loadCloudSession(session) {
  if (!session) return;
  closeLiveStream();
  setLive(false);
  state.currentSession = session;
  els.statusText.textContent = `Loading ${session.date} / ${session.expiry}`;
  const query = new URLSearchParams({ date: session.date, expiry: session.expiry, limit: "100000" });
  const response = await fetch(apiUrl(`/api/session?${query}`), { cache: "no-store" });
  if (!response.ok) throw new Error(`Session load failed: ${response.status}`);
  const payload = await response.json();
  state.ticks = dedupeTicks((payload.ticks || []).map(normalizeTick));
  state.trades = (payload.trades || []).map(normalizeTrade);
  state.signals = payload.signals || [];
  state.portfolio = (payload.portfolio || []).map(normalizePortfolio);
  state.seriesKey = "";
  state.selectedTrade = null;
  state.liveCount = 0;
  resetView(true);
  render();
  await refreshHealth();
  startHealthPolling();
  startLiveStream(session);
}

function startLiveStream(session) {
  if (!window.EventSource || !session) return;
  const query = new URLSearchParams({ date: session.date, expiry: session.expiry });
  const source = new EventSource(apiUrl(`/api/live/stream?${query}`));
  state.eventSource = source;
  source.addEventListener("open", () => setLive(true));
  source.addEventListener("tick", (event) => {
    const tick = normalizeTick(JSON.parse(event.data));
    if (!tick.ts || !tick.ltp || !tick.index || !tick.stream) return;
    appendTick(tick);
  });
  source.addEventListener("trade", (event) => {
    state.trades.push(normalizeTrade(JSON.parse(event.data), state.trades.length));
    render();
  });
  source.addEventListener("portfolio", (event) => {
    state.portfolio.push(normalizePortfolio(JSON.parse(event.data)));
    if (state.portfolio.length > 12000) state.portfolio.splice(0, state.portfolio.length - 12000);
    renderThrottled();
  });
  source.addEventListener("signal", (event) => {
    state.signals.push(JSON.parse(event.data));
  });
  source.onerror = () => setLive(false);
}

function closeLiveStream() {
  if (state.eventSource) state.eventSource.close();
  state.eventSource = null;
}

function startHealthPolling() {
  if (state.healthTimer) clearInterval(state.healthTimer);
  state.healthTimer = setInterval(refreshHealth, 5000);
}

async function refreshHealth() {
  if (!state.currentSession) return;
  try {
    const query = new URLSearchParams({ date: state.currentSession.date, expiry: state.currentSession.expiry });
    const response = await fetch(apiUrl(`/api/session/health?${query}`), { cache: "no-store" });
    if (!response.ok) return;
    state.health = await response.json();
    renderHealth();
  } catch {
    state.health = {
      status: "error",
      summary: {},
      feeds: [],
      events: [{ level: "error", message: "Dashboard could not load health API" }],
    };
    renderHealth();
  }
}

function setLive(active) {
  els.liveBadge.textContent = active ? "Live" : "Offline";
  els.liveBadge.classList.toggle("on", active);
  els.liveBadge.classList.toggle("off", !active);
}

function apiUrl(path) {
  if (!viewerToken) return path;
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}token=${encodeURIComponent(viewerToken)}`;
}

function normalizeTick(row) {
  const features = row.features || {};
  const raw = row.raw || {};
  const ts = Number(row.ts || features.ts || raw.ts || 0);
  const ltp = Number(row.ltp ?? features.ltp ?? raw.LTP ?? raw.ltp ?? 0);
  const index = String(row.index || features.index || "").toUpperCase();
  const stream = String(row.stream || features.stream || "").toUpperCase();
  const secid = Number(row.secid || features.secid || raw.security_id || raw.SecurityId || 0);
  return { ...row, features, ts, ltp, index, stream, secid, key: `${index}_${stream}_${secid}` };
}

function normalizeTrade(row, idx = 0) {
  const t = row.trade || row;
  const tag = String(t.tag || "");
  const parts = tag.split("_");
  return {
    ...t,
    number: idx + 1,
    index: String(t.index || parts[0] || "").toUpperCase(),
    stream: String(parts[1] || t.side || "").toUpperCase(),
    secid: Number(t.secid || 0),
    entry_ts: Number(t.entry_ts || 0),
    exit_ts: Number(t.exit_ts || 0),
    entry: Number(t.entry || 0),
    exit: Number(t.exit || 0),
    net_pnl: Number(t.net_pnl || 0),
    gross_pnl: Number(t.gross_pnl || 0),
    entry_reason: String(t.entry_reason || ""),
    exit_reason: String(t.exit_reason || ""),
    tag,
  };
}

function normalizePortfolio(row) {
  const portfolio = row.portfolio || row;
  const positions = Array.isArray(portfolio.positions) ? portfolio.positions : [];
  const realized = Number(portfolio.realized_pnl ?? 0);
  const unrealized = Number(portfolio.unrealized_pnl ?? 0);
  return {
    ...portfolio,
    ts: Number(row.ts || portfolio.ts || 0),
    time: String(row.time || portfolio.time || ""),
    initial_capital: Number(portfolio.initial_capital ?? portfolio.capital ?? 0),
    cash: Number(portfolio.cash ?? 0),
    realized_pnl: realized,
    unrealized_pnl: unrealized,
    net_pnl: Number(portfolio.net_pnl ?? (realized + unrealized)),
    premium_deployed: Number(portfolio.premium_deployed ?? 0),
    total_fees: Number(portfolio.total_fees ?? portfolio.fees_paid ?? 0),
    opened_today: Number(portfolio.opened_today ?? 0),
    closed_today: Number(portfolio.closed_today ?? 0),
    open_positions: Number(portfolio.open_positions ?? positions.length),
    positions: positions.map(normalizePosition),
  };
}

function normalizePosition(position) {
  return {
    ...position,
    secid: Number(position.secid || 0),
    tag: String(position.tag || ""),
    qty: Number(position.qty || 0),
    entry: Number(position.entry || 0),
    ltp: Number(position.ltp || 0),
    pnl: Number(position.pnl || 0),
    pnl_pct: Number(position.pnl_pct || 0),
    hold_sec: Number(position.hold_sec || 0),
    last_tick_ts: Number(position.last_tick_ts || 0),
    entry_reason: String(position.entry_reason || ""),
  };
}

function dedupeTicks(ticks) {
  const seen = new Set();
  return ticks.filter((tick) => {
    if (!tick.ts || !tick.ltp || !tick.index || !tick.stream) return false;
    const key = `${tick.key}:${tick.ts}:${tick.ltp}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function appendTick(tick) {
  state.ticks.push(tick);
  if (state.ticks.length > 160000) state.ticks.splice(0, state.ticks.length - 160000);
  state.liveCount += 1;
  state.lastLiveTs = Date.now();
  if (!state.seriesKey) state.seriesKey = tick.key;
  if (state.autoFollow) {
    const candles = buildCandles(getSeriesTicks(), state.timeframeSec);
    followView(candles.length);
  }
  renderThrottled();
}

let renderTimer = 0;
function renderThrottled() {
  if (renderTimer) return;
  renderTimer = requestAnimationFrame(() => {
    renderTimer = 0;
    render();
  });
}

function getSeriesTicks() {
  return state.ticks.filter((tick) => tick.key === state.seriesKey).sort((a, b) => a.ts - b.ts);
}

function getSeriesTrades() {
  const firstTick = getSeriesTicks()[0];
  if (!firstTick) return [];
  return state.trades.filter((trade) => {
    if (trade.secid && trade.secid === firstTick.secid) return true;
    return trade.index === firstTick.index && trade.stream === firstTick.stream;
  });
}

function buildCandles(ticks, timeframeSec) {
  const buckets = new Map();
  ticks.forEach((tick) => {
    const bucketTs = Math.floor(tick.ts / timeframeSec) * timeframeSec;
    let candle = buckets.get(bucketTs);
    if (!candle) {
      candle = { ts: bucketTs, open: tick.ltp, high: tick.ltp, low: tick.ltp, close: tick.ltp, volume: 0, buyQty: 0, sellQty: 0, samples: 0, waves: {} };
      buckets.set(bucketTs, candle);
    }
    candle.high = Math.max(candle.high, tick.ltp);
    candle.low = Math.min(candle.low, tick.ltp);
    candle.close = tick.ltp;
    candle.samples += 1;
    const f = tick.features || {};
    candle.volume += Number(f.volume || f.volume_change_tick || f.LTQ || f.ltq || 0);
    candle.buyQty += Number(f.total_buy_quantity || sumArray(f.bid_qty) || 0);
    candle.sellQty += Number(f.total_sell_quantity || sumArray(f.ask_qty) || 0);
    state.selectedFeatures.forEach((key) => {
      const waveValue = Number(f[key]);
      if (Number.isFinite(waveValue)) {
        candle.waves[key] = candle.waves[key] || [];
        candle.waves[key].push(waveValue);
      }
    });
  });
  return Array.from(buckets.values()).sort((a, b) => a.ts - b.ts).map((c) => {
    const waveValues = {};
    Object.entries(c.waves).forEach(([key, values]) => {
      waveValues[key] = values.reduce((a, b) => a + b, 0) / values.length;
    });
    return { ...c, waveValues };
  });
}

function sumArray(value) {
  return Array.isArray(value) ? value.reduce((sum, item) => sum + Number(item || 0), 0) : 0;
}

function numericFeatureKeys(ticks) {
  const keys = new Set();
  ticks.forEach((tick) => {
    Object.entries(tick.features || {}).forEach(([key, value]) => {
      if (NUMERIC_SKIP.has(key)) return;
      if (typeof value === "number" && Number.isFinite(value)) keys.add(key);
    });
  });
  return Array.from(keys).sort();
}

function refreshTabs() {
  const keys = Array.from(new Set(state.ticks.map((tick) => tick.key))).sort();
  if (!keys.includes(state.seriesKey)) state.seriesKey = keys[0] || "";
  els.instrumentTabs.innerHTML = keys.map((key) => `<button class="${key === state.seriesKey ? "active" : ""}" data-key="${escapeHtml(key)}">${escapeHtml(labelForSeries(key))}</button>`).join("");
  els.instrumentTabs.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => {
      state.seriesKey = button.dataset.key;
      state.selectedTrade = null;
      resetView(true);
      render();
    });
  });
}

function refreshFeatures() {
  const features = numericFeatureKeys(getSeriesTicks());
  if (!state.selectedFeatures.size && features.length) {
    ["recovery_score", "clean_trade_score", "exhaustion_score"].forEach((key) => {
      if (features.includes(key)) state.selectedFeatures.add(key);
    });
    if (!state.selectedFeatures.size) state.selectedFeatures.add(features[0]);
  }
  state.selectedFeatures = new Set([...state.selectedFeatures].filter((key) => features.includes(key)));
  els.featureList.innerHTML = features.map((key) => `
    <label><input type="checkbox" value="${escapeHtml(key)}" ${state.selectedFeatures.has(key) ? "checked" : ""}>${escapeHtml(key)}</label>
  `).join("") || `<span class="inspect-text">No numeric fields</span>`;
  els.featureList.querySelectorAll("input").forEach((input) => {
    input.addEventListener("change", () => {
      if (input.checked) state.selectedFeatures.add(input.value);
      else state.selectedFeatures.delete(input.value);
      render();
    });
  });
}

function refreshTrades() {
  const trades = getSeriesTrades();
  els.tradeList.innerHTML = trades.map((trade, idx) => {
    const active = state.selectedTrade === trade ? " active" : "";
    const pnlClass = trade.net_pnl >= 0 ? "profit" : "loss";
    return `
      <button class="trade-item${active}" data-idx="${idx}">
        <span class="meta"><strong>${escapeHtml(trade.tag || `${trade.index}_${trade.stream}`)}</strong><strong class="${pnlClass}">${formatMoney(trade.net_pnl)}</strong></span>
        <span class="reason">${escapeHtml(shortReason(trade.entry_reason))} -> ${escapeHtml(shortReason(trade.exit_reason))}</span>
      </button>
    `;
  }).join("");
  els.tradeList.querySelectorAll(".trade-item").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedTrade = trades[Number(button.dataset.idx)] || null;
      zoomToTrade(state.selectedTrade);
      render();
    });
  });
}

function latestPortfolio() {
  return state.portfolio[state.portfolio.length - 1] || null;
}

function latestPositions() {
  const latest = latestPortfolio();
  return latest && Array.isArray(latest.positions) ? latest.positions : [];
}

function renderDashboard() {
  renderHealth();
  renderPortfolioCards();
  renderOpenPositions();
  renderTradeSummaryTable();
}

function renderHealth() {
  const health = state.health || {};
  const summary = health.summary || {};
  const feeds = Array.isArray(health.feeds) ? health.feeds : [];
  const events = Array.isArray(health.events) ? health.events : [];
  const status = String(health.status || (state.eventSource ? "live" : "waiting")).toUpperCase();
  const statusClass = status === "HEALTHY" ? "profit" : status === "ERROR" || status === "STALE" ? "loss" : "";

  els.botStatus.textContent = status;
  els.botStatus.className = statusClass;
  els.feedStatus.textContent = `${Number(summary.feeds || feeds.length)} feeds / ${Number(summary.stale_feeds || 0)} stale`;
  els.healthUpdated.textContent = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  els.healthCounts.textContent = `${Number(summary.open_positions || 0)} open / ${Number(summary.trades || state.trades.length)} trades`;
  els.feedHealthStatus.textContent = feeds.length ? `${feeds.length} feeds` : "Waiting";
  els.errorStatus.textContent = events.length ? `${events.length} events` : "No errors";

  if (!feeds.length) {
    els.feedHealthTable.innerHTML = emptyTable("No feed health yet");
  } else {
    const rows = feeds.slice(0, 20).map((feed) => {
      const age = Number(feed.age_sec || 0);
      const ageClass = age > 20 ? "loss" : age > 8 ? "warn" : "profit";
      return `
        <div class="data-row">
          <span title="${escapeHtml(feed.key)}">${escapeHtml(feed.index || "-")}</span>
          <span>${escapeHtml(feed.stream || "-")}</span>
          <span>${Number(feed.ltp || 0).toFixed(2)}</span>
          <span class="${ageClass}">${age.toFixed(0)}s</span>
          <span>${escapeHtml(feed.secid || "")}</span>
        </div>
      `;
    }).join("");
    els.feedHealthTable.innerHTML = `
      <div class="data-header"><span>Index</span><span>Side</span><span>LTP</span><span>Age</span><span>Secid</span></div>
      ${rows}
    `;
  }

  els.errorList.innerHTML = events.length
    ? events.slice(0, 12).map((event) => `
        <div class="event-item ${escapeHtml(event.level || "warning")}">
          <strong>${escapeHtml(String(event.level || "warning").toUpperCase())}</strong>
          <span>${escapeHtml(event.time || "")}</span>
          <p>${escapeHtml(event.message || "")}</p>
        </div>
      `).join("")
    : `<div class="inspect-text">No warning/error events found in session files</div>`;
}

function renderPortfolioCards() {
  const latest = latestPortfolio();
  if (!latest) {
    els.portfolioCards.innerHTML = emptyInline("No portfolio snapshots yet");
    els.portfolioUpdated.textContent = "Waiting";
    return;
  }
  els.portfolioUpdated.textContent = latest.time ? `Updated ${latest.time}` : "Live";
  const cards = [
    ["Open", latest.open_positions],
    ["Net PnL", formatMoney(latest.net_pnl), latest.net_pnl >= 0 ? "profit" : "loss"],
    ["Realized", formatMoney(latest.realized_pnl), latest.realized_pnl >= 0 ? "profit" : "loss"],
    ["Unrealized", formatMoney(latest.unrealized_pnl), latest.unrealized_pnl >= 0 ? "profit" : "loss"],
    ["Premium", formatMoneyPlain(latest.premium_deployed)],
    ["Cash", formatMoneyPlain(latest.cash)],
    ["Fees", formatMoneyPlain(latest.total_fees)],
    ["Trades", `${latest.opened_today}/${latest.closed_today}`],
  ];
  els.portfolioCards.innerHTML = cards.map(([label, value, klass]) => `
    <div class="portfolio-card">
      <span>${escapeHtml(label)}</span>
      <strong class="${klass || ""}">${escapeHtml(value)}</strong>
    </div>
  `).join("");
}

function renderOpenPositions() {
  const positions = latestPositions();
  els.positionCount.textContent = `${positions.length} open`;
  if (!positions.length) {
    els.openPositionsTable.innerHTML = emptyTable("No open positions");
    return;
  }
  const rows = positions
    .slice()
    .sort((a, b) => Math.abs(b.pnl) - Math.abs(a.pnl))
    .map((pos) => {
      const pnlClass = pos.pnl >= 0 ? "profit" : "loss";
      return `
        <div class="data-row">
          <span title="${escapeHtml(pos.tag)}">${escapeHtml(pos.tag)}</span>
          <span>${pos.entry.toFixed(2)}</span>
          <span>${pos.ltp.toFixed(2)}</span>
          <span class="${pnlClass}">${formatMoney(pos.pnl)}</span>
          <span>${formatDuration(pos.hold_sec)}</span>
        </div>
      `;
    }).join("");
  els.openPositionsTable.innerHTML = `
    <div class="data-header"><span>Instrument</span><span>Entry</span><span>LTP</span><span>PnL</span><span>Hold</span></div>
    ${rows}
  `;
}

function renderTradeSummaryTable() {
  const trades = state.trades.slice().sort((a, b) => (b.exit_ts || b.entry_ts || 0) - (a.exit_ts || a.entry_ts || 0));
  const total = trades.reduce((sum, trade) => sum + trade.net_pnl, 0);
  const wins = trades.filter((trade) => trade.net_pnl > 0).length;
  els.tradeSummaryStatus.textContent = trades.length ? `${trades.length} trades | ${formatMoney(total)} | ${((wins / trades.length) * 100).toFixed(0)}% win` : "No trades";
  if (!trades.length) {
    els.tradeSummaryTable.innerHTML = emptyTable("No closed trades yet");
    return;
  }
  const rows = trades.slice(0, 40).map((trade) => {
    const pnlClass = trade.net_pnl >= 0 ? "profit" : "loss";
    return `
      <div class="data-row" title="${escapeHtml(trade.entry_reason)} -> ${escapeHtml(trade.exit_reason)}">
        <span>${escapeHtml(trade.tag || `${trade.index}_${trade.stream}`)}</span>
        <span>${trade.entry.toFixed(2)}</span>
        <span>${trade.exit.toFixed(2)}</span>
        <span class="${pnlClass}">${formatMoney(trade.net_pnl)}</span>
        <span>${escapeHtml(shortReason(trade.exit_reason))}</span>
      </div>
    `;
  }).join("");
  els.tradeSummaryTable.innerHTML = `
    <div class="data-header"><span>Instrument</span><span>Entry</span><span>Exit</span><span>Net</span><span>Reason</span></div>
    ${rows}
  `;
}

function renderStats(candles, trades) {
  const ticks = getSeriesTicks();
  const wins = trades.filter((t) => t.net_pnl > 0).length;
  const net = trades.reduce((sum, t) => sum + t.net_pnl, 0);
  const last = ticks[ticks.length - 1];
  els.statsPanel.innerHTML = [
    ["LTP", last ? last.ltp.toFixed(2) : "-"],
    ["Ticks", ticks.length],
    ["Candles", candles.length],
    ["Trades", trades.length],
    ["Win Rate", trades.length ? `${((wins / trades.length) * 100).toFixed(1)}%` : "0.0%"],
    ["Net PnL", formatMoney(net)],
  ].map(([label, value]) => `<div class="stat"><span>${label}</span><strong>${value}</strong></div>`).join("");
}

function visibleCandles(candles) {
  if (!candles.length) return [];
  clampView(candles.length);
  return candles.slice(Math.floor(state.viewStart), Math.ceil(state.viewEnd));
}

function resetView(follow = false) {
  const candles = buildCandles(getSeriesTicks(), state.timeframeSec);
  if (follow) followView(candles.length);
  else {
    state.viewStart = 0;
    state.viewEnd = Math.max(1, candles.length);
  }
}

function followView(count) {
  const width = Math.min(Math.max(80, state.viewEnd - state.viewStart || 220), 420);
  state.viewEnd = Math.max(1, count);
  state.viewStart = Math.max(0, state.viewEnd - width);
}

function clampView(count) {
  const minWidth = 12;
  const maxWidth = Math.max(minWidth, count || 1);
  let width = Math.max(minWidth, state.viewEnd - state.viewStart);
  width = Math.min(width, maxWidth);
  if (state.viewStart < 0) {
    state.viewStart = 0;
    state.viewEnd = width;
  }
  if (state.viewEnd > maxWidth) {
    state.viewEnd = maxWidth;
    state.viewStart = Math.max(0, state.viewEnd - width);
  }
}

function zoomToTrade(trade) {
  if (!trade) return;
  const candles = buildCandles(getSeriesTicks(), state.timeframeSec);
  const start = candles.findIndex((c) => c.ts >= trade.entry_ts);
  const end = candles.findIndex((c) => c.ts >= trade.exit_ts);
  if (start >= 0) {
    state.autoFollow = false;
    els.autoFollow.checked = false;
    state.viewStart = Math.max(0, start - 8);
    state.viewEnd = Math.min(candles.length, (end >= 0 ? end : start) + 12);
  }
}

function render() {
  refreshTabs();
  refreshFeatures();
  const ticks = getSeriesTicks();
  const candles = buildCandles(ticks, state.timeframeSec);
  const visible = visibleCandles(candles);
  const trades = getSeriesTrades();
  refreshTrades();
  renderStats(candles, trades);
  renderDashboard();
  syncPanelVisibility();
  const liveAge = state.lastLiveTs ? `${Math.round((Date.now() - state.lastLiveTs) / 1000)}s ago` : "-";
  els.statusText.textContent = ticks.length ? `${labelForSeries(state.seriesKey)} | ${ticks.length} ticks | live ${liveAge}` : "Load session files";
  els.inspectText.textContent = `${Math.floor(state.viewStart) + 1}-${Math.ceil(state.viewEnd)} of ${candles.length} candles | Wheel zoom, drag pan, double click reset`;
  drawPriceChart(els.priceCanvas, visible, candles, trades);
  drawVolumeChart(els.volumeCanvas, visible);
  drawWaveChart(els.waveCanvas, visible);
}

function syncPanelVisibility() {
  els.chartStack.classList.toggle("hide-volume", !els.showVolume.checked);
  els.chartStack.classList.toggle("hide-wave", !els.showWave.checked);
}

function drawPriceChart(canvas, candles, allCandles, trades) {
  const ctx = prepareCanvas(canvas);
  const box = chartBox(canvas);
  drawGrid(ctx, box);
  if (!candles.length) return drawEmpty(ctx, canvas, "No instrument selected");
  const min = Math.min(...candles.map((c) => c.low));
  const max = Math.max(...candles.map((c) => c.high));
  const y = scaler(max, min, box.y, box.y + box.h);
  const x = indexScaler(candles.length, box.x, box.x + box.w);
  const body = Math.max(3, Math.min(14, box.w / Math.max(candles.length, 1) * .62));
  candles.forEach((c, i) => {
    const up = c.close >= c.open;
    ctx.strokeStyle = up ? css("--up") : css("--down");
    ctx.fillStyle = ctx.strokeStyle;
    const cx = x(i);
    ctx.beginPath();
    ctx.moveTo(cx, y(c.high));
    ctx.lineTo(cx, y(c.low));
    ctx.stroke();
    const top = y(Math.max(c.open, c.close));
    const bot = y(Math.min(c.open, c.close));
    ctx.fillRect(cx - body / 2, top, body, Math.max(1, bot - top));
  });
  if (els.showTrades.checked) drawTradeMarkers(ctx, box, candles, trades, y);
  drawAxis(ctx, box, min, max);
  drawTimeAxis(ctx, box, candles);
}

function drawVolumeChart(canvas, candles) {
  const ctx = prepareCanvas(canvas);
  const box = chartBox(canvas);
  drawGrid(ctx, box);
  if (!candles.length) return drawEmpty(ctx, canvas, "No volume data");
  const max = Math.max(1, ...candles.map((c) => Math.max(c.volume, c.buyQty, c.sellQty)));
  const x = indexScaler(candles.length, box.x, box.x + box.w);
  const barW = Math.max(2, Math.min(10, box.w / Math.max(candles.length, 1) * .5));
  candles.forEach((c, i) => {
    const cx = x(i);
    drawBar(ctx, cx - barW, box, c.buyQty, max, css("--up"), barW);
    drawBar(ctx, cx, box, c.sellQty, max, css("--down"), barW);
    if (!c.buyQty && !c.sellQty) drawBar(ctx, cx - barW / 2, box, c.volume, max, "#7e8ca3", barW);
  });
  drawLabel(ctx, box, "Volume / Bid Qty / Ask Qty");
}

function drawWaveChart(canvas, candles) {
  const ctx = prepareCanvas(canvas);
  const box = chartBox(canvas);
  drawGrid(ctx, box);
  const series = [...state.selectedFeatures].map((key) => ({
    key,
    points: candles.map((c, i) => ({ i, value: c.waveValues[key] })).filter((p) => Number.isFinite(p.value)),
  })).filter((s) => s.points.length);
  if (!series.length) return drawEmpty(ctx, canvas, "Select wave fields");
  const values = series.flatMap((s) => s.points.map((p) => p.value));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const y = scaler(max, min, box.y, box.y + box.h);
  const x = indexScaler(candles.length, box.x, box.x + box.w);
  series.forEach((s, idx) => {
    ctx.strokeStyle = css(WAVE_COLORS[idx % WAVE_COLORS.length]);
    ctx.lineWidth = 2;
    ctx.beginPath();
    s.points.forEach((p, pIdx) => {
      const px = x(p.i);
      const py = y(p.value);
      if (pIdx === 0) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
    });
    ctx.stroke();
    drawLabel(ctx, { ...box, y: box.y + idx * 17 }, s.key, ctx.strokeStyle);
  });
  drawAxis(ctx, box, min, max);
}

function drawTradeMarkers(ctx, box, candles, trades, y) {
  const first = candles[0]?.ts || 0;
  const last = candles[candles.length - 1]?.ts || first + 1;
  const tx = (ts) => box.x + ((ts - first) / Math.max(1, last - first)) * box.w;
  trades.forEach((trade) => {
    if (trade.exit_ts && (trade.exit_ts < first || trade.entry_ts > last)) return;
    if (state.selectedTrade === trade && trade.entry_ts && trade.exit_ts) {
      ctx.fillStyle = "rgba(255, 210, 63, .08)";
      ctx.fillRect(tx(trade.entry_ts), box.y, Math.max(1, tx(trade.exit_ts) - tx(trade.entry_ts)), box.h);
    }
    if (trade.entry_ts >= first && trade.entry_ts <= last) marker(ctx, tx(trade.entry_ts), y(trade.entry), css("--entry"), "E");
    if (trade.exit_ts >= first && trade.exit_ts <= last) marker(ctx, tx(trade.exit_ts), y(trade.exit), css("--exit"), "X");
  });
}

function marker(ctx, x, y, color, text) {
  ctx.fillStyle = color;
  ctx.beginPath();
  ctx.arc(x, y, 8, 0, Math.PI * 2);
  ctx.fill();
  ctx.fillStyle = "#101318";
  ctx.font = "700 10px Segoe UI";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(text, x, y + .5);
}

function prepareCanvas(canvas) {
  const rect = canvas.getBoundingClientRect();
  const scale = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.floor(rect.width * scale));
  canvas.height = Math.max(1, Math.floor(rect.height * scale));
  const ctx = canvas.getContext("2d");
  ctx.setTransform(scale, 0, 0, scale, 0, 0);
  ctx.clearRect(0, 0, rect.width, rect.height);
  return ctx;
}

function chartBox(canvas) {
  const rect = canvas.getBoundingClientRect();
  const left = 52;
  const right = 58;
  const top = 14;
  const bottom = 34;
  return {
    x: left,
    y: top,
    w: Math.max(90, rect.width - left - right),
    h: Math.max(50, rect.height - top - bottom),
  };
}

function drawGrid(ctx, box) {
  ctx.strokeStyle = "rgba(255,255,255,.06)";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = box.y + (box.h / 4) * i;
    ctx.beginPath();
    ctx.moveTo(box.x, y);
    ctx.lineTo(box.x + box.w, y);
    ctx.stroke();
  }
  for (let i = 0; i <= 4; i++) {
    const x = box.x + (box.w / 4) * i;
    ctx.beginPath();
    ctx.moveTo(x, box.y);
    ctx.lineTo(x, box.y + box.h);
    ctx.stroke();
  }
  ctx.strokeStyle = "rgba(154,165,181,.34)";
  ctx.beginPath();
  ctx.moveTo(box.x, box.y);
  ctx.lineTo(box.x, box.y + box.h);
  ctx.lineTo(box.x + box.w, box.y + box.h);
  ctx.lineTo(box.x + box.w, box.y);
  ctx.stroke();
}

function drawAxis(ctx, box, min, max) {
  ctx.fillStyle = "#9aa5b5";
  ctx.font = "12px Segoe UI";
  ctx.textAlign = "left";
  ctx.textBaseline = "middle";
  for (let i = 0; i <= 4; i++) {
    const value = max - ((max - min) / 4) * i;
    const y = box.y + (box.h / 4) * i;
    ctx.fillText(value.toFixed(2), box.x + box.w + 8, y);
  }
}

function drawTimeAxis(ctx, box, candles) {
  ctx.fillStyle = "#7e8ca3";
  ctx.font = "11px Segoe UI";
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  for (let i = 0; i <= 4; i++) {
    const idx = Math.min(candles.length - 1, Math.floor((candles.length - 1) * (i / 4)));
    const x = box.x + (box.w / 4) * i;
    ctx.fillText(formatTime(candles[idx]?.ts), x, box.y + box.h + 8);
  }
}

function drawBar(ctx, x, box, value, max, color, width) {
  const h = (Number(value || 0) / max) * box.h;
  ctx.fillStyle = color;
  ctx.fillRect(x, box.y + box.h - h, width, h);
}

function drawLabel(ctx, box, text, color = "#9aa5b5") {
  ctx.fillStyle = color;
  ctx.font = "12px Segoe UI";
  ctx.textAlign = "left";
  ctx.textBaseline = "top";
  ctx.fillText(text || "", box.x, box.y);
}

function drawEmpty(ctx, canvas, text) {
  const rect = canvas.getBoundingClientRect();
  ctx.fillStyle = "#9aa5b5";
  ctx.font = "14px Segoe UI";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(text, rect.width / 2, rect.height / 2);
}

function scaler(max, min, top, bottom) {
  const spread = Math.max(1e-9, max - min);
  return (value) => bottom - ((value - min) / spread) * (bottom - top);
}

function indexScaler(count, left, right) {
  const spread = Math.max(1, count - 1);
  return (idx) => left + (idx / spread) * (right - left);
}

function installChartControls(canvas) {
  canvas.addEventListener("wheel", (event) => {
    event.preventDefault();
    const candles = buildCandles(getSeriesTicks(), state.timeframeSec);
    if (!candles.length) return;
    state.autoFollow = false;
    els.autoFollow.checked = false;
    const rect = canvas.getBoundingClientRect();
    const box = chartBox(canvas);
    const ratio = Math.min(1, Math.max(0, (event.clientX - rect.left - box.x) / Math.max(1, box.w)));
    const focus = state.viewStart + (state.viewEnd - state.viewStart) * ratio;
    const factor = event.deltaY > 0 ? 1.18 : 0.84;
    const width = Math.max(12, Math.min(candles.length, (state.viewEnd - state.viewStart) * factor));
    state.viewStart = focus - width * ratio;
    state.viewEnd = state.viewStart + width;
    clampView(candles.length);
    render();
  }, { passive: false });

  canvas.addEventListener("mousedown", (event) => {
    state.autoFollow = false;
    els.autoFollow.checked = false;
    state.dragging = { x: event.clientX, start: state.viewStart, end: state.viewEnd };
  });
  window.addEventListener("mousemove", (event) => {
    if (!state.dragging) return;
    const candles = buildCandles(getSeriesTicks(), state.timeframeSec);
    const deltaPx = event.clientX - state.dragging.x;
    const box = chartBox(canvas);
    const deltaCandles = -(deltaPx / Math.max(1, box.w)) * (state.dragging.end - state.dragging.start);
    state.viewStart = state.dragging.start + deltaCandles;
    state.viewEnd = state.dragging.end + deltaCandles;
    clampView(candles.length);
    render();
  });
  window.addEventListener("mouseup", () => {
    state.dragging = null;
  });
  canvas.addEventListener("dblclick", () => {
    state.autoFollow = true;
    els.autoFollow.checked = true;
    resetView(true);
    render();
  });
}

function shortReason(reason) {
  return String(reason || "").replace("TRI_WAVE_V2_ENTRY:", "").replace("TRI_WAVE_V2_EXIT:", "");
}

function formatMoney(value) {
  const num = Number(value || 0);
  return `${num >= 0 ? "+" : ""}${num.toFixed(2)}`;
}

function formatMoneyPlain(value) {
  return Number(value || 0).toFixed(2);
}

function formatDuration(seconds) {
  const total = Math.max(0, Number(seconds || 0));
  const m = Math.floor(total / 60);
  const s = Math.floor(total % 60);
  return `${m}m${String(s).padStart(2, "0")}s`;
}

function emptyInline(text) {
  return `<div class="inspect-text">${escapeHtml(text)}</div>`;
}

function emptyTable(text) {
  return `<div class="data-row"><span>${escapeHtml(text)}</span><span></span><span></span><span></span><span></span></div>`;
}

function downloadSessionFile(file) {
  if (!state.currentSession) return;
  const query = new URLSearchParams({
    date: state.currentSession.date,
    expiry: state.currentSession.expiry,
    file,
  });
  window.location.href = apiUrl(`/api/session/download?${query}`);
}

function formatTime(ts) {
  if (!ts) return "";
  return new Date(ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function labelForSeries(key) {
  return String(key || "").replace(/_(\d+)$/, "");
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[ch]));
}

function css(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

els.ticksFile.addEventListener("change", (event) => readJsonlFile(event.target.files[0], (rows) => {
  closeLiveStream();
  state.ticks = dedupeTicks(rows.map(normalizeTick));
  state.seriesKey = "";
  resetView(true);
  render();
}));

els.tradesFile.addEventListener("change", (event) => readJsonlFile(event.target.files[0], (rows) => {
  state.trades = rows.map(normalizeTrade);
  state.selectedTrade = null;
  render();
}));

els.signalsFile.addEventListener("change", (event) => readJsonlFile(event.target.files[0], (rows) => {
  state.signals = rows;
  render();
}));

els.downloadTrades.addEventListener("click", () => downloadSessionFile("trades"));
els.downloadPortfolio.addEventListener("click", () => downloadSessionFile("portfolio"));
els.downloadSignals.addEventListener("click", () => downloadSessionFile("signals"));
els.downloadTicks.addEventListener("click", () => downloadSessionFile("ticks"));

els.sessionSelect.addEventListener("change", async () => {
  const idx = Number(els.sessionSelect.value);
  if (Number.isFinite(idx) && state.sessions[idx]) await loadCloudSession(state.sessions[idx]);
});

els.timeframeSelect.addEventListener("change", () => {
  state.timeframeSec = Number(els.timeframeSelect.value || 5);
  resetView(state.autoFollow);
  render();
});

[els.showVolume, els.showWave, els.showTrades].forEach((input) => input.addEventListener("change", render));
els.autoFollow.addEventListener("change", () => {
  state.autoFollow = els.autoFollow.checked;
  if (state.autoFollow) resetView(true);
  render();
});

[els.priceCanvas, els.volumeCanvas, els.waveCanvas].forEach(installChartControls);
window.addEventListener("resize", render);
render();
loadCloudSessions();
