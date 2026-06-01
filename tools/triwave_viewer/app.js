const state = {
  ticks: [],
  trades: [],
  signals: [],
  sessions: [],
  seriesKey: "",
  featureKey: "",
  timeframeSec: 5,
  selectedTrade: null,
};

const els = {
  ticksFile: document.getElementById("ticksFile"),
  tradesFile: document.getElementById("tradesFile"),
  signalsFile: document.getElementById("signalsFile"),
  sessionSelect: document.getElementById("sessionSelect"),
  seriesSelect: document.getElementById("seriesSelect"),
  timeframeSelect: document.getElementById("timeframeSelect"),
  featureSelect: document.getElementById("featureSelect"),
  statsPanel: document.getElementById("statsPanel"),
  tradeList: document.getElementById("tradeList"),
  statusText: document.getElementById("statusText"),
  priceCanvas: document.getElementById("priceCanvas"),
  volumeCanvas: document.getElementById("volumeCanvas"),
  waveCanvas: document.getElementById("waveCanvas"),
};

const NUMERIC_SKIP = new Set([
  "ts", "secid", "security_id", "ltp", "open", "high", "low", "close",
  "bid_price", "ask_price", "bid_qty", "ask_qty", "raw", "depth"
]);

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
  if (!els.sessionSelect) return;
  els.sessionSelect.innerHTML = state.sessions.length
    ? state.sessions.map((session, index) => `<option value="${index}">${escapeHtml(session.date)} / ${escapeHtml(session.expiry)}</option>`).join("")
    : `<option value="">Local files</option>`;
}

async function loadCloudSession(session) {
  if (!session) return;
  els.statusText.textContent = `Loading ${session.date} / ${session.expiry}`;
  const query = new URLSearchParams({ date: session.date, expiry: session.expiry, limit: "75000" });
  const response = await fetch(apiUrl(`/api/session?${query}`), { cache: "no-store" });
  if (!response.ok) throw new Error(`Session load failed: ${response.status}`);
  const payload = await response.json();
  state.ticks = (payload.ticks || []).map(normalizeTick).filter((tick) => tick.ts && tick.ltp && tick.index && tick.stream);
  state.trades = (payload.trades || []).map(normalizeTrade);
  state.signals = payload.signals || [];
  state.seriesKey = "";
  state.selectedTrade = null;
  render();
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
  return {
    ...row,
    features,
    ts,
    ltp,
    index,
    stream,
    secid,
    key: `${index}_${stream}_${secid}`,
  };
}

function normalizeTrade(row, idx) {
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
    if (!tick.ts || !tick.ltp) return;
    const bucketTs = Math.floor(tick.ts / timeframeSec) * timeframeSec;
    let candle = buckets.get(bucketTs);
    if (!candle) {
      candle = {
        ts: bucketTs,
        open: tick.ltp,
        high: tick.ltp,
        low: tick.ltp,
        close: tick.ltp,
        volume: 0,
        buyQty: 0,
        sellQty: 0,
        samples: 0,
        wave: [],
      };
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
    const waveValue = Number(f[state.featureKey]);
    if (Number.isFinite(waveValue)) candle.wave.push(waveValue);
  });
  return Array.from(buckets.values()).sort((a, b) => a.ts - b.ts).map((c) => ({
    ...c,
    waveValue: c.wave.length ? c.wave.reduce((a, b) => a + b, 0) / c.wave.length : null,
  }));
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

function refreshSelectors() {
  const keys = Array.from(new Set(state.ticks.map((tick) => tick.key))).sort();
  els.seriesSelect.innerHTML = keys.map((key) => `<option value="${escapeHtml(key)}">${escapeHtml(key)}</option>`).join("");
  if (!keys.includes(state.seriesKey)) state.seriesKey = keys[0] || "";
  els.seriesSelect.value = state.seriesKey;

  const features = numericFeatureKeys(getSeriesTicks());
  els.featureSelect.innerHTML = features.map((key) => `<option value="${escapeHtml(key)}">${escapeHtml(key)}</option>`).join("");
  if (!features.includes(state.featureKey)) state.featureKey = features[0] || "";
  els.featureSelect.value = state.featureKey;
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
      render();
    });
  });
}

function renderStats(candles, trades) {
  const wins = trades.filter((t) => t.net_pnl > 0).length;
  const net = trades.reduce((sum, t) => sum + t.net_pnl, 0);
  const hold = trades.length ? trades.reduce((sum, t) => sum + Math.max(0, t.exit_ts - t.entry_ts), 0) / trades.length : 0;
  els.statsPanel.innerHTML = [
    ["Ticks", getSeriesTicks().length],
    ["Candles", candles.length],
    ["Trades", trades.length],
    ["Win Rate", trades.length ? `${((wins / trades.length) * 100).toFixed(1)}%` : "0.0%"],
    ["Net PnL", formatMoney(net)],
    ["Avg Hold", `${hold.toFixed(0)}s`],
  ].map(([label, value]) => `<div class="stat"><span>${label}</span><strong>${value}</strong></div>`).join("");
}

function render() {
  refreshSelectors();
  const ticks = getSeriesTicks();
  const candles = buildCandles(ticks, state.timeframeSec);
  const trades = getSeriesTrades();
  refreshTrades();
  renderStats(candles, trades);
  els.statusText.textContent = ticks.length ? `${state.seriesKey} | ${ticks.length} ticks | ${state.timeframeSec}s candles` : "Load session files";
  drawPriceChart(els.priceCanvas, candles, trades);
  drawVolumeChart(els.volumeCanvas, candles);
  drawWaveChart(els.waveCanvas, candles, state.featureKey);
}

function drawPriceChart(canvas, candles, trades) {
  const ctx = prepareCanvas(canvas);
  const box = chartBox(canvas);
  drawGrid(ctx, box);
  if (!candles.length) return drawEmpty(ctx, canvas, "No tick series selected");
  const min = Math.min(...candles.map((c) => c.low));
  const max = Math.max(...candles.map((c) => c.high));
  const y = scaler(max, min, box.y, box.y + box.h);
  const x = indexScaler(candles.length, box.x, box.x + box.w);
  const body = Math.max(3, Math.min(12, box.w / Math.max(candles.length, 1) * .62));

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

  drawTradeMarkers(ctx, box, candles, trades, y);
  drawAxis(ctx, box, min, max);
}

function drawVolumeChart(canvas, candles) {
  const ctx = prepareCanvas(canvas);
  const box = chartBox(canvas);
  drawGrid(ctx, box);
  if (!candles.length) return drawEmpty(ctx, canvas, "No volume data");
  const max = Math.max(1, ...candles.map((c) => Math.max(c.volume, c.buyQty, c.sellQty)));
  const x = indexScaler(candles.length, box.x, box.x + box.w);
  const barW = Math.max(2, Math.min(10, box.w / Math.max(candles.length, 1) * .55));
  candles.forEach((c, i) => {
    const cx = x(i);
    drawBar(ctx, cx - barW, box, c.buyQty, max, css("--up"), barW);
    drawBar(ctx, cx, box, c.sellQty, max, css("--down"), barW);
    if (!c.buyQty && !c.sellQty) drawBar(ctx, cx - barW / 2, box, c.volume, max, "#7e8ca3", barW);
  });
  drawLabel(ctx, box, "Volume / Bid Qty / Ask Qty");
}

function drawWaveChart(canvas, candles, featureKey) {
  const ctx = prepareCanvas(canvas);
  const box = chartBox(canvas);
  drawGrid(ctx, box);
  const points = candles.map((c, i) => ({ i, value: c.waveValue })).filter((p) => p.value !== null);
  if (!points.length) return drawEmpty(ctx, canvas, featureKey ? `No numeric wave: ${featureKey}` : "No wave selected");
  const min = Math.min(...points.map((p) => p.value));
  const max = Math.max(...points.map((p) => p.value));
  const y = scaler(max, min, box.y, box.y + box.h);
  const x = indexScaler(candles.length, box.x, box.x + box.w);
  ctx.strokeStyle = css("--wave");
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((p, idx) => {
    const px = x(p.i);
    const py = y(p.value);
    if (idx === 0) ctx.moveTo(px, py);
    else ctx.lineTo(px, py);
  });
  ctx.stroke();
  drawLabel(ctx, box, featureKey);
  drawAxis(ctx, box, min, max);
}

function drawTradeMarkers(ctx, box, candles, trades, y) {
  const first = candles[0]?.ts || 0;
  const last = candles[candles.length - 1]?.ts || first + 1;
  const tx = (ts) => box.x + ((ts - first) / Math.max(1, last - first)) * box.w;
  trades.forEach((trade) => {
    if (trade.entry_ts) marker(ctx, tx(trade.entry_ts), y(trade.entry), css("--entry"), "E");
    if (trade.exit_ts) marker(ctx, tx(trade.exit_ts), y(trade.exit), css("--exit"), "X");
    if (state.selectedTrade === trade && trade.entry_ts && trade.exit_ts) {
      ctx.fillStyle = "rgba(255, 210, 63, .08)";
      ctx.fillRect(tx(trade.entry_ts), box.y, Math.max(1, tx(trade.exit_ts) - tx(trade.entry_ts)), box.h);
    }
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
  return { x: 58, y: 18, w: Math.max(80, rect.width - 88), h: Math.max(50, rect.height - 42) };
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
}

function drawAxis(ctx, box, min, max) {
  ctx.fillStyle = "#9aa5b5";
  ctx.font = "12px Segoe UI";
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let i = 0; i <= 4; i++) {
    const value = max - ((max - min) / 4) * i;
    const y = box.y + (box.h / 4) * i;
    ctx.fillText(value.toFixed(2), box.x - 8, y);
  }
}

function drawBar(ctx, x, box, value, max, color, width) {
  const h = (Number(value || 0) / max) * box.h;
  ctx.fillStyle = color;
  ctx.fillRect(x, box.y + box.h - h, width, h);
}

function drawLabel(ctx, box, text) {
  ctx.fillStyle = "#9aa5b5";
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

function shortReason(reason) {
  return String(reason || "").replace("TRI_WAVE_V2_ENTRY:", "").replace("TRI_WAVE_V2_EXIT:", "");
}

function formatMoney(value) {
  const num = Number(value || 0);
  return `${num >= 0 ? "+" : ""}${num.toFixed(2)}`;
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
  state.ticks = rows.map(normalizeTick).filter((tick) => tick.ts && tick.ltp && tick.index && tick.stream);
  state.seriesKey = "";
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

els.sessionSelect.addEventListener("change", async () => {
  const idx = Number(els.sessionSelect.value);
  if (Number.isFinite(idx) && state.sessions[idx]) await loadCloudSession(state.sessions[idx]);
});

els.seriesSelect.addEventListener("change", () => {
  state.seriesKey = els.seriesSelect.value;
  state.selectedTrade = null;
  render();
});

els.timeframeSelect.addEventListener("change", () => {
  state.timeframeSec = Number(els.timeframeSelect.value || 5);
  render();
});

els.featureSelect.addEventListener("change", () => {
  state.featureKey = els.featureSelect.value;
  render();
});

window.addEventListener("resize", render);
render();
loadCloudSessions();
