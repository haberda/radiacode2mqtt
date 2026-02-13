const sel = document.getElementById("deviceSelect");
const metrics = document.getElementById("metrics");
const statusEl = document.getElementById("status");
const specMeta = document.getElementById("specMeta");
const canvas = document.getElementById("specCanvas");
const ctx = canvas.getContext("2d");

let state = {};
let spectrum = {};
let devices = [];
let currentDevice = null;

function fmt(v) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") {
    if (Math.abs(v) < 1e-3 && v !== 0) return v.toExponential(3);
    return v.toFixed(3).replace(/\.?0+$/,"");
  }
  return String(v);
}

function renderMetrics() {
  const s = state[currentDevice] || {};
  const rows = [
    ["Dose rate", `${fmt(s.dose_rate)} ${s.dose_rate_unit || ""}`],
    ["CPS", fmt(s.cps)],
    ["Dose rate err", `${fmt(s.dose_rate_err)} %`],
    ["CPS err", `${fmt(s.count_rate_err)} %`],
    ["Temp", `${fmt(s.temperature_c)} °C`],
    ["Battery", `${fmt(s.battery_pct)} %`],
    ["Total dose", `${fmt(s.dose_total)} ${s.dose_total_unit || ""}`],
    ["Rare duration", `${fmt(s.spectrum_duration_s)} s`],
  ];
  metrics.innerHTML = rows.map(([k,v]) => `<div class="kv"><div>${k}</div><div><b>${v}</b></div></div>`).join("");
}

function clearPlot() {
  ctx.clearRect(0,0,canvas.width,canvas.height);
  ctx.fillText("No spectrum yet.", 20, 40);
}

function drawSpectrum(counts) {
  ctx.clearRect(0,0,canvas.width,canvas.height);

  const w = canvas.width;
  const h = canvas.height;
  const padL = 45, padR = 10, padT = 10, padB = 25;
  const plotW = w - padL - padR;
  const plotH = h - padT - padB;

  // find max
  let max = 1;
  for (const c of counts) if (c > max) max = c;

  // axes
  ctx.strokeStyle = "#bbb";
  ctx.beginPath();
  ctx.moveTo(padL, padT);
  ctx.lineTo(padL, padT + plotH);
  ctx.lineTo(padL + plotW, padT + plotH);
  ctx.stroke();

  // line
  ctx.strokeStyle = "#111";
  ctx.beginPath();
  const n = counts.length;
  for (let i=0; i<n; i++) {
    const x = padL + (i/(n-1))*plotW;
    const y = padT + plotH - (counts[i]/max)*plotH;
    if (i === 0) ctx.moveTo(x,y);
    else ctx.lineTo(x,y);
  }
  ctx.stroke();

  // labels
  ctx.fillStyle = "#333";
  ctx.font = "12px system-ui, sans-serif";
  ctx.fillText("0", padL, padT + plotH + 16);
  ctx.fillText(String(n-1), padL + plotW - 18, padT + plotH + 16);
  ctx.fillText(String(max), 6, padT + 12);
}

function renderSpectrum() {
  const sp = spectrum[currentDevice] || {};
  const counts = sp.counts;
  if (!Array.isArray(counts) || counts.length === 0) {
    specMeta.textContent = "No spectrum received yet.";
    clearPlot();
    return;
  }
  drawSpectrum(counts);

  const cal = sp.calibration || {};
  specMeta.textContent =
    `channels=${sp.channels} duration_s=${fmt(sp.duration_s)} ` +
    `cal(a0=${fmt(cal.a0)}, a1=${fmt(cal.a1)}, a2=${fmt(cal.a2)})`;
}

function setDevice(d) {
  currentDevice = d;
  renderMetrics();
  renderSpectrum();
}

async function refreshDevices() {
  const res = await fetch("./api/devices");
  const data = await res.json();
  devices = data.devices.map(x => x.device_id);

  sel.innerHTML = devices.map(d => `<option value="${d}">${d}</option>`).join("");
  if (!currentDevice && devices.length) setDevice(devices[0]);
  if (currentDevice) sel.value = currentDevice;
}

sel.addEventListener("change", () => setDevice(sel.value));

function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}${location.pathname.replace(/\/$/,"")}/ws`);

  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    if (msg.type === "snapshot") {
      state = msg.state || {};
      spectrum = msg.spectrum || {};
      if (Array.isArray(msg.devices) && msg.devices.length) {
        devices = msg.devices;
        sel.innerHTML = devices.map(d => `<option value="${d}">${d}</option>`).join("");
        if (!currentDevice) setDevice(devices[0]);
        sel.value = currentDevice;
      }
      renderMetrics();
      renderSpectrum();
      statusEl.textContent = "Connected (snapshot).";
    } else if (msg.type === "state") {
      state[msg.device_id] = msg.payload;
      if (msg.device_id === currentDevice) renderMetrics();
    } else if (msg.type === "spectrum") {
      spectrum[msg.device_id] = msg.payload;
      if (msg.device_id === currentDevice) renderSpectrum();
    }
  };

  ws.onopen = () => statusEl.textContent = "Connected.";
  ws.onclose = () => {
    statusEl.textContent = "Disconnected; retrying…";
    setTimeout(connectWS, 2000);
  };
  ws.onerror = () => ws.close();
}

(async () => {
  await refreshDevices();
  connectWS();
})();
