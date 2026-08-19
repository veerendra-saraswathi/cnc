const API = "";
let selectedId = null;
let assets = {};
let tracks = {};
let map = null;
let assetMarkers = {};
let trackMarkers = {};
let ws = null;
let gotoMode = false;

function initMap() {
  map = L.map("map", { zoomControl: true }).setView([17.4505, 78.4810], 15);
  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    attribution: "&copy; OSM &copy; CARTO", maxZoom: 19,
  }).addTo(map);
  map.on("click", (e) => {
    if (!gotoMode || !selectedId) return;
    document.getElementById("goto-lat").value = e.latlng.lat.toFixed(6);
    document.getElementById("goto-lon").value = e.latlng.lng.toFixed(6);
  });
}

function upsertAssetMarker(a) {
  const isUav = a.asset_type === "uav";
  const label = a.name.replace(/[^0-9]/g, "") || a.name.slice(0, 2);
  const selected = a.id === selectedId ? " selected" : "";
  const icon = L.divIcon({
    className: "marker-label",
    html: `<div class="marker-inner ${isUav ? "uav" : "ugv"}${selected}">${label}</div>`,
    iconSize: [28, 28], iconAnchor: [14, 14],
  });
  if (assetMarkers[a.id]) {
    assetMarkers[a.id].setLatLng([a.lat, a.lon]);
    assetMarkers[a.id].setIcon(icon);
  } else {
    assetMarkers[a.id] = L.marker([a.lat, a.lon], { icon }).addTo(map);
    assetMarkers[a.id].on("click", () => selectAsset(a.id));
  }
}

function upsertTrackMarker(t) {
  // Tracks are shown as a slightly larger ring / different colour
  const color = t.track_type === "friendly" ? "#22c55e" :
                t.track_type === "hostile"  ? "#ef4444" : "#f59e0b";

  const icon = L.divIcon({
    className: "marker-label",
    html: `<div style="
      width:34px;height:34px;border-radius:50%;
      border:3px solid ${color};
      background:rgba(0,0,0,0.35);
      display:flex;align-items:center;justify-content:center;
      font-size:10px;font-weight:700;color:#fff;
      box-shadow:0 0 0 1px rgba(0,0,0,0.5);
    ">${t.label.replace(/[^0-9A-Z]/gi,'').slice(0,3)}</div>`,
    iconSize: [34, 34], iconAnchor: [17, 17],
  });

  if (trackMarkers[t.id]) {
    trackMarkers[t.id].setLatLng([t.lat, t.lon]);
    trackMarkers[t.id].setIcon(icon);
  } else {
    trackMarkers[t.id] = L.marker([t.lat, t.lon], { icon, zIndexOffset: -100 }).addTo(map);
  }
}

function renderAssets() {
  const list = document.getElementById("asset-list");
  const order = Object.values(assets).sort((x, y) => x.name.localeCompare(y.name));
  list.innerHTML = order.map((a) => {
    const battClass = a.battery > 30 ? "ok" : a.battery > 15 ? "low" : "crit";
    const statusClass = a.status === "offline" || a.status === "lost" ? "" :
      a.status === "mission" ? "mission" :
      a.status === "returning" ? "returning" :
      a.status === "emergency" ? "emergency" : "online";
    return `
      <div class="asset-card ${a.id === selectedId ? "selected" : ""}" data-id="${a.id}">
        <div class="top">
          <span class="name">${a.name}</span>
          <span class="type-badge ${a.asset_type}">${a.asset_type}</span>
        </div>
        <div class="status-row">
          <span class="status-dot ${statusClass}"></span>
          <span>${a.status}</span>
        </div>
        <div class="metrics">
          <div>Lat <strong>${a.lat.toFixed(5)}</strong></div>
          <div>Lon <strong>${a.lon.toFixed(5)}</strong></div>
          <div>Alt <strong>${a.alt.toFixed(1)} m</strong></div>
          <div>Hdg <strong>${a.heading.toFixed(0)}°</strong></div>
          <div>Spd <strong>${a.speed.toFixed(1)} m/s</strong></div>
          <div>Batt <strong>${a.battery.toFixed(0)}%</strong></div>
        </div>
        <div class="battery-bar"><div class="fill ${battClass}" style="width:${Math.min(100, a.battery)}%"></div></div>
      </div>`;
  }).join("");
  list.querySelectorAll(".asset-card").forEach((el) => {
    el.addEventListener("click", () => selectAsset(el.dataset.id));
  });
}

function selectAsset(id) {
  selectedId = id;
  const a = assets[id];
  document.getElementById("selected-info").innerHTML = a
    ? `Selected: <strong>${a.name}</strong> (${a.asset_type.toUpperCase()})`
    : "Select an asset";
  const isUav = a && a.asset_type === "uav";
  document.querySelectorAll(".cmd-btn").forEach((btn) => {
    const needsUav = btn.dataset.uavOnly === "1";
    btn.disabled = !a || (needsUav && !isUav);
  });
  renderAssets();
  Object.values(assets).forEach(upsertAssetMarker);
}

async function sendCommand(type, params = {}) {
  if (!selectedId) return;
  try {
    await fetch(`${API}/assets/${selectedId}/command`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command_type: type, parameters: params }),
    });
  } catch (err) { console.error("Command failed", err); }
}

function showGotoForm(show) {
  gotoMode = show;
  document.getElementById("goto-form").classList.toggle("visible", show);
  if (show && selectedId) {
    const a = assets[selectedId];
    if (a) {
      document.getElementById("goto-lat").value = a.lat.toFixed(6);
      document.getElementById("goto-lon").value = a.lon.toFixed(6);
      document.getElementById("goto-alt").value = a.asset_type === "uav" ? "50" : "0";
    }
  }
}

async function executeGoto() {
  const lat = parseFloat(document.getElementById("goto-lat").value);
  const lon = parseFloat(document.getElementById("goto-lon").value);
  const alt = parseFloat(document.getElementById("goto-alt").value) || 40;
  if (isNaN(lat) || isNaN(lon)) return;
  await sendCommand("goto", { lat, lon, alt });
  showGotoForm(false);
}

function renderAlerts(alerts) {
  const el = document.getElementById("alerts-list");
  if (!alerts || alerts.length === 0) {
    el.innerHTML = `<div style="color:var(--muted);font-size:12px;padding:8px;">No active alerts</div>`;
    return;
  }
  el.innerHTML = alerts.slice(0, 12).map((al) => `
    <div class="alert-item ${al.level}">
      <div class="title">${al.title}</div>
      <div class="msg">${al.message}</div>
      <div class="time">${new Date(al.timestamp).toLocaleTimeString()}</div>
    </div>`).join("");
}

function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws/cop`);
  ws.onmessage = (ev) => {
    const snap = JSON.parse(ev.data);
    document.getElementById("clock").textContent = new Date(snap.timestamp).toLocaleTimeString();

    // Assets
    const nextAssets = {};
    (snap.assets || []).forEach((a) => {
      nextAssets[a.id] = a;
      upsertAssetMarker(a);
    });
    assets = nextAssets;
    renderAssets();
    if (selectedId && !assets[selectedId]) selectAsset(null);
    else if (selectedId) selectAsset(selectedId);

    // Tracks
    const nextTracks = {};
    (snap.tracks || []).forEach((t) => {
      nextTracks[t.id] = t;
      upsertTrackMarker(t);
    });
    tracks = nextTracks;

    renderAlerts(snap.alerts || []);
    const online = Object.values(assets).filter((a) => a.status !== "offline").length;
    document.getElementById("asset-count").textContent = `${online}/${Object.keys(assets).length}`;
  };
  ws.onclose = () => {
    document.getElementById("conn-status").textContent = "Reconnecting…";
    document.getElementById("conn-status").style.color = "var(--amber)";
    setTimeout(connectWS, 1500);
  };
  ws.onopen = () => {
    document.getElementById("conn-status").textContent = "Live";
    document.getElementById("conn-status").style.color = "var(--green)";
  };
}

document.addEventListener("DOMContentLoaded", () => {
  initMap();
  connectWS();
  document.getElementById("btn-takeoff").addEventListener("click", () => sendCommand("takeoff", { altitude: 50 }));
  document.getElementById("btn-land").addEventListener("click", () => sendCommand("land"));
  document.getElementById("btn-hold").addEventListener("click", () => sendCommand("hold"));
  document.getElementById("btn-rtl").addEventListener("click", () => sendCommand("rtl"));
  document.getElementById("btn-goto").addEventListener("click", () => showGotoForm(true));
  document.getElementById("btn-goto-exec").addEventListener("click", executeGoto);
  document.getElementById("btn-goto-cancel").addEventListener("click", () => showGotoForm(false));
});
