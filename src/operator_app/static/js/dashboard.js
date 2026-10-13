const API = "";
let selectedId = null;
let assets = {};
let tracks = {};
let map = null;
let assetMarkers = {};
let trackMarkers = {};
let zoneLayers = {};
let ws = null;
let gotoMode = false;
let mapFitted = false;

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

function finite(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function upsertAssetMarker(a) {
  const lat = finite(a.lat);
  const lon = finite(a.lon);
  if (lat === null || lon === null) return;
  const isUav = a.asset_type === "uav";
  const name = String(a.name || a.id || "");
  const label = name.replace(/[^0-9]/g, "") || name.slice(0, 2);
  const selected = a.id === selectedId ? " selected" : "";
  const icon = L.divIcon({
    className: "marker-label",
    html: `<div class="marker-inner ${isUav ? "uav" : "ugv"}${selected}">${label}</div>`,
    iconSize: [28, 28], iconAnchor: [14, 14],
  });
  if (assetMarkers[a.id]) {
    assetMarkers[a.id].setLatLng([lat, lon]);
    assetMarkers[a.id].setIcon(icon);
  } else {
    assetMarkers[a.id] = L.marker([lat, lon], { icon }).addTo(map);
    assetMarkers[a.id].on("click", () => selectAsset(a.id));
  }
}

function upsertTrackMarker(t) {
  const lat = finite(t.lat);
  const lon = finite(t.lon);
  if (lat === null || lon === null) return;
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
    ">${(t.label || "").replace(/[^0-9A-Z]/gi,'').slice(0,3) || "?"}</div>`,
    iconSize: [34, 34], iconAnchor: [17, 17],
  });

  if (trackMarkers[t.id]) {
    trackMarkers[t.id].setLatLng([lat, lon]);
    trackMarkers[t.id].setIcon(icon);
  } else {
    trackMarkers[t.id] = L.marker([lat, lon], { icon, zIndexOffset: -100 }).addTo(map);
  }
}

function renderAssets() {
  const list = document.getElementById("asset-list");
  const order = Object.values(assets).sort((x, y) => x.name.localeCompare(y.name));
  list.innerHTML = order.map((a) => {
    const battery = finite(a.battery) === null ? 0 : finite(a.battery);
    const battClass = battery > 30 ? "ok" : battery > 15 ? "low" : "crit";
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
          <div>Lat <strong>${finite(a.lat) === null ? "–" : finite(a.lat).toFixed(5)}</strong></div>
          <div>Lon <strong>${finite(a.lon) === null ? "–" : finite(a.lon).toFixed(5)}</strong></div>
          <div>Alt <strong>${finite(a.alt) === null ? "–" : finite(a.alt).toFixed(1)} m</strong></div>
          <div>Hdg <strong>${finite(a.heading) === null ? "–" : finite(a.heading).toFixed(0)}°</strong></div>
          <div>Spd <strong>${finite(a.speed) === null ? "–" : finite(a.speed).toFixed(1)} m/s</strong></div>
          <div>Batt <strong>${finite(a.battery) === null ? "–" : finite(a.battery).toFixed(0)}%</strong></div>
        </div>
        <div class="battery-bar"><div class="fill ${battClass}" style="width:${Math.min(100, battery)}%"></div></div>
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
    if (btn.classList.contains("task-btn")) return;
    const needsUav = btn.dataset.uavOnly === "1";
    btn.disabled = !a || (needsUav && !isUav);
  });
  renderAssets();
  Object.values(assets).forEach(upsertAssetMarker);
}

async function sendCommand(type, params = {}) {
  if (!selectedId) return null;
  try {
    const res = await fetch(`${API}/assets/${selectedId}/command`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command_type: type, parameters: params }),
    });
    const body = await res.json();
    if (!res.ok || body.status === "failed") {
      const reason = (body.result && body.result.error) || body.detail || "command failed";
      document.getElementById("selected-info").textContent = `Rejected: ${reason}`;
    }
    return body;
  } catch (err) {
    console.error("Command failed", err);
    return null;
  }
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

function zoneStyle(kind) {
  if (kind === "keep-out") return { color: "#ef4444", weight: 2, fillOpacity: 0.15 };
  if (kind === "keep-in") return { color: "#22c55e", weight: 2, fillOpacity: 0.08 };
  return { color: "#3b82f6", weight: 2, fillOpacity: 0.08 };
}

function renderZones(zones) {
  const seen = new Set();
  (zones || []).forEach((zone) => {
    const ring = zone.ring || [];
    if (ring.length < 3) return;
    seen.add(zone.id);
    if (zoneLayers[zone.id]) {
      zoneLayers[zone.id].setLatLngs(ring);
      return;
    }
    zoneLayers[zone.id] = L.polygon(ring, zoneStyle(zone.kind)).addTo(map);
    zoneLayers[zone.id].bindTooltip(`${zone.kind}: ${zone.name || zone.id}`);
  });
  Object.keys(zoneLayers).forEach((id) => {
    if (!seen.has(id)) {
      map.removeLayer(zoneLayers[id]);
      delete zoneLayers[id];
    }
  });
}

function renderMissions(missions) {
  const el = document.getElementById("mission-list");
  if (!missions || missions.length === 0) {
    el.innerHTML = `<div class="empty">No missions</div>`;
    return;
  }
  el.innerHTML = missions.map((mission) => {
    const pct = Math.round(Number(mission.progress || 0) * 100);
    return `
      <div class="mission-item">
        <div class="title">${mission.name}</div>
        <div class="msg">${mission.status} · ${pct}%</div>
        <div class="progress-bar"><div class="fill" style="width:${pct}%"></div></div>
      </div>`;
  }).join("");
}

function renderCommands(commands) {
  const el = document.getElementById("command-list");
  if (!commands || commands.length === 0) {
    el.innerHTML = `<div class="empty">No commands</div>`;
    return;
  }
  el.innerHTML = commands.slice(0, 8).map((command) => {
    const phase = command.phase || command.status || "pending";
    return `
      <div class="command-item">
        <span class="phase ${phase}">${phase}</span>
        <span>${command.command_type}</span>
        <span class="asset">${command.asset_id || ""}</span>
      </div>`;
  }).join("");
}

function renderDetectionTracks(tracks) {
  const el = document.getElementById("track-list");
  const rows = (tracks || []).filter((track) => String(track.id || "").startsWith("det-"));
  if (rows.length === 0) {
    el.innerHTML = `<div class="empty">No detection tracks</div>`;
    return;
  }
  el.innerHTML = rows.map((track) => `
    <div class="track-item">
      <div class="title">${track.label || track.id}</div>
      <div class="msg">${Number(track.lat).toFixed(5)}, ${Number(track.lon).toFixed(5)}</div>
      <button class="cmd-btn task-btn" data-track="${track.id}">Task nearest UGV</button>
    </div>`).join("");
  el.querySelectorAll(".task-btn").forEach((btn) => {
    btn.addEventListener("click", () => taskNearestUgv(btn.dataset.track));
  });
}

async function taskNearestUgv(trackId) {
  try {
    const res = await fetch(`${API}/tracks/${encodeURIComponent(trackId)}/task-nearest-ugv`, { method: "POST" });
    const body = await res.json();
    if (!res.ok || body.status === "failed") {
      const reason = (body.result && body.result.error) || body.detail || "task failed";
      document.getElementById("selected-info").textContent = `Rejected: ${reason}`;
    }
  } catch (err) {
    console.error("Task failed", err);
  }
}

async function markDetection() {
  if (!selectedId) return;
  try {
    await fetch(`${API}/assets/${encodeURIComponent(selectedId)}/detect`, { method: "POST" });
  } catch (err) {
    console.error("Detection failed", err);
  }
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

function mergeById(current, incoming) {
  const next = Object.assign({}, current);
  (incoming || []).forEach((item) => {
    if (!item || !item.id) return;
    next[item.id] = Object.assign({}, next[item.id] || {}, item);
  });
  return next;
}

function dropMarkers(markers, keep) {
  Object.keys(markers).forEach((id) => {
    if (keep[id]) return;
    if (map) map.removeLayer(markers[id]);
    delete markers[id];
  });
}

function connectWS() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws/cop`);
  ws.onmessage = (ev) => {
    const snap = JSON.parse(ev.data);
    const isDelta = snap.type === "delta";
    if (snap.timestamp) {
      document.getElementById("clock").textContent = new Date(snap.timestamp).toLocaleTimeString();
    }

    if (snap.assets) {
      if (isDelta) {
        assets = mergeById(assets, snap.assets);
        snap.assets.forEach((a) => upsertAssetMarker(a));
      } else {
        const nextAssets = {};
        snap.assets.forEach((a) => {
          nextAssets[a.id] = a;
          upsertAssetMarker(a);
        });
        dropMarkers(assetMarkers, nextAssets);
        assets = nextAssets;
      }
      if (!mapFitted && map && Object.keys(assets).length > 0) {
        const pts = Object.values(assets)
          .filter((a) => finite(a.lat) !== null && finite(a.lon) !== null)
          .map((a) => [a.lat, a.lon]);
        if (pts.length > 0) {
          map.fitBounds(pts, { padding: [40, 40], maxZoom: 15 });
          mapFitted = true;
        }
      }
      renderAssets();
      if (selectedId && !assets[selectedId]) selectAsset(null);
      else if (selectedId) selectAsset(selectedId);
    }

    if (snap.tracks) {
      if (isDelta) {
        tracks = mergeById(tracks, snap.tracks);
        snap.tracks.forEach((t) => upsertTrackMarker(t));
      } else {
        const nextTracks = {};
        snap.tracks.forEach((t) => {
          nextTracks[t.id] = t;
          upsertTrackMarker(t);
        });
        dropMarkers(trackMarkers, nextTracks);
        tracks = nextTracks;
      }
      renderDetectionTracks(Object.values(tracks));
    }
    if (snap.alerts) renderAlerts(snap.alerts);
    if (snap.missions) renderMissions(snap.missions);
    if (snap.recent_commands) renderCommands(snap.recent_commands);
    if (snap.zones) renderZones(snap.zones);
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
  document.getElementById("btn-detect").addEventListener("click", markDetection);
  document.getElementById("btn-goto-exec").addEventListener("click", executeGoto);
  document.getElementById("btn-goto-cancel").addEventListener("click", () => showGotoForm(false));
});
