// Pati Vadisi oda sunucusu (relay)
// Oyuncular oda koduyla aynı odaya girer, sunucu sadece mesajları iletir.
// Dünyayı odayı kuranın telefonu çalıştırır; sunucu hafif kalır.
'use strict';
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8080;
const MAX_PLAYERS = +process.env.MAX_PLAYERS || 2;   // oda başına oyuncu
const GRACE_MS = +process.env.GRACE_MS || 20000;     // kopan oyuncunun yeri bu kadar tutulur
const MAX_ROOMS = +process.env.MAX_ROOMS || 2000;
const ROOM_RE = /^[a-z0-9][a-z0-9_.-]{1,47}$/;

const rooms = new Map(); // oda adı -> Map(oyuncuId -> {ws, p, timer})
const stats = { joins: 0, started: Date.now() };

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    let players = 0; for (const r of rooms.values()) players += r.size;
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, players, joins: stats.joins, uptime: Math.round((Date.now() - stats.started) / 1000) }));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Pati Vadisi sunucusu çalışıyor');
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

function send(ws, msg) { if (ws && ws.readyState === 1) ws.send(typeof msg === 'string' ? msg : JSON.stringify(msg)); }
function bcast(room, from, msg) { const s = JSON.stringify(msg); for (const [id, c] of room) if (id !== from) send(c.ws, s); }

wss.on('connection', (ws) => {
  let room = null, name = null, id = null;
  let tokens = 40, last = Date.now();
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  function removeMe(immediate) {
    if (!room) return;
    const r = room, n = name, pid = id, c = r.get(pid);
    room = null;
    if (!c) return;
    const done = () => { if (r.get(pid) !== c) return; r.delete(pid); bcast(r, pid, { t: 'left', id: pid }); if (r.size === 0) rooms.delete(n); };
    clearTimeout(c.timer);
    if (immediate) done();
    else { c.ws = null; c.timer = setTimeout(done, GRACE_MS); }
  }

  ws.on('message', (raw) => {
    // basit hız sınırı: saniyede ~25 mesaj
    const now = Date.now(); tokens = Math.min(40, tokens + (now - last) / 1000 * 25); last = now;
    if (tokens < 1) return; tokens--;
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'join') {
      if (room) return;
      if (typeof m.room !== 'string' || !ROOM_RE.test(m.room) || typeof m.id !== 'string' || !/^[a-z0-9]{4,40}$/.test(m.id)) { ws.close(); return; }
      let r = rooms.get(m.room);
      if (!r) { if (rooms.size >= MAX_ROOMS) { send(ws, { t: 'busy' }); ws.close(); return; } r = new Map(); rooms.set(m.room, r); }
      const existing = r.get(m.id);
      if (existing) { // aynı oyuncu geri döndü (yeniden bağlanma)
        clearTimeout(existing.timer);
        if (existing.ws && existing.ws !== ws) { try { existing.ws.terminate(); } catch {} }
        existing.ws = ws;
      } else {
        if (r.size >= MAX_PLAYERS) { send(ws, { t: 'full', max: MAX_PLAYERS }); if (r.size === 0) rooms.delete(m.room); ws.close(); return; }
        r.set(m.id, { ws, p: {}, timer: null });
        stats.joins++;
      }
      room = r; name = m.room; id = m.id;
      const peers = []; for (const [pid, c] of r) if (pid !== id) peers.push({ id: pid, p: c.p, on: !!c.ws });
      send(ws, { t: 'joined', peers, max: MAX_PLAYERS });
      return;
    }
    if (!room) return;
    if (m.t === 'p') {
      const c = room.get(id); if (!c || c.ws !== ws) return;
      c.p = (m.p && typeof m.p === 'object') ? m.p : {};
      bcast(room, id, { t: 'p', id, p: c.p });
    } else if (m.t === 'leave') {
      removeMe(true);
    }
  });

  ws.on('close', () => { if (room) { const c = room.get(id); if (c && c.ws === ws) removeMe(false); } });
  ws.on('error', () => {});
});

const beat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; try { ws.ping(); } catch {}
  }
}, 15000);
wss.on('close', () => clearInterval(beat));

server.listen(PORT, () => console.log('Pati Vadisi sunucusu port ' + PORT + ' üzerinde, oda başına ' + MAX_PLAYERS + ' oyuncu'));
