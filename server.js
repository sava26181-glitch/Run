// Мото Стант — мультиплеер-сервер. Без зависимостей (чистый Node.js 18+).
// Запуск: node server.js   (порт из переменной PORT, по умолчанию 8080)
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 4;
const QUICK_WAIT = 15000;      // ожидание после 2-го игрока в быстрой игре
const COUNTDOWN = 3000;        // 3-2-1
const FINISH_WAIT = 45000;     // сколько ждать остальных после первого финиша
const RACE_LIMIT = 5 * 60000;  // максимум гонки
const LEVELS = 5;

// ---------- HTTP: веб-версия игры + health ----------
const indexPath = [path.join(__dirname, 'index.html'), path.join(__dirname, 'public', 'index.html')].find(p => fs.existsSync(p));
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, {'Content-Type': 'text/plain'}); return res.end('ok'); }
  if (req.url === '/stats') { res.writeHead(200, {'Content-Type': 'application/json'}); return res.end(JSON.stringify({players: clients.size, rooms: rooms.size})); }
  if (indexPath && (req.url === '/' || req.url.startsWith('/?') || req.url === '/index.html')) {
    res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache'});
    return fs.createReadStream(indexPath).pipe(res);
  }
  res.writeHead(404); res.end('Moto Stunt server');
});

// ---------- минимальный WebSocket ----------
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function wsSend(sock, str) {
  if (sock.destroyed) return;
  const data = Buffer.from(str);
  let head;
  if (data.length < 126) head = Buffer.from([0x81, data.length]);
  else if (data.length < 65536) { head = Buffer.alloc(4); head[0] = 0x81; head[1] = 126; head.writeUInt16BE(data.length, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x81; head[1] = 127; head.writeBigUInt64BE(BigInt(data.length), 2); }
  try { sock.write(Buffer.concat([head, data])); } catch (e) {}
}
function wsControl(sock, op, payload) { if (!sock.destroyed) try { sock.write(Buffer.concat([Buffer.from([0x80 | op, payload.length]), payload])); } catch (e) {} }

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = newClient(sock);
  let buf = Buffer.alloc(0), frag = [];
  sock.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const fin = buf[0] & 0x80, op = buf[0] & 0x0f, masked = buf[1] & 0x80;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) { sock.destroy(); return; }
      const mOff = off; if (masked) off += 4;
      if (buf.length < off + len) return;
      let payload = buf.slice(off, off + len);
      if (masked) { const m = buf.slice(mOff, mOff + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      buf = buf.slice(off + len);
      if (op === 8) { wsControl(sock, 8, Buffer.alloc(0)); sock.end(); return; }
      if (op === 9) { wsControl(sock, 10, payload.slice(0, 125)); continue; }
      if (op === 10) { c.alive = true; continue; }
      if (op === 1 || op === 0) {
        frag.push(payload);
        if (fin) { const msg = Buffer.concat(frag).toString(); frag = []; c.alive = true; try { onMessage(c, JSON.parse(msg)); } catch (e) {} }
      }
    }
  });
  const bye = () => dropClient(c);
  sock.on('close', bye); sock.on('error', bye); sock.on('end', bye);
});

// ---------- игровая логика ----------
const clients = new Map();   // id -> client
const rooms = new Map();     // code -> room
let nextId = 1;

function newClient(sock) {
  const c = {id: nextId++, sock, name: 'Гонщик', bike: 'moped', color: '#b8322a', room: null, alive: true};
  clients.set(c.id, c);
  send(c, {t: 'welcome', id: c.id});
  return c;
}
function send(c, obj) { wsSend(c.sock, JSON.stringify(obj)); }
function clean(s, n) { return String(s || '').replace(/[<>&"'`]/g, '').trim().slice(0, n); }

function makeCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code; do { code = Array.from({length: 4}, () => A[Math.floor(Math.random() * A.length)]).join(''); } while (rooms.has(code));
  return code;
}
function createRoom(quick) {
  const r = {code: makeCode(), quick, level: Math.floor(Math.random() * (quick ? 3 : LEVELS)), host: null,
             players: new Map(), state: 'wait', timer: null, startAt: 0, fin: new Map(), raceTimer: null, finTimer: null};
  rooms.set(r.code, r); return r;
}
function roomInfo(r) {
  return {t: 'room', code: r.code, quick: r.quick, level: r.level, host: r.host, state: r.state,
          startIn: r.startAt ? Math.max(0, r.startAt - Date.now()) : 0,
          players: [...r.players.values()].map(p => ({id: p.id, name: p.name, bike: p.bike, color: p.color}))};
}
function bcast(r, obj, exceptId) { const s = JSON.stringify(obj); for (const p of r.players.values()) if (p.id !== exceptId) wsSend(p.sock, s); }
function syncRoom(r) { bcast(r, roomInfo(r)); }

function joinRoom(c, r) {
  leaveRoom(c);
  if (r.players.size >= MAX_PLAYERS) return send(c, {t: 'err', msg: 'Комната заполнена'});
  if (r.state !== 'wait') return send(c, {t: 'err', msg: 'Гонка уже идёт'});
  r.players.set(c.id, c); c.room = r; c.x = 0;
  if (!r.host) r.host = c.id;
  if (r.quick) scheduleQuick(r);
  syncRoom(r);
}
function leaveRoom(c) {
  const r = c.room; if (!r) return;
  r.players.delete(c.id); c.room = null;
  if (r.players.size === 0) { killRoom(r); return; }
  if (r.host === c.id) r.host = r.players.keys().next().value;
  bcast(r, {t: 'left', id: c.id});
  if (r.state === 'wait' && r.quick) scheduleQuick(r);
  if (r.state === 'race') checkAllFinished(r);
  syncRoom(r);
}
function killRoom(r) { clearTimeout(r.timer); clearTimeout(r.raceTimer); clearTimeout(r.finTimer); rooms.delete(r.code); }
function dropClient(c) { if (!clients.has(c.id)) return; clients.delete(c.id); leaveRoom(c); try { c.sock.destroy(); } catch (e) {} }

function scheduleQuick(r) {
  clearTimeout(r.timer); r.timer = null; r.startAt = 0;
  if (r.players.size >= MAX_PLAYERS) { r.startAt = Date.now() + 3000; r.timer = setTimeout(() => go(r), 3000); }
  else if (r.players.size >= 2) { r.startAt = Date.now() + QUICK_WAIT; r.timer = setTimeout(() => go(r), QUICK_WAIT); }
}
function go(r) {
  if (r.state !== 'wait' || r.players.size === 0) return;
  clearTimeout(r.timer); r.state = 'race'; r.startAt = 0; r.fin = new Map();
  for (const p of r.players.values()) p.x = 0;
  r.seed = Date.now() & 0xffff;
  bcast(r, {t: 'go', level: r.level, countdown: COUNTDOWN});
  syncRoom(r);
  r.raceTimer = setTimeout(() => results(r), RACE_LIMIT + COUNTDOWN);
}
function checkAllFinished(r) {
  if (r.state !== 'race') return;
  if ([...r.players.keys()].every(id => r.fin.has(id))) results(r);
}
function results(r) {
  if (r.state !== 'race') return;
  clearTimeout(r.raceTimer); clearTimeout(r.finTimer); r.finTimer = null;
  const list = [...r.players.values()].map(p => ({id: p.id, name: p.name, time: r.fin.has(p.id) ? r.fin.get(p.id) : null, x: p.x || 0}));
  list.sort((a, b) => (a.time === null) - (b.time === null) || (a.time !== null ? a.time - b.time : b.x - a.x));
  list.forEach((e, i) => e.place = i + 1);
  bcast(r, {t: 'results', list, count: list.length});
  r.state = 'wait'; r.level = Math.floor(Math.random() * (r.quick ? 3 : LEVELS));
  if (r.quick) scheduleQuick(r);
  syncRoom(r);
}

function onMessage(c, m) {
  if (!m || typeof m.t !== 'string') return;
  const r = c.room;
  switch (m.t) {
    case 'hello':
      c.name = clean(m.name, 16) || 'Гонщик';
      c.bike = ['moped', 'dirt', 'enduro'].includes(m.bike) ? m.bike : 'moped';
      c.color = /^#[0-9a-fA-F]{6}$/.test(m.color) ? m.color : '#b8322a';
      if (r) syncRoom(r);
      break;
    case 'quick': {
      let room = [...rooms.values()].find(x => x.quick && x.state === 'wait' && x.players.size < MAX_PLAYERS && x !== r);
      if (!room) room = createRoom(true);
      joinRoom(c, room); break;
    }
    case 'create': joinRoom(c, createRoom(false)); break;
    case 'join': {
      const room = rooms.get(clean(m.code, 4).toUpperCase());
      if (!room) send(c, {t: 'err', msg: 'Комната не найдена'}); else joinRoom(c, room);
      break;
    }
    case 'leave': leaveRoom(c); send(c, {t: 'lobbyLeft'}); break;
    case 'level':
      if (r && !r.quick && r.host === c.id && r.state === 'wait') { r.level = Math.max(0, Math.min(LEVELS - 1, m.level | 0)); syncRoom(r); }
      break;
    case 'start':
      if (r && !r.quick && r.host === c.id && r.state === 'wait') go(r);
      break;
    case 'st':
      if (r && r.state === 'race' && Array.isArray(m.s) && m.s.length <= 12) {
        c.x = +m.s[0] || 0;
        bcast(r, {t: 'st', id: c.id, s: m.s}, c.id);
      }
      break;
    case 'fin':
      if (r && r.state === 'race' && !r.fin.has(c.id)) {
        const time = Math.max(1, +m.time || 0);
        r.fin.set(c.id, time);
        bcast(r, {t: 'fin', id: c.id, time});
        if (!r.finTimer) r.finTimer = setTimeout(() => results(r), FINISH_WAIT);
        checkAllFinished(r);
      }
      break;
    case 'ping': send(c, {t: 'pong', ts: m.ts}); break;
  }
}

// пинг, чтобы соединения не засыпали и мёртвые отваливались
setInterval(() => {
  for (const c of clients.values()) {
    if (!c.alive) { dropClient(c); continue; }
    c.alive = false; wsControl(c.sock, 9, Buffer.alloc(0));
  }
}, 25000);

server.listen(PORT, () => console.log('Moto Stunt server on port ' + PORT));
