'use strict';

const http = require('http');
const net = require('net');
const dgram = require('dgram');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const UUID_HEX = String(process.env.UUID || '').replace(/-/g, '').toLowerCase();
const UUID_BUF = UUID_HEX.length === 32 ? Buffer.from(UUID_HEX, 'hex') : null;
const RAW_WSPATH = String(process.env.WSPATH || '');
const WS_PATH = RAW_WSPATH.startsWith('/') ? RAW_WSPATH : '/' + RAW_WSPATH;

function closeBoth(ws, sock) {
  try { ws.close(); } catch (_) {}
  try { sock.destroy(); } catch (_) {}
}

// Parse a VLESS address starting at offset. Returns {host, port, addrLen, next} or null.
function parseAddress(buf, offset, port) {
  const addrType = buf[offset];
  if (addrType === 1) {
    if (buf.length < offset + 5) return null;
    const host = Array.from(buf.subarray(offset + 1, offset + 5)).join('.');
    return { host, next: offset + 5 };
  }
  if (addrType === 2) {
    if (buf.length < offset + 2) return null;
    const len = buf[offset + 1];
    if (buf.length < offset + 2 + len) return null;
    const host = buf.toString('utf8', offset + 2, offset + 2 + len);
    return { host, next: offset + 2 + len };
  }
  if (addrType === 3) {
    if (buf.length < offset + 17) return null;
    const parts = [];
    for (let i = 0; i < 16; i += 2) {
      parts.push(buf.readUInt16BE(offset + 1 + i).toString(16));
    }
    return { host: parts.join(':'), next: offset + 17 };
  }
  return null;
}

function encodeAddress(host) {
  // Return {type, buf} for a host string (IPv4, IPv6, or domain).
  if (net.isIPv4(host)) {
    const parts = host.split('.').map(Number);
    return { type: 1, buf: Buffer.from(parts) };
  }
  if (net.isIPv6(host)) {
    // Expand IPv6 via a simple parse: use dgram/net helpers by splitting.
    // Node's net does not expose a parser, so handle full and :: forms manually.
    let groups = [];
    if (host.includes('::')) {
      const [left, right] = host.split('::');
      const l = left ? left.split(':') : [];
      const r = right ? right.split(':') : [];
      const missing = 8 - l.length - r.length;
      groups = [...l, ...Array(missing).fill('0'), ...r];
    } else {
      groups = host.split(':');
    }
    const buf = Buffer.alloc(16);
    groups.slice(0, 8).forEach((g, i) => buf.writeUInt16BE(parseInt(g, 16) || 0, i * 2));
    return { type: 3, buf };
  }
  const domain = Buffer.from(host, 'utf8');
  return { type: 2, buf: Buffer.concat([Buffer.from([domain.length]), domain]) };
}

function handleTcp(ws, targetHost, targetPort, firstPayload) {
  let headerSent = false;
  const sendToClient = (data) => {
    if (!headerSent) {
      headerSent = true;
      const head = Buffer.from([0, 0]);
      ws.send(data && data.length ? Buffer.concat([head, data]) : head, { binary: true });
    } else if (data && data.length) {
      ws.send(data, { binary: true });
    }
  };

  const sock = net.createConnection({ host: targetHost, port: targetPort }, () => {
    // Send the VLESS response header to the client, and forward the
    // client's first payload to the target (opposite directions).
    sendToClient(null);
    if (firstPayload && firstPayload.length) {
      try { sock.write(firstPayload); } catch (_) {}
    }
  });
  sock.on('data', (data) => {
    try { sendToClient(data); } catch (_) {}
  });
  sock.on('error', () => closeBoth(ws, sock));
  sock.on('close', () => { try { ws.close(); } catch (_) {} });
  ws.on('message', (data) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    try { sock.write(buf); } catch (_) {}
  });
  ws.on('close', () => { try { sock.destroy(); } catch (_) {} });
  ws.on('error', () => { try { sock.destroy(); } catch (_) {} });
  // If the socket fails to connect, net 'error' above closes both sides.
}

function handleUdp(ws, targetHost, targetPort, firstPayload) {
  // Datagram format used by this server (and its test client):
  //   port(2 BE) + addrType(1) + address + dataLen(2 BE) + data, repeated.
  // The initial VLESS header already selected the first target; subsequent
  // datagrams in the same WS stream may each carry their own target.
  let headerSent = false;
  const udp = dgram.createSocket('udp4');
  let closed = false;
  const cleanup = () => {
    if (closed) return;
    closed = true;
    try { udp.close(); } catch (_) {}
    try { ws.close(); } catch (_) {}
  };
  udp.on('message', (msg, rinfo) => {
    // Reply datagram addressed to the original target host/port.
    const enc = encodeAddress(targetHost);
    const lenBuf = Buffer.alloc(2);
    lenBuf.writeUInt16BE(msg.length, 0);
    const portBuf = Buffer.alloc(2);
    portBuf.writeUInt16BE(targetPort, 0);
    const datagram = Buffer.concat([portBuf, Buffer.from([enc.type]), enc.buf, lenBuf, msg]);
    try {
      if (!headerSent) {
        headerSent = true;
        ws.send(Buffer.concat([Buffer.from([0, 0]), datagram]), { binary: true });
      } else {
        ws.send(datagram, { binary: true });
      }
    } catch (_) { cleanup(); }
  });
  udp.on('error', cleanup);
  ws.on('close', cleanup);
  ws.on('error', cleanup);

  const handleDatagrams = (buf) => {
    let off = 0;
    while (off + 5 <= buf.length) {
      const port = buf.readUInt16BE(off);
      const parsed = parseAddress(buf, off + 2, port);
      if (!parsed) break;
      const lenOff = parsed.next;
      if (buf.length < lenOff + 2) break;
      const dataLen = buf.readUInt16BE(lenOff);
      const dataStart = lenOff + 2;
      if (buf.length < dataStart + dataLen) break;
      const data = buf.subarray(dataStart, dataStart + dataLen);
      // Callback swallows per-datagram send errors so one failed datagram
      // does not tear down the whole WS connection.
      try { udp.send(data, port, parsed.host, () => {}); } catch (_) {}
      off = dataStart + dataLen;
    }
  };

  if (firstPayload && firstPayload.length) handleDatagrams(firstPayload);
  ws.on('message', (data) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    handleDatagrams(buf);
  });
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET' && req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('not found');
  }
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  let pathname = '';
  try { pathname = new URL(req.url, 'http://localhost').pathname; } catch (_) { pathname = ''; }
  if (pathname !== WS_PATH) {
    try {
      socket.write('HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n');
      socket.destroy();
    } catch (_) {}
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    let headerParsed = false;
    ws.on('message', (data) => {
      if (headerParsed) return; // TCP/UDP handlers attach their own listeners below.
      headerParsed = true;
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      // VLESS header: version(1) + uuid(16) + addonLen(1) + addon + command(1) + port(2) + addrType + address
      if (!UUID_BUF || buf.length < 22) { try { ws.close(); } catch (_) {} return; }
      const version = buf[0];
      if (version !== 0) { try { ws.close(); } catch (_) {} return; }
      if (!buf.subarray(1, 17).equals(UUID_BUF)) { try { ws.close(); } catch (_) {} return; }
      const addonLen = buf[17];
      // Layout: version(0), uuid(1..16), addonLen(17), addon(18..17+addonLen),
      // command, port(2), addrType + address, payload.
      const cmdOff = 18 + addonLen;
      if (buf.length < cmdOff + 4) { try { ws.close(); } catch (_) {} return; }
      const command = buf[cmdOff];
      const port = buf.readUInt16BE(cmdOff + 1);
      const parsed = parseAddress(buf, cmdOff + 3, port);
      if (!parsed) { try { ws.close(); } catch (_) {} return; }
      const payloadStart = parsed.next;
      const firstPayload = buf.length > payloadStart ? buf.subarray(payloadStart) : Buffer.alloc(0);
      // Remove this one-shot listener's effect: subsequent messages must go to the
      // handler, not back here. We achieve this by detaching this listener and
      // letting the handler attach its own; the current message is already consumed.
      ws.removeAllListeners('message');
      if (command === 1) {
        handleTcp(ws, parsed.host, port, firstPayload);
      } else if (command === 2) {
        handleUdp(ws, parsed.host, port, firstPayload);
      } else {
        try { ws.close(); } catch (_) {}
      }
      // Re-emit nothing: firstPayload was already passed to the handler.
      // Subsequent WS messages are handled by the handler's own listener.
    });
    ws.on('error', () => { try { ws.close(); } catch (_) {} });
  });
});

server.listen(PORT, () => {
  console.log('vless-ws listening');
});
