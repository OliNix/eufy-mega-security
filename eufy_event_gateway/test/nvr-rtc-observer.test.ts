/**
 * Exercises the read-only NVR observer against local synthetic signalling and
 * real WebRTC peers. Fixtures own all bytes and sockets. No device, setting or
 * security action is contacted, and no physical NVR behaviour is claimed.
 */
import assert from 'node:assert/strict';
import { once } from 'node:events';
import test from 'node:test';
import { zstdCompressSync } from 'node:zlib';
import WebSocket, { WebSocketServer } from 'ws';
import { RTCPeerConnection, RTCSessionDescription, type RTCDataChannel } from 'werift';
import { NvrRtcSession, type NvrRtcOptions } from '../src/stream/nvr-rtc-session.js';
import { nvrApplication, nvrDeviceList, nvrHeartbeat, nvrPackets, NvrTrialError } from '../src/stream/nvr-protocol.js';
import { NvrPtcsReassembler } from '../src/stream/nvr-ptcs-reassembly.js';

const context = { host: 'security-smart-ie.eufylife.com', region: 'IE', authToken: 'synthetic-token', gtoken: 'synthetic-gtoken', sign: 'synthetic-sign' } as const;

function application(command: number, segment: number, body: Buffer): Buffer {
  const frame = Buffer.alloc(16 + body.length);
  frame.write('XZYH'); frame.writeUInt16LE(command, 4); frame.writeUInt32LE(body.length, 6);
  frame[11] = segment; frame[12] = 255; frame[15] = 2; body.copy(frame, 16);
  return frame;
}

/** Compressed receive-direction packets, as the NVR sends its broadcasts. */
function receivePackets(frame: Buffer, sequence: number, channel: number): Buffer[] {
  const wire = zstdCompressSync(frame), count = Math.ceil(wire.length / 1000);
  return Array.from({ length: count }, (_, index) => {
    const length = Math.min(1000, wire.length - index * 1000), packet = Buffer.alloc(28 + length);
    packet.write('PTCS'); packet[4] = 3; packet[5] = channel;
    packet.writeUInt16LE(sequence, 6); packet.writeUInt32LE(77, 8);
    packet.writeUInt32LE(wire.length, 12); packet.writeUInt16LE(index, 16);
    packet.writeUInt16LE(0x4800 | (index === count - 1 ? 0x0400 : 0) | length, 18);
    wire.copy(packet, 28, index * 1000, index * 1000 + length);
    return packet;
  });
}

const until = async (condition: () => boolean, milliseconds = 3000): Promise<void> => {
  const deadline = performance.now() + milliseconds;
  while (!condition() && performance.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
  assert.ok(condition());
};

/** One synthetic NVR: signalling, a real answering peer, and a recorder for every request. */
async function nvr(options: { closeOnJoin?: boolean; deviceList?: unknown } = {}) {
  const server = new WebSocketServer({ port: 0 });
  await once(server, 'listening');
  const port = (server.address() as { port: number }).port;
  const peer = new RTCPeerConnection({ iceServers: [], iceUseIpv6: false });
  peer.createDataChannel('synthetic-offer', { negotiated: true, id: 12 });
  await peer.setLocalDescription(await peer.createOffer());
  const commands: number[] = [], receiver = new NvrPtcsReassembler();
  let heartbeats = 0, replies = 0, socket: WebSocket | undefined, data: RTCDataChannel | undefined;
  const sequences = new Map<number, number>();
  const send = (frame: Buffer, transportChannel: number): void => {
    const sequence = sequences.get(transportChannel) ?? 0; sequences.set(transportChannel, sequence + 1);
    for (const packet of receivePackets(frame, sequence, transportChannel)) data!.send(packet);
  };
  peer.onDataChannel.subscribe(channel => {
    if (channel.label !== 'WebrtcDataChannel') return;
    data = channel;
    channel.onmessage = event => {
      if (Buffer.isBuffer(event.data) && event.data.equals(nvrHeartbeat())) { heartbeats++; return; }
      if (!Buffer.isBuffer(event.data) || event.data[4] !== 3) return;
      const assembled = receiver.push(event.data, Math.floor(performance.now()));
      if (assembled.status !== 'complete') return;
      const request = nvrApplication(assembled.frame), body = JSON.parse(request.payload.toString());
      commands.push(body.cmd);
      if (body.cmd !== 9100) return;
      const list = options.deviceList ?? { dev_list: [{ sn: 'synthetic-camera', ch: 3, dev_type: 301 }] };
      replies++;
      for (const packet of nvrPackets(application(1351, request.segment, Buffer.from(JSON.stringify({ cmd: 9100, payload: list }))), replies, 1)) channel.send(packet);
    };
  });
  server.on('connection', connection => {
    socket = connection;
    const reply = (value: Record<string, unknown>): void => { connection.send(JSON.stringify({ data: JSON.stringify(value) })); };
    connection.on('message', async raw => {
      const message = JSON.parse(JSON.parse(raw.toString()).data);
      if (message.action === 1) {
        if (options.closeOnJoin) { connection.close(); return; }
        reply({ action: 1, code: 200 });
      } else if (message.dataType === 'call') {
        reply({ action: 3, sessionId: 'synthetic-sign', sn: 'synthetic-station', channelId: 0, dataType: 'call', data: JSON.stringify({ status: 200 }) });
        reply({ action: 3, sessionId: 'synthetic-sign', sn: 'synthetic-station', channelId: 0, dataType: 'info', data: JSON.stringify({ format: 'SDP', value: peer.localDescription!.sdp }) });
      } else if (message.dataType === 'info') {
        const value = JSON.parse(message.data);
        if (value.sdp) await peer.setRemoteDescription(new RTCSessionDescription(value.sdp, 'answer'));
        else if (value.candidate) await peer.addIceCandidate({ candidate: value.candidate, sdpMid: '0', sdpMLineIndex: 0 });
      }
    });
  });
  return {
    commands, heartbeats: () => heartbeats,
    closeSignalling: () => socket!.close(),
    closeData: () => data!.close(),

    /** Send one 1151 frame; defaults are the observed broadcast form. */
    mode: (value: number, transportChannel = 2, response = 0, length = 8): void => {
      const frame = application(1151, 0, Buffer.alloc(length)); frame[14] = response; frame.writeInt32LE(value, 16);
      send(frame, transportChannel);
    },
    push: (body: Record<string, unknown>): void => send(application(1351, 0, Buffer.from(JSON.stringify(body))), 2),
    dependencies: {
      socket: (_url: string, protocols: string[], settings: WebSocket.ClientOptions) => new WebSocket(`ws://127.0.0.1:${port}`, protocols, settings),
      peer: () => new RTCPeerConnection({ iceServers: [], iceUseIpv6: false }),
    },
    close: async () => { await peer.close(); await new Promise<void>(resolve => server.close(() => resolve())); },
  };
}

function observer(extra: Partial<NvrRtcOptions> = {}, controller = new AbortController()) {
  const modes: ({ mode: number | null } | null)[] = [];
  const options: NvrRtcOptions = { observer: true, maximumLifetimeMilliseconds: null, onEffectiveMode: value => modes.push(value),
    station: 'synthetic-station', camera: '', channel: 0, administrator: 'synthetic-administrator', context,
    signal: controller.signal, current: () => true, ready: () => { throw new Error('An observer must never attach media'); }, ...extra };
  return { options, modes, controller };
}

test('an observer sends only heartbeats and one device list, and follows broadcasts through a signalling close', { timeout: 30000 }, async () => {
  const device = await nvr();
  const { options, modes } = observer();
  const session = new NvrRtcSession(options, device.dependencies);
  try {
    await session.observe();
    await until(() => device.heartbeats() >= 1);
    assert.deepEqual(modes, []);

    // A −106 query reply, a reply flag, a wrong transport channel and a JSON push are not broadcasts.
    device.mode(-106, 0, 1, 132);
    device.mode(1, 2, 1);
    device.mode(3, 0);
    device.push({ cmd: 61210, payload: { status: 0 } });
    device.mode(1);
    device.mode(47);
    device.mode(3);
    await until(() => modes.length >= 3);
    assert.deepEqual(modes, [{ mode: 1 }, { mode: null }, { mode: 3 }]);

    // Eufy's routine signalling close does not retire an established observer.
    device.closeSignalling();
    await until(() => session.stats.signallingClosuresWhileObserving === 1);
    device.mode(0);
    await until(() => modes.length >= 4);
    assert.deepEqual(modes.at(-1), { mode: 0 });
    assert.equal(session.stats.signallingClosuresAfterVideo, 0);
    assert.equal(session.output.destroyed, false);
    assert.equal(session.stats.video, 0);
    await session.close(); await session.close();
    assert.equal(await session.closed, null);
    assert.deepEqual(modes.at(-1), null);
    assert.equal(modes.filter(value => value === null).length, 1);
    assert.deepEqual(device.commands, [9100]);
  } finally { await session.close(); await device.close(); }
});

test('an observer data-channel drop settles closed with its failure and retires the reading', { timeout: 30000 }, async () => {
  const device = await nvr();
  const { options, modes } = observer();
  const session = new NvrRtcSession(options, device.dependencies);
  try {
    await session.observe();
    device.mode(4);
    await until(() => modes.length === 1);
    device.closeData();
    const failure = await session.closed;
    assert.ok(failure instanceof NvrTrialError);
    assert.deepEqual(modes, [{ mode: 4 }, null]);
    assert.deepEqual(device.commands, [9100]);
  } finally { await session.close(); await device.close(); }
});

test('an observer fails closed when signalling ends before its device list', { timeout: 30000 }, async () => {
  const device = await nvr({ closeOnJoin: true });
  const { options, modes } = observer();
  const session = new NvrRtcSession(options, device.dependencies);
  try {
    await assert.rejects(session.observe(), NvrTrialError);
    assert.ok(await session.closed instanceof NvrTrialError);
    assert.deepEqual(modes, []);
    assert.deepEqual(device.commands, []);
  } finally { await session.close(); await device.close(); }
});

test('an observer rejects a malformed device list', { timeout: 30000 }, async () => {
  const device = await nvr({ deviceList: { dev_list: [{ sn: 5, ch: 3 }] } });
  const { options } = observer();
  const session = new NvrRtcSession(options, device.dependencies);
  try {
    await assert.rejects(session.observe(), (error: NvrTrialError) => error.stage === 'identity_field_missing');
    assert.deepEqual(device.commands, [9100]);
  } finally { await session.close(); await device.close(); }
});

test('observer admission excludes media, queries, controls, trials and finite lifetimes', async () => {
  for (const extra of [{ maximumLifetimeMilliseconds: 60_000 }, { maximumLifetimeMilliseconds: undefined }, { liveControls: true }, { powerTrial: true }, { guardTrial: true }] as Partial<NvrRtcOptions>[]) {
    assert.throws(() => new NvrRtcSession(observer(extra).options), (error: NvrTrialError) => error.stage === 'admission');
  }
  const session = new NvrRtcSession(observer().options);
  await assert.rejects(session.start(), (error: NvrTrialError) => error.stage === 'admission');
  await assert.rejects(session.readState(), (error: NvrTrialError) => error.stage === 'admission');
  await assert.rejects(session.readLiveSettings());
  await assert.rejects(session.preparePowerTrial());
  await assert.rejects(session.prepareGuardTrial());
  await session.close();
  const normal = new NvrRtcSession({ ...observer().options, observer: false });
  await assert.rejects(normal.observe(), (error: NvrTrialError) => error.stage === 'admission');
  await normal.close();
});

test('device lists are bounded and need no camera binding', () => {
  assert.equal(nvrDeviceList({ payload: { dev_list: [] } }), 0);
  assert.equal(nvrDeviceList({ payload: { dev_list: [{ sn: 'a', ch: 1 }, { sn: 'b', ch: 2, dev_type: 301 }] } }), 2);
  for (const response of [{}, { payload: [] }, { payload: { dev_list: {} } }, { payload: { dev_list: [null] } },
    { payload: { dev_list: [{ sn: 'a', ch: 1.5 }] } }, { payload: { dev_list: Array.from({ length: 257 }, (_, ch) => ({ sn: 'a', ch })) } }]) {
    assert.throws(() => nvrDeviceList(response as Record<string, unknown>), (error: NvrTrialError) => error.stage === 'identity_field_missing');
  }
});
