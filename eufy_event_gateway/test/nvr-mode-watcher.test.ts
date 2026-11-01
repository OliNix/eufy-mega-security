/**
 * Tests the read-only NVR watcher's attempt loop with synthetic sessions.
 * No socket, peer, credential or device is used, and waits are injected.
 */
import assert from "node:assert/strict";
import test from "node:test";
import { NvrModeWatcher, NVR_MODE_WATCH_BACKOFF_MILLISECONDS } from "../src/provider/nvr-mode-watcher.js";
import { NvrTrialError } from "../src/stream/nvr-protocol.js";
import type { NvrRtcSession } from "../src/stream/nvr-rtc-session.js";

const tick = () => new Promise<void>(resolve => setImmediate(resolve));

/** A synthetic observer that settles `closed` when told to drop or when closed. */
function fakeSession(options: { observe?: () => Promise<void> } = {}) {
  let settle!: (value: NvrTrialError | null) => void;
  const closed = new Promise<NvrTrialError | null>(resolve => { settle = resolve; });
  let closes = 0, observed = 0;
  const session = {
    stats: { frames: 0, channel2Frames: 0, staleReplies: 0, rejected: 0, signallingClosuresWhileObserving: 0, video: 0 },
    observe: options.observe ?? (async () => { observed++; }),
    closed,
    close: async () => { closes++; settle(null); },
  };
  return { session: session as unknown as NvrRtcSession, drop: (error = new NvrTrialError("channel")) => settle(error),
    closes: () => closes, observed: () => observed };
}

/** A watcher whose waits are recorded and released by the test. */
function harness(current = () => true) {
  const sessions: ReturnType<typeof fakeSession>[] = [], waits: number[] = [], signals: AbortSignal[] = [];
  let clock = 0, releaseWait: (() => void) | null = null, connectFailure: Error | null = null;
  const watcher = new NvrModeWatcher({ model: "T8N00", current, now: () => clock,
    connect: async signal => {
      signals.push(signal);
      if (connectFailure) throw connectFailure;
      const value = fakeSession(); sessions.push(value); return value.session;
    },
    wait: (milliseconds, signal) => new Promise<void>((resolve, reject) => {
      waits.push(milliseconds);
      releaseWait = resolve;
      signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    }) });
  return { watcher, sessions, waits, signals,
    advance: (milliseconds: number) => { clock += milliseconds; },
    release: async () => { const release = releaseWait; releaseWait = null; release?.(); await tick(); await tick(); },
    failConnect: (error: Error | null) => { connectFailure = error; } };
}

test("each attempt gets a fresh connection, and back-off is bounded and resets after a stable connection", async () => {
  const value = harness();
  value.watcher.start(); value.watcher.start();
  await tick(); await tick();
  assert.equal(value.sessions.length, 1);
  assert.equal(value.sessions[0]!.observed(), 1);
  assert.equal(value.watcher.observing, true);

  // Short-lived connections climb the ladder.
  for (let index = 0; index < 8; index++) {
    value.advance(1000);
    value.sessions.at(-1)!.drop();
    await tick(); await tick();
    assert.equal(value.watcher.observing, false);
    assert.equal(value.sessions.at(-1)!.closes() >= 1, true);
    await value.release();
  }
  assert.deepEqual(value.waits, [...NVR_MODE_WATCH_BACKOFF_MILLISECONDS, 300_000, 300_000]);
  assert.equal(value.sessions.length, 9);
  assert.equal(new Set(value.sessions.map(entry => entry.session)).size, 9);

  // A connection that stayed up restarts the ladder.
  value.advance(60_000);
  value.sessions.at(-1)!.drop();
  await tick(); await tick();
  assert.equal(value.waits.at(-1), NVR_MODE_WATCH_BACKOFF_MILLISECONDS[0]);
  assert.equal(value.watcher.stats.drops, 9);
  await value.watcher.close();
});

test("setup failures back off without a connection and close stops a wait", async () => {
  const value = harness();
  value.failConnect(new NvrTrialError("region_or_authorization_rejected"));
  value.watcher.start();
  await tick(); await tick();
  assert.deepEqual(value.waits, [5_000]);
  await value.release();
  assert.deepEqual(value.waits, [5_000, 15_000]);
  assert.equal(value.watcher.stats.connections, 0);
  await value.watcher.close();
  assert.equal(value.signals.every(signal => signal.aborted), true);
  await value.release();
  assert.equal(value.watcher.stats.attempts, 2);
});

test("close retires an established session and nothing restarts", async () => {
  const value = harness();
  value.watcher.start();
  await tick(); await tick();
  await value.watcher.close(); await value.watcher.close();
  assert.equal(value.sessions[0]!.closes() >= 1, true);
  assert.deepEqual(value.waits, []);
  value.watcher.start();
  await tick();
  assert.equal(value.sessions.length, 1);
  assert.equal(value.watcher.observing, false);
});

test("lost admission ends the loop after a drop instead of reconnecting", async () => {
  let admitted = true;
  const value = harness(() => admitted);
  value.watcher.start();
  await tick(); await tick();
  admitted = false;
  value.sessions[0]!.drop(new NvrTrialError("cancelled"));
  await tick(); await tick();
  assert.deepEqual(value.waits, []);
  assert.equal(value.sessions.length, 1);
  await value.watcher.close();
});

test("an inadmissible watcher never connects", async () => {
  const value = harness(() => false);
  value.watcher.start();
  await tick();
  assert.equal(value.signals.length, 0);
  await value.watcher.close();
});

test("close during a pending observe retires that session", async () => {
  let resolveObserve!: () => void;
  const pending = fakeSession({ observe: () => new Promise<void>(resolve => { resolveObserve = resolve; }) });
  const watcher = new NvrModeWatcher({ model: "T8N00", current: () => true, connect: async () => pending.session,
    wait: async () => { throw new Error("no retry expected"); } });
  watcher.start();
  await tick(); await tick();
  const closing = watcher.close();
  resolveObserve();
  await closing;
  assert.equal(pending.closes() >= 1, true);
  assert.equal(watcher.stats.connections <= 1, true);
});
