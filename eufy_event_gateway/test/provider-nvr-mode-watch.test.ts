/**
 * Exercises the standing read-only NVR observer through the real provider.
 * Fixtures own fake cloud, push and session effects, capture the inventory
 * refresh timer, and drive mode observations through each session's listener.
 * No socket, peer, device or security action is contacted.
 */
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import test, { type TestContext } from "node:test";
import { observeLogEvents } from "../src/logging.js";
import { MegaClient } from "../src/mega/client.js";
import { MegaPushReceiver } from "../src/mega/push.js";
import { EufyProvider } from "../src/provider/eufy-provider.js";
import type { ProviderEvents } from "../src/provider/provider.js";
import { NvrRtcSession, type NvrRtcOptions } from "../src/stream/nvr-rtc-session.js";

const camera = "synthetic-camera", station = "synthetic-station";
const tick = async (count = 4) => { for (let index = 0; index < count; index++) await new Promise<void>(resolve => setImmediate(resolve)); };
const options = (session: NvrRtcSession) => Reflect.get(session, "options") as NvrRtcOptions;

async function fixture(context: TestContext, settings: { watch?: boolean; live?: boolean; parent?: "T8N00" | "T9000" } = {}) {
  const { watch = true, live = false, parent = "T8N00" } = settings;
  const directory = await mkdtemp(join(tmpdir(), "nvr-mode-watch-"));
  const network = context.mock.method(globalThis, "fetch", async () => { throw Error("Network forbidden"); });
  let guardMode = "2", inventoryFailure: Error | null = null;
  context.mock.method(MegaClient.prototype, "connect", async () => ({ state: "authenticated" }));
  context.mock.getter(MegaClient.prototype, "isAuthenticated", () => true);
  context.mock.method(MegaClient.prototype, "isSessionInvalidError", (error: unknown) => error instanceof Error && error.message === "synthetic-session-invalid");
  context.mock.method(MegaClient.prototype, "inventory", async () => {
    if (inventoryFailure) { const failure = inventoryFailure; inventoryFailure = null; throw failure; }
    return { devices: [
      { device_sn: station, device_model: parent, device_type: parent === "T8N00" ? 300 : 27, category: "eufy_security",
        member: { admin_user_id: "synthetic-owner" }, params: [{ param_type: 1224, param_value: guardMode }] },
      { device_sn: camera, device_model: parent === "T8N00" ? "T8E00" : "T8140-Z", device_type: parent === "T8N00" ? 301 : 14, category: "eufy_security",
        parent_sn: station, device_channel: 3, member: { admin_user_id: "synthetic-owner" } },
    ] };
  });
  context.mock.method(MegaPushReceiver.prototype, "start", async () => {});
  let joins = 0;
  context.mock.method(MegaClient.prototype, "nvrSignallingContext", async () => {
    joins++;
    return { host: "security-smart-ie.eufylife.com", region: "IE", authToken: "synthetic", gtoken: "synthetic", sign: "synthetic" };
  });
  const observers: NvrRtcSession[] = [], viewers: NvrRtcSession[] = [], closed = new Set<NvrRtcSession>();
  context.mock.method(NvrRtcSession.prototype, "observe", async function(this: NvrRtcSession) { observers.push(this); });
  context.mock.method(NvrRtcSession.prototype, "start", async function(this: NvrRtcSession) {
    viewers.push(this);
    options(this).ready(this.output, () => "h264");
  });
  const retire = NvrRtcSession.prototype.close;
  context.mock.method(NvrRtcSession.prototype, "close", function(this: NvrRtcSession) { closed.add(this); return retire.call(this); });

  // Capture the provider's minute timers so a refresh can run on demand.
  const refreshes: (() => void)[] = [], schedule = globalThis.setInterval;
  context.mock.method(globalThis, "setInterval", ((callback: () => void, delay?: number) => {
    const handle = schedule(callback, delay);
    if (delay === 60_000) refreshes.push(callback);
    return handle;
  }) as typeof setInterval);
  const logs: string[] = [], events: string[] = [];
  context.mock.method(process.stderr, "write", ((chunk: unknown) => { logs.push(String(chunk)); return true; }) as typeof process.stderr.write);
  const stopObserving = observeLogEvents((_component, event) => { events.push(event); });
  const stations: Parameters<ProviderEvents["station"]>[0][] = [];
  const sink = new Proxy({
    station: (value: Parameters<ProviderEvents["station"]>[0]) => stations.push(value),
    streamStarted: (_id: string, output: PassThrough) => { output.resume(); },
  }, { get: (target, key) => Reflect.get(target, key) ?? (() => {}) }) as unknown as ProviderEvents;
  const provider = new EufyProvider({ username: "fixture", password: "redacted", country: "AU", persistentDirectory: directory,
    maxStreamSeconds: 120, nvrModeWatch: watch, experimentalNvrLive: live, normalNvrLive: live });
  await provider.start(sink); await tick();
  const reading = () => stations.filter(value => value.serial === station).at(-1);
  return { provider, observers, viewers, closed, stations, reading, events, joins: () => joins,
    setGuardMode: (value: string) => { guardMode = value; },
    failNextInventory: (error: Error) => { inventoryFailure = error; },

    /** Run the latest inventory and station refresh callbacks captured for this generation. */
    refresh: async () => { for (const callback of refreshes.splice(0)) callback(); await tick(8); },
    close: async () => {
      try { await provider.close(); await tick(); }
      finally { stopObserving(); await rm(directory, { recursive: true, force: true }); }
      assert.equal(network.mock.callCount(), 0);
      assert.ok(logs.every(line => !line.includes(camera) && !line.includes(station) && !line.includes("synthetic-owner")));
    },
  };
}

test("an opted-out gateway starts no observer and fetches no sign token", async context => {
  const value = await fixture(context, { watch: false, live: true });
  try {
    assert.equal(value.joins(), 0);
    assert.equal(value.observers.length, 0);
    assert.equal(value.events.includes("nvr_mode_watch_started"), false);
  } finally { await value.close(); }
});

test("a T9000 parent gets no observer", async context => {
  const value = await fixture(context, { parent: "T9000" });
  try {
    assert.equal(value.joins(), 0);
    assert.equal(value.observers.length, 0);
  } finally { await value.close(); }
});

test("one observer per T8N00 publishes broadcasts, keeps them across a configured change and clears on retirement", async context => {
  const value = await fixture(context);
  try {
    assert.equal(value.joins(), 1);
    assert.equal(value.observers.length, 1);
    const watcher = options(value.observers[0]!);
    assert.equal(watcher.observer, true);
    assert.equal(watcher.maximumLifetimeMilliseconds, null);
    assert.equal(watcher.liveControls, undefined);
    assert.equal(watcher.administrator, "synthetic-owner");
    assert.equal(value.reading()?.effectiveMode ?? null, null);
    assert.equal(value.reading()?.effectiveModeReportedAt ?? null, null);

    watcher.onEffectiveMode!({ mode: 3 });
    assert.equal(value.reading()!.effectiveMode, 3);
    assert.equal(value.reading()!.effectiveModeSource, "nvr_rtc");
    assert.equal(typeof value.reading()!.effectiveModeReportedAt, "string");
    assert.equal(value.events.includes("nvr_mode_watch_observed"), true);

    // The app moved the configured mode without changing the effective one, so no broadcast came.
    value.setGuardMode("3");
    await value.refresh();
    assert.equal(value.reading()!.guardMode, 3);
    assert.equal(value.reading()!.effectiveMode, 3);
    assert.equal(value.observers.length, 1);

    watcher.onEffectiveMode!({ mode: null });
    assert.equal(value.reading()!.effectiveMode, null);
    watcher.onEffectiveMode!({ mode: 1 });
    watcher.onEffectiveMode!(null);
    assert.equal(value.reading()!.effectiveMode, null);
    assert.equal(value.reading()!.effectiveModeReportedAt, null);
  } finally { await value.close(); }
});

test("a live view beside the observer shares the reading and either can retire first", async context => {
  const value = await fixture(context, { live: true });
  try {
    const watcher = options(value.observers[0]!);
    await value.provider.startStream(camera); await tick();
    const viewer = options(value.viewers[0]!);
    assert.equal(typeof viewer.onEffectiveMode, "function");

    watcher.onEffectiveMode!({ mode: 3 });
    const published = value.stations.length;
    viewer.onEffectiveMode!({ mode: 3 });
    assert.equal(value.stations.length, published);
    viewer.onEffectiveMode!(null);
    assert.equal(value.reading()!.effectiveMode, 3);

    // A new change owned by the viewer first is joined by the observer.
    viewer.onEffectiveMode!({ mode: 1 });
    watcher.onEffectiveMode!({ mode: 1 });
    watcher.onEffectiveMode!(null);
    assert.equal(value.reading()!.effectiveMode, 1);
    viewer.onEffectiveMode!(null);
    assert.equal(value.reading()!.effectiveMode, null);
    assert.equal(value.joins(), 2);
    await value.provider.stopStream(camera);
  } finally { await value.close(); }
});

test("a dropped observer reconnects with a fresh sign token", { timeout: 20000 }, async context => {
  const value = await fixture(context);
  try {
    const first = value.observers[0]!;
    options(first).onEffectiveMode!({ mode: 1 });
    await first.close();
    options(first).onEffectiveMode!(null);
    assert.equal(value.reading()!.effectiveMode, null);
    await tick();
    assert.equal(value.events.includes("nvr_mode_watch_ended"), true);
    assert.equal(value.events.includes("nvr_mode_watch_retry"), true);
    const deadline = Date.now() + 8000;
    while (value.observers.length < 2 && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 50));
    assert.equal(value.observers.length, 2);
    assert.equal(value.joins(), 2);
    assert.notEqual(value.observers[1], first);
  } finally { await value.close(); }
});

test("session recovery and shutdown retire the observer", async context => {
  const value = await fixture(context);
  try {
    const first = value.observers[0]!;
    options(first).onEffectiveMode!({ mode: 3 });
    value.failNextInventory(new Error("synthetic-session-invalid"));
    await value.refresh(); await tick(8);
    assert.equal(value.closed.has(first), true);
    assert.equal(value.reading()!.effectiveMode ?? null, null);
    assert.equal(value.observers.length, 2);
    assert.equal(options(first).current(), false);
    const second = value.observers[1]!;
    assert.equal(options(second).current(), true);
    await value.provider.close(); await tick();
    assert.equal(value.closed.has(second), true);
    assert.equal(value.events.includes("nvr_mode_watch_stopped"), true);
    assert.equal(value.observers.length, 2);
  } finally { await value.close(); }
});
