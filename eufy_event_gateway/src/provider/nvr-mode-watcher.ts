/**
 * Owns one T8N00's standing read-only observer connection and its reconnects.
 *
 * The provider admits the station, supplies fresh signalling credentials for
 * every attempt, and owns the effective-mode reading the session reports. This
 * class owns only the attempt loop, its bounded back-off and the current
 * session. A dropped connection is retired before the next attempt starts, so
 * at most one observer connection per watcher exists at any time. Close aborts
 * a wait, a pending connect or an established session and settles after all of
 * them have released their resources.
 */
import { setTimeout as sleep } from "node:timers/promises";
import { createLogger } from "../logging.js";
import { NvrTrialError } from "../stream/nvr-protocol.js";
import type { NvrRtcSession } from "../stream/nvr-rtc-session.js";

const logger = createLogger("provider");

/**
 * Delays between attempts. The ladder is bounded at five minutes and restarts
 * after a connection that stayed up for STABLE_MILLISECONDS, so a routine drop
 * reconnects quickly while a refusing NVR or cloud is not hammered.
 */
export const NVR_MODE_WATCH_BACKOFF_MILLISECONDS = [5_000, 15_000, 30_000, 60_000, 120_000, 300_000] as const;
const STABLE_MILLISECONDS = 60_000;

/** Provider-owned admission and transport for one station. */
export interface NvrModeWatcherOptions {

  /** Fetch a fresh sign token and build an observer session. Honours the signal. */
  readonly connect: (signal: AbortSignal) => Promise<NvrRtcSession>;

  /** Provider admission, rechecked before every attempt and every wait. */
  readonly current: () => boolean;

  /** Safe model label for logs. Serials and identifiers never reach this class. */
  readonly model: string;

  /** Injectable delay for tests. Rejects when the signal aborts. */
  readonly wait?: (milliseconds: number, signal: AbortSignal) => Promise<void>;

  /** Injectable monotonic clock for tests. */
  readonly now?: () => number;
}

/** Content-free lifetime counters for tests and diagnostics. */
export interface NvrModeWatcherStats {
  attempts: number;
  connections: number;
  drops: number;
}

/** One station's reconnecting read-only observer. Start once, close repeatedly. */
export class NvrModeWatcher {
  readonly stats: NvrModeWatcherStats = { attempts: 0, connections: 0, drops: 0 };
  readonly #controller = new AbortController();
  #session: NvrRtcSession | null = null;
  #observing = false;
  #loop: Promise<void> | null = null;
  #closing: Promise<void> | null = null;

  constructor(private readonly options: NvrModeWatcherOptions) {}

  /** Whether an established observer currently holds the data channel. */
  get observing(): boolean { return this.#observing; }

  /** Begin the attempt loop. Repeated calls and calls after close do nothing. */
  start(): void {
    if (this.#loop || this.#controller.signal.aborted) return;
    logger.info("nvr_mode_watch_started", `model=${this.options.model}`);
    this.#loop = this.#run().catch(() => {});
  }

  /** Stop the loop and retire its session. Settles once both are released. */
  close(): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#controller.abort();
    this.#closing = (async () => {
      await this.#session?.close().catch(() => {});
      await this.#loop;
      if (this.#loop) logger.info("nvr_mode_watch_stopped", `model=${this.options.model} attempts=${this.stats.attempts} connections=${this.stats.connections} drops=${this.stats.drops}`);
    })();
    return this.#closing;
  }

  async #run(): Promise<void> {
    const signal = this.#controller.signal, now = this.options.now ?? (() => performance.now());
    const wait = this.options.wait ?? ((milliseconds, cancellation) => sleep(milliseconds, undefined, { signal: cancellation }));
    let failures = 0;
    while (!signal.aborted && this.options.current()) {
      const attempt = ++this.stats.attempts, startedAt = now();
      let session: NvrRtcSession | null = null, connectedAt: number | null = null, outcome: NvrTrialError | null = null;
      try {
        session = this.#session = await this.options.connect(signal);
        if (signal.aborted) break;
        await session.observe();
        connectedAt = now(); this.stats.connections++; this.#observing = true;
        logger.info("nvr_mode_watch_connected", `model=${this.options.model} attempt=${attempt} setup_ms=${Math.round(connectedAt - startedAt)}`);
        outcome = await session.closed;
      } catch (error) {
        outcome = error instanceof NvrTrialError ? error : new NvrTrialError(signal.aborted ? "cancelled" : "sign");
      } finally {
        this.#observing = false;
        await session?.close().catch(() => {});
        if (this.#session === session) this.#session = null;
      }
      if (signal.aborted) break;
      const connectedMilliseconds = connectedAt === null ? null : now() - connectedAt;
      if (connectedAt !== null) this.stats.drops++;
      logger.warn("nvr_mode_watch_ended", `model=${this.options.model} stage=${outcome?.stage ?? "closed"} reason=${outcome?.reason ?? "none"}`
        + ` connected_s=${connectedMilliseconds === null ? "none" : Math.round(connectedMilliseconds / 1000)}${session ? " " + observerCounters(session) : ""}`);
      if (connectedMilliseconds !== null && connectedMilliseconds >= STABLE_MILLISECONDS) failures = 0;
      const delay = NVR_MODE_WATCH_BACKOFF_MILLISECONDS[Math.min(failures, NVR_MODE_WATCH_BACKOFF_MILLISECONDS.length - 1)]!;
      failures++;
      if (!this.options.current()) break;
      logger.info("nvr_mode_watch_retry", `model=${this.options.model} delay_s=${delay / 1000} failures=${failures}`);
      try { await wait(delay, signal); } catch { break; }
    }
  }
}

/** Counters that explain a drop without packet contents or connection identity. */
function observerCounters(session: NvrRtcSession): string {
  const stats = session.stats;
  return `frames=${stats.frames} channel2_frames=${stats.channel2Frames} stale=${stats.staleReplies} rejected=${stats.rejected}`
    + ` signalling_closes=${stats.signallingClosuresWhileObserving} video=${stats.video}`;
}
