export const DEFAULT_WINDOWS = [
  256 * 1024,
  1024 * 1024,
  4 * 1024 * 1024,
  16 * 1024 * 1024,
  4 * 1024 * 1024,
  1024 * 1024,
  256 * 1024,
];
const median = (v) =>
  v.length ? [...v].sort((a, b) => a - b)[Math.floor(v.length / 2)] : null;
/** Experimental raw reliable-channel meter. Bytes are counted at the receiver, not inferred from send(). */
export function createRampMeter(
  dc,
  {
    now = () => performance.now(),
    chunkBytes = 65536,
    onRemoteStage = () => {},
  } = {},
) {
  let closed = false,
    incoming = null,
    outgoing = null;
  const pending = new Map();
  const send = (m) => {
    if (closed || dc.readyState !== "open") throw Error("Test channel closed");
    dc.send(JSON.stringify(m));
  };
  const settle = (id, error, value) => {
    const row = pending.get(id);
    if (!row) return;
    pending.delete(id);
    clearTimeout(row.timer);
    error ? row.reject(error) : row.resolve(value);
  };
  function ask(kind, data = {}, timeout = 10000) {
    const id = crypto.randomUUID();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => settle(id, Error("Test response timed out")),
        timeout,
      );
      pending.set(id, { resolve, reject, timer });
      try {
        send({ kind, id, ...data });
      } catch (e) {
        settle(id, e);
      }
    });
  }
  dc.binaryType = "arraybuffer";
  dc.onmessage = ({ data }) => {
    if (closed) return;
    if (data instanceof ArrayBuffer) {
      if (!incoming || data.byteLength > 65536) {
        close();
        return;
      }
      incoming.bytes += data.byteLength;
      incoming.sinceAck += data.byteLength;
      if (
        incoming.flowControl === "receiver-credit" &&
        incoming.sinceAck >= Math.min(incoming.window / 2, 256 * 1024)
      ) {
        incoming.sinceAck = 0;
        send({ kind: "credit", stage: incoming.id, bytes: incoming.bytes });
      }
      return;
    }
    if (typeof data !== "string" || data.length > 4096) return;
    let m;
    try {
      m = JSON.parse(data);
    } catch {
      return;
    }
    if (m.kind === "reply") {
      settle(m.id, m.error ? Error(m.error) : null, m.value);
      return;
    }
    if (m.kind === "ping") {
      send({ kind: "reply", id: m.id, value: true });
      return;
    }
    if (
      m.kind === "credit" &&
      outgoing?.id === m.stage &&
      Number.isSafeInteger(m.bytes) &&
      m.bytes >= outgoing.acked &&
      m.bytes <= outgoing.sent
    ) {
      outgoing.acked = m.bytes;
      outgoing.wake();
      return;
    }
    if (m.kind === "start") {
      if (
        incoming ||
        !["receiver-credit", "native-buffer"].includes(
          m.flowControl ?? "receiver-credit",
        ) ||
        !Number.isInteger(m.window) ||
        m.window < chunkBytes ||
        m.window > 64 * 1024 * 1024
      ) {
        send({ kind: "reply", id: m.id, error: "Invalid test window" });
        return;
      }
      incoming = {
        id: m.stage,
        window: m.window,
        flowControl: m.flowControl ?? "receiver-credit",
        bytes: 0,
        sinceAck: 0,
        start: now(),
      };
      onRemoteStage({ windowBytes: m.window });
      send({ kind: "reply", id: m.id, value: true });
      return;
    }
    if (m.kind === "end" && incoming?.id === m.stage) {
      const value = {
        receivedBytes: incoming.bytes,
        receiverMs: now() - incoming.start,
      };
      incoming = null;
      send({ kind: "reply", id: m.id, value });
    }
  };
  dc.onbufferedamountlow = () => outgoing?.wake();
  dc.onclose = () => close();
  dc.onerror = () => close();
  function close() {
    if (closed) return;
    closed = true;
    outgoing?.abort(Error("Test channel closed"));
    for (const id of [...pending.keys()])
      settle(id, Error("Test channel closed"));
  }
  async function ping() {
    const start = now();
    await ask("ping");
    return now() - start;
  }
  async function run({
    windows = DEFAULT_WINDOWS,
    flowControls = ["receiver-credit"],
    durationMs = 3000,
    maxStageBytes = 256 * 1024 * 1024,
    signal,
    onStage = () => {},
  } = {}) {
    if (outgoing) throw Error("A ramp is already running");
    if (
      !Array.isArray(flowControls) ||
      !flowControls.length ||
      flowControls.length > 2 ||
      new Set(flowControls).size !== flowControls.length ||
      flowControls.some(
        (mode) => !["receiver-credit", "native-buffer"].includes(mode),
      ) ||
      !Array.isArray(windows) ||
      !windows.length ||
      windows.length > 20 ||
      windows.some(
        (w) => !Number.isInteger(w) || w < chunkBytes || w > 64 * 1024 * 1024,
      ) ||
      !Number.isFinite(durationMs) ||
      durationMs < 20 ||
      durationMs > 10000 ||
      !Number.isSafeInteger(maxStageBytes) ||
      maxStageBytes < chunkBytes ||
      maxStageBytes > 1024 * 1024 * 1024
    )
      throw Error("Invalid ramp settings");
    const baseline = [];
    for (let i = 0; i < 3; i++) baseline.push(await ping());
    const rows = [],
      payload = new Uint8Array(chunkBytes);
    crypto.getRandomValues(payload);
    for (let stage = 0; stage < windows.length * flowControls.length; stage++) {
      const step = Math.floor(stage / flowControls.length),
        order = step % 2 ? [...flowControls].reverse() : flowControls,
        flowControl = order[stage % flowControls.length];
      if (signal?.aborted || closed) throw Error("Test cancelled");
      const id = crypto.randomUUID(),
        window = windows[step],
        rtts = [],
        wakeDelays = [];
      await ask("start", { stage: id, window, flowControl });
      const start = now();
      let last = start,
        waiting = null,
        queued = false,
        bufferStallMs = 0,
        creditStallMs = 0,
        peakBufferedBytes = 0,
        peakOutstandingBytes = 0,
        limitReached = false;
      const row = { id, sent: 0, acked: 0, wake: () => {}, abort: () => {} };
      outgoing = row;
      dc.bufferedAmountLowThreshold = Math.floor(window / 2);
      await new Promise((resolve, reject) => {
        let timer,
          pinger,
          finished = false;
        const finish = (error) => {
          if (finished) return;
          finished = true;
          clearTimeout(timer);
          clearInterval(pinger);
          signal?.removeEventListener("abort", cancel);
          row.wake = () => {};
          error ? reject(error) : resolve();
        };
        const cancel = () => finish(Error("Test cancelled"));
        row.abort = finish;
        const pump = () => {
          if (finished) return;
          queued = false;
          const at = now();
          if (waiting === "buffer") bufferStallMs += at - last;
          if (waiting === "credit") creditStallMs += at - last;
          waiting = null;
          last = at;
          if (closed || signal?.aborted) {
            cancel();
            return;
          }
          if (at - start >= durationMs) return;
          try {
            let count = 0;
            while (
              now() - start < durationMs &&
              row.sent + chunkBytes <= maxStageBytes
            ) {
              if (
                flowControl === "receiver-credit" &&
                row.sent - row.acked + chunkBytes > window
              ) {
                waiting = "credit";
                break;
              }
              if (dc.bufferedAmount + chunkBytes > window) {
                waiting = "buffer";
                break;
              }
              dc.send(payload.buffer);
              row.sent += chunkBytes;
              count++;
              peakBufferedBytes = Math.max(
                peakBufferedBytes,
                dc.bufferedAmount,
              );
              peakOutstandingBytes = Math.max(
                peakOutstandingBytes,
                row.sent - row.acked,
              );
              if (count === 32) {
                row.wake();
                break;
              }
            }
            if (row.sent + chunkBytes > maxStageBytes) {
              limitReached = true;
              finish();
            }
          } catch (e) {
            finish(e);
          }
        };
        row.wake = () => {
          if (!queued) {
            queued = true;
            const scheduledAt = now();
            setTimeout(() => {
              if (!finished && wakeDelays.length < 256)
                wakeDelays.push(now() - scheduledAt);
              pump();
            }, 0);
          }
        };
        signal?.addEventListener("abort", cancel, { once: true });
        timer = setTimeout(() => {
          const at = now();
          if (waiting === "buffer") bufferStallMs += at - last;
          if (waiting === "credit") creditStallMs += at - last;
          finish();
        }, durationMs);
        let pingPending = false;
        pinger = setInterval(() => {
          if (pingPending) return;
          pingPending = true;
          ping()
            .then((rtt) => rtts.push(rtt))
            .catch(() => {})
            .finally(() => (pingPending = false));
        }, 250);
        pump();
      }).catch((e) => {
        outgoing = null;
        throw e;
      });
      const activeMs = now() - start;
      const result = await ask("end", { stage: id }, 180000);
      outgoing = null;
      if (result.receivedBytes !== row.sent)
        throw Error("Receiver byte count mismatch");
      const sample = {
        step: stage,
        windowStep: step,
        flowControl,
        windowBytes: window,
        sentBytes: row.sent,
        ...result,
        activeMs,
        completionMs: now() - start,
        receiverBytesPerSecond:
          result.receivedBytes / (result.receiverMs / 1000),
        rttSamplesMs: [...rtts],
        rttMedianMs: median(rtts),
        rttMaxMs: rtts.length ? Math.max(...rtts) : null,
        peakBufferedBytes,
        peakOutstandingBytes:
          flowControl === "receiver-credit" ? peakOutstandingBytes : null,
        wakeDelayMedianMs: median(wakeDelays),
        wakeDelayP95Ms: wakeDelays.length
          ? [...wakeDelays].sort((a, b) => a - b)[
              Math.ceil(wakeDelays.length * 0.95) - 1
            ]
          : null,
        wakeDelaySamples: wakeDelays.length,
        bufferStallMs,
        creditStallMs,
        limitedByByteBudget: limitReached,
      };
      rows.push(sample);
      onStage(sample);
    }
    return {
      flowControls,
      baselineRttMs: median(baseline),
      chunkBytes,
      durationMs,
      maxStageBytes,
      rows,
      recommendation:
        flowControls.length > 1
          ? {
              status: "comparison",
              reason: "Matched pacing trials; no SDK default recommendation",
            }
          : recommendWindow(rows, median(baseline)),
    };
  }
  return { run, close };
}
export function recommendWindow(rows, baselineRttMs) {
  const valid = rows.filter(
    (r) =>
      !r.limitedByByteBudget &&
      r.receivedBytes > 0 &&
      Number.isFinite(r.receiverBytesPerSecond),
  );
  if (valid.length < 3)
    return { status: "inconclusive", reason: "Too few uncensored samples" };
  const groups = new Map();
  for (const row of valid) {
    if (!groups.has(row.windowBytes)) groups.set(row.windowBytes, []);
    groups.get(row.windowBytes).push(row);
  }
  const repeated = [...groups]
    .filter(([, rs]) => rs.length >= 2)
    .map(([window, rs]) => ({
      window,
      rate: Math.min(...rs.map((r) => r.receiverBytesPerSecond)),
      rtt: Math.max(...rs.map((r) => r.rttMedianMs ?? Infinity)),
    }))
    .sort((a, b) => a.window - b.window);
  if (repeated.length < 2)
    return {
      status: "inconclusive",
      reason: "Need both ascending and descending samples",
    };
  const peak = Math.max(...valid.map((r) => r.receiverBytesPerSecond)),
    rttLimit = Math.max(50, (baselineRttMs || 0) * 2);
  const candidate = repeated.find(
    (r) => r.rate >= peak * 0.9 && r.rtt <= rttLimit,
  );
  if (!candidate)
    return {
      status: "inconclusive",
      reason:
        "No repeated window stayed near peak throughput without elevated RTT",
    };
  if (candidate.window === Math.max(...rows.map((r) => r.windowBytes)))
    return {
      status: "inconclusive",
      reason: "Ramp ceiling reached; test a wider range",
    };
  return {
    status: "candidate",
    windowBytes: candidate.window,
    observedBytesPerSecond: candidate.rate,
    pacingCandidateBytesPerSecond: candidate.rate * 0.8,
    note: "Experimental per-path candidate, not an SDK default or universal safety guarantee",
  };
}
