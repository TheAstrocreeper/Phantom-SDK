/** Wait for native send-buffer capacity. Timers fail stalled work; they never poll or grant capacity. */
export function waitForDataChannelCapacity(
  dc,
  {
    highWaterMark = 16 * 1024 * 1024,
    lowWaterMark = 4 * 1024 * 1024,
    bytes = 0,
    timeoutMs = 15000,
    signal,
    setTimeout: later = globalThis.setTimeout,
    clearTimeout: cancelLater = globalThis.clearTimeout,
  } = {},
) {
  if (
    !Number.isSafeInteger(highWaterMark) ||
    highWaterMark < 1 ||
    !Number.isSafeInteger(lowWaterMark) ||
    lowWaterMark < 0 ||
    lowWaterMark >= highWaterMark ||
    !Number.isSafeInteger(bytes) ||
    bytes < 0 ||
    bytes > highWaterMark ||
    !Number.isFinite(timeoutMs) ||
    timeoutMs <= 0
  )
    return Promise.reject(new TypeError("Invalid data channel buffer limits"));
  if (signal?.aborted)
    return Promise.reject(Error("Data channel send aborted"));
  if (dc?.readyState !== "open")
    return Promise.reject(Error("Data channel closed"));
  if (dc.bufferedAmount + bytes <= highWaterMark) return Promise.resolve();
  return new Promise((resolve, reject) => {
    let settled = false,
      timer;
    const finish = (error) => {
      if (settled) return;
      settled = true;
      cancelLater(timer);
      dc.removeEventListener("bufferedamountlow", check);
      dc.removeEventListener("close", closed);
      dc.removeEventListener("error", closed);
      signal?.removeEventListener("abort", aborted);
      error ? reject(error) : resolve();
    };
    const closed = () => finish(Error("Data channel closed while draining")),
      aborted = () => finish(Error("Data channel send aborted"));
    const check = () => {
      if (signal?.aborted) return aborted();
      if (dc.readyState !== "open") return closed();
      if (
        dc.bufferedAmount <= lowWaterMark &&
        dc.bufferedAmount + bytes <= highWaterMark
      )
        finish();
    };
    dc.bufferedAmountLowThreshold = lowWaterMark;
    dc.addEventListener("bufferedamountlow", check);
    dc.addEventListener("close", closed);
    dc.addEventListener("error", closed);
    signal?.addEventListener("abort", aborted, { once: true });
    timer = later(
      () => finish(Error("Data channel buffer drain timed out")),
      timeoutMs,
    );
    check();
  });
}
