import test from "node:test";
import assert from "node:assert/strict";
import { createRampMeter, recommendWindow } from "../src/ramp-meter.js";
function pair(delay = 0) {
  const a = { readyState: "open", bufferedAmount: 0 },
    b = { readyState: "open", bufferedAmount: 0 };
  for (const [local, remote] of [
    [a, b],
    [b, a],
  ])
    local.send = (data) => {
      const copy =
        typeof data === "string"
          ? data
          : data instanceof ArrayBuffer
            ? data.slice(0)
            : data.buffer.slice(
                data.byteOffset,
                data.byteOffset + data.byteLength,
              );
      if (delay) setTimeout(() => remote.onmessage?.({ data: copy }), delay);
      else queueMicrotask(() => remote.onmessage?.({ data: copy }));
    };
  return [a, b];
}
test("ramp reports receiver bytes and respects credit/buffer windows on ascending and descending stages", async () => {
  const [a, b] = pair(),
    sender = createRampMeter(a, { chunkBytes: 1024 }),
    receiver = createRampMeter(b, { chunkBytes: 1024 });
  try {
    const report = await sender.run({
      windows: [1024, 4096, 1024],
      durationMs: 30,
      maxStageBytes: 16 * 1024,
    });
    assert.equal(report.rows.length, 3);
    for (const row of report.rows) {
      assert.equal(row.sentBytes, row.receivedBytes);
      assert.ok(row.receivedBytes > 0);
      assert.ok(row.peakOutstandingBytes <= row.windowBytes);
      assert.ok(row.peakBufferedBytes <= row.windowBytes);
    }
    assert.equal(report.recommendation.status, "inconclusive");
  } finally {
    sender.close();
    receiver.close();
  }
});
test("closing a blocked ramp rejects pending work", async () => {
  const [a, b] = pair(),
    sender = createRampMeter(a, { chunkBytes: 1024 }),
    receiver = createRampMeter(b, { chunkBytes: 1024 });
  const run = sender.run({ durationMs: 50, windows: [1024] });
  sender.close();
  await assert.rejects(run, /closed/);
  receiver.close();
});
test("recommendation requires repeated near-plateau throughput with acceptable latency and excludes byte-capped samples", () => {
  const row = (window, rate, rtt = 10) => ({
    windowBytes: window,
    receiverBytesPerSecond: rate,
    rttMedianMs: rtt,
    receivedBytes: 100,
    limitedByByteBudget: false,
  });
  const rows = [
    row(1024, 10),
    row(4096, 95),
    row(16384, 100, 90),
    row(4096, 94),
    row(1024, 11),
  ];
  assert.equal(recommendWindow(rows, 10).windowBytes, 4096);
  assert.equal(
    recommendWindow(
      rows.map((r) => ({ ...r, limitedByByteBudget: true })),
      10,
    ).status,
    "inconclusive",
  );
  assert.equal(
    recommendWindow(
      rows.map((r) => ({ ...r, rttMedianMs: 200 })),
      10,
    ).status,
    "inconclusive",
  );
});

test("ramp exposes a credit-window bottleneck under controlled latency without claiming a hardware speed", async () => {
  for (const delay of [5, 30]) {
    const [a, b] = pair(delay),
      sender = createRampMeter(a, { chunkBytes: 1024 }),
      receiver = createRampMeter(b, { chunkBytes: 1024 });
    try {
      const report = await sender.run({
        windows: [1024, 8192, 1024],
        durationMs: 300,
        maxStageBytes: 1048576,
      });
      assert.ok(
        report.rows[1].receiverBytesPerSecond >
          report.rows[0].receiverBytesPerSecond * 2,
      );
      for (const row of report.rows) {
        assert.equal(row.sentBytes, row.receivedBytes);
        assert.ok(row.peakOutstandingBytes <= row.windowBytes);
        assert.equal(row.limitedByByteBudget, false);
      }
    } finally {
      sender.close();
      receiver.close();
    }
  }
});
