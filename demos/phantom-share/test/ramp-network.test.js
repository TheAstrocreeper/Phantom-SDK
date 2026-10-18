import test from "node:test";
import assert from "node:assert/strict";
import { attachRampTest } from "../src/ramp-test.js";
import { createRampMeter } from "../src/ramp-meter.js";
import { memoryRTC } from "../../../tests/fixtures/memory-rtc.js";
test("admitted peers negotiate a raw ramp, reverse direction, and retain both completed reports", async () => {
  const ids = ["1".repeat(16), "2".repeat(16)],
    listeners = [new Set(), new Set()],
    updates = [[], []],
    RTC = memoryRTC({ emitCandidates: true });
  const networks = ids.map((id, i) => ({
    getPeerConnections: () => [{ peerId: ids[1 - i], state: "ready" }],
    on(type, fn) {
      listeners[i].add(fn);
      return () => listeners[i].delete(fn);
    },
    send(to, text) {
      queueMicrotask(() => {
        for (const fn of listeners[1 - i]) fn({ from: id, text });
      });
    },
  }));
  const controls = networks.map((network, i) =>
    attachRampTest({
      network,
      runtime: {
        RTCPeerConnection: RTC,
        navigator: { userAgent: "test fixture" },
      },
      settings: {
        windows: [65536, 131072, 65536],
        durationMs: 30,
        maxStageBytes: 131072,
      },
      onRequest: () => true,
      onUpdate: (u) => updates[i].push(u),
    }),
  );
  try {
    await controls[0].start(ids[1]);
    const end = Date.now() + 2000;
    while (
      Date.now() < end &&
      !controls.every((c) => c.getReport()?.directions.length === 2)
    )
      await new Promise((r) => setTimeout(r, 5));
    for (const c of controls) {
      const report = c.getReport();
      assert.equal(report?.directions.length, 2, JSON.stringify(updates));
      assert.equal(report.tool, "phantom-ramp-v1");
      for (const direction of report.directions)
        for (const row of direction.rows) {
          assert.equal(row.sentBytes, row.receivedBytes);
          assert.ok(row.peakOutstandingBytes <= row.windowBytes);
        }
    }
  } finally {
    controls.forEach((c) => c.close());
  }
});
