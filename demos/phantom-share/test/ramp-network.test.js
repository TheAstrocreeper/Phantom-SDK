import test from "node:test";
import assert from "node:assert/strict";
import { attachRampTest } from "../src/ramp-test.js";
import { createRampMeter } from "../src/ramp-meter.js";
import { memoryRTC } from "../../../tests/fixtures/memory-rtc.js";
for (const initiator of [0, 1])
  test(`admitted peers negotiate after delayed approval from endpoint ${initiator}, reverse direction, and retain both reports`, async () => {
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
          flowControls: ["receiver-credit", "native-buffer"],
          durationMs: 30,
          maxStageBytes: 131072,
        },
        onRequest: async () => {
          await new Promise((r) => setTimeout(r, 10));
          return true;
        },
        onUpdate: (u) => updates[i].push(u),
      }),
    );
    try {
      await controls[initiator].start(ids[1 - initiator]);
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
            if (row.flowControl === "native-buffer")
              assert.equal(row.peakOutstandingBytes, null);
            else assert.ok(row.peakOutstandingBytes <= row.windowBytes);
          }
      }
    } finally {
      controls.forEach((c) => c.close());
    }
  });

test("canceling while the peer approval is pending cannot create a late test connection", async () => {
  const ids = ["1".repeat(16), "2".repeat(16)],
    listeners = [new Set(), new Set()],
    updates = [[], []],
    pcs = [];
  let accept;
  const Base = memoryRTC(),
    RTC = class extends Base {
      constructor(...args) {
        super(...args);
        pcs.push(this);
      }
    };
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
      runtime: { RTCPeerConnection: RTC },
      onRequest: () => new Promise((r) => (accept = r)),
      onUpdate: (u) => updates[i].push(u),
    }),
  );
  try {
    await controls[1].start(ids[0]);
    await new Promise((r) => setTimeout(r, 5));
    assert.equal(typeof accept, "function");
    controls[1].cancel("Page hidden during approval");
    await new Promise((r) => setTimeout(r, 5));
    accept(true);
    await new Promise((r) => setTimeout(r, 10));
    assert.equal(
      pcs.length,
      1,
      "accepting a canceled offer must not create a receiver RTC connection",
    );
    assert.equal(updates[0].at(-1).busy, false);
    assert.equal(controls[1].getReport().reason, "Page hidden during approval");
    assert.match(updates[0].at(-1).status, /Page hidden during approval/);
  } finally {
    controls.forEach((c) => c.close());
  }
});
