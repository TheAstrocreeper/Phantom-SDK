import { createRampMeter } from "./ramp-meter.js";
/** Opt-in diagnostic: separate raw RTC channel, signaling only over the admitted SDK peer channel. */
export function attachRampTest({
  network,
  onRequest = () => false,
  onUpdate = () => {},
  onDiagnostic = () => {},
  runtime = globalThis,
  settings = { durationMs: 3000, maxStageBytes: 256 * 1024 * 1024 },
  createMeter = (dc, options) => createRampMeter(dc, options),
  rtcConfiguration = { iceServers: [{ urls: "stun:stun.l.google.com:19302" }] },
}) {
  const extendedSettings = {
    durationMs: 3000,
    maxStageBytes: 1024 * 1024 * 1024,
    windows: [
      256 * 1024,
      1024 * 1024,
      4 * 1024 * 1024,
      16 * 1024 * 1024,
      64 * 1024 * 1024,
      16 * 1024 * 1024,
      4 * 1024 * 1024,
      1024 * 1024,
      256 * 1024,
    ],
  };
  const comparisonSettings = {
    durationMs: 3000,
    maxStageBytes: 64 * 1024 * 1024,
    windows: [
      256 * 1024,
      1024 * 1024,
      4 * 1024 * 1024,
      1024 * 1024,
      256 * 1024,
    ],
    flowControls: ["receiver-credit", "native-buffer"],
  };
  const app = "phantom-ramp/1";
  let active = null,
    lastReport = null,
    closed = false,
    pendingOffer = null,
    lastSession = null;
  const trace = (type, data = {}) => onDiagnostic(type, data);
  const update = (m) => {
    trace("ramp-state", {
      status: m.status,
      peer: active?.peer ?? pendingOffer?.peer,
    });
    onUpdate({ ...m, busy: !!(active || pendingOffer) });
  };
  const ready = (peer) =>
    network
      .getPeerConnections()
      .some((p) => p.peerId === peer && p.state === "ready");
  const send = (peer, message) => {
    if (closed || !ready(peer)) throw Error("Test peer unavailable");
    network.send(peer, JSON.stringify({ app, ...message }));
  };
  function stop(reason = "Test stopped", notify = true) {
    const s = active,
      offer = pendingOffer;
    if (!s && !offer) return;
    const target = s ?? offer;
    trace("ramp-stop", { id: target.id, peer: target.peer, reason });
    if (notify)
      try {
        send(target.peer, { kind: "stop", id: target.id, reason });
      } catch {}
    active = null;
    pendingOffer = null;
    if (offer) {
      clearTimeout(offer.timer);
      offer.controller.abort();
    }
    if (s) {
      clearTimeout(s.timer);
      s.controller.abort();
      s.meter?.close();
      s.dc?.close();
      s.pc?.close();
      s.reject?.(Error(reason));
      lastReport = { ...s.report, status: "incomplete", reason };
    }
    update({ status: reason, ...(s ? { report: lastReport } : {}) });
  }
  async function selectedPath(pc) {
    const stats = await pc.getStats(),
      pairs = [...stats.values()];
    const transport = pairs.find(
      (s) => s.type === "transport" && s.selectedCandidatePairId,
    );
    const pair =
      stats.get(transport?.selectedCandidatePairId) ||
      pairs.find(
        (s) =>
          s.type === "candidate-pair" && s.nominated && s.state === "succeeded",
      );
    const local = stats.get(pair?.localCandidateId),
      remote = stats.get(pair?.remoteCandidateId);
    return {
      localType: local?.candidateType,
      remoteType: remote?.candidateType,
      protocol: local?.protocol,
      relayProtocol: local?.relayProtocol,
      currentRoundTripTimeSeconds: pair?.currentRoundTripTime,
    };
  }
  function create(id, peer, initiator, settings) {
    const s = {
      id,
      peer,
      initiator,
      settings,
      controller: new AbortController(),
      ice: [],
      outIce: [],
      signaled: false,
      report: {
        schema: 1,
        tool: "phantom-ramp-v1",
        startedAt: new Date().toISOString(),
        localAgent: runtime.navigator?.userAgent,
        settings,
        directions: [],
      },
    };
    s.pc = new runtime.RTCPeerConnection(rtcConfiguration);
    active = s;
    lastSession = s;
    update({ status: "Preparing raw test" });
    s.timer = setTimeout(() => stop("Test setup timed out"), 60000);
    s.pc.onicecandidate = (e) => {
      if (e.candidate && active === s)
        try {
          s.candidateCount = (s.candidateCount ?? 0) + 1;
          const candidate = e.candidate.toJSON?.() ?? e.candidate;
          if (s.signaled) send(peer, { kind: "ice", id, candidate });
          else if (s.outIce.length < 64) s.outIce.push(candidate);
        } catch {
          stop("Test signaling unavailable");
        }
    };
    s.pc.onicegatheringstatechange = () =>
      trace("ramp-ice", {
        id: s.id,
        peer: s.peer,
        iceGatheringState: s.pc.iceGatheringState,
        total: s.candidateCount ?? 0,
      });
    s.pc.onconnectionstatechange = () => {
      trace("ramp-rtc", {
        id: s.id,
        peer: s.peer,
        connectionState: s.pc.connectionState,
        iceConnectionState: s.pc.iceConnectionState,
        iceGatheringState: s.pc.iceGatheringState,
      });
      if (
        active === s &&
        ["failed", "closed"].includes(s.pc.connectionState) &&
        !s.resultSent
      )
        stop("Test connection lost");
    };
    function bind(dc) {
      if (dc.label !== "phantom-ramp") {
        dc.close();
        return;
      }
      s.dc = dc;

      dc.onopen = () => {
        trace("ramp-channel-open", { id: s.id, peer: s.peer });
        const maximum = s.pc.sctp?.maxMessageSize;
        const chunkBytes =
          Number.isFinite(maximum) && maximum > 0
            ? Math.min(65536, maximum)
            : 65536;
        if (chunkBytes < 1024) {
          stop("Negotiated RTC message size too small for this test");
          return;
        }
        s.meter = createMeter(dc, { chunkBytes });
        clearTimeout(s.timer);
        if (initiator)
          void run(s).catch((error) => {
            if (active === s) stop(error.message);
          });
        else update({ status: "Receiving raw test data" });
      };
    }
    s.pc.ondatachannel = (e) => bind(e.channel);
    if (initiator) bind(s.pc.createDataChannel("phantom-ramp"));
    return s;
  }
  async function run(s) {
    s.report.path = await selectedPath(s.pc);
    s.report.localAgent = runtime.navigator?.userAgent;
    const forward = await s.meter.run({
      ...s.settings,
      signal: s.controller.signal,
      onStage: (row) =>
        update({ status: "Ramp up/down: this device → peer", row }),
    });
    if (active !== s) return;
    s.report.directions.push({ direction: "initiator-to-peer", ...forward });
    update({ status: "Ramp up/down: peer → this device" });
    await new Promise((resolve, reject) => {
      s.resolve = resolve;
      s.reject = reject;
      s.timer = setTimeout(
        () => reject(Error("Reverse ramp timed out")),
        600000,
      );
      send(s.peer, { kind: "reverse", id: s.id });
    });
    clearTimeout(s.timer);
    s.report.finishedAt = new Date().toISOString();
    lastReport = s.report;
    s.finished = true;
    send(s.peer, { kind: "complete", id: s.id, report: s.report });
    update({ status: "Complete", report: s.report });
    active = null;
    s.meter.close();
    s.dc.close();
    s.pc.close();
  }
  async function handle({ from, text }) {
    if (closed || typeof text !== "string" || text.length > 50000) return;
    let m;
    try {
      m = JSON.parse(text);
    } catch {
      return;
    }
    if (
      m.app !== app ||
      !ready(from) ||
      typeof m.id !== "string" ||
      m.id.length > 64
    )
      return;
    if (
      m.kind === "ice" &&
      pendingOffer?.id === m.id &&
      pendingOffer.peer === from
    ) {
      if (pendingOffer.ice.length < 64) pendingOffer.ice.push(m.candidate);
      return;
    }
    if (m.kind === "stop") {
      const reason =
        typeof m.reason === "string"
          ? m.reason.slice(0, 160)
          : "Peer stopped the test";
      if (
        (active?.id === m.id && active.peer === from) ||
        (pendingOffer?.id === m.id && pendingOffer.peer === from)
      ) {
        stop("Peer: " + reason, false);
        return;
      }
      if (
        lastSession?.id === m.id &&
        lastSession.peer === from &&
        !lastSession.finished
      ) {
        lastReport = {
          ...lastSession.report,
          status: "incomplete",
          reason: "Peer: " + reason,
        };
        trace("ramp-stop", { id: m.id, peer: from, reason: lastReport.reason });
        update({ status: lastReport.reason, report: lastReport });
      }
      return;
    }
    if (m.kind === "offer") {
      if (active || pendingOffer) {
        send(from, { kind: "reject", id: m.id });
        return;
      }
      const offer = {
        id: m.id,
        peer: from,
        ice: [],
        controller: new AbortController(),
      };
      pendingOffer = offer;
      trace("ramp-approval-request", { id: m.id, peer: from });
      update({ status: "Approve the connection test on this device" });
      offer.timer = setTimeout(() => {
        if (pendingOffer === offer) stop("Test approval timed out");
      }, 60000);
      let accepted;
      try {
        accepted = await onRequest(from, m.settings, {
          signal: offer.controller.signal,
        });
      } finally {
        clearTimeout(offer.timer);
      }
      if (pendingOffer !== offer || offer.controller.signal.aborted || closed)
        return;
      pendingOffer = null;
      trace("ramp-approval-result", {
        id: m.id,
        peer: from,
        admitted: !!accepted,
      });
      if (!accepted) {
        send(from, { kind: "reject", id: m.id });
        update({ status: "Test declined" });
        return;
      }
      if (active || !ready(from)) {
        update({ status: "Test peer unavailable" });
        return;
      }
      // Settings are deliberately fixed on the accepting endpoint as well.
      if (
        JSON.stringify(m.settings) !== JSON.stringify(settings) &&
        JSON.stringify(m.settings) !== JSON.stringify(extendedSettings) &&
        JSON.stringify(m.settings) !== JSON.stringify(comparisonSettings)
      ) {
        send(from, { kind: "reject", id: m.id });
        update({ status: "Unsupported test settings" });
        return;
      }
      const s = create(m.id, from, false, m.settings);
      await s.pc.setRemoteDescription(m.description);
      for (const c of offer.ice) await s.pc.addIceCandidate(c);
      await s.pc.setLocalDescription(await s.pc.createAnswer());
      send(from, {
        kind: "answer",
        id: m.id,
        description: s.pc.localDescription,
      });
      s.signaled = true;
      for (const candidate of s.outIce.splice(0))
        send(from, { kind: "ice", id: m.id, candidate });
      return;
    }
    if (
      m.kind === "complete" &&
      lastSession?.id === m.id &&
      lastSession.peer === from &&
      !lastSession.initiator &&
      (!active || active === lastSession)
    ) {
      const s = lastSession;
      clearTimeout(s.timer);
      lastReport = m.report;
      s.finished = true;
      active = null;
      update({ status: "Complete", report: lastReport });
      s.meter.close();
      s.dc.close();
      s.pc.close();
      return;
    }
    const s = active;
    if (!s || s.id !== m.id || s.peer !== from) return;
    if (m.kind === "answer" && s.initiator) {
      update({ status: "Connecting the test channel" });
      await s.pc.setRemoteDescription(m.description);
      for (const c of s.ice.splice(0)) await s.pc.addIceCandidate(c);
    }
    if (m.kind === "ice") {
      if (s.pc.remoteDescription) await s.pc.addIceCandidate(m.candidate);
      else if (s.ice.length < 64) s.ice.push(m.candidate);
    }
    if (m.kind === "reject" || m.kind === "stop") {
      stop(
        m.kind === "reject"
          ? "Peer declined the test"
          : "Peer stopped the test",
      );
    }
    if (m.kind === "reverse" && !s.initiator && !s.reversing) {
      s.reversing = true;
      s.report.path = await selectedPath(s.pc);
      const result = await s.meter.run({
        ...s.settings,
        signal: s.controller.signal,
        onStage: (row) =>
          update({ status: "Ramp up/down: this device → peer", row }),
      });
      s.resultSent = true;
      s.timer = setTimeout(() => {
        if (active === s) stop("Final test report not received");
      }, 10000);
      send(from, {
        kind: "result",
        id: m.id,
        result,
        agent: runtime.navigator?.userAgent,
        path: s.report.path,
      });
    }
    if (m.kind === "result" && s.initiator && s.resolve) {
      s.report.directions.push({ direction: "peer-to-initiator", ...m.result });
      s.report.remoteAgent = m.agent;
      s.report.remotePath = m.path;
      const done = s.resolve;
      s.resolve = null;
      done();
    }
  }
  const off = network.on("message", (message) => {
    void handle(message).catch((error) => stop(error.message));
  });
  return {
    async start(peer, { extended = false, comparePacing = false } = {}) {
      if (closed || active || pendingOffer || !ready(peer))
        throw Error(
          "Choose one ready peer and finish active file transfers first",
        );
      const selected = comparePacing
        ? comparisonSettings
        : extended
          ? extendedSettings
          : settings;
      const s = create(crypto.randomUUID(), peer, true, selected);
      trace("ramp-start", { id: s.id, peer });
      update({ status: "Waiting for peer approval" });
      try {
        await s.pc.setLocalDescription(await s.pc.createOffer());
        send(peer, {
          kind: "offer",
          id: s.id,
          description: s.pc.localDescription,
          settings: selected,
        });
        s.signaled = true;
        for (const candidate of s.outIce.splice(0))
          send(peer, { kind: "ice", id: s.id, candidate });
      } catch (error) {
        stop(error.message);
        throw error;
      }
    },
    cancel(reason = "Test stopped by user") {
      stop(reason);
    },
    getReport: () => lastReport,
    close() {
      stop("Session ended");
      closed = true;
      off();
    },
  };
}
