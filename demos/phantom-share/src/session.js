import { createAdminAuthorization as sa } from "../../../packages/phantom-core/src/authorization.js";
var ZT = "phantom-share/1",
  GT = (e) => typeof e == "string" && /^[0-9a-f]{16}$/.test(e);
function WT(e, a) {
  let o = e.filter((l) => a.get(l.id) === "direct");
  return (o.length ? o : e.filter((l) => a.get(l.id) !== "exclude"))
    .map((l) => l.id)
    .sort();
}
function YT({
  network: e,
  admin: a,
  adminPublicKey: o,
  onChange: l = () => {},
  onError: p = () => {},
  approvalTimeoutMs: u = 3e4,
}) {
  let h = new Map(),
    g = new Map(),
    b = new Set(),
    C = [],
    w = new Map(),
    H = !1,
    N = null,
    preparing = null,
    Z = () => e.getState().peerId,
    B = () =>
      e
        .getPeerConnections()
        .filter((P) => P.state === "ready")
        .map((P) => P.peerId),
    z = sa({ networkId: e.networkId, adminPublicKey: o }),
    V = (P, tt) => {
      !H && B().includes(P) && e.send(P, JSON.stringify({ app: ZT, ...tt }));
    },
    U = () => {
      H || l();
    };
  function j(P) {
    return {
      id: P.id,
      name: P.name,
      size: P.size,
      type: P.type,
      total: P.total,
      recipients: P.recipients,
      proof: P.proof,
    };
  }
  async function I(P) {
    if (a) {
      let tt = await a.sign({
        networkId: e.networkId,
        action: "content.publish",
        target: "phantom-share-owner:" + Z(),
        operationId: crypto.randomUUID(),
      });
      V(P, { type: "owner", proof: tt });
    }
    for (let tt of h.values())
      tt.proof &&
        tt.recipients.includes(P) &&
        V(P, { type: "asset", asset: j(tt) });
  }
  function S(P, tt) {
    if (
      ((P.blob = tt),
      (P.received = P.total),
      (P.status = "received"),
      !b.has(P.id))
    ) {
      b.add(P.id);
      for (let et of B()) V(et, { type: "receipt", id: P.id });
    }
    U();
  }
  (C.push(
    e.on("peerConnection", (P) => {
      (P.state === "ready" &&
        (V(P.peerId, { type: "hello" }), I(P.peerId).catch(p)),
        U());
    }),
  ),
    C.push(
      e.on("distributionFileEvent", (P) => {
        if (P.fileEvent === "filePreparationProgress" && preparing) {
          preparing.received = P.processed;
          preparing.total = P.total;
          U();
          return;
        }
        if (P.fileEvent !== "fileOffer") return;
        let tt = P.manifest,
          et = h.get(tt.id);
        (et ||
          ((et = { ...tt, recipients: [], received: 0, status: "receiving" }),
          h.set(tt.id, et)),
          Object.assign(et, {
            name: tt.name,
            size: tt.size,
            type: tt.type,
            total: tt.total,
          }),
          U());
      }),
    ),
    C.push(
      e.on("distributionProgress", (P) => {
        let tt = h.get(P.id);
        tt &&
          ((tt.received = P.received),
          (tt.total = P.total),
          (tt.status = "receiving"),
          U());
      }),
    ),
    C.push(
      e.on("distributionComplete", (P) => {
        let tt = h.get(P.id);
        (tt ||
          ((tt = {
            id: P.id,
            name: "Received file",
            size: P.blob.size,
            total: 1,
            received: 1,
            recipients: [],
          }),
          h.set(P.id, tt)),
          S(tt, P.blob));
      }),
    ),
    C.push(
      e.on("distributionConnectionFailed", () =>
        p(Error("Transfer interrupted. Phantom is attempting recovery.")),
      ),
    ),
    C.push(
      e.on("message", ({ from: P, text: tt }) => {
        O(P, tt).catch(p);
      }),
    ));
  async function O(P, tt) {
    if (H || !B().includes(P) || typeof tt != "string" || tt.length > 64e3)
      return;
    let et;
    try {
      et = JSON.parse(tt);
    } catch {
      return;
    }
    if (et?.app === ZT) {
      if (et.type === "hello") {
        await I(P);
        return;
      }
      if (et.type === "owner") {
        let it = await z.verify(et.proof);
        it?.target === "phantom-share-owner:" + P &&
          it.action === "content.publish" &&
          (N = P);
        return;
      }
      if (et.type === "authorize" && a) {
        let it = Date.now(),
          T = w.get(P) || 0;
        if (it - T < 500) {
          V(P, { type: "denied", request: et.request });
          return;
        }
        if (
          (w.set(P, it),
          typeof et.request != "string" ||
            et.request.length > 100 ||
            !/^sha256:[0-9a-f]{64}$/.test(et.id) ||
            !Array.isArray(et.recipients) ||
            !et.recipients.length ||
            et.recipients.length > 128 ||
            !et.recipients.every(
              (st) => GT(st) && e.getPeers().some((ct) => ct.hKey === st),
            ))
        )
          return;
        let At = await a.sign({
          networkId: e.networkId,
          action: "content.publish",
          target: et.id,
          operationId: crypto.randomUUID(),
          recipients: et.recipients,
        });
        V(P, { type: "authorization", request: et.request, proof: At });
        return;
      }
      if (et.type === "authorization") {
        let it = g.get(et.request);
        if (!it) return;
        let T = await z.verify(et.proof);
        if (
          !T ||
          T.action !== "content.publish" ||
          T.target !== it.id ||
          JSON.stringify(T.recipients) !== JSON.stringify(it.recipients)
        )
          return;
        (g.delete(et.request), clearTimeout(it.timer), it.resolve(T));
        return;
      }
      if (et.type === "denied") {
        let it = g.get(et.request);
        it &&
          (g.delete(et.request),
          clearTimeout(it.timer),
          it.reject(
            Error("Please wait a moment before sending the next file."),
          ));
        return;
      }
      if (et.type === "asset") {
        let it = et.asset;
        if (
          !it ||
          !/^sha256:[0-9a-f]{64}$/.test(it.id) ||
          typeof it.name != "string" ||
          it.name.length > 256 ||
          !Number.isSafeInteger(it.size) ||
          it.size < 0 ||
          it.size > 1073741824 ||
          !Number.isInteger(it.total) ||
          it.total < 0 ||
          it.total > 65536
        )
          return;
        let T = await z.verify(it.proof);
        if (
          !T ||
          T.target !== it.id ||
          T.action !== "content.publish" ||
          !T.recipients?.includes(Z())
        )
          return;
        let At = h.get(it.id);
        if (!At && h.size >= 32) return;
        let st = At ?? { received: 0, status: "receiving" };
        (Object.assign(st, {
          id: it.id,
          name: it.name,
          size: it.size,
          type: String(it.type || ""),
          total: it.total,
          recipients: T.recipients,
          proof: T,
        }),
          h.set(it.id, st));
        try {
          (S(st, await e.getDistributedFile(it.id)),
            V(P, { type: "receipt", id: st.id }));
        } catch {}
        U();
        return;
      }
      if (et.type === "receipt") {
        let it = h.get(et.id);
        it?.outgoing &&
          it.recipients.includes(P) &&
          (it.delivered.add(P),
          (it.status =
            it.delivered.size === it.recipients.length ? "sent" : "sharing"),
          U());
      }
    }
  }
  function Y(P, tt) {
    if (a)
      return a.sign({
        networkId: e.networkId,
        action: "content.publish",
        target: P,
        operationId: crypto.randomUUID(),
        recipients: tt,
      });
    if (!N || !B().includes(N))
      throw Error(
        "The network owner must be connected to authorize a new file.",
      );
    return new Promise((et, it) => {
      let T = crypto.randomUUID(),
        At = setTimeout(() => {
          (g.delete(T),
            it(Error("Owner authorization timed out. Please retry.")));
        }, u);
      (g.set(T, { id: P, recipients: tt, resolve: et, reject: it, timer: At }),
        V(N, { type: "authorize", request: T, id: P, recipients: tt }));
    });
  }
  async function W(P, tt) {
    if (H) throw Error("Network closed");
    if (
      ((tt = [...new Set(tt)].sort()),
      !tt.length || !tt.every((At) => GT(At) && B().includes(At)))
    )
      throw Error("Select connected recipients before sending.");
    if (P.size > 1073741824)
      throw Error("This version supports files up to 1 GiB.");
    if (h.size >= 32)
      throw Error(
        "This session has reached its 32-file limit. Start a new session.",
      );
    if (preparing)
      throw Error("Wait for the current file preparation to finish.");
    const temporaryId = "preparing:" + crypto.randomUUID();
    const T = {
      id: temporaryId,
      name: P.name || "File",
      type: P.type,
      size: P.size,
      total: Math.ceil(P.size / 65536),
      recipients: tt,
      received: 0,
      outgoing: true,
      delivered: new Set(),
      status: "preparing",
      blob: P,
    };
    preparing = T;
    h.set(temporaryId, T);
    U();
    try {
      const prepared = await e.prepareDistributionFile(P, {
        distributionId: crypto.randomUUID(),
        chunkSize: 65536,
      });
      if (H) throw Error("Network closed during file preparation.");
      h.delete(temporaryId);
      Object.assign(T, prepared, { status: "authorizing", received: 0 });
      h.set(T.id, T);
      preparing = null;
      U();
      const authorization = await Y(T.id, tt);
      if (H) throw Error("Network closed during file authorization.");
      T.proof = authorization;
      T.status = "sharing";
      U();
      await e.distributeFile(P, { prepared, recipients: tt, authorization });
      if (H) throw Error("Network closed during file publication.");
      for (const peer of tt) V(peer, { type: "asset", asset: j(T) });
      U();
      return T;
    } catch (error) {
      T.status = "failed";
      T.error = error.message;
      U();
      throw error;
    } finally {
      if (preparing === T) preparing = null;
    }
  }

  // Sessions may be attached after the transport is already ready.
  queueMicrotask(() => {
    if (H) return;
    for (const peer of B()) {
      V(peer, { type: "hello" });
      I(peer).catch(p);
    }
  });
  return {
    assets: h,
    publish: W,
    close() {
      H = !0;
      for (let P of C) P();
      for (let P of g.values())
        (clearTimeout(P.timer), P.reject(Error("Network closed")));
      g.clear();
    },
  };
}
export { WT as selectRecipients, YT as createShareSession };
