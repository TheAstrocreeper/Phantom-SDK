import {acceptsSuccessorStatus} from './successor-status.js';
import { canCarrySysRecovery } from './recovery-signaling.js';
import { resumeDeferredAdmissions } from './deferred-admission.js';
import { sysBirthDisposition, isCurrentSysBirth, runSysBirth, completeSysBirth, armSysBirthDeadline } from './sys-birth.js';
import { acceptsSuccessorHandshake } from './successor-handshake.js';
// Extracted from SIX7 2.0.6; original state-machine functions retained.
// Compatibility checkpoint: legacy packet names and deadlines remain intentional.
export function createExtractedEngine(host) {
  const { window, document, localStorage, sessionStorage, location,
    WebSocket, RTCPeerConnection, crypto, setTimeout, clearTimeout,
    setInterval, clearInterval } = host;

  "use strict";
  const CORE_DEBUG = false;

  const TAG = "[six7][CORE]";

  // ---- Config ----
  const DEFAULT_BOOTSTRAP_WS = host.doorwayUrl;
  const TOFU_ORIGIN_KEY = "six7_tofu_origin";
  const TOFU_HKEY_KEY = "six7_tofu_hkey";
  const TOFU_FIRST_KEY = "six7_tofu_firstSeenAt";
  const KEYSTORE_KEY = "six7_keystore_v1";

  // ---- State ----
  let ctx = null;
  let enabled = false;

  let selfHandle = null;
  let selfHex = null;
  let selfHKey = null;
  let selfAvatar = null;


  // ---- Keystore (v1) ----
  let ks = { v: 1, self: null, hexToHKey: [], nameToHKey: [] };
  let ksHexToHKey = new Map();
  let ksNameToHKey = new Map();

  let origin = null;
  let videoId = null;

  let bootOnce = false;
  let six7BootId = crypto.randomUUID();
  const six7RemoteBootIds = new Map();
  const six7RemoteEndpointIds = new Map();
  let six7SysTransportAttemptSeq = 0;
  const six7DoorwayAdmittedPeers = new Set();
  // Terminal membership commands are authority evidence waiting for the
  // current or successor syscon1 to commit the corresponding removal.
  const six7PendingTerminalExits = new Map(); // hKey -> { bootId, epoch, at, via }
  let ws = null;

  let tofu = {
    pinned: false,
    origin: null,
    hKey: null,
    firstSeenAt: null,
    mismatch: false,
    reason: null,
  };

  let bootstrap = {
    url: DEFAULT_BOOTSTRAP_WS,
    state: "idle", // idle|connecting|open|closed|failed
    ok: false,
    openAt: null,
    closeAt: null,
    connectAt: null,
    attempts: 0,
    lastErr: null,
    lastMsg: null,
    lastCloseCode: null,
    lastCloseReason: null,
    lastCloseClean: null,
    lastCloseClass: null,

    recoveryStartedAt: null,
    recoveredAt: null,
    recoveryMs: null,
    recoveryAttemptId: null,

    doorwayAttemptId: 0,
    doorwayDeadlinePhase: "none",
    doorwayDeadlineAt: 0,
    doorwayDeadlineObservedAt: 0,
    doorwayDeadlineObservedBy: "none",

    lastSendType: "none",
    lastSendAt: null,
    lastSendOk: null,

    cfHits: {
      total: 0,
      doorway: 0,
      authority: 0,
      carrier: 0,
      fallback: 0,
      dirty: 0,
      lastDir: "none",
      lastType: "none",
      lastBucket: "none",
      lastReason: "none",
      lastAt: 0,
    },
  };

  let wsConnectWatchdog = null;
  let wsDoorwayRecovering = false;
  let wsDoorwayBirthFailures = 0;
  let wsDoorwayRecoveryExhausted = false;
  const SIX7_WS_CONNECT_DEADLINE_MS = 10000;
  const SIX7_WS_READY_DEADLINE_MS = 10000;
  const SIX7_WS_WATCH_TICK_MS = 1000;

  // ======================================================
  // CONCIERGE / SYS v0 — diagnostic state only
  // No routing changes yet.
  // ======================================================
  let concierge = {
    mode: "bootstrap", // bootstrap | sys | fallback
    role: "peer",
    roleDemoted: false,
    roleDemotedBy: "none",
    roleDemotedEpoch: 0,
    roleDemotedAt: 0,
    route: "bootstrap_ws",
    routeReason: "initial",
    routeMode: "bootstrap",
    lastRouteClass: "none",
    lastRouteMode: "bootstrap",
    lastRouteAt: 0,
    lastRouteType: "none",
    lastRouteReason: "none",
    wouldRoute: "bootstrap_ws",
    wouldRouteReason: "none",

    sysconEpoch: 0,
    roomJoinedAt: 0,

    cfConciergeState: "none",
    cfConciergeAt: 0,
    cfConciergeSyscon1: null,
    cfConciergeSyscon2: null,
    cfConciergeSyscon1EndpointId: null,
    cfConciergeSyscon2EndpointId: null,
    cfConciergeEpoch: 0,

    syscon1: null,
    syscon2: null,

    syscon1Label: "none",
    syscon2Label: "none",
    localRoleCandidate: "peer",
    candidateReason: "not_computed",
    lastCandidateAt: 0,

    sysEligible: false,
    sysReady: false,
    sysUnavailableReason: "not_joined",
    localRoleReason: "peer_default",
    lastSysRefreshAt: 0,
    lastSysRefreshReason: "none",

    lastRelationshipTxAt: 0,
    lastRelationshipTxType: "none",
    lastRelationshipRoute: "none",
    lastRelationshipReason: "none",
    lastRelationshipRxAt: 0,
    lastRelationshipRxType: "none",
    lastRelationshipRxVia: "none",

    lastRtcTxAt: 0,
    lastRtcTxType: "none",
    lastRtcTxRoute: "none",
    lastRtcTxMode: "none",
    lastRtcTxReason: "none",
    lastRtcWouldRoute: "none",
    lastRtcWouldReason: "none",
    lastRtcRxAt: 0,
    lastRtcRxType: "none",
    lastRtcRxVia: "none",

    lastRtcIceCandidateAt: 0,
    lastRtcIceCandidateState: "none",
    lastRtcIceCandidateCount: 0,
    lastRtcIceCompleteAt: 0,

    lastRtcIceTxAt: 0,
    lastRtcIceTxRoute: "none",
    lastRtcIceTxMode: "none",
    lastRtcIceTxReason: "none",

    lastRtcIceRxAt: 0,
    lastRtcIceRxVia: "none",

    sysPipeState: "missing", // missing | connecting | open | ready | closed | error
    sysPeer: null,
    sysReadyPeer: null,
    sysReadyAt: 0,
    sysReadyProof: "none",
    sysReadyEpoch: 0,
    sysLastTxAt: 0,
    sysLastTxType: "none",
    sysLastRxAt: 0,
    sysLastRxType: "none",
    sysLastErr: null,

    // Temporary chat-pipe soak diagnostics.
    chatDropCount: 0,
    chatReconnectCount: 0,
    chatLastDropAt: 0,
    chatLastReconnectAt: 0,
    chatLastDropPeer: null,
    chatLastReconnectPeer: null,
  };
  const SIX7_EMPTY_CONCIERGE = JSON.parse(JSON.stringify(concierge));

  const six7SeenRtcSignals = new Map();
  const SIX7_RTC_SIGNAL_MAX = 500;

  const six7PendingRtcIce = new Map();
  const SIX7_RTC_ICE_QUEUE_MAX = 24;

  const six7PendingRelationship = new Map(); // hKey -> [{ msg, type, at }]
  const SIX7_REL_PENDING_MAX = 12;
  const six7PendingSysRelay = new Map(); // destination hKey -> SYS_CONTROL envelopes
  const SIX7_SYS_RELAY_PENDING_MAX = 24;
  const six7PendingDm = new Map(); // hKey -> [{ text, sys, file, at }]
  const SIX7_DM_PENDING_MAX = 20;
  const six7PendingRefreshRelationships = new Set();
  let six7PendingRefreshRoom = "";
  const six7PendingIncomingFileOffers = new Map(); // hKey -> Map(fileId -> file)

  // Temporary SYS is an existing SYS transport in transaction mode. It is
  // used only when no room SYS route exists, carries one allowlisted control
  // transaction, then collapses after acknowledgment.
  const six7TemporarySysTransactions = new Map(); // hKey -> transaction
  const SIX7_TEMP_SYS_PURPOSES = new Set(["relationship_unblock"]);

  // Gossip is never authority. It is only a remembered route hint used to
  // find the accepted syscons and request normal SYS authority again.
  const six7AuthorityGossipHints = new Map(); // source hKey -> accepted snapshot hint
  let six7GossipRecovery = {
    source: null,
    targets: [],
    index: 0,
    epoch: 0,
    rosterSeq: 0,
    at: 0,
  };

  function six7RtcSignalKey(msg, via = "") {
    if (!msg || typeof msg !== "object") return "";

    const type = String(msg.type || "");
    const from = six7ShortKey(msg.from);
    const to = six7ShortKey(msg.to);

    if (!six7IsRtcSignalType(type) || !from || !to) return "";

    if (type === "rtc_ice") {
      const cand = msg.cand || {};
      return [
        type,
        String(msg.pipe || (msg.sysCarrier === true ? "sys" : "chat")),
        from,
        to,
        String(cand.candidate || ""),
        String(cand.sdpMid || ""),
        String(cand.sdpMLineIndex ?? ""),
        String(msg.sysTxId || ""),
        String(via || ""),
      ].join("|");
    }

    return "";
  }

  function six7RememberRtcSignal(msg, via = "") {
    const key = six7RtcSignalKey(msg, via);
    if (!key) return false;

    if (six7SeenRtcSignals.has(key)) {
      concierge.lastErr =
        "rtc_duplicate_ignored:" + String(msg?.type || "unknown");
      emitSnapshot("rtc_duplicate_ignored_" + String(msg?.type || "unknown"));
      return true;
    }

    six7SeenRtcSignals.set(key, now());

    if (six7SeenRtcSignals.size > SIX7_RTC_SIGNAL_MAX) {
      const rows = Array.from(six7SeenRtcSignals.entries()).sort(
        (a, b) => Number(a[1] || 0) - Number(b[1] || 0),
      );

      while (
        six7SeenRtcSignals.size > Math.floor(SIX7_RTC_SIGNAL_MAX * 0.75) &&
        rows.length
      ) {
        six7SeenRtcSignals.delete(rows.shift()[0]);
      }
    }

    return false;
  }

  function six7IsRtcSignalType(type) {
    type = String(type || "");
    return type === "rtc_offer" || type === "rtc_answer" || type === "rtc_ice";
  }

  function six7MarkControlTx(type, route = concierge.route) {
    concierge.lastTxAt = now();
    concierge.lastTxType = String(type || "unknown");
    concierge.route = String(route || concierge.route || "bootstrap_ws");
  }

  function six7MarkControlRx(type) {
    concierge.lastRxAt = now();
    concierge.lastRxType = String(type || "unknown");
  }

  function six7ControlClassFor(type) {
    type = String(type || "unknown");

    if (
      type === "hello" ||
      type === "status_request" ||
      type === "status_response" ||
      type === "roster_request" ||
      type === "concierge_connect" ||
      type === "doorway_reconcile"
    ) {
      return "bootstrap_core";
    }

    if (type === "presence") {
      return "presence";
    }

    if (
      type === "veil_invite" ||
      type === "veil_accept" ||
      type === "veil_deny" ||
      type === "veil_status" ||
      type === "veil_typing" ||
      type === "veil_chat_failed" ||
      type === "sys_block" ||
      type === "sys_unblock" ||
      type === "sys_unblock_ack"
    ) {
      return "relationship_control";
    }

    if (type === "rtc_offer" || type === "rtc_answer" || type === "rtc_ice") {
      return "rtc_signal";
    }

    return "unknown";
  }

  function six7ControlClassMode(cls, type = "") {
    cls = String(cls || "unknown");
    type = String(type || "unknown");

    if (cls === "bootstrap_core") return "bootstrap";
    if (cls === "presence") return "sys_primary";
    if (cls === "relationship_control") return "sys_primary";

    if (cls === "rtc_signal") {
      if (type === "rtc_offer" || type === "rtc_answer" || type === "rtc_ice") {
        return "sys_primary";
      }

      return "observe";
    }

    return "bootstrap";
  }

  function six7NewSysTransportAttemptId(remoteHKey) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return "";

    six7SysTransportAttemptSeq += 1;

    return [six7BootId, key, six7SysTransportAttemptSeq].join(":");
  }

  function six7CommittedMember(remoteHKey) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    return six7AuthorityRosterRows(six7LocalCommittedAuthority()?.roster).some(
      (row) => row.hKey === key,
    );
  }

  function six7RetireRemoteIncarnationTransport(
    remoteHKey,
    reason = "remote_incarnation_changed",
  ) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    const oldChatEnt = six7ChatPeers.get(key);
    const oldSysEnt = six7SysPeers.get(key);

    six7CancelReconnect(key);
    reconnectingPeers.delete(key);

    const reconnectDeadline = six7ChatReconnectDeadlines.get(key);
    if (reconnectDeadline) clearTimeout(reconnectDeadline);
    six7ChatReconnectDeadlines.delete(key);

    rtcClosing.add(key);
    six7ChatPeers.delete(key);
    six7SysPeers.delete(key);
    rtcDialing.delete(key);
    six7SysDialing.delete(key);
    six7SysPreflightUnstableRetries.delete(key);
    six7PendingRtcIce.delete(key);
    six7SysLastGoodRx.delete(key);
    six7SysAdmissionLastSent.delete(key);

    if (oldChatEnt) {
      oldChatEnt.dc = null;
      try {
        oldChatEnt.pc?.close();
      } catch {}
    }

    if (oldSysEnt) {
      oldSysEnt.sysdc = null;
      oldSysEnt.sysAttemptId = null;
      oldSysEnt.ownerAttemptId = null;
      oldSysEnt.sysCarrier = false;
      try {
        oldSysEnt.pc?.close();
      } catch {}
    }

    rtcClosing.delete(key);

    if (six7ShortKey(concierge.sysReadyPeer) === key) {
      concierge.sysReady = false;
      concierge.sysReadyPeer = null;
      concierge.sysReadyAt = 0;
      concierge.sysReadyProof = "none";
      concierge.sysReadyEpoch = 0;
    }

    if (
      six7ShortKey(concierge.sysPeer) === key ||
      six7ShortKey(six7SysTargetFromCandidates()) === key
    ) {
      concierge.sysPeer = key;
      concierge.sysPipeState = "missing";
      concierge.sysUnavailableReason = String(
        reason || "remote_incarnation_changed",
      );
    }

    emitSnapshot(
      "remote_incarnation_transport_retired_" + String(reason || "changed"),
    );
    return true;
  }

  function six7ObserveRemoteIncarnation(remoteHKey, observation = {}) {
    const key = six7ShortKey(remoteHKey);
    const self = six7ShortKey(selfHKey);
    if (!key || key === self) return { changed: false, committed: false };

    const incomingBootId = String(observation.bootId || "");
    const incomingEndpointId = String(observation.endpointId || "");
    const incomingAttemptId = String(observation.attemptId || "");
    const signalType = String(observation.signalType || "");
    const bindCurrent = observation.bindCurrent === true;
    const via = String(observation.via || "remote_incarnation");

    const previousBootId = String(six7RemoteBootIds.get(key) || "");
    const previousEndpointId = String(six7RemoteEndpointIds.get(key) || "");
    const currentEnt = six7SysPeers.get(key);
    const currentAttemptId = String(
      currentEnt?.attempt?.id || currentEnt?.sysAttemptId || "",
    );
    const currentPipeOpen = six7HasOpenSysPipe(key);

    const bootChanged = !!(
      incomingBootId &&
      previousBootId &&
      incomingBootId !== previousBootId
    );

    const endpointChanged = !!(
      !bootChanged &&
      !incomingBootId &&
      incomingEndpointId &&
      previousEndpointId &&
      incomingEndpointId !== previousEndpointId &&
      !currentPipeOpen
    );

    const offerAttemptChanged = !!(
      signalType === "rtc_offer" &&
      incomingAttemptId &&
      currentAttemptId &&
      incomingAttemptId !== currentAttemptId &&
      !currentPipeOpen
    );

    if (incomingBootId) six7RemoteBootIds.set(key, incomingBootId);
    if (incomingEndpointId) {
      six7RemoteEndpointIds.set(key, incomingEndpointId);
    }

    const changed = bootChanged || endpointChanged || offerAttemptChanged;
    const committed = six7CommittedMember(key);

    if (!changed) {
      if (bindCurrent && currentEnt && incomingBootId) {
        currentEnt.remoteBootId = incomingBootId;
      }
      return { changed: false, committed };
    }

    if (bindCurrent) {
      if (currentEnt && incomingBootId) {
        currentEnt.remoteBootId = incomingBootId;
      }

      if (committed) {
        six7SupersedeSysReplacementForCommittedRefresh(
          key,
          previousBootId || previousEndpointId || currentAttemptId,
          incomingBootId || incomingEndpointId || incomingAttemptId,
        );
      }

      emitSnapshot("remote_incarnation_bound_current_transport_" + via);
      return { changed: true, committed };
    }

    if (committed) {
      six7SupersedeSysReplacementForCommittedRefresh(
        key,
        previousBootId || previousEndpointId || currentAttemptId,
        incomingBootId || incomingEndpointId || incomingAttemptId,
      );

      const relationship = six7Relationships.get(key);
      if (relationship && !relationship.endedAt) {
        relationship.remoteBootId = incomingBootId || null;
        relationship.updatedAt = now();
        six7Relationships.set(key, relationship);
      }

      emitSnapshot("committed_member_remote_incarnation_transition");
    } else if (bootChanged || endpointChanged) {
      // A new transport for an absent member is admission evidence only.
      // Membership-exit latches remain closed until a committed authority
      // snapshot explicitly adds this new incarnation.
      emitSnapshot("fresh_incarnation_waiting_for_committed_admission");
    }

    six7RetireRemoteIncarnationTransport(key, "remote_incarnation_" + via);

    return { changed: true, committed };
  }

  function six7RefreshSysReadiness(reason = "refresh") {
    const wasSysReady = concierge.sysReady === true;

    const hasRoom = !!videoId;
    const hasSelf = !!selfHKey;
    const hasRoster = !!lastRoster && lastRoster.size > 0;

    const readyPeer = six7ShortKey(concierge.sysReadyPeer);
    const expectedPeer = six7ShortKey(six7SysTargetFromCandidates());

    // SYS truth is derived from the legal concierge target's actual datachannel.
    const targetPeer = expectedPeer;

    const roleNow = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const epochNow = Number(concierge.sysconEpoch || 0);

    const cfSyscon1 = six7ShortKey(concierge.cfConciergeSyscon1);
    const cfSyscon2 = six7ShortKey(concierge.cfConciergeSyscon2);

    let targetEndpointId = "";

    if (targetPeer && targetPeer === cfSyscon1) {
      targetEndpointId = String(concierge.cfConciergeSyscon1EndpointId || "");
    } else if (targetPeer && targetPeer === cfSyscon2) {
      targetEndpointId = String(concierge.cfConciergeSyscon2EndpointId || "");
    }

    const attemptId = [epochNow, roleNow, targetPeer || "none"].join(":");
    if (!concierge.sysAttempt) {
      concierge.sysAttempt = {
        id: "",
        epoch: 0,
        role: "peer",
        target: null,
        targetEndpointId: null,
        state: "idle",
        startedAt: 0,
        lastEvent: "none",
        faultReason: "none",
        recoveryCount: 0,
      };
    }

    if (concierge.sysAttempt.id && concierge.sysAttempt.id !== attemptId) {
      const oldAttemptId = concierge.sysAttempt.id;
      const oldTarget = six7ShortKey(concierge.sysAttempt.target);

      const oldEnt = oldTarget ? six7SysPeers.get(oldTarget) : null;
      const oldSysDc = oldEnt?.sysdc || null;

      concierge.sysAttempt.state = "superseded";
      concierge.sysAttempt.lastEvent = "authority_or_target_changed";

      six7SysDialing.delete(oldTarget);
      six7SysPreflightUnstableRetries.delete(oldTarget);

      /*
       * A SYS attempt is superseded only by a real authority/role/target change.
       * CF endpoint replacement is doorway metadata and never retires the
       * established peer carrier.
       */
      if (oldEnt && oldTarget === targetPeer) {
        oldEnt.ownerAttemptId = attemptId;
        emitSnapshot("sys_birth_preserved_same_target");
      } else if (oldEnt && oldSysDc && oldEnt.ownerAttemptId === oldAttemptId) {
        oldEnt.sysdc = null;
        oldEnt.sysAttemptId = null;
        oldEnt.ownerAttemptId = null;
        oldEnt.sysCarrier = false;

        try {
          oldSysDc.close();
        } catch {}

        emitSnapshot("superseded_sys_birth_retired");
      }
    }

    if (attemptId !== "0:peer:none" && concierge.sysAttempt.id !== attemptId) {
      concierge.sysAttempt = {
        id: attemptId,
        epoch: epochNow,
        role: roleNow,
        target: targetPeer || null,
        targetEndpointId: targetEndpointId || null,
        state: targetPeer ? "connecting" : "idle",
        startedAt: now(),
        lastEvent: String(reason || "refresh"),
        faultReason: "none",
        recoveryCount: 0,
      };
    } else {
      concierge.sysAttempt.lastEvent = String(reason || "refresh");

      if (targetPeer && concierge.sysAttempt.id === attemptId) {
        concierge.sysAttempt.target = targetPeer;
        concierge.sysAttempt.targetEndpointId = targetEndpointId || null;
      }
    }

    const ent = targetPeer ? six7SysPeers.get(targetPeer) : null;
    const dcState = String(ent?.sysdc?.readyState || "");

    const sysConnecting = dcState === "connecting";
    const sysOpen = dcState === "open";

    const readyEpoch = Number(concierge.sysReadyEpoch || 0);
    const currentEpoch = Number(concierge.sysconEpoch || 0);

    const targetMatches = !!(
      targetPeer &&
      readyPeer &&
      readyPeer === targetPeer
    );

    const epochMatches = !!(
      currentEpoch &&
      readyEpoch &&
      readyEpoch === currentEpoch
    );

    const staleProof = !!(
      readyPeer &&
      (!targetPeer || readyPeer !== targetPeer || !epochMatches)
    );

    if (staleProof) {
      concierge.sysReadyPeer = null;
      concierge.sysReadyAt = 0;
      concierge.sysReadyProof = "none";
      concierge.sysReadyEpoch = 0;
    }

    const hasReadyProof = !!(
      targetPeer &&
      readyPeer &&
      readyPeer === targetPeer &&
      targetMatches &&
      epochMatches &&
      concierge.sysReadyAt &&
      concierge.sysReadyProof !== "none"
    );

    concierge.sysEligible = !!(hasRoom && hasSelf);
    concierge.sysReady = !!(concierge.sysEligible && sysOpen && hasReadyProof);
    concierge.sysPeer = targetPeer || null;

    if (concierge.sysReady) {
      concierge.sysAttempt.state = "ready";
      concierge.sysAttempt.faultReason = "none";
      concierge.sysAttempt.recoveryCount = 0;
      concierge.sysPipeState = "ready";

      if (String(concierge.localRoleCandidate || "") === "peer") {
        const provenTarget = six7ShortKey(targetPeer);
        const primary = six7ShortKey(concierge.syscon1);

        six7PeerSysCarrier.target = provenTarget || null;

        if (provenTarget && provenTarget === primary) {
          six7PeerSysCarrier.primaryRecoveryUsed = false;
        }
      }

      // Diagnostic normalization only.
      // Once SYS is proven ready, stale pending/waiting labels are no longer useful.
      const staleSysWait =
        /six7_sys_pending|waiting_for_sys_pipe|waiting_for_target_sys_pipe|waiting_for_proof|sys_pipe_not_open|sys_pipe_connecting/i;

      if (staleSysWait.test(String(concierge.route || ""))) {
        concierge.route = "six7_sys";
        concierge.routeReason = "sys_ready";
        concierge.wouldRoute = "six7_sys";
        concierge.wouldRouteReason = "sys_ready";
      }

      if (staleSysWait.test(String(concierge.routeReason || ""))) {
        concierge.routeReason = "sys_ready";
      }

      if (staleSysWait.test(String(concierge.lastErr || ""))) {
        concierge.lastErr = null;
      }

      if (staleSysWait.test(String(concierge.lastRouteReason || ""))) {
        concierge.lastRouteReason = "sys_ready";
      }

      if (staleSysWait.test(String(concierge.lastRtcTxReason || ""))) {
        concierge.lastRtcTxReason = "sys_ready";
      }

      if (staleSysWait.test(String(concierge.lastRtcWouldReason || ""))) {
        concierge.lastRtcWouldReason = "sys_ready";
      }

      if (staleSysWait.test(String(concierge.lastRtcIceTxReason || ""))) {
        concierge.lastRtcIceTxReason = "sys_ready";
      }

      if (targetPeer && six7PendingRtcIce.has(targetPeer)) {
        six7FlushQueuedRtcIce(targetPeer, "sys_ready_flush_queued_ice");
      }
    } else if (sysOpen) {
      concierge.sysAttempt.state = "open";
      concierge.sysPipeState = "open";
    } else if (sysConnecting) {
      concierge.sysAttempt.state = "connecting";
      concierge.sysPipeState = "connecting";
    } else {
      if (targetPeer && concierge.sysAttempt.state !== "idle") {
        concierge.sysAttempt.state = "faulted";
        concierge.sysAttempt.faultReason = String(reason || "sys_pipe_missing");
      }
      concierge.sysPipeState = targetPeer ? "faulted" : "missing";
    }

    concierge.lastSysRefreshAt = now();
    concierge.lastSysRefreshReason = String(reason || "refresh");

    if (!hasRoom) {
      concierge.sysUnavailableReason = "missing_room";
    } else if (!hasSelf) {
      concierge.sysUnavailableReason = "missing_identity";
    } else if (!hasRoster) {
      concierge.sysUnavailableReason = "waiting_for_roster";
    } else if (!targetPeer) {
      concierge.sysUnavailableReason = "sys_target_missing";
    } else if (!sysOpen) {
      concierge.sysUnavailableReason = sysConnecting
        ? "sys_pipe_connecting"
        : "sys_pipe_not_open";
    } else if (!hasReadyProof) {
      concierge.sysUnavailableReason = "sys_pipe_waiting_for_proof";
    } else {
      concierge.sysUnavailableReason = "none";
    }

    if (!String(concierge.localRoleReason || "").startsWith("role_")) {
      concierge.localRoleReason = concierge.sysReady
        ? "sys_pipe_ready"
        : concierge.sysEligible
          ? "eligible_but_sys_not_ready"
          : concierge.sysUnavailableReason;
    }

    if (!wasSysReady && concierge.sysReady) {
      six7ReconcileRelationshipStatus("relationship_status_sys_ready");
    }

    six7RefreshSysFallbackIndicator();
  }

  function six7RefreshSysCandidates(rosterList = [], reason = "refresh") {
    const self = six7ShortKey(selfHKey);

    const sysAuthForRoster = six7LocalCommittedAuthority();
    const authorityRoster =
      sysAuthForRoster && Array.isArray(sysAuthForRoster.roster)
        ? sysAuthForRoster.roster
        : [];

    const sourceRoster = authorityRoster.length
      ? authorityRoster
      : Array.isArray(rosterList)
        ? rosterList
        : [];

    const rows = [];

    const pushCandidateRow = (row, fallbackHandle = "") => {
      const hKey = six7ShortKey(
        typeof row === "string" ? row : row?.hKey || row?.from,
      );
      if (!hKey || six7PendingTerminalExits.has(hKey)) return null;

      const old = six7PresenceBook.get(hKey) || {};

      const out = {
        hKey,
        handle:
          typeof row === "string"
            ? fallbackHandle || old.handle || ""
            : String(row?.handle || fallbackHandle || old.handle || ""),
        isSelf: !!self && hKey === self,
      };

      rows.push(out);
      return out;
    };

    for (const p of sourceRoster) {
      pushCandidateRow(p);
    }

    const cfOne = six7ShortKey(concierge.cfConciergeSyscon1);
    const cfTwo = six7ShortKey(concierge.cfConciergeSyscon2);

    if (cfOne) pushCandidateRow({ hKey: cfOne }, "cf_syscon1");
    if (cfTwo && cfTwo !== cfOne)
      pushCandidateRow({ hKey: cfTwo }, "cf_syscon2");

    const authOne = six7ShortKey(sysAuthForRoster?.syscon1);
    const authTwo = six7ShortKey(sysAuthForRoster?.syscon2);

    if (authOne) pushCandidateRow({ hKey: authOne }, "auth_syscon1");
    if (authTwo && authTwo !== authOne) {
      pushCandidateRow({ hKey: authTwo }, "auth_syscon2");
    }

    if (self && !rows.some((x) => x.hKey === self)) {
      rows.push({
        hKey: self,
        handle: selfHandle || "",
        isSelf: true,
      });
    }

    const unique = Array.from(new Map(rows.map((x) => [x.hKey, x])).values());
    const byKey = new Map(unique.map((x) => [six7ShortKey(x.hKey), x]));

    let one = null;
    let two = null;
    let candidateSource = "hkey_fallback";

    const sysAuth = six7FreshSysAuthority();

    if (sysAuth) {
      const a = six7ShortKey(sysAuth.syscon1);
      const b = six7ShortKey(sysAuth.syscon2);

      if (a && byKey.has(a)) one = byKey.get(a);
      if (b && b !== a && byKey.has(b)) two = byKey.get(b);

      if (a && six7PendingTerminalExits.has(a) && b && byKey.has(b)) {
        one = byKey.get(b);
        two = unique.find(row => row.hKey !== b) || null;
        candidateSource = "syscon2_promoted_primary";
      } else if (one || two) {
        candidateSource = "sys_authority";
      }
    }

    const currentSelf = six7ShortKey(selfHKey);
    const oldOne = six7ShortKey(concierge.syscon1);
    const oldTwo = six7ShortKey(concierge.syscon2);

    if (
      !one &&
      oldOne &&
      oldTwo &&
      currentSelf &&
      oldTwo === currentSelf &&
      !byKey.has(oldOne)
    ) {
      one = byKey.get(currentSelf) || null;

      const next = unique.find((x) => {
        const k = six7ShortKey(x?.hKey);
        return k && k !== currentSelf;
      });

      two = next || null;
      candidateSource = "syscon2_promoted_primary";
    }

    if (!one && !two) {
      const a = six7ShortKey(concierge.cfConciergeSyscon1);
      const b = concierge.bootstrapAdmission === true ? "" : six7ShortKey(concierge.cfConciergeSyscon2);

      if (a && byKey.has(a)) one = byKey.get(a);
      if (b && b !== a && byKey.has(b)) two = byKey.get(b);

      if (one || two) {
        candidateSource = concierge.sysReady
          ? "cf_doorway_cached"
          : "cf_doorway";
      }
    }

    if (!one && !two) {
      unique.sort((a, b) => a.hKey.localeCompare(b.hKey));
      one = unique[0] || null;
      two = unique[1] || null;
      candidateSource = "hkey_fallback";
    }

    if (one && !two && concierge.bootstrapAdmission !== true) {
      const next = unique.find((x) => x.hKey && x.hKey !== one.hKey);
      if (next) two = next;
    }

    concierge.syscon1 = one ? one.hKey : null;
    concierge.syscon2 = two ? two.hKey : null;

    concierge.syscon1Label = one
      ? `${one.isSelf ? "self" : "peer"}:${one.handle || one.hKey.slice(0, 6)}`
      : "none";

    concierge.syscon2Label = two
      ? `${two.isSelf ? "self" : "peer"}:${two.handle || two.hKey.slice(0, 6)}`
      : "none";

    concierge.localRoleCandidate = one?.isSelf
      ? "syscon1"
      : two?.isSelf
        ? "syscon2"
        : "peer";

    let cleanReason = String(reason || "refresh");

    if (
      candidateSource === "sys_authority" &&
      cleanReason === "presence_six7_sys"
    ) {
      cleanReason = "boss_identity";
    }

    concierge.candidateReason = unique.length
      ? `${candidateSource}_${cleanReason}`
      : "no_roster_candidates";

    concierge.lastCandidateAt = now();

    six7ApplyLocalRoleCandidate(reason);
  }

  function six7ApplyConciergeRoute(msg, reason = "concierge_route") {
    if (!msg || typeof msg !== "object") return false;

    const advertisedEpoch = Number(msg.epoch || 0);
    const committedEpoch = Number(six7LocalCommittedAuthority()?.epoch || 0);
    const incomingEpoch = committedEpoch || advertisedEpoch;
    const currentEpoch = Number(concierge.sysconEpoch || 0);

    if (incomingEpoch && currentEpoch && incomingEpoch < currentEpoch) {
      emitSnapshot("cf_concierge_older_epoch_ignored");
      return false;
    }

    const authorityReply = String(reason || msg.type || "");

    if (
      six7PeerSysCarrier.authorityRefreshPending === true &&
      authorityReply === "concierge_update" &&
      Array.isArray(msg.list) &&
      incomingEpoch &&
      (!currentEpoch || incomingEpoch >= currentEpoch)
    ) {
      six7PeerSysCarrier.authorityRefreshPending = false;
    }

    // Candidate admission affects bootstrap direction only; committed SYS truth wins.
    concierge.bootstrapAdmission = msg.bootstrapAdmission === true &&
      six7ShortKey(msg.syscon2) === six7ShortKey(selfHKey) &&
      six7ShortKey(msg.syscon1) !== six7ShortKey(selfHKey) &&
      !six7LocalCommittedAuthority();
    concierge.cfConciergeState = String(reason || msg.type || "unknown");
    concierge.cfConciergeAt = now();
    concierge.cfConciergeSyscon1 = six7ShortKey(msg.syscon1);
    concierge.cfConciergeSyscon2 = six7ShortKey(msg.syscon2);

    concierge.cfConciergeSyscon1EndpointId =
      String(msg.syscon1EndpointId || "") || null;

    concierge.cfConciergeSyscon2EndpointId =
      String(msg.syscon2EndpointId || "") || null;

    concierge.cfConciergeEpoch = advertisedEpoch;

    if (incomingEpoch && incomingEpoch > currentEpoch) {
      concierge.roleDemoted = false;
      concierge.roleDemotedBy = "none";
      concierge.roleDemotedEpoch = 0;
      concierge.roleDemotedAt = 0;
    }

    const list = [];
    const seen = new Set();

    const pushAnchorPeer = (row, via = "cf_anchor") => {
      const hKey = six7ShortKey(row?.hKey || row?.from || row);
      if (!hKey || seen.has(hKey)) return;

      const old = six7PresenceBook.get(hKey) || {};

      seen.add(hKey);
      list.push({
        hKey,
        from: hKey,
        handle: row?.handle || old.handle || "",
        hex: row?.hex || old.hex || null,
        avatar: row?.avatar || old.avatar || undefined,
        videoId: row?.videoId || msg.videoId || videoId || null,
        seenVia: via,
        t: row?.t || row?.lastSeen || now(),
      });
    };

    const one = six7ShortKey(msg.syscon1);
    const two = six7ShortKey(msg.syscon2);

    const cfRowsByKey = new Map();

    if (Array.isArray(msg.list)) {
      const admittedNow = new Set();

      for (const p of msg.list) {
        const k = six7ShortKey(p?.hKey || p?.from || p);
        if (!k) continue;

        cfRowsByKey.set(k, p);
        admittedNow.add(k);
      }

      // Doorway admission is bootstrap eligibility, not room authority.
      // Keep it separate from the SYS-stamped roster so syscon1 can accept
      // a newly arrived plain peer's owned SYS tether before that peer has
      // been incorporated into the next SYS authority publication.
      six7DoorwayAdmittedPeers.clear();
      for (const k of admittedNow) six7DoorwayAdmittedPeers.add(k);
    }

    const self = six7ShortKey(selfHKey);
    const incarnationRows = new Map(cfRowsByKey);

    if (one && !incarnationRows.has(one)) {
      incarnationRows.set(one, { hKey: one });
    }
    if (two && !incarnationRows.has(two)) {
      incarnationRows.set(two, { hKey: two });
    }

    for (const [remoteKey, row] of incarnationRows.entries()) {
      if (!remoteKey || remoteKey === self) continue;

      const endpointId =
        remoteKey === one
          ? String(msg.syscon1EndpointId || "")
          : remoteKey === two
            ? String(msg.syscon2EndpointId || "")
            : "";

      six7ObserveRemoteIncarnation(remoteKey, {
        bootId: String(row?.bootId || ""),
        endpointId,
        via: "doorway",
      });
    }

    if (one) {
      pushAnchorPeer(cfRowsByKey.get(one) || { hKey: one }, "cf_anchor_syscon");
    }

    if (two && two !== one) {
      pushAnchorPeer(cfRowsByKey.get(two) || { hKey: two }, "cf_anchor_syscon");
    }

    if (Array.isArray(msg.list)) {
      for (const p of msg.list) {
        pushAnchorPeer(p, "cf_anchor_roster");
      }
    }

    if (!list.length) return false;

    concierge.sysconEpoch = incomingEpoch || Number(concierge.sysconEpoch || 0);

    const cfHasAuthoritativeList = !!(
      authorityReply === "concierge_update" && Array.isArray(msg.list)
    );

    const doorwayMembershipAdvance = !!(
      cfHasAuthoritativeList &&
      incomingEpoch &&
      incomingEpoch > currentEpoch
    );

    const ownerRoleNow = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );

    const sysAuthorityReady = !!(
      concierge.sysReady || six7LocalCommittedAuthority()
    );

    const cfAllowedKeys = new Set();

    if (one) cfAllowedKeys.add(one);
    if (two) cfAllowedKeys.add(two);

    if (Array.isArray(msg.list)) {
      for (const p of msg.list) {
        const k = six7ShortKey(p?.hKey || p?.from || p);
        if (k) cfAllowedKeys.add(k);
      }
    }

    const committedKeys = new Set(
      (six7LocalCommittedAuthority()?.roster || Array.from(lastRoster || []))
        .map((row) => six7ShortKey(row?.hKey || row?.from || row))
        .filter(Boolean),
    );

    const doorwayRemovesCommitted = Array.from(committedKeys).some(
      (key) => key && !cfAllowedKeys.has(key),
    );

    const committedAuthority = six7LocalCommittedAuthority();

    const syscon1ConsumesDoorwayRoster = !!(
      doorwayMembershipAdvance &&
      ownerRoleNow === "syscon1" &&
      !committedAuthority &&
      !doorwayRemovesCommitted
    );

    const sysOwnsRoster = !!(
      committedAuthority ||
      (sysAuthorityReady && !syscon1ConsumesDoorwayRoster)
    );

    if (doorwayMembershipAdvance && doorwayRemovesCommitted) {
      emitSnapshot("doorway_roster_reduction_held_for_sys_authority");
    }

    const latchedSysList = sysOwnsRoster ? six7LatchedSysRosterList() : [];

    const cfSelfOnly = !!(
      cfHasAuthoritativeList &&
      Array.isArray(msg.list) &&
      msg.list.length <= 1 &&
      one &&
      cfAllowedKeys.size <= 1
    );

    const merged = new Map();

    for (const p of latchedSysList) {
      const hKey = six7ShortKey(p?.hKey || p?.from);
      if (!hKey) continue;

      merged.set(hKey, {
        hKey,
        handle: p.handle || "",
        seenVia: p.seenVia || "six7_sys",
      });
    }

    for (const p of list) {
      const hKey = six7ShortKey(p?.hKey || p?.from);
      if (!hKey) continue;

      // After boss authority exists, doorway rows may refresh only the
      // concierge anchors. Membership remains the stamped SYS roster.
      if (sysOwnsRoster && hKey !== one && hKey !== two) {
        continue;
      }

      const old = merged.get(hKey) || {};

      merged.set(hKey, {
        hKey,
        handle: p.handle || old.handle || "",
        seenVia: p.seenVia || old.seenVia || "cf_anchor",
      });
    }

    const effectiveList = Array.from(merged.values());

    if (effectiveList.length > list.length) {
      concierge.lastErr = null;
    }

    if (doorwayMembershipAdvance && !sysOwnsRoster && !cfSelfOnly) {
      const allowed = new Set(effectiveList.map((p) => six7ShortKey(p.hKey)));

      for (const [hKey, row] of Array.from(six7PresenceBook.entries())) {
        const key = six7ShortKey(hKey || row?.hKey || row?.from);

        if (!key || key === six7ShortKey(selfHKey)) continue;
        if (allowed.has(key)) continue;

        const via = String(row?.seenVia || row?.via || "");

        if (via === "six7_sys" && sysOwnsRoster) {
          continue;
        }

        if (
          via === "six7_sys" ||
          via === "cf_anchor" ||
          via === "cf_anchor_roster" ||
          via === "cf_anchor_syscon"
        ) {
          six7PresenceBook.delete(hKey);
        }
      }
    }

    const nextConciergeKeys = new Set(
      effectiveList
        .map((p) => p?.hKey || p?.from)
        .filter(Boolean)
        .map((k) => six7ShortKey(k)),
    );

    for (const oldKey of Array.from(lastRoster || [])) {
      const gone = six7ShortKey(oldKey);

      if (!gone || gone === six7ShortKey(selfHKey)) continue;
      if (nextConciergeKeys.has(gone)) continue;

      if (syscon1ConsumesDoorwayRoster) {
        // Initial doorway birth may establish the exact authority spine, but
        // absence here is never terminal membership evidence.
        continue;
      }

      // A same-epoch doorway list is endpoint visibility only.
      // It cannot remove established room membership.
      if (cfHasAuthoritativeList && !doorwayMembershipAdvance) {
        emitSnapshot("cf_same_epoch_gone_ignored");
        continue;
      }

      // Once SYS authority exists, doorway observations cannot outvote it.
      if (sysOwnsRoster) {
        emitSnapshot("cf_gone_waiting_for_syscon1_authority");
        continue;
      }

      emitSnapshot("cf_gone_waiting_for_committed_authority");
      continue;
    }

    if (syscon1ConsumesDoorwayRoster || !sysOwnsRoster) {
      lastRoster = six7MarkRosterAudit(
        reason || "concierge_anchor",
        Array.from(nextConciergeKeys),
        "concierge_effective_list",
      );
    } else {
      emitSnapshot("concierge_roster_baseline_retained_for_sys_authority");
    }

    if (syscon1ConsumesDoorwayRoster) {
      // Room birth commits the authority spine atomically: leader, mirror,
      // and their membership. Other doorway-visible peers remain admission
      // candidates until their legal owned SYS tether is proven.
      const initialAuthorityRows = [];

      if (one) {
        initialAuthorityRows.push(
          cfRowsByKey.get(one) ||
            six7PresenceBook.get(one) || {
              hKey: one,
              from: one,
              handle: one === self ? selfHandle || null : null,
              videoId: videoId || null,
            },
        );
      }

      if (two && two !== one) {
        initialAuthorityRows.push(
          cfRowsByKey.get(two) ||
            six7PresenceBook.get(two) || {
              hKey: two,
              from: two,
              handle: two === self ? selfHandle || null : null,
              videoId: videoId || null,
            },
        );
      }

      if (
        six7CommitSyscon1Roster(
          initialAuthorityRows,
          "syscon1_initial_room_birth",
        )
      ) {
        six7PublishCommittedAuthority("syscon1_initial_room_birth_published");
      } else {
        emitSnapshot("syscon1_initial_room_birth_commit_failed");
      }
    }

    for (const p of effectiveList) {
      six7RememberPresencePeer(
        {
          hKey: p.hKey,
          from: p.hKey,
          handle: p.handle || null,
          videoId: videoId || null,
          t: now(),
        },
        p.seenVia || "cf_anchor",
        {
          peerEvent: false,
          rosterEvent: false,
        },
      );
    }

    six7RefreshSysCandidates(effectiveList, reason);

    /*
     * If both historical authority relays disappear, a committed survivor may
     * be elected into the current syscon1 slot. It still cannot rewrite room
     * truth from the doorway roster. Instead it opens the existing replacement
     * observation window for each missing historical spine member; only proven
     * SYS loss plus doorway absence may become terminal authority evidence.
     */
    const handoffBase = six7LocalCommittedAuthority();
    const handoffRole = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    if (
      handoffBase &&
      self &&
      handoffRole === "syscon1" &&
      six7ShortKey(concierge.syscon1) === self &&
      six7ShortKey(handoffBase.syscon1) !== self &&
      six7AuthorityRosterRows(handoffBase.roster).some(
        (row) => row.hKey === self,
      )
    ) {
      for (const oldSpineKey of [
        six7ShortKey(handoffBase.syscon1),
        six7ShortKey(handoffBase.syscon2),
      ]) {
        if (
          !oldSpineKey ||
          oldSpineKey === self ||
          (host.doorwayPresence === 'live-sockets' && six7DoorwayAdmittedPeers.has(oldSpineKey)) ||
          six7HasOpenSysPipe(oldSpineKey) ||
          six7PendingTerminalExits.has(oldSpineKey) ||
          six7SysReplacementWindows.has(oldSpineKey)
        ) {
          continue;
        }
        six7ArmSysReplacement(
          oldSpineKey,
          "elected_successor_missing_historical_spine",
        );
      }
    }

    for (const [remoteKey, rec] of Array.from(
      six7SysReplacementWindows.entries(),
    )) {
      if (
        String(rec?.state || "") === "awaiting_doorway_absence" &&
        !(host.doorwayPresence === 'live-sockets' && six7DoorwayAdmittedPeers.has(remoteKey)) &&
        !six7HasOpenSysPipe(remoteKey)
      ) {
        six7CompleteFailedSysReplacement(
          remoteKey,
          "doorway_absence_after_sys_loss",
        );
      }
    }

    six7ResumeTerminalAuthority("concierge_role_refresh");

    const epochBase = six7LocalCommittedAuthority();
    const acceptedEpoch = Number(concierge.sysconEpoch || 0);
    const epochOwner = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );

    // A doorway epoch change is topology metadata, not membership truth. The
    // continuing leader advances the exact committed value through the same
    // mirror-accepted authority transaction, even when the roster is unchanged.
    if (
      !six7SysAuthority.pending &&
      !six7PendingTerminalExits.size &&
      epochBase &&
      self &&
      epochOwner === "syscon1" &&
      six7ShortKey(concierge.syscon1) === self &&
      six7ShortKey(epochBase.syscon1) === self &&
      acceptedEpoch > Number(epochBase.epoch || 0)
    ) {
      if (
        six7StageSyscon1Authority(
          epochBase.roster,
          "syscon1_epoch_advance",
          epochBase,
          {
            syscon1: self,
            syscon2: six7ShortKey(epochBase.syscon2) || null,
          },
        )
      ) {
        six7PublishCommittedAuthority("syscon1_epoch_advance_published");
      }
    }

    six7RefreshSysReadiness(
      "concierge_owner_resolve_" + String(reason || "route"),
    );

    six7ReconcileRelationshipStatus(
      "relationship_status_" + String(reason || "concierge"),
    );

    const ownerRole = String(concierge.localRoleCandidate || "peer");
    const authorityHeld =
      six7PeerSysCarrier.authorityRefreshPending === true &&
      String(reason || msg.type || "") !== "concierge_update";

    if (authorityHeld) {
      emitSnapshot("concierge_reconcile_waiting_for_authority");
    } else if (ownerRole === "syscon1") {
      six7MaybeRelinkSysconPair("concierge_owner_" + String(reason || "route"));
    } else if (ownerRole === "syscon2") {
      six7MaybeRelinkSysconPair("concierge_owner_" + String(reason || "route"));
      six7ScheduleSysPreflight(
        "concierge_owner_syscon2_" + String(reason || "route"),
      );
    } else {
      six7ScheduleSysAdmission("concierge_owner_" + String(reason || "route"));
    }

    if (ownerRole !== "peer" && !six7CommittedMember(self)) {
      six7ScheduleSysAdmission(
        "concierge_uncommitted_role_" + String(reason || "route"),
      );
    }

    six7EmitPresenceRoster("concierge_anchor");

    if (cfSelfOnly && doorwayMembershipAdvance) {
      emitSnapshot("doorway_self_only_held_for_committed_authority");
    }

    emitSnapshot("concierge_owner_" + ownerRole);
    return true;
  }

  function six7MaybeRelinkSysconPair(reason = "syscon_relink") {
    const self = six7ShortKey(selfHKey);
    if (!self) return false;

    const role = String(concierge.localRoleCandidate || "peer");

    // Syscon pair pipe has one opener:
    // syscon1 dials syscon2.
    // syscon2 observes/accepts unless it has promoted to syscon1.
    if (role !== "syscon1") {
      const target = six7ShortKey(
        concierge.sysPeer || six7SysTargetFromCandidates(),
      );
      if (!target || target === self) return false;

      const ent = six7SysPeers.get(target);
      const dc = ent?.sysdc;

      if (dc && (dc.readyState === "open" || dc.readyState === "connecting")) {
        concierge.sysPeer = target;
        concierge.sysPipeState = dc.readyState;
        six7RefreshSysReadiness("syscon_relink_observe_" + dc.readyState);
        emitSnapshot("syscon_relink_observe_" + dc.readyState);
        return true;
      }

      six7RefreshSysReadiness("syscon_relink_observe_no_pipe");
      emitSnapshot("syscon_relink_observe_no_pipe");
      return false;
    }

    const target = six7ShortKey(concierge.syscon2);
    if (!target || target === self) return false;

    // Accepted terminal membership evidence preempts SYS rebirth while the
    // authority transaction is pending. The committed-removal owner decides
    // the final roster and teardown; transport cannot reopen this target.
    if (six7SysReplacementTerminal(target)) {
      six7SysDialing.delete(target);
      six7SysPreflightUnstableRetries.delete(target);
      emitSnapshot("syscon_relink_blocked_terminal_exit_pending");
      return false;
    }

    const ent = six7SysPeers.get(target);
    const dc = ent?.sysdc;
    const pcState = String(ent?.pc?.connectionState || "");
    const iceState = String(ent?.pc?.iceConnectionState || "");
    const currentAttemptId = String(concierge.sysAttempt?.id || "");
    const channelOwnerAttemptId = String(ent?.ownerAttemptId || "");

    const carrierAlive =
      pcState !== "failed" &&
      pcState !== "closed" &&
      iceState !== "failed" &&
      iceState !== "closed";

    const currentConnectingAttempt = !!(
      dc?.readyState === "connecting" &&
      currentAttemptId &&
      channelOwnerAttemptId === currentAttemptId &&
      carrierAlive
    );

    if (dc && (dc.readyState === "open" || currentConnectingAttempt)) {
      concierge.sysPeer = target;
      concierge.sysPipeState = dc.readyState;

      if (dc.readyState === "open") {
        six7RefreshSysReadiness("syscon_relink_primary_already_open");
        six7RefreshOpenSysPeer("syscon_relink_primary_open");
        return true;
      }

      six7RefreshSysReadiness("syscon_relink_primary_already_connecting");
      emitSnapshot("syscon_relink_primary_already_connecting");
      return true;
    }

    concierge.sysPeer = target;
    concierge.sysLastErr = null;

    six7RefreshSysReadiness(
      "syscon_relink_primary_request_" + String(reason || "relink"),
    );
    emitSnapshot("syscon_relink_primary_request_" + String(reason || "relink"));

    return six7EnsureSysPipe(
      target,
      "syscon1_pair_owner_" + String(reason || "relink"),
    );
  }

  function six7RefreshOpenSysPeer(reason = "sys_refresh") {
    const self = six7ShortKey(selfHKey);

    const expected = six7ShortKey(six7SysTargetFromCandidates());

    const candidates = [
      expected,
      concierge.sysPeer,
      concierge.syscon1,
      concierge.syscon2,
    ]
      .map(six7ShortKey)
      .filter((x) => x && x !== self);

    for (const hKey of Array.from(lastRoster || [])) {
      const key = six7ShortKey(hKey);
      if (key && key !== self && key !== expected) candidates.push(key);
    }

    let target = "";

    for (const key of [...new Set(candidates)]) {
      if (six7HasOpenSysPipe(key)) {
        target = key;
        break;
      }
    }

    if (!target) {
      six7RefreshSysReadiness(reason + "_no_open_sys_peer");
      emitSnapshot(reason + "_no_open_sys_peer");
      return false;
    }

    concierge.sysPeer = target;
    concierge.sysPipeState = "open";
    concierge.sysLastErr = null;

    const old = six7PresenceBook.get(target) || {};
    six7RememberPresencePeer(
      {
        ...old,
        hKey: target,
        from: target,
        videoId: videoId || null,
        t: now(),
      },
      "six7_sys",
      {
        peerEvent: false,
        rosterEvent: false,
      },
    );

    six7RefreshSysReadiness(reason);

    const readyTarget = six7ShortKey(concierge.sysReadyPeer);
    const alreadyReadyForTarget = !!(
      concierge.sysReady &&
      readyTarget &&
      readyTarget === target &&
      concierge.sysUnavailableReason === "none"
    );

    if (alreadyReadyForTarget) {
      emitSnapshot(String(reason || "sys_refresh") + "_already_ready");
      return true;
    }

    if (
      !concierge.sysReady &&
      concierge.sysUnavailableReason === "sys_pipe_waiting_for_proof"
    ) {
      six7SendSysDC(target, six7SysStatusPayload("SYS_STATUS"));
      emitSnapshot("sys_status_sent_open_pipe_waiting_for_proof");
    }

    const msg = six7PresencePayload();
    msg.to = target;
    six7SendSysControl(msg, "presence");

    six7EmitPresenceRoster("presence_six7_sys");
    six7RefreshSysReadiness(reason);
    emitSnapshot(reason);
    return true;
  }

  function six7ControlRouteFor(msg) {
    const type = String(msg?.type || "unknown");
    const cls = six7ControlClassFor(type);
    const mode = six7ControlClassMode(cls, type);
    const target = six7ShortKey(msg?.to || concierge.sysPeer);

    const isRtcSignal =
      type === "rtc_offer" || type === "rtc_answer" || type === "rtc_ice";

    const isSysCarrier = !!(msg?.sysCarrier === true && isRtcSignal);

    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );

    const self = six7ShortKey(selfHKey);
    const boss = six7ShortKey(concierge.syscon1);
    const secondary = six7ShortKey(concierge.syscon2);
    const currentNeighbor = six7ShortKey(concierge.sysPeer);

    const targetSysOpen = target ? six7HasOpenSysPipe(target) : false;

    const nextHop = targetSysOpen
      ? target
      : role === "syscon1"
        ? secondary
        : currentNeighbor || boss;

    const nextHopSysOpen = !!(
      nextHop &&
      nextHop !== self &&
      six7HasOpenSysPipe(nextHop)
    );

    const syscon1CanHoldDirected = !!(
      role === "syscon1" &&
      target &&
      target !== self &&
      cls !== "presence" &&
      lastRoster.has(target)
    );

    const ownerSysOpen =
      !!concierge.sysReady &&
      !!six7ShortKey(concierge.sysPeer) &&
      six7HasOpenSysPipe(concierge.sysPeer);

    let route = "bootstrap_ws";
    let reason = "bootstrap_default";
    let wouldRoute = "bootstrap_ws";
    let wouldRouteReason = "bootstrap_default";

    // SYS carrier is legal only while birthing the SYS pipe.
    // Once the target SYS pipe is open, carrier must not force CF/bootstrap.
    if (isSysCarrier && !targetSysOpen) {
      route = "bootstrap_ws";
      reason = "sys_carrier_bootstrap";
      wouldRoute = "bootstrap_ws";
      wouldRouteReason = "sys_carrier_bootstrap";
    } else if (isSysCarrier && targetSysOpen) {
      route = "six7_sys";
      reason = "sys_carrier_ignored_target_sys_ready";
      wouldRoute = "six7_sys";
      wouldRouteReason = "sys_ready_route_rtc_signal";
    } else if (mode === "bootstrap") {
      route = "bootstrap_ws";
      reason =
        cls === "bootstrap_core"
          ? "bootstrap_core_stays_bootstrap"
          : `bootstrap_mode_${cls}`;
      wouldRoute = "bootstrap_ws";
      wouldRouteReason = reason;
    } else if (mode === "sys_primary") {
      wouldRoute = "six7_sys";

      if (!concierge.sysEligible) {
        route = "six7_sys_pending";
        reason =
          cls === "rtc_signal"
            ? `rtc_sys_pending_ineligible_${concierge.sysUnavailableReason || "unknown"}`
            : `sys_pending_ineligible_${concierge.sysUnavailableReason || "unknown"}`;
        wouldRouteReason = reason;
      } else if (cls === "relationship_control" && !target) {
        route = "six7_sys_pending";
        reason = "relationship_missing_target";
        wouldRouteReason = reason;
      } else if (
        cls === "relationship_control" &&
        !nextHopSysOpen &&
        !syscon1CanHoldDirected
      ) {
        route = "six7_sys_pending";
        reason = "relationship_waiting_for_sys_next_hop";
        wouldRouteReason = reason;
      } else if (cls !== "relationship_control" && !concierge.sysReady) {
        route = "six7_sys_pending";
        reason =
          cls === "rtc_signal"
            ? `rtc_sys_pending_before_ready_${concierge.sysUnavailableReason || "not_ready"}`
            : `sys_pending_before_ready_${concierge.sysUnavailableReason || "not_ready"}`;
        wouldRouteReason = reason;
      } else if (
        cls === "rtc_signal" &&
        !nextHopSysOpen &&
        !syscon1CanHoldDirected
      ) {
        route = "six7_sys_pending";
        reason = `rtc_${type}_waiting_for_sys_next_hop`;
        wouldRouteReason = reason;
      } else if (
        nextHopSysOpen ||
        syscon1CanHoldDirected ||
        (cls === "presence" && ownerSysOpen)
      ) {
        route = "six7_sys";
        reason =
          cls === "relationship_control"
            ? "peer_sys_route_relationship_control"
            : `sys_route_${cls}`;
        wouldRouteReason =
          cls === "relationship_control"
            ? "peer_sys_ready_route_relationship_control"
            : `sys_ready_route_${cls}`;
      } else {
        route = "six7_sys_pending";
        reason = `sys_pending_${cls}`;
        wouldRouteReason = reason;
      }
    } else if (mode === "observe") {
      route = "six7_sys_pending";
      reason = `observe_hold_${cls}`;
      wouldRoute = "six7_sys";
      wouldRouteReason = `sys_ready_observe_${cls}`;
    } else {
      route = "bootstrap_ws";
      reason = `bootstrap_mode_${cls}`;
      wouldRoute = "bootstrap_ws";
      wouldRouteReason = reason;
    }

    return {
      route,
      reason,
      type,
      cls,
      mode,
      wouldRoute,
      wouldRouteReason,
    };
  }

  function six7MaybeDelayRelationshipFallback(msg, type, routeReason) {
    if (!msg || typeof msg !== "object") return false;
    if (six7ControlClassFor(type) !== "relationship_control") return false;
    const target = six7ShortKey(
      msg.to || concierge.sysAttempt?.target || concierge.sysPeer,
    );
    if (!target) return false;

    const pipeWaitReasons = new Set([
      "sys_unavailable_target_pipe_not_open",
      "sys_unavailable_sys_pipe_not_open",
      "sys_unavailable_sys_pipe_connecting",
      "sys_unavailable_sys_pipe_waiting_for_proof",
    ]);

    const routeIsPipeWait = pipeWaitReasons.has(String(routeReason || ""));

    const sysUnavailable = String(concierge.sysUnavailableReason || "");
    const readinessIsPipeWait =
      sysUnavailable === "sys_pipe_not_open" ||
      sysUnavailable === "sys_pipe_connecting" ||
      sysUnavailable === "sys_pipe_waiting_for_proof";

    if (!routeIsPipeWait && !readinessIsPipeWait) {
      return false;
    }

    if (!concierge.sysEligible) return false;

    const attempt = concierge.sysAttempt || {};
    const attemptTarget = six7ShortKey(attempt.target || concierge.sysPeer);

    if (attemptTarget && attemptTarget !== target) {
      return false;
    }

    if (six7OwnsPrimarySysState(target)) {
      concierge.sysPeer = target;
    }

    const ent = six7SysPeers.get(target);
    const dcState = String(ent?.sysdc?.readyState || "");

    if (dcState !== "open" && dcState !== "connecting") {
      emitSnapshot("relationship_pending_wait_no_birth_" + type);
    }

    concierge.lastErr = "relationship_waiting_for_sysdc:" + type;
    concierge.lastRelationshipTxAt = now();
    concierge.lastRelationshipTxType = type;
    concierge.lastRelationshipRoute = "six7_sys_pending";
    concierge.lastRelationshipReason = "waiting_for_target_sysdc";

    emitSnapshot("relationship_waiting_for_sysdc_" + type);

    let pending = six7PendingRelationship.get(target);

    if (!pending) {
      pending = [];
      six7PendingRelationship.set(target, pending);
    }

    pending.push({
      msg: { ...msg },
      type,
      at: now(),
    });

    if (pending.length > SIX7_REL_PENDING_MAX) {
      pending.splice(0, pending.length - SIX7_REL_PENDING_MAX);
    }

    emitSnapshot("relationship_pending_for_sysdc_" + type);

    return true;
  }

  function six7SendQueuedRtcIceFallbackWs(msg, why = "ice_queue_fallback_ws") {
    const type = "rtc_ice";

    bootstrap.cfHits.total++;
    bootstrap.cfHits.fallback++;
    bootstrap.cfHits.lastDir = "tx";
    bootstrap.cfHits.lastType = type;
    bootstrap.cfHits.lastBucket = "fallback";
    bootstrap.cfHits.lastReason = String(why || "ice_queue_fallback_ws");
    bootstrap.cfHits.lastAt = now();

    six7MarkControlTx(type, "bootstrap_ws");

    concierge.routeReason = String(why || "ice_queue_fallback_ws");
    concierge.lastRouteAt = now();
    concierge.lastRouteType = type;
    concierge.lastRouteReason = concierge.routeReason;
    concierge.lastRouteClass = "rtc_signal";
    concierge.routeMode = "sys_primary";
    concierge.lastRouteMode = "sys_primary";

    concierge.lastRtcTxAt = now();
    concierge.lastRtcTxType = type;
    concierge.lastRtcTxRoute = "bootstrap_ws";
    concierge.lastRtcTxMode = "sys_primary";
    concierge.lastRtcTxReason = concierge.routeReason;

    concierge.lastRtcIceTxAt = now();
    concierge.lastRtcIceTxRoute = "bootstrap_ws";
    concierge.lastRtcIceTxMode = "sys_primary";
    concierge.lastRtcIceTxReason = concierge.routeReason;

    if (!ws || ws.readyState !== WebSocket.OPEN) {
      concierge.lastErr = "rtc_ice_queue_ws_not_open";
      bootstrap.lastErr = concierge.lastErr;
      emitSnapshot("rtc_ice_queue_ws_not_open");
      return false;
    }

    try {
      ws.send(JSON.stringify(msg));
      bootstrap.lastSendType = type;
      bootstrap.lastSendAt = now();
      bootstrap.lastSendOk = true;
      concierge.lastErr = null;
      emitSnapshot("rtc_ice_queue_ws_sent");
      return true;
    } catch (e) {
      concierge.lastErr = "rtc_ice_queue_ws_failed:" + String(e?.message || e);
      bootstrap.lastErr = concierge.lastErr;
      emitSnapshot("rtc_ice_queue_ws_failed");
      return false;
    }
  }

  function six7FlushQueuedRtcIce(remoteHKey, reason = "rtc_ice_queue_flush") {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    const rec = six7PendingRtcIce.get(key);
    if (!rec) return false;

    six7PendingRtcIce.delete(key);

    const items = Array.isArray(rec.items) ? rec.items : [];
    if (!items.length) return false;

    if (!six7HasOpenSysPipe(key)) {
      const ent = six7SysPeers.get(key);
      const sysState = String(ent?.sysdc?.readyState || "");

      if (sysState === "connecting" || six7SysDialing.has(key)) {
        const current = six7ShortKey(concierge.sysPeer);
        const readyPeer = six7ShortKey(concierge.sysReadyPeer);
        const alreadyReady = !!(
          concierge.sysReady &&
          current &&
          readyPeer &&
          current === readyPeer
        );

        // Queued ICE can wait on this target,
        // but cannot downgrade a proven SYS pipe.
        if (!alreadyReady) {
          concierge.sysPeer = key;
          concierge.sysPipeState = "connecting";
        }
      } else {
        emitSnapshot("rtc_ice_queue_waiting_for_sys_pipe_no_birth");
      }

      six7PendingRtcIce.set(key, {
        items,
      });

      concierge.lastRtcIceTxAt = now();
      concierge.lastRtcIceTxRoute = "six7_sys_pending";
      concierge.lastRtcIceTxMode = "sys_primary";
      concierge.lastRtcIceTxReason = "rtc_ice_queue_waiting_for_sys_pipe";

      concierge.lastRtcTxAt = now();
      concierge.lastRtcTxType = "rtc_ice";
      concierge.lastRtcTxRoute = "six7_sys_pending";
      concierge.lastRtcTxMode = "sys_primary";
      concierge.lastRtcTxReason = "rtc_ice_queue_waiting_for_sys_pipe";

      concierge.lastErr = "rtc_ice_queue_waiting_for_sys_pipe";

      six7RefreshSysReadiness("rtc_ice_queue_waiting_for_sys_pipe");
      emitSnapshot("rtc_ice_queue_waiting_for_sys_pipe");
      return true;
    }

    const held = [];

    for (const item of items) {
      six7MarkControlTx("rtc_ice", "six7_sys");

      concierge.routeReason = "sys_route_rtc_signal_queued";
      concierge.lastRouteAt = now();
      concierge.lastRouteType = "rtc_ice";
      concierge.lastRouteReason = concierge.routeReason;
      concierge.lastRouteClass = "rtc_signal";
      concierge.routeMode = "sys_primary";
      concierge.lastRouteMode = "sys_primary";
      concierge.wouldRoute = "six7_sys";
      concierge.wouldRouteReason = "sys_ready_route_rtc_signal_queued";

      concierge.lastRtcTxAt = now();
      concierge.lastRtcTxType = "rtc_ice";
      concierge.lastRtcTxRoute = "six7_sys";
      concierge.lastRtcTxMode = "sys_primary";
      concierge.lastRtcTxReason = concierge.routeReason;

      concierge.lastRtcIceTxAt = now();
      concierge.lastRtcIceTxRoute = "six7_sys";
      concierge.lastRtcIceTxMode = "sys_primary";
      concierge.lastRtcIceTxReason = concierge.routeReason;

      const ok = six7SendSysControl(item.msg, "rtc_ice");

      if (ok) {
        concierge.lastErr = null;
      } else {
        held.push(item);
      }
    }

    if (held.length) {
      six7PendingRtcIce.set(key, {
        items: held.slice(-SIX7_RTC_ICE_QUEUE_MAX),
      });

      concierge.lastErr = "rtc_ice_queue_sys_send_failed_held";
      emitSnapshot("rtc_ice_queue_sys_send_failed_held");
      return false;
    }

    emitSnapshot(String(reason || "rtc_ice_queue_flush"));
    return true;
  }

  function six7MaybeQueueRtcIceFallback(msg, type, routeReason) {
    if (!msg || typeof msg !== "object") return false;
    if (String(type || "") !== "rtc_ice") return false;

    // ICE is transport bootstrap. It cannot wait behind the SYS pipe
    // before that pipe is ready, because SYS depends on ICE to open.
    if (!concierge.sysReady) return false;

    const reason = String(routeReason || "");

    if (
      !reason.includes("sys_pipe_not_open") &&
      !reason.includes("sys_pipe_connecting") &&
      !reason.includes("sys_pipe_waiting_for_proof") &&
      !reason.includes("target_sys_pipe")
    ) {
      return false;
    }

    const target = six7ShortKey(msg.to || concierge.sysPeer);
    if (!target) return false;

    if (six7HasOpenSysPipe(target)) return false;

    six7SendSysAdmissionPresence("rtc_ice_queue", target);

    const ent = six7SysPeers.get(target);
    const sysState = String(ent?.sysdc?.readyState || "");

    if (sysState === "connecting") {
      const current = six7ShortKey(concierge.sysPeer);
      const readyPeer = six7ShortKey(concierge.sysReadyPeer);
      const alreadyReady = !!(
        concierge.sysReady &&
        current &&
        readyPeer &&
        current === readyPeer
      );

      // ICE queue may observe a connecting target pipe,
      // but it must not demote an already proofed SYS pipe.
      if (!alreadyReady) {
        concierge.sysPeer = target;
        concierge.sysPipeState = "connecting";
        six7RefreshSysReadiness("rtc_ice_queue_target_sys_connecting");
      } else {
        emitSnapshot("rtc_ice_queue_target_sys_connecting_observed");
      }
    } else if (!sysState || sysState === "closed") {
      emitSnapshot("rtc_ice_queue_target_pipe_missing");
    }

    let rec = six7PendingRtcIce.get(target);

    if (!rec) {
      rec = {
        items: [],
      };
      six7PendingRtcIce.set(target, rec);
    }

    rec.items.push({
      msg: { ...msg },
      at: now(),
    });

    if (rec.items.length > SIX7_RTC_ICE_QUEUE_MAX) {
      rec.items.splice(0, rec.items.length - SIX7_RTC_ICE_QUEUE_MAX);
    }

    concierge.lastRtcIceTxAt = now();
    concierge.lastRtcIceTxRoute = "six7_sys_pending";
    concierge.lastRtcIceTxMode = "sys_primary";
    concierge.lastRtcIceTxReason = "rtc_ice_waiting_for_target_sys_pipe";

    concierge.lastRtcTxAt = now();
    concierge.lastRtcTxType = "rtc_ice";
    concierge.lastRtcTxRoute = "six7_sys_pending";
    concierge.lastRtcTxMode = "sys_primary";
    concierge.lastRtcTxReason = "rtc_ice_waiting_for_target_sys_pipe";

    concierge.lastErr = "rtc_ice_waiting_for_sys_pipe";

    emitSnapshot("rtc_ice_queued_for_sys");
    return true;
  }

  function six7SendControl(msg, reason = "") {
    if (!msg || typeof msg !== "object") return false;

    const decision = six7ControlRouteFor(msg);
    const type = String(decision.type || msg.type || reason || "unknown");
    const route = String(decision.route || "bootstrap_ws");

    if (route === "bootstrap_ws") {
      const cfReason = String(decision.reason || reason || route);
      let cfBucket = "fallback";

      const cfTarget = six7ShortKey(msg?.to || concierge.sysPeer);
      const cfIsRtcSignal =
        type === "rtc_offer" || type === "rtc_answer" || type === "rtc_ice";
      const cfTargetSysOpen = cfTarget ? six7HasOpenSysPipe(cfTarget) : false;
      const cfIsSysCarrier = !!(
        cfIsRtcSignal &&
        (msg?.sysCarrier === true || cfReason.includes("sys_carrier"))
      );

      if (
        type === "concierge_connect" ||
        type === "doorway_reconcile" ||
        type === "concierge_route" ||
        type === "concierge_bootstrap_needed" ||
        type === "roster"
      ) {
        cfBucket = "doorway";
      } else if (
        type === "concierge_update" ||
        cfReason.includes("cf_anchor")
      ) {
        cfBucket = "authority";
      } else if (cfIsSysCarrier && !cfTargetSysOpen) {
        cfBucket = "carrier";
      } else if (
        (cfIsSysCarrier && cfTargetSysOpen) ||
        (concierge.sysReady &&
          (type === "rtc_offer" ||
            type === "rtc_answer" ||
            type === "rtc_ice" ||
            type === "veil_invite" ||
            type === "veil_accept" ||
            type === "veil_deny"))
      ) {
        cfBucket = "dirty";
      }

      bootstrap.cfHits.total++;
      bootstrap.cfHits[cfBucket]++;
      bootstrap.cfHits.lastDir = "tx";
      bootstrap.cfHits.lastType = type;
      bootstrap.cfHits.lastBucket = cfBucket;
      bootstrap.cfHits.lastReason = cfReason;
      bootstrap.cfHits.lastAt = now();
    }

    if (decision.cls === "relationship_control") {
      six7PrepareRelationshipControl(msg, type);
    }

    concierge.routeReason = String(decision.reason || "none");
    concierge.lastRouteAt = now();
    concierge.lastRouteType = type;
    concierge.lastRouteReason = concierge.routeReason;
    concierge.lastRouteClass = String(decision.cls || "unknown");
    concierge.routeMode = String(decision.mode || "bootstrap");
    concierge.lastRouteMode = concierge.routeMode;
    concierge.wouldRoute = String(decision.wouldRoute || "bootstrap_ws");
    concierge.wouldRouteReason = String(decision.wouldRouteReason || "none");

    six7MarkControlTx(type, route);

    if (decision.cls === "relationship_control") {
      concierge.lastRelationshipTxAt = now();
      concierge.lastRelationshipTxType = type;
      concierge.lastRelationshipRoute = route;
      concierge.lastRelationshipReason = concierge.routeReason;
    }

    if (decision.cls === "rtc_signal") {
      concierge.lastRtcTxAt = now();
      concierge.lastRtcTxType = type;
      concierge.lastRtcTxRoute = route;
      concierge.lastRtcTxMode = String(decision.mode || "bootstrap");
      concierge.lastRtcTxReason = concierge.routeReason;
      concierge.lastRtcWouldRoute = String(
        decision.wouldRoute || "bootstrap_ws",
      );
      concierge.lastRtcWouldReason = String(
        decision.wouldRouteReason || "none",
      );
    }

    if (type === "rtc_ice") {
      concierge.lastRtcIceTxAt = now();
      concierge.lastRtcIceTxRoute = route;
      concierge.lastRtcIceTxMode = String(decision.mode || "bootstrap");
      concierge.lastRtcIceTxReason = concierge.routeReason;
    }

    if (route === "six7_sys_pending") {
      // Directed chat signaling can enter the pending route before
      // six7SendSysControl is called at all. Preserve the whole sequence here,
      // not just ICE, so offer/answer cannot disappear or be overtaken.
      if (decision.cls === "rtc_signal" && msg?.sysCarrier !== true) {
        const target = six7ShortKey(msg.to || concierge.sysPeer);

        if (target) {
          const envelope = {
            t: "SYS_CONTROL",
            at: now(),
            from: six7ShortKey(selfHKey),
            room: videoId || null,
            kind: type,
            cid: msg.cid || null,
            msg,
          };

          const held = six7QueueDirectedSysControl(
            target,
            envelope,
            "six7_sys_pending_" + type,
          );

          if (held) {
            concierge.lastErr =
              "directed_rtc_signal_queued_waiting_for_sys_pipe:" + type;
            emitSnapshot(
              "directed_rtc_signal_queued_waiting_for_sys_pipe_" + type,
            );
            return true;
          }
        }

        concierge.lastErr =
          "directed_rtc_signal_pending_without_target:" + type;
        emitSnapshot("directed_rtc_signal_pending_without_target_" + type);
        return false;
      }

      if (type === "rtc_ice") {
        return six7MaybeQueueRtcIceFallback(msg, type, concierge.routeReason);
      }

      if (decision.cls === "relationship_control") {
        const target = six7ShortKey(msg.to || concierge.sysPeer);

        if (target) {
          const normalSysTarget = six7ShortKey(six7SysTargetFromCandidates());
          const temporaryUnblock = !!(
            type === "sys_unblock" &&
            !normalSysTarget &&
            !six7HasOpenSysPipe(concierge.sysPeer)
          );
          const queuePeer = six7ShortKey(
            (type === "sys_unblock" || type === "sys_unblock_ack") &&
              normalSysTarget
              ? normalSysTarget
              : target,
          );

          if (temporaryUnblock) {
            msg.sysTxId = String(msg.sysTxId || msg.cid || "") || null;
            msg.recoveryProbe = true;
          }

          let q = six7PendingRelationship.get(queuePeer);
          if (!q) {
            q = [];
            six7PendingRelationship.set(queuePeer, q);
          }

          q.push({ msg: { ...msg }, type, at: now() });

          if (q.length > SIX7_REL_PENDING_MAX) {
            q.splice(0, q.length - SIX7_REL_PENDING_MAX);
          }

          if (type === "sys_unblock") {
            if (temporaryUnblock) {
              six7StartTemporarySysTransaction(target, msg);
            } else {
              six7ScheduleSysAdmission("sys_unblock_waiting_for_room_sys");
            }
          }

          emitSnapshot(
            "relationship_queued_waiting_for_existing_sys_pipe_" + type,
          );

          concierge.lastRelationshipRoute = "six7_sys_pending";
          concierge.lastRelationshipReason = concierge.routeReason;
          concierge.lastErr = "relationship_waiting_for_peer_sys_pipe:" + type;
          emitSnapshot("relationship_queued_for_sys_" + type);
          return true;
        }
      }

      if (decision.cls === "presence") {
        const target = six7ShortKey(msg.to || concierge.sysPeer);

        if (target) {
          const ent = six7SysPeers.get(target);
          const sysState = String(ent?.sysdc?.readyState || "");

          if (sysState === "connecting" || six7SysDialing.has(target)) {
            six7RefreshSysReadiness("presence_waiting_for_sys_pipe");
            emitSnapshot("presence_waiting_for_sys_pipe");
          } else {
            emitSnapshot("presence_waiting_for_sys_pipe_no_birth");
          }
        } else {
          six7ScheduleSysAdmission("presence_waiting_for_sys_target");
        }

        concierge.lastErr = "presence_waiting_for_sys_pipe";
        return false;
      }

      concierge.lastErr = "sys_primary_waiting_for_pipe:" + type;
      emitSnapshot("sys_primary_waiting_for_pipe_" + type);
      return false;
    }

    if (route === "six7_sys") {
      const ok = six7SendSysControl(msg, type);
      if (ok) return true;

      concierge.lastErr = "sys_route_failed_no_ws_fallback:" + type;

      if (decision.cls === "relationship_control") {
        const target = six7ShortKey(msg.to || concierge.sysPeer);

        if (type === "veil_typing") {
          concierge.lastErr = "typing_dropped_no_open_sys_route";
          emitSnapshot("typing_dropped_no_open_sys_route");
          return false;
        }

        if (target) {
          const normalSysTarget = six7ShortKey(six7SysTargetFromCandidates());
          const temporaryUnblock = !!(
            type === "sys_unblock" &&
            !normalSysTarget &&
            !six7HasOpenSysPipe(concierge.sysPeer)
          );
          const queuePeer = six7ShortKey(
            (type === "sys_unblock" || type === "sys_unblock_ack") &&
              normalSysTarget
              ? normalSysTarget
              : target,
          );

          if (temporaryUnblock) {
            msg.sysTxId = String(msg.sysTxId || msg.cid || "") || null;
            msg.recoveryProbe = true;
          }

          let q = six7PendingRelationship.get(queuePeer);
          if (!q) {
            q = [];
            six7PendingRelationship.set(queuePeer, q);
          }

          q.push({ msg: { ...msg }, type, at: now() });

          if (q.length > SIX7_REL_PENDING_MAX) {
            q.splice(0, q.length - SIX7_REL_PENDING_MAX);
          }

          if (type === "sys_unblock") {
            if (temporaryUnblock) {
              six7StartTemporarySysTransaction(target, msg);
            } else {
              six7ScheduleSysAdmission("sys_unblock_requeued_room_sys");
            }
          }

          if (type === "veil_invite") {
            emitSnapshot("relationship_sys_send_failed_requeue_no_birth");
          } else {
            emitSnapshot("relationship_requeued_no_birth_" + type);
          }

          concierge.lastRelationshipRoute = "six7_sys_pending";
          concierge.lastRelationshipReason = "sys_send_failed_requeue";
          emitSnapshot("relationship_requeued_no_ws_fallback_" + type);
          return true;
        }
      }

      emitSnapshot("sys_route_failed_no_ws_fallback_" + type);
      return false;
    }

    bootstrap.lastSendType = type;
    bootstrap.lastSendAt = now();
    bootstrap.lastSendOk = false;

    if (!ws || ws.readyState !== WebSocket.OPEN) {
      concierge.lastErr = "control_send_ws_not_open:" + type;
      bootstrap.lastErr = "control_send_ws_not_open:" + type;
      emitSnapshot("control_send_blocked_" + type);
      return false;
    }

    try {
      ws.send(JSON.stringify(msg));
      bootstrap.lastSendOk = true;
      return true;
    } catch (e) {
      concierge.lastErr =
        "control_send_failed:" + type + ":" + String(e?.message || e);
      bootstrap.lastErr = concierge.lastErr;
      emitSnapshot("control_send_failed_" + type);
      return false;
    }
  }

  let rtcInFlight = false;
  let six7SessionKilling = false;
  let lastRosterRequestAt = 0;
  const ROSTER_MIN_INTERVAL = 15000; // 15s
  let lastRoster = new Set();
  const six7PresenceBook = new Map(); // hKey -> latest presence-ish peer row

  let six7RosterAudit = {
    source: "none",
    count: 0,
    added: "none",
    removed: "none",
    pruneReason: "none",
    at: 0,
  };

  function six7MarkRosterAudit(source, nextKeys, pruneReason = "none") {
    const prev = lastRoster instanceof Set ? lastRoster : new Set();
    const next = new Set(
      Array.from(nextKeys || [])
        .map(six7ShortKey)
        .filter(Boolean),
    );

    const added = Array.from(next).find((k) => !prev.has(k)) || "none";
    const removed = Array.from(prev).find((k) => !next.has(k)) || "none";

    six7RosterAudit = {
      source: String(source || "unknown"),
      count: next.size,
      added,
      removed,
      pruneReason:
        removed !== "none"
          ? String(pruneReason || source || "unknown")
          : "none",
      at: now(),
    };

    return next;
  }

  function six7LatchedSysRosterList() {
    const self = six7ShortKey(selfHKey);
    const authority = six7LocalCommittedAuthority();

    const source =
      authority && Array.isArray(authority.roster) ? authority.roster : [];

    const rows = [];

    for (const row of source) {
      const key = six7ShortKey(row?.hKey || row?.from);
      if (!key) continue;

      const presence = six7PresenceBook.get(key) || {};

      rows.push({
        hKey: key,
        handle: row?.handle || presence.handle || "",
        isSelf: !!self && key === self,
      });
    }

    if (self && !rows.some((x) => x.hKey === self)) {
      rows.push({
        hKey: self,
        handle: selfHandle || "",
        isSelf: true,
      });
    }

    return Array.from(new Map(rows.map((x) => [x.hKey, x])).values());
  }

  let dialReady = false;
  let dialQueued = null;
  const loungeWaiters = new Set(); // hKey waiting for roster event
  const rtcDialing = new Set(); // hKey currently dialing
  const rtcClosing = new Set(); // hKey currently tearing down

  function now() {
    return Date.now();
  }

  // ======================================================
  // P2P WebRTC (DataChannel) — brain owns it
  // Signaling is relayed via TOP/BG using:
  //   post("RTC_SEND", {to, kind, data})
  // and inbound:
  //   d.type === "RTC_SIGNAL" with payload {from, kind, data}
  // ======================================================

  // NOTE: STUN is optional. With empty iceServers, some NAT pairs won't connect.
  const RTC_CFG = { iceServers: [{ urls: "stun:stun.l.google.com:19302" }] };

  // remoteHKey -> { pc, dc }
  const six7SysPeers = new Map();
  const six7ChatPeers = new Map();

  // ======================================================
  // CONCIERGE / SYS preflight
  // Opens six7-sys after roster so Phase 4 relationship
  // control has a pipe before invite/accept/block traffic.
  // ======================================================

  const six7SysDialing = new Set();
  const six7SysPreflightUnstableRetries = new Map();
  const SIX7_SYS_PREFLIGHT_UNSTABLE_MAX = 6;
  const six7SysAdmissionLastSent = new Map();
  const six7SysLastGoodRx = new Map();

  const six7PeerSysCarrier = {
    epoch: 0,
    target: null,
    primaryRecoveryUsed: false,
    authorityRefreshPending: false,
  };

  let six7LastSysFallbackIndicator = "unknown";

  function six7RefreshSysFallbackIndicator() {
    if (six7LastSysFallbackIndicator === "off") return;

    six7LastSysFallbackIndicator = "off";

    post("SYS_FALLBACK_STATE", {
      state: "off",
      epoch: Number(concierge.sysconEpoch || 0),
      target: null,
      syscon2: null,
      at: now(),
    });
  }

  const six7SysAuthority = {
    at: 0,
    via: "none",
    from: null,
    syscon1: null,
    syscon2: null,
    epoch: 0,
    rosterSeq: 0,
    roster: [],

    // The authority owner has one uncommitted slot. Projection reads only the
    // committed fields above. A next value stays here until the exact mirror
    // accepts it over the existing SYS STATUS/ACK authority path.
    pending: null,
  };

  // Diagnostic-only membership transaction trace. This object never
  // participates in routing, authority, membership, or retry decisions.
  const six7MembershipAudit = {
    at: 0,
    stage: "idle",
    peer: null,
    id: "none",
    detail: "none",
  };

  function six7MembershipAuditToken(value) {
    const text = String(value || "");
    if (!text) return "none";

    let hash = 2166136261;
    for (let i = 0; i < text.length; i++) {
      hash ^= text.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }

    return (hash >>> 0).toString(16).padStart(8, "0");
  }

  function six7MembershipAuditSet(stage, fields = {}) {
    const peer = six7ShortKey(fields.peer || fields.from || fields.transport);
    const id = fields.id ? six7MembershipAuditToken(fields.id) : "none";
    const topology = six7MembershipAuditTopology();

    const merged = {
      ...fields,
      c1: topology.committedOne,
      c2: topology.committedTwo,
      ce: topology.committedEpoch,
      cs: topology.committedSeq,
      ck: topology.committedKeys || "none",
      pid: topology.pendingId,
      pm: topology.pendingMirror,
      pe: topology.pendingEpoch,
      pbe: topology.pendingBaseEpoch,
      pbs: topology.pendingBaseSeq,
      pns: topology.pendingNextSeq,
      pk: topology.pendingKeys || "none",
      cf1: topology.cfOne,
      cf2: topology.cfTwo,
      cfe: topology.cfEpoch,
      term: topology.terminalKeys || "none",
      target: topology.sysTarget,
      attempt: topology.sysAttempt,
      pipe: topology.sysPipe,
    };

    const detail = Object.entries(merged)
      .filter(
        ([key]) =>
          key !== "peer" &&
          key !== "from" &&
          key !== "transport" &&
          key !== "id",
      )
      .map(([key, value]) => {
        const clean =
          value === true
            ? "1"
            : value === false
              ? "0"
              : value === null || value === undefined || value === ""
                ? "none"
                : String(value);
        return `${key}=${clean}`;
      })
      .join(" ");

    const at = now();

    six7MembershipAudit.at = at;
    six7MembershipAudit.stage = String(stage || "unknown");
    six7MembershipAudit.peer = peer || null;
    six7MembershipAudit.id = id;
    six7MembershipAudit.detail = detail || "none";

    six7MembershipAuditTrace.push({
      at,
      stage: six7MembershipAudit.stage,
      peer: six7MembershipAudit.peer,
      id: six7MembershipAudit.id,
      detail: six7MembershipAudit.detail,
    });

    if (six7MembershipAuditTrace.length > SIX7_MEMBERSHIP_AUDIT_TRACE_MAX) {
      six7MembershipAuditTrace.splice(
        0,
        six7MembershipAuditTrace.length - SIX7_MEMBERSHIP_AUDIT_TRACE_MAX,
      );
    }

    emitSnapshot("membership_audit_" + six7MembershipAudit.stage);
  }

  function six7MembershipAuditText() {
    if (!six7MembershipAudit.at) return "";

    const peer = six7MembershipAudit.peer
      ? six7MembershipAudit.peer.slice(0, 8)
      : "none";
    const trace = six7MembershipAuditTraceText();

    return [
      "MEMAUDIT",
      six7MembershipAudit.stage,
      `peer=${peer}`,
      `id=${six7MembershipAudit.id || "none"}`,
      six7MembershipAudit.detail || "none",
      trace ? `TRACE[${trace}]` : "",
      six7ChatSignalAuditText()
        ? `CHATTRACE[${six7ChatSignalAuditText()}]`
        : "",
    ]
      .filter(Boolean)
      .join(" ");
  }

  const six7MembershipAuditTrace = [];
  const SIX7_MEMBERSHIP_AUDIT_TRACE_MAX = 16;
  const six7ChatSignalTrace = [];
  const SIX7_CHAT_SIGNAL_TRACE_MAX = 20;

  function six7ChatSignalAudit(stage, fields = {}) {
    const row = {
      at: now(),
      stage: String(stage || "unknown"),
      peer:
        six7ShortKey(fields.peer || fields.to || fields.from)?.slice(0, 8) ||
        "none",
      kind: String(fields.kind || "none"),
      route: String(fields.route || concierge.lastRouteMode || "none"),
      signal: String(fields.signal || "none"),
      pc: String(fields.pc || "none"),
      dc: String(fields.dc || "none"),
      cycle: String(fields.cycle || "none").slice(0, 8),
      ok:
        fields.ok === true
          ? "1"
          : fields.ok === false
            ? "0"
            : String(fields.ok || "none"),
      detail: String(fields.detail || "none"),
    };

    six7ChatSignalTrace.push(row);

    if (six7ChatSignalTrace.length > SIX7_CHAT_SIGNAL_TRACE_MAX) {
      six7ChatSignalTrace.splice(
        0,
        six7ChatSignalTrace.length - SIX7_CHAT_SIGNAL_TRACE_MAX,
      );
    }

    emitSnapshot("chat_signal_audit_" + row.stage);
  }

  function six7ChatSignalAuditText() {
    if (!six7ChatSignalTrace.length) return "";

    return six7ChatSignalTrace
      .slice(-12)
      .map((row) =>
        [
          row.at,
          row.stage,
          row.peer,
          row.kind,
          row.route,
          row.signal,
          row.pc,
          row.dc,
          row.cycle,
          row.ok,
          row.detail,
        ].join("~"),
      )
      .join(" || ");
  }

  function six7MembershipAuditTopology() {
    const self = six7ShortKey(selfHKey);
    const committedRows = six7AuthorityRosterRows(six7SysAuthority.roster);
    const pending = six7SysAuthority.pending;
    const pendingRows = six7AuthorityRosterRows(pending?.snapshot?.roster);

    return {
      self: self ? self.slice(0, 8) : "none",
      role: String(concierge.localRoleCandidate || concierge.role || "peer"),
      epoch: Number(concierge.sysconEpoch || 0),
      cfEpoch: Number(concierge.cfConciergeEpoch || 0),

      committedFrom: six7ShortKey(six7SysAuthority.from)?.slice(0, 8) || "none",
      committedOne:
        six7ShortKey(six7SysAuthority.syscon1)?.slice(0, 8) || "none",
      committedTwo:
        six7ShortKey(six7SysAuthority.syscon2)?.slice(0, 8) || "none",
      committedEpoch: Number(six7SysAuthority.epoch || 0),
      committedSeq: Number(six7SysAuthority.rosterSeq || 0),
      committedCount: committedRows.length,
      committedKeys: committedRows.map((row) => row.hKey.slice(0, 8)).join(","),

      pendingId: pending?.id ? six7MembershipAuditToken(pending.id) : "none",
      pendingMirror:
        six7ShortKey(pending?.mirror || pending?.snapshot?.syscon2)?.slice(
          0,
          8,
        ) || "none",
      pendingEpoch: Number(pending?.snapshot?.epoch || 0),
      pendingBaseEpoch: Number(pending?.baseEpoch || 0),
      pendingBaseSeq: Number(pending?.baseRosterSeq || 0),
      pendingNextSeq: Number(pending?.snapshot?.rosterSeq || 0),
      pendingCount: pendingRows.length,
      pendingKeys: pendingRows.map((row) => row.hKey.slice(0, 8)).join(","),

      cfOne: six7ShortKey(concierge.cfConciergeSyscon1)?.slice(0, 8) || "none",
      cfTwo: six7ShortKey(concierge.cfConciergeSyscon2)?.slice(0, 8) || "none",

      terminalKeys: Array.from(six7PendingTerminalExits.keys())
        .map((key) => six7ShortKey(key)?.slice(0, 8))
        .filter(Boolean)
        .join(","),

      sysTarget:
        six7ShortKey(concierge.sysAttempt?.target)?.slice(0, 8) || "none",
      sysAttempt: String(concierge.sysAttempt?.state || "none"),
      sysPipe: String(concierge.sysPipeState || "none"),
    };
  }

  function six7MembershipAuditTraceText() {
    if (!six7MembershipAuditTrace.length) return "";

    return six7MembershipAuditTrace
      .slice(-10)
      .map((row) => {
        const peer = row.peer ? String(row.peer).slice(0, 8) : "none";
        return [
          String(row.at || 0),
          String(row.stage || "unknown"),
          peer,
          String(row.id || "none"),
          String(row.detail || "none"),
        ].join("~");
      })
      .join(" || ");
  }

  // One stateful reconnect observation per peer. Transport repair may begin
  // immediately, but shell presentation is withheld until the same
  // incarnation proves it is still alive after the chat edge dropped.
  const reconnectingPeers = new Map(); // hKey -> { reason, startedAt, presented, probeCid, probeSent }

  const SIX7_CHAT_RECONNECT_MS = 20000;
  const six7ChatReconnectDeadlines = new Map();
  let six7ChatAttemptSeq = 0;

  function six7NewChatAttemptId(remoteHKey) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return "";
    six7ChatAttemptSeq += 1;
    return [six7BootId, key, six7ChatAttemptSeq].join(":");
  }

  function six7CancelChatAttemptDeadline(remoteHKey) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return;
    const timer = six7ChatReconnectDeadlines.get(key);
    if (timer) clearTimeout(timer);
    six7ChatReconnectDeadlines.delete(key);
  }

  function six7ArmChatAttemptDeadline(
    remoteHKey,
    attemptId,
    reason = "chat_birth",
  ) {
    const key = six7ShortKey(remoteHKey);
    if (!key || !attemptId) return false;

    six7CancelChatAttemptDeadline(key);

    const timer = setTimeout(() => {
      six7ChatReconnectDeadlines.delete(key);

      const ent = six7ChatPeers.get(key);
      if (!ent || String(ent.chatAttemptId || "") !== String(attemptId)) {
        emitSnapshot("chat_attempt_deadline_ignored_stale");
        return;
      }

      const ready = !!(
        ent.dc?.readyState === "open" &&
        ent.chatState === "ready" &&
        ent.chatProof === "chat_hello"
      );
      if (ready) return;

      const relationship = six7RelationshipGet(key);
      if (
        !six7RelationshipIsAccepted(relationship) ||
        !lastRoster.has(key) ||
        six7PendingTerminalExits.has(key) ||
        __six7GoneNotified.has(key)
      ) {
        emitSnapshot("chat_attempt_deadline_observed_terminal_relationship");
        return;
      }

      if (!six7OwnsChatPipeOffer(key)) {
        ent.chatAttemptState = "waiting_owner";
        ent.chatFault = "owner_attempt_expired_remote";
        emitSnapshot("chat_attempt_deadline_waiting_for_pair_owner");
        return;
      }

      const recoveryCount = Number(ent.chatAttemptRecoveryCount || 0);
      if (recoveryCount >= 1) {
        ent.chatAttemptState = "terminal";
        ent.chatState = "failed";
        ent.chatReadyAt = 0;
        ent.chatProof = "none";
        ent.chatFault = "chat_attempt_terminal_nonprogress";
        concierge.lastErr = "chat_attempt_terminal_nonprogress:" + key;
        emitSnapshot("chat_attempt_terminal_nonprogress");
        return;
      }

      const nextRecoveryCount = recoveryCount + 1;
      ent.chatAttemptState = "terminal";
      ent.chatFault = "chat_attempt_nonprogress";
      emitSnapshot("chat_attempt_nonprogress_retiring_owner_attempt");

      rtcTeardown(key, "chat_attempt_nonprogress");

      const successor = six7GetOrCreateChatPeer(key);
      if (successor) {
        successor.relationshipCycleId = relationship.cycleId;
        successor.chatAttemptRecoveryCount = nextRecoveryCount;
        successor.chatState = "needed";
        successor.chatFault = "none";
      }

      void rtcDial(key, {
        pipe: "chat",
        reason: "chat_attempt_successor_" + String(reason || "nonprogress"),
      });
    }, SIX7_CHAT_RECONNECT_MS);

    six7ChatReconnectDeadlines.set(key, timer);
    return true;
  }

  const SIX7_SYS_REPLACEMENT_MS = 10000;
  const six7SysReplacementWindows = new Map(); // hKey -> { state, timer, attemptId, bootId, startedAt, terminalAt, committedAt }

  function six7SysReplacementState(remoteHKey) {
    const key = six7ShortKey(remoteHKey);
    return key
      ? String(six7SysReplacementWindows.get(key)?.state || "healthy")
      : "healthy";
  }

  function six7SysReplacementTerminal(remoteHKey) {
    const state = six7SysReplacementState(remoteHKey);
    return state === "terminal_staged" || state === "committed";
  }

  function six7SupersedeSysReplacementForCommittedRefresh(
    remoteHKey,
    previousGeneration,
    incomingGeneration,
  ) {
    const key = six7ShortKey(remoteHKey);
    if (
      !key ||
      !previousGeneration ||
      !incomingGeneration ||
      previousGeneration === incomingGeneration
    ) {
      return false;
    }

    const committed = six7LocalCommittedAuthority();
    const committedKeys = new Set(
      six7AuthorityRosterRows(committed?.roster).map((row) => row.hKey),
    );
    if (!committedKeys.has(key)) return false;

    const rec = six7SysReplacementWindows.get(key);
    if (
      rec &&
      (String(rec.state || "") === "replacing" ||
        String(rec.state || "") === "awaiting_doorway_absence")
    ) {
      try {
        if (rec.timer) clearTimeout(rec.timer);
      } catch {}
      six7SysReplacementWindows.delete(key);
    }

    six7SysDialing.delete(key);
    six7SysPreflightUnstableRetries.delete(key);
    six7PendingRtcIce.delete(key);

    if (six7ShortKey(concierge.sysAttempt?.target) === key) {
      concierge.sysAttempt.recoveryCount = 0;
      concierge.sysAttempt.state = "connecting";
      concierge.sysAttempt.lastEvent = "committed_member_new_boot";
      concierge.sysAttempt.faultReason = "none";
    }

    emitSnapshot("sys_replacement_superseded_committed_refresh");
    return true;
  }
  function six7CancelSysReplacement(
    remoteHKey,
    reason = "replacement_cancelled",
  ) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    const rec = six7SysReplacementWindows.get(key);
    const state = String(rec?.state || "");
    if (
      !rec ||
      (state !== "replacing" && state !== "awaiting_doorway_absence")
    ) {
      return false;
    }

    try {
      if (rec.timer) clearTimeout(rec.timer);
    } catch {}

    six7SysReplacementWindows.delete(key);

    if (six7ShortKey(concierge.sysAttempt?.target) === key) {
      concierge.sysAttempt.recoveryCount = 0;
      concierge.sysAttempt.lastEvent = String(
        reason || "replacement_cancelled",
      );
      concierge.sysAttempt.faultReason = "none";
    }

    emitSnapshot(
      "sys_peer_replacement_cancelled_" + String(reason || "cancelled"),
    );
    return true;
  }
  function six7CompleteFailedSysReplacement(
    remoteHKey,
    reason = "replacement_failed",
  ) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    const rec = six7SysReplacementWindows.get(key);
    const state = String(rec?.state || "");
    if (
      !rec ||
      (state !== "replacing" && state !== "awaiting_doorway_absence")
    ) {
      return false;
    }

    const self = six7ShortKey(selfHKey);
    const committed = six7LocalCommittedAuthority();
    const committedKeys = new Set(
      six7AuthorityRosterRows(committed?.roster).map((row) => row.hKey),
    );
    const mode = String(rec.mode || "");
    const leaderOwnsMember = !!(
      mode === "leader_observes_member" &&
      self &&
      six7ShortKey(committed?.syscon1) === self &&
      committedKeys.has(key)
    );
    const mirrorOwnsLeaderLoss = !!(
      mode === "mirror_observes_leader" &&
      self &&
      six7ShortKey(committed?.syscon2) === self &&
      six7ShortKey(committed?.syscon1) === key &&
      committedKeys.has(key)
    );
    const currentRole = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const currentOne = six7ShortKey(concierge.syscon1);
    const successorOwnsSpineLoss = !!(
      mode === "successor_observes_spine" &&
      self &&
      currentRole === "syscon1" &&
      currentOne === self &&
      six7ShortKey(committed?.syscon1) !== self &&
      committedKeys.has(self) &&
      committedKeys.has(key) &&
      (six7ShortKey(committed?.syscon1) === key ||
        six7ShortKey(committed?.syscon2) === key)
    );

    const restored = six7HasOpenSysPipe(key);
    const knownBoot = String(six7RemoteBootIds.get(key) || "");
    const sameIncarnation = !(
      rec.bootId &&
      knownBoot &&
      String(rec.bootId) !== knownBoot
    );

    if (
      !committed ||
      !committedKeys.has(key) ||
      restored ||
      !sameIncarnation ||
      __six7GoneNotified.has(key) ||
      (!leaderOwnsMember && !mirrorOwnsLeaderLoss && !successorOwnsSpineLoss)
    ) {
      six7CancelSysReplacement(
        key,
        restored
          ? "replacement_restored"
          : !sameIncarnation
            ? "replacement_new_incarnation"
            : "replacement_superseded",
      );
      return false;
    }

    const elapsed = now() - Number(rec.startedAt || 0);
    if (elapsed < SIX7_SYS_REPLACEMENT_MS) return false;

    // Doorway visibility is corroborating liveness only. It never mutates
    // membership; it merely prevents a transient SYS fault from becoming a
    // terminal command while the same incarnation still owns a live socket.
    if (host.doorwayPresence === 'live-sockets' && six7DoorwayAdmittedPeers.has(key)) {
      try {
        if (rec.timer) clearTimeout(rec.timer);
      } catch {}
      rec.timer = null;
      rec.state = "awaiting_doorway_absence";
      rec.awaitingDoorwayAt = now();
      six7SysReplacementWindows.set(key, rec);
      emitSnapshot("sys_replacement_waiting_for_doorway_absence");
      return false;
    }

    try {
      if (rec.timer) clearTimeout(rec.timer);
    } catch {}
    rec.timer = null;
    rec.state = "terminal_staged";
    rec.terminalAt = now();
    rec.terminalReason = String(reason || "replacement_failed");
    six7SysReplacementWindows.set(key, rec);

    if (six7ShortKey(concierge.sysAttempt?.target) === key) {
      concierge.sysAttempt.state = "terminal";
      concierge.sysAttempt.lastEvent = "replacement_failed_terminal";
      concierge.sysAttempt.faultReason = String(reason || "replacement_failed");
      concierge.sysAttempt.recoveryCount = 1;
    }

    six7TeardownSysPeer(
      key,
      "sys_replacement_terminal_" + String(reason || "failed"),
    );

    const terminal = six7TerminalExitCommand(
      {
        type: "authority_exit",
        from: key,
        member: key,
        room: videoId || null,
        videoId: videoId || null,
        bootId: String(rec.bootId || knownBoot || ""),
        epoch: Number(committed?.epoch || concierge.sysconEpoch || 0),
        reason: String(reason || "sys_replacement_failed"),
        at: now(),
      },
      "sys_replacement_failed",
    );

    const accepted = terminal || six7PendingTerminalExits.has(key);
    if (accepted) {
      six7SysDialing.delete(key);
      six7SysPreflightUnstableRetries.delete(key);
      six7PendingRtcIce.delete(key);
    }

    emitSnapshot(
      accepted
        ? "sys_replacement_terminal_exit_recorded"
        : "sys_replacement_terminal_exit_failed",
    );
    return accepted;
  }
  function six7ArmSysReplacement(remoteHKey, reason = "sys_transport_lost") {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    const currentState = six7SysReplacementState(key);
    if (
      currentState === "replacing" ||
      currentState === "awaiting_doorway_absence" ||
      currentState === "terminal_staged" ||
      currentState === "committed"
    ) {
      emitSnapshot("sys_replacement_arm_ignored_state_" + currentState);
      return false;
    }

    const self = six7ShortKey(selfHKey);
    const committed = six7LocalCommittedAuthority();
    const committedKeys = new Set(
      six7AuthorityRosterRows(committed?.roster).map((row) => row.hKey),
    );
    const committedOne = six7ShortKey(committed?.syscon1);
    const committedTwo = six7ShortKey(committed?.syscon2);

    const leaderObservesMember = !!(
      self &&
      committedOne === self &&
      key !== self &&
      committedKeys.has(key)
    );
    const mirrorObservesLeader = !!(
      self &&
      committedTwo === self &&
      committedOne === key &&
      committedKeys.has(key)
    );
    const currentRole = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const currentOne = six7ShortKey(concierge.syscon1);
    const successorObservesSpine = !!(
      self &&
      currentRole === "syscon1" &&
      currentOne === self &&
      committedOne !== self &&
      committedKeys.has(self) &&
      committedKeys.has(key) &&
      (key === committedOne || key === committedTwo)
    );

    if (
      !leaderObservesMember &&
      !mirrorObservesLeader &&
      !successorObservesSpine
    ) {
      return false;
    }

    const mode = leaderObservesMember
      ? "leader_observes_member"
      : mirrorObservesLeader
        ? "mirror_observes_leader"
        : "successor_observes_spine";
    const attemptId = String(
      six7SysPeers.get(key)?.attempt?.id || concierge.sysAttempt?.id || "",
    );
    const bootId = String(six7RemoteBootIds.get(key) || "");
    const timer = setTimeout(() => {
      six7CompleteFailedSysReplacement(key, "replacement_window_expired");
    }, SIX7_SYS_REPLACEMENT_MS);

    six7SysReplacementWindows.set(key, {
      state: "replacing",
      mode,
      timer,
      attemptId,
      bootId,
      startedAt: now(),
      reason: String(reason || "sys_transport_lost"),
    });

    if (six7ShortKey(concierge.sysAttempt?.target) === key) {
      concierge.sysAttempt.recoveryCount = 1;
      concierge.sysAttempt.state = "recovering";
      concierge.sysAttempt.lastEvent = "replacement_window_started";
      concierge.sysAttempt.faultReason = String(reason || "sys_transport_lost");
    }

    emitSnapshot("sys_peer_replacement_window_started_" + mode);
    return true;
  }

  // ======================================================
  // VEIL RELATIONSHIP STATE (session-only)
  // ======================================================

  const VEIL = {
    OUTSIDE: "outside",
    INVITE_SENT: "invite_sent",
    INVITE_RECEIVED: "invite_received",
    ESTABLISHED: "established",
    INSIDE: "inside",
    REMOVED: "removed",
  };

  // One authoritative relationship lifecycle record per peer.
  // Transport generations may come and go, but they must bind to this cycle.
  const six7Relationships = new Map(); // hKey -> relationship record

  const six7RelationshipRevoked = new Set();

  function six7RelationshipCycleId(peer, seed = "cycle") {
    const self = six7ShortKey(selfHKey) || "self";
    const remote = six7ShortKey(peer) || "peer";
    return `${[self, remote].sort().join(":")}:${String(seed || "cycle")}:${now()}:${Math.random().toString(16).slice(2)}`;
  }

  function six7RelationshipGet(hKey) {
    const key = six7ShortKey(hKey);
    if (!key) return null;

    let rec = six7Relationships.get(key);
    if (!rec) {
      rec = {
        hKey: key,
        state: VEIL.OUTSIDE,
        cycleId: null,
        inviteCid: null,
        localBootId: six7BootId,
        remoteBootId: String(six7RemoteBootIds.get(key) || "") || null,
        membershipEpoch: Number(concierge.sysconEpoch || 0),
        createdAt: 0,
        updatedAt: now(),
        endedAt: 0,
        endReason: "none",
      };
      six7Relationships.set(key, rec);
    }
    return rec;
  }

  function six7RelationshipIsAccepted(rec) {
    return (
      !!rec &&
      !rec.endedAt &&
      (rec.state === VEIL.INSIDE || rec.state === VEIL.ESTABLISHED)
    );
  }

  function six7RelationshipBegin(hKey, state, options = {}) {
    const key = six7ShortKey(hKey);
    if (!key) return null;

    const at = Number(options.at || now());
    const rec = six7RelationshipGet(key);
    rec.state = String(state || VEIL.OUTSIDE);
    rec.cycleId = String(
      options.cycleId ||
        six7RelationshipCycleId(key, options.origin || state || "cycle"),
    );
    rec.inviteCid = String(options.inviteCid || "") || null;
    rec.localBootId = six7BootId;
    rec.remoteBootId =
      String(options.remoteBootId || six7RemoteBootIds.get(key) || "") || null;
    rec.membershipEpoch = Number(
      options.membershipEpoch || concierge.sysconEpoch || 0,
    );
    rec.createdAt = at;
    rec.updatedAt = at;
    rec.endedAt = 0;
    rec.endReason = "none";
    six7Relationships.set(key, rec);
    return rec;
  }

  function six7RelationshipEnd(
    hKey,
    reason = "ended",
    at = now(),
    revoke = true,
  ) {
    const key = six7ShortKey(hKey);
    if (!key) return null;

    const rec = six7RelationshipGet(key);
    rec.state = VEIL.OUTSIDE;
    rec.updatedAt = Number(at || now());
    rec.endedAt = rec.updatedAt;
    rec.endReason = String(reason || "ended");
    six7Relationships.set(key, rec);

    if (revoke) six7RelationshipRevoked.add(key);
    return rec;
  }

  function six7RelationshipMatches(msg, rec) {
    if (!rec || !rec.cycleId) return false;
    const cycleId = String(msg?.cycleId || "");
    if (cycleId && cycleId === String(rec.cycleId)) return true;

    const replyTo = String(msg?.replyTo || "");
    if (replyTo && rec.inviteCid && replyTo === String(rec.inviteCid))
      return true;

    return false;
  }

  const six7BlockedPeers = new Set(); // peers I blocked
  const six7BlockedByPeers = new Set(); // peers who blocked me

  const six7SeenControlCids = new Map();
  const SIX7_CONTROL_CID_MAX = 300;

  function six7NewControlCid(type) {
    const me = six7ShortKey(selfHKey) || "self";
    return `${me}:${String(type || "control")}:${now()}:${Math.random().toString(16).slice(2)}`;
  }

  function six7PrepareRelationshipControl(msg, type) {
    if (!msg || typeof msg !== "object") return msg;

    msg.type = String(msg.type || type || "unknown");
    msg.from = six7ShortKey(msg.from || selfHKey);
    msg.to = six7ShortKey(msg.to);
    msg.videoId = msg.videoId || videoId || null;
    msg.cid = msg.cid || six7NewControlCid(msg.type);
    msg.sentAt = msg.sentAt || now();

    const relationship = six7Relationships.get(msg.to);
    if (relationship?.cycleId && !msg.cycleId) {
      msg.cycleId = relationship.cycleId;
    }

    if (msg.type === "sys_unblock" || msg.type === "sys_unblock_ack") {
      msg.sysTxId = String(msg.sysTxId || msg.ackFor || msg.cid || "") || null;
    }

    return msg;
  }

  function six7AuthorityGossipSnapshot() {
    const authority = six7LocalCommittedAuthority();
    if (!authority) return null;

    return {
      gossip: true,
      room: String(authority.room || videoId || ""),
      from: six7ShortKey(authority.from || authority.syscon1),
      role: "syscon1",
      syscon1: six7ShortKey(authority.syscon1),
      syscon2: six7ShortKey(authority.syscon2) || null,
      epoch: Number(authority.epoch || 0),
      rosterSeq: Number(authority.rosterSeq || 0),
      roster: six7AuthorityRosterRows(authority.roster),
      acceptedAt: Number(authority.at || 0),
      relayedAt: now(),
    };
  }

  function six7RememberAuthorityGossip(sourceHKey, hint) {
    const source = six7ShortKey(sourceHKey);
    const self = six7ShortKey(selfHKey);
    if (!source || !self || !hint || hint.gossip !== true) return false;

    const room = String(hint.room || "");
    const one = six7ShortKey(hint.syscon1);
    const two = six7ShortKey(hint.syscon2);
    const from = six7ShortKey(hint.from || one);
    const epoch = Number(hint.epoch || 0);
    const rosterSeq = Number(hint.rosterSeq || 0);
    const roster = six7AuthorityRosterRows(hint.roster);
    const rosterKeys = new Set(roster.map((row) => row.hKey));

    if (
      !room ||
      (videoId && room !== String(videoId)) ||
      !one ||
      from !== one ||
      String(hint.role || "") !== "syscon1" ||
      !epoch ||
      !rosterSeq ||
      !roster.length ||
      !rosterKeys.has(one) ||
      (two && !rosterKeys.has(two)) ||
      !rosterKeys.has(self) ||
      !rosterKeys.has(source)
    ) {
      emitSnapshot("authority_gossip_invalid_ignored");
      return false;
    }

    const normalized = {
      room,
      from,
      role: "syscon1",
      syscon1: one,
      syscon2: two || null,
      epoch,
      rosterSeq,
      roster,
      source,
      at: now(),
    };

    six7AuthorityGossipHints.set(source, normalized);

    // Local committed authority remains the only source of truth. Gossip
    // contributes route hints only; it never replaces the committed value.
    const localAuthority = six7LocalCommittedAuthority();
    if (localAuthority) {
      six7RefreshSysCandidates(
        six7LatchedSysRosterList(),
        "authority_gossip_local_authority_rebind",
      );
    }

    const localIsAtLeastAsFresh = !!(
      localAuthority &&
      (Number(localAuthority.epoch || 0) > epoch ||
        (Number(localAuthority.epoch || 0) === epoch &&
          Number(localAuthority.rosterSeq || 0) >= rosterSeq))
    );

    const routeOne = localIsAtLeastAsFresh
      ? six7ShortKey(localAuthority?.syscon1)
      : one;
    const routeTwo = localIsAtLeastAsFresh
      ? six7ShortKey(localAuthority?.syscon2)
      : two;

    const targets = [routeOne, routeTwo]
      .map(six7ShortKey)
      .filter(
        (key, index, rows) =>
          key && key !== self && rows.indexOf(key) === index,
      );

    six7GossipRecovery = {
      source,
      targets,
      index: 0,
      epoch,
      rosterSeq,
      at: now(),
    };

    if (targets.length) {
      six7ScheduleSysAdmission("authority_gossip_recovery_target");
      emitSnapshot("authority_gossip_recovery_started");
    } else {
      emitSnapshot("authority_gossip_no_remote_syscon_target");
    }

    return true;
  }

  function six7BeginGossipSuccessorRecovery(
    reason = "authority_gossip_syscons_unreachable",
  ) {
    const self = six7ShortKey(selfHKey);
    const authority = six7LocalCommittedAuthority();
    if (!self || !authority) {
      emitSnapshot("authority_gossip_successor_missing_committed_base");
      return false;
    }

    const oldOne = six7ShortKey(authority.syscon1);
    const oldTwo = six7ShortKey(authority.syscon2);
    const oldSpine = [oldOne, oldTwo].filter(Boolean);
    const attempted = new Set(
      (six7GossipRecovery.targets || []).map(six7ShortKey).filter(Boolean),
    );

    if (oldSpine.some((key) => key !== self && !attempted.has(key))) {
      emitSnapshot("authority_gossip_successor_spine_not_exhausted");
      return false;
    }

    const survivors = six7AuthorityRosterRows(authority.roster).filter(
      (row) => row.hKey !== oldOne && row.hKey !== oldTwo,
    );
    const winner = six7ElectAuthorityCandidate(survivors, "", false, true);

    if (!winner || winner !== self) {
      six7GossipRecovery.successor = winner || null;
      six7GossipRecovery.state = "waiting_for_elected_survivor";
      emitSnapshot("authority_gossip_waiting_for_elected_survivor");
      return false;
    }

    const mirror = six7ElectAuthorityCandidate(survivors, self, false, false);

    // This is only the same provisional topology candidate produced by the
    // normal concierge reconciliation path. Committed authority is unchanged;
    // the existing replacement windows, terminal proof, election, stage, and
    // mirror-accepted authority transaction remain the only promotion owner.
    concierge.syscon1 = self;
    concierge.syscon2 = mirror || null;
    concierge.localRoleCandidate = "syscon1";
    concierge.candidateReason =
      "authority_gossip_provisional_successor_" + String(reason || "loss");
    concierge.lastCandidateAt = now();
    six7ApplyLocalRoleCandidate("authority_gossip_provisional_successor");

    six7GossipRecovery.successor = self;
    six7GossipRecovery.state = "replacement_proof";
    six7GossipRecovery.exhaustedAt = now();

    let armed = false;
    for (const oldKey of oldSpine) {
      if (!oldKey || oldKey === self) continue;
      if (
        six7ArmSysReplacement(oldKey, "authority_gossip_syscon_unreachable")
      ) {
        armed = true;
      }
    }

    six7RefreshSysReadiness("authority_gossip_provisional_successor");
    emitSnapshot(
      armed
        ? "authority_gossip_successor_replacement_proof_started"
        : "authority_gossip_successor_replacement_proof_not_armed",
    );
    return armed;
  }

  function six7StartTemporarySysTransaction(targetHKey, msg) {
    const target = six7ShortKey(targetHKey);
    const self = six7ShortKey(selfHKey);
    const purpose = "relationship_unblock";
    const id = String(msg?.sysTxId || msg?.cid || "");

    if (
      !target ||
      !self ||
      target === self ||
      !id ||
      !SIX7_TEMP_SYS_PURPOSES.has(purpose) ||
      !six7CommittedMember(self) ||
      !six7CommittedMember(target) ||
      six7PendingTerminalExits.has(target) ||
      __six7MembershipExitApplied.has(target)
    ) {
      emitSnapshot("temporary_sys_transaction_start_rejected");
      return false;
    }

    const current = six7TemporarySysTransactions.get(target);
    if (current && current.id !== id && current.state !== "closed") {
      emitSnapshot("temporary_sys_transaction_busy");
      return false;
    }

    const createdAt = now();
    const deadlineAt = createdAt + SIX7_SYS_REPLACEMENT_MS;
    const deadlineTimer = setTimeout(() => {
      const active = six7TemporarySysTransactions.get(target);
      if (
        !active ||
        String(active.id || "") !== id ||
        active.state === "closed" ||
        now() < Number(active.deadlineAt || 0)
      ) {
        return;
      }
      six7CloseTemporarySysTransaction(target, "transaction_deadline");
    }, SIX7_SYS_REPLACEMENT_MS + 25);

    six7TemporarySysTransactions.set(target, {
      id,
      purpose,
      peer: target,
      initiator: self,
      state: "birthing",
      inbound: false,
      createdAt,
      openedAt: 0,
      ackAt: 0,
      deadlineAt,
      deadlineTimer,
    });

    const ent = six7SysPeers.get(target);
    if (ent) {
      ent.temporary = true;
      ent.tempSysPurpose = purpose;
      ent.tempSysTxId = id;
    }

    void six7EnsureSysPipe(target, "temporary_sys_" + purpose);
    emitSnapshot("temporary_sys_transaction_birth_requested");
    return true;
  }

  function six7BindInboundTemporarySysTransaction(fromHKey, meta = {}) {
    const from = six7ShortKey(fromHKey);
    const self = six7ShortKey(selfHKey);
    const purpose = String(meta?.sysPurpose || "");
    const id = String(meta?.sysTxId || "");
    const room = String(meta?.room || videoId || "");

    if (meta?.sysTemporary !== true) return false;

    const relationship = six7RelationshipGet(from);
    const expectedUnblock = !!(
      six7BlockedByPeers.has(from) ||
      six7RelationshipRevoked.has(from) ||
      String(relationship?.endReason || "") === "blocked_by_peer"
    );

    if (
      !from ||
      !self ||
      from === self ||
      !id ||
      !SIX7_TEMP_SYS_PURPOSES.has(purpose) ||
      purpose !== "relationship_unblock" ||
      !expectedUnblock ||
      !six7CommittedMember(self) ||
      !six7CommittedMember(from) ||
      six7PendingTerminalExits.has(from) ||
      __six7MembershipExitApplied.has(from) ||
      !room ||
      (videoId && room !== String(videoId))
    ) {
      emitSnapshot("temporary_sys_transaction_offer_rejected");
      return false;
    }

    const current = six7TemporarySysTransactions.get(from);
    if (current && current.id !== id && current.state !== "closed") {
      emitSnapshot("temporary_sys_transaction_offer_conflict");
      return false;
    }

    const existing = six7SysPeers.get(from);
    if (existing && !existing.temporary && six7HasOpenSysPipe(from)) {
      emitSnapshot("temporary_sys_transaction_rejected_normal_pipe_open");
      return false;
    }

    if (existing && !existing.temporary) {
      six7TeardownSysPeer(from, "temporary_sys_retires_stale_edge");
    }

    const createdAt = now();
    const deadlineAt = createdAt + SIX7_SYS_REPLACEMENT_MS;
    const deadlineTimer = setTimeout(() => {
      const active = six7TemporarySysTransactions.get(from);
      if (
        !active ||
        String(active.id || "") !== id ||
        active.state === "closed" ||
        now() < Number(active.deadlineAt || 0)
      ) {
        return;
      }
      six7CloseTemporarySysTransaction(from, "transaction_deadline");
    }, SIX7_SYS_REPLACEMENT_MS + 25);

    six7TemporarySysTransactions.set(from, {
      id,
      purpose,
      peer: from,
      initiator: from,
      state: "offer_received",
      inbound: true,
      createdAt,
      openedAt: 0,
      ackAt: 0,
      deadlineAt,
      deadlineTimer,
    });

    emitSnapshot("temporary_sys_transaction_offer_accepted");
    return true;
  }

  function six7CloseTemporarySysTransaction(remoteHKey, reason = "complete") {
    const key = six7ShortKey(remoteHKey);
    const rec = six7TemporarySysTransactions.get(key);
    if (!key || !rec) return false;

    rec.state = "closed";
    rec.closedAt = now();
    rec.closeReason = String(reason || "complete");
    try {
      if (rec.deadlineTimer) clearTimeout(rec.deadlineTimer);
    } catch {}
    rec.deadlineTimer = null;
    six7TemporarySysTransactions.delete(key);

    const pending = six7PendingRelationship.get(key);
    if (pending?.length) {
      const kept = pending.filter(
        (item) => String(item?.msg?.sysTxId || "") !== String(rec.id || ""),
      );
      if (kept.length) six7PendingRelationship.set(key, kept);
      else six7PendingRelationship.delete(key);
    }

    const ent = six7SysPeers.get(key);
    if (
      ent?.temporary === true &&
      String(ent.tempSysTxId || "") === String(rec.id || "")
    ) {
      six7TeardownSysPeer(
        key,
        "temporary_sys_transaction_" + String(reason || "complete"),
      );
    }

    emitSnapshot(
      "temporary_sys_transaction_closed_" + String(reason || "complete"),
    );
    return true;
  }

  function six7TemporarySysAllowsEnvelope(remoteHKey, envelope) {
    const key = six7ShortKey(remoteHKey);
    const rec = six7TemporarySysTransactions.get(key);
    if (!rec || rec.state === "closed") return true;

    const type = String(envelope?.t || envelope?.type || "");
    if (type === "SYS_CONTROL_ACK") return true;
    if (type !== "SYS_CONTROL") return false;

    const inner = envelope?.msg || null;
    const kind = String(envelope?.kind || inner?.type || "");
    const txId = String(inner?.sysTxId || "");

    return !!(
      (kind === "sys_unblock" || kind === "sys_unblock_ack") &&
      txId &&
      txId === String(rec.id || "")
    );
  }

  function six7ControlSeenKey(msg, via = "") {
    const cid = String(msg?.cid || "");
    if (!cid) return "";
    return [
      String(msg?.type || "unknown"),
      six7ShortKey(msg?.from),
      six7ShortKey(msg?.to),
      cid,
    ].join("|");
  }

  function six7RememberControlCid(msg, via = "") {
    const key = six7ControlSeenKey(msg, via);
    if (!key) return false;

    if (six7SeenControlCids.has(key)) {
      concierge.lastErr =
        "control_duplicate_ignored:" + String(msg?.type || "unknown");
      emitSnapshot(
        "control_duplicate_ignored_" + String(msg?.type || "unknown"),
      );
      return true;
    }

    six7SeenControlCids.set(key, now());

    if (six7SeenControlCids.size > SIX7_CONTROL_CID_MAX) {
      const rows = Array.from(six7SeenControlCids.entries()).sort(
        (a, b) => Number(a[1] || 0) - Number(b[1] || 0),
      );

      while (
        six7SeenControlCids.size > Math.floor(SIX7_CONTROL_CID_MAX * 0.75) &&
        rows.length
      ) {
        six7SeenControlCids.delete(rows.shift()[0]);
      }
    }

    return false;
  }

  function six7HandleInboundRelationshipControl(msg, via = "unknown") {
    if (!msg || typeof msg !== "object") return false;

    const type = String(msg.type || "");
    if (six7ControlClassFor(type) !== "relationship_control") return false;
    if (!isDirectMsgForMe(msg)) return true;

    const from = six7ShortKey(msg.from);
    if (!from) return true;

    if (six7RememberControlCid(msg, via)) return true;

    six7MarkControlRx(type);
    concierge.lastRelationshipRxAt = now();
    concierge.lastRelationshipRxType = type;
    concierge.lastRelationshipRxVia = String(via || "unknown");
    concierge.lastRxType = type;
    concierge.lastRxAt = now();

    const pendingExit = six7PendingTerminalExits.has(from);
    const committedMember = !!(
      lastRoster.has(from) && six7CommittedMember(from)
    );
    const membershipGone = !!(
      pendingExit ||
      __six7MembershipExitApplied.has(from) ||
      !committedMember
    );

    if (
      membershipGone &&
      type !== "sys_unblock" &&
      type !== "sys_unblock_ack"
    ) {
      emitSnapshot("relationship_control_ignored_terminal_peer_" + type);
      return true;
    }

    if (
      (type === "sys_unblock" || type === "sys_unblock_ack") &&
      !committedMember
    ) {
      emitSnapshot("relationship_unblock_ignored_nonmember");
      return true;
    }

    if (via === "six7_sys_temporary") {
      const transaction = six7TemporarySysTransactions.get(from);
      const txId = String(msg?.sysTxId || "");
      if (
        !transaction ||
        transaction.state === "closed" ||
        transaction.purpose !== "relationship_unblock" ||
        !txId ||
        txId !== String(transaction.id || "") ||
        (type !== "sys_unblock" && type !== "sys_unblock_ack")
      ) {
        emitSnapshot("temporary_sys_relationship_control_rejected");
        return true;
      }
    }

    if (type === "veil_invite") {
      if (via === "six7_sys") {
        const ent = six7SysPeers.get(from);
        const sysState = String(ent?.sysdc?.readyState || "");
        concierge.sysPeer = from;
        if (sysState === "open" || sysState === "connecting") {
          six7RefreshSysReadiness("relationship_rx_invite_observe_" + sysState);
          emitSnapshot("relationship_rx_invite_observe_" + sysState);
        }
      }

      if (six7IsBlocked(from) || six7IsBlockedBy(from)) {
        six7RelationshipEnd(from, "invite_blocked", now(), true);
        setState(from, VEIL.OUTSIDE, true);
        six7SendControl(
          {
            type: "veil_deny",
            from: selfHKey,
            to: from,
            videoId,
            replyTo: msg.cid || null,
            cycleId: msg.cycleId || msg.cid || null,
          },
          "veil_deny",
        );
        return true;
      }

      // CHAT_ENDED closes only the relationship cycle. A still-committed
      // member may begin a new cycle; membership-exit latches are never
      // cleared here.
      __six7GoneNotified.delete(from);
      six7RelationshipRevoked.delete(from);

      const current = six7RelationshipGet(from);
      const incomingCycle = String(
        msg.cycleId ||
          msg.cid ||
          six7RelationshipCycleId(from, "remote_invite"),
      );
      const incomingInviteCid = String(msg.cid || "") || null;

      if (six7RelationshipIsAccepted(current)) {
        if (String(current.cycleId || "") !== incomingCycle) {
          emitSnapshot("relationship_invite_ignored_active_cycle");
        }
        return true;
      }

      if (current.state === VEIL.INVITE_SENT && !current.endedAt) {
        const canonicalCycle =
          [String(current.cycleId || ""), incomingCycle]
            .filter(Boolean)
            .sort()[0] || incomingCycle;
        current.cycleId = canonicalCycle;
        current.state = VEIL.INSIDE;
        current.updatedAt = now();
        current.endedAt = 0;
        current.endReason = "none";
        six7Relationships.set(from, current);
        six7RelationshipRevoked.delete(from);
        setState(from, VEIL.INSIDE);
        six7SendControl(
          {
            type: "veil_accept",
            from: selfHKey,
            to: from,
            videoId,
            replyTo: incomingInviteCid,
            cycleId: canonicalCycle,
          },
          "veil_accept",
        );
        post("VEIL_ACCEPTED", { from, simultaneous: true });
        six7RequestChatPipe(from, "relationship_simultaneous_accept");
        return true;
      }

      six7RelationshipRevoked.delete(from);
      six7CancelReconnect(from);
      rtcTeardown(from, "relationship_new_cycle");
      six7RelationshipBegin(from, VEIL.INVITE_RECEIVED, {
        cycleId: incomingCycle,
        inviteCid: incomingInviteCid,
        origin: "remote_invite",
      });
      setState(from, VEIL.INVITE_RECEIVED, true);
      post("VEIL_INVITE_RECEIVED", { from });
      return true;
    }

    if (type === "veil_accept") {
      const current = six7RelationshipGet(from);
      if (six7IsBlocked(from) || six7IsBlockedBy(from)) {
        six7RelationshipEnd(from, "accept_blocked", now(), true);
        setState(from, VEIL.OUTSIDE, true);
        return true;
      }

      if (
        current.state !== VEIL.INVITE_SENT ||
        current.endedAt ||
        !six7RelationshipMatches(msg, current)
      ) {
        concierge.lastErr = "veil_accept_ignored_wrong_relationship_cycle";
        emitSnapshot("veil_accept_ignored_wrong_relationship_cycle");
        return true;
      }

      six7RelationshipRevoked.delete(from);
      current.state = VEIL.INSIDE;
      current.updatedAt = now();
      current.endedAt = 0;
      current.endReason = "none";
      six7Relationships.set(from, current);
      setState(from, VEIL.INSIDE);
      post("VEIL_ACCEPTED", { from });
      six7RequestChatPipe(from, "relationship_accept");
      return true;
    }

    if (type === "veil_deny") {
      const current = six7RelationshipGet(from);
      if (
        !current.cycleId ||
        !msg.cycleId ||
        six7RelationshipMatches(msg, current)
      ) {
        six7RelationshipEnd(from, "remote_deny", now(), false);
        setState(from, VEIL.OUTSIDE, true);
        six7CancelReconnect(from);
        rtcTeardown(from, "relationship_denied");
      } else {
        emitSnapshot("veil_deny_ignored_wrong_relationship_cycle");
      }
      return true;
    }

    if (type === "veil_chat_failed") {
      emitSnapshot(
        "legacy_veil_chat_failed_ignored_transport_cannot_end_relationship",
      );
      return true;
    }

    if (type === "veil_typing") {
      const current = six7RelationshipGet(from);

      if (
        via !== "six7_sys" ||
        six7IsBlocked(from) ||
        six7IsBlockedBy(from) ||
        !six7RelationshipIsAccepted(current) ||
        !six7RelationshipMatches(msg, current)
      ) {
        emitSnapshot("veil_typing_ignored_invalid_relationship");
        return true;
      }

      post("VEIL_TYPING", {
        from,
        active: msg.active === true,
        at: Number(msg.sentAt || msg.t || now()),
      });

      return true;
    }

    if (type === "veil_status") {
      let current = six7RelationshipGet(from);
      if (six7IsBlocked(from) || six7IsBlockedBy(from)) return true;
      if (
        !(lastRoster instanceof Set && lastRoster.has(from)) ||
        via !== "six7_sys"
      )
        return true;

      const incomingCycleId = String(msg?.cycleId || "");
      const incomingState = String(msg?.state || "");
      const incomingAccepted =
        incomingState === VEIL.INSIDE || incomingState === VEIL.ESTABLISHED;

      const continuityPending = !!(
        six7PendingRefreshRoom === String(videoId || "") &&
        six7PendingRefreshRelationships.has(from)
      );

      if (
        !six7RelationshipRevoked.has(from) &&
        !six7RelationshipIsAccepted(current) &&
        !current?.cycleId &&
        incomingAccepted &&
        incomingCycleId
      ) {
        current = six7RelationshipBegin(from, VEIL.ESTABLISHED, {
          cycleId: incomingCycleId,
          remoteBootId:
            String(msg?.bootId || six7RemoteBootIds.get(from) || "") || null,
          membershipEpoch: Number(concierge.sysconEpoch || 0),
          origin: "surviving_peer_continuity_proof",
        });

        six7PendingRefreshRelationships.delete(from);
        if (!six7PendingRefreshRelationships.size) {
          six7PendingRefreshRoom = "";
          try {
            sessionStorage.removeItem("six7_refresh_relationships_v1");
          } catch {}
        }

        emitSnapshot("relationship_continuity_adopted_from_surviving_peer");
      } else if (continuityPending) {
        if (
          six7RelationshipRevoked.has(from) ||
          !current?.cycleId ||
          current.endedAt ||
          !incomingAccepted ||
          !six7RelationshipMatches(msg, current)
        ) {
          emitSnapshot("veil_status_ignored_wrong_continuity_cycle");
          return true;
        }

        six7RelationshipRevoked.delete(from);
        setState(from, VEIL.ESTABLISHED, true);
        six7PendingRefreshRelationships.delete(from);

        if (!six7PendingRefreshRelationships.size) {
          six7PendingRefreshRoom = "";
          try {
            sessionStorage.removeItem("six7_refresh_relationships_v1");
          } catch {}
        }

        emitSnapshot("relationship_continuity_peer_validated");
      } else {
        if (
          six7RelationshipRevoked.has(from) ||
          !six7RelationshipIsAccepted(current)
        ) {
          emitSnapshot("veil_status_ignored_no_active_relationship_cycle");
          return true;
        }
        if (!six7RelationshipMatches(msg, current)) {
          emitSnapshot("veil_status_ignored_wrong_relationship_cycle");
          return true;
        }
      }

      const reconnect = reconnectingPeers.get(from) || null;
      const reconnectProof = !!(
        reconnect &&
        reconnect.probeCid &&
        msg?.reconnectProbe === true &&
        String(msg?.replyTo || "") === String(reconnect.probeCid)
      );

      if (reconnectProof) {
        six7PresentReconnect(
          from,
          "veil_status_reply",
          String(msg?.bootId || ""),
        );
      }

      if (!msg.replyTo) {
        six7SendControl(
          {
            type: "veil_status",
            from: selfHKey,
            to: from,
            videoId,
            state: getState(from),
            cycleId: current.cycleId,
            replyTo: msg.cid || null,
            reconnectProbe: msg?.reconnectProbe === true,
            bootId: six7BootId,
          },
          "veil_status",
        );
      }

      six7RequestChatPipe(from, "relationship_status_reconcile");
      return true;
    }

    if (type === "sys_block") {
      six7BlockedByPeers.add(from);
      six7BlockedPeers.delete(from);
      six7RelationshipEnd(from, "blocked_by_peer", now(), true);
      six7CancelReconnect(from);
      loungeWaiters.delete(from);
      six7PendingRelationship.delete(from);
      six7PendingDm.delete(from);
      six7PendingRtcIce.delete(from);
      setState(from, VEIL.OUTSIDE, true);
      rtcTeardown(from, "blocked_by_peer");
      post("BLOCKED_BY", { from, at: msg.t || msg.sentAt || Date.now() });
      return true;
    }

    if (type === "sys_unblock") {
      six7BlockedByPeers.delete(from);
      six7BlockedPeers.delete(from);
      six7RelationshipRevoked.delete(from);
      if (
        six7CommittedMember(from) &&
        !six7PendingTerminalExits.has(from) &&
        !__six7MembershipExitApplied.has(from)
      ) {
        __six7GoneNotified.delete(from);
      }
      six7RelationshipEnd(from, "unblocked", now(), false);
      setState(from, VEIL.OUTSIDE, true);
      post("UNBLOCKED_BY", { from, at: msg.t || msg.sentAt || Date.now() });

      const transaction = six7TemporarySysTransactions.get(from);
      if (transaction) {
        transaction.state = "command_received";
      }

      const ackSent = six7SendControl(
        {
          type: "sys_unblock_ack",
          from: selfHKey,
          to: from,
          videoId,
          ackFor: msg.cid || null,
          sysTxId: msg.sysTxId || msg.cid || null,
          authorityHint:
            msg.recoveryProbe === true || via === "six7_sys_temporary"
              ? six7AuthorityGossipSnapshot()
              : null,
          t: now(),
        },
        "sys_unblock_ack",
      );

      if (transaction && ackSent) {
        transaction.state = "ack_sent";
        transaction.ackAt = now();
      }

      return true;
    }

    if (type === "sys_unblock_ack") {
      six7BlockedPeers.delete(from);
      six7BlockedByPeers.delete(from);
      six7RelationshipRevoked.delete(from);
      if (
        six7CommittedMember(from) &&
        !six7PendingTerminalExits.has(from) &&
        !__six7MembershipExitApplied.has(from)
      ) {
        __six7GoneNotified.delete(from);
      }
      six7RelationshipEnd(from, "unblocked_ack", now(), false);
      setState(from, VEIL.OUTSIDE, true);

      if (msg.authorityHint) {
        six7RememberAuthorityGossip(from, msg.authorityHint);
      }

      post("SYS_UNBLOCK_ACK", {
        from,
        ackFor: msg.ackFor || null,
        at: msg.t || msg.sentAt || Date.now(),
      });

      const transaction = six7TemporarySysTransactions.get(from);
      if (
        transaction &&
        String(msg.sysTxId || "") === String(transaction.id || "")
      ) {
        transaction.state = "acknowledged";
        transaction.ackAt = now();
        six7CloseTemporarySysTransaction(from, "acknowledged");
      }

      return true;
    }

    return true;
  }

  function six7BlockKey(hKey) {
    return String(hKey || "")
      .slice(0, 16)
      .toLowerCase();
  }

  function six7IsBlocked(hKey) {
    const key = six7BlockKey(hKey);
    return !!key && six7BlockedPeers.has(key);
  }

  function six7IsBlockedBy(hKey) {
    const key = six7BlockKey(hKey);
    return !!key && six7BlockedByPeers.has(key);
  }

  function getState(hKey) {
    return six7RelationshipGet(hKey)?.state || VEIL.OUTSIDE;
  }

  function six7RequestChatPipe(hKey, reason = "relationship") {
    if (!selfHKey) return false;

    const key = six7ShortKey(hKey);
    if (!key) return false;

    if (
      six7RelationshipRevoked.has(key) ||
      six7PendingTerminalExits.has(key) ||
      __six7GoneNotified.has(key)
    ) {
      concierge.lastErr =
        "chat_pipe_blocked_relationship_terminal:" +
        String(reason || "relationship");
      emitSnapshot("chat_pipe_blocked_relationship_terminal");
      return false;
    }

    const relationship = six7RelationshipGet(key);
    const st = relationship?.state || VEIL.OUTSIDE;

    if (
      !six7RelationshipIsAccepted(relationship) ||
      (st !== VEIL.ESTABLISHED && st !== VEIL.INSIDE)
    ) {
      concierge.lastErr =
        "chat_pipe_blocked_until_accept:" + String(reason || "relationship");
      emitSnapshot("chat_pipe_blocked_until_accept");
      return false;
    }

    return six7EnsureChatPipe(key, reason);
  }

  function six7OwnsChatPipeOffer(hKey) {
    const key = six7ShortKey(hKey);
    const self = six7ShortKey(selfHKey);
    if (!key || !self || key === self) return false;

    // Chat pipe ownership is per accepted peer pair.
    // SYS/relay role only carries signaling; it must not decide chat birth.
    return self < key;
  }

  async function six7EnsureChatPipe(hKey, reason = "relationship") {
    if (!selfHKey) return false;

    const key = six7ShortKey(hKey);
    if (!key) return false;

    const relationship = six7RelationshipGet(key);
    if (
      !six7RelationshipIsAccepted(relationship) ||
      six7PendingTerminalExits.has(key) ||
      __six7GoneNotified.has(key)
    ) {
      emitSnapshot("chat_pipe_ensure_blocked_relationship_cycle");
      return false;
    }

    if (!lastRoster.has(key)) {
      loungeWaiters.add(key);
      concierge.lastErr =
        "chat_pipe_waiting_for_roster:" + String(reason || "relationship");
      emitSnapshot("chat_pipe_waiting_for_roster");
      return false;
    }

    loungeWaiters.delete(key);

    let ent = six7ChatPeers.get(key);
    if (
      ent &&
      ent.relationshipCycleId &&
      ent.relationshipCycleId !== relationship.cycleId
    ) {
      rtcTeardown(key, "relationship_cycle_superseded");
      ent = null;
    }
    if (ent) ent.relationshipCycleId = relationship.cycleId;
    const pc = ent?.pc || null;
    const dc = ent?.dc || null;
    const dcState = String(dc?.readyState || "");

    if (dcState === "open") {
      const relationshipState = getState(key);
      const priorChatProof = String(ent?.chatProof || "none");

      const hasHelloProof =
        priorChatProof === "chat_hello" ||
        priorChatProof === "hello_identity_only";

      const deferredReadyTransition =
        (relationshipState === VEIL.ESTABLISHED ||
          relationshipState === VEIL.INSIDE) &&
        priorChatProof === "hello_identity_only";

      if (
        hasHelloProof &&
        (relationshipState === VEIL.ESTABLISHED ||
          relationshipState === VEIL.INSIDE)
      ) {
        if (relationshipState === VEIL.ESTABLISHED) {
          setState(key, VEIL.INSIDE);
        }

        if (ent) {
          ent.chatState = "ready";
          ent.chatAttemptState = "ready";
          ent.chatReadyAt = ent.chatReadyAt || now();
          ent.chatProof = "chat_hello";
          ent.chatFault = "none";
          six7CancelChatAttemptDeadline(key);
        }

        if (deferredReadyTransition) {
          const repairedDrop = ent?.chatDropPending === true;

          if (ent) {
            ent.chatDropPending = false;
          }

          if (repairedDrop) {
            concierge.chatReconnectCount =
              Number(concierge.chatReconnectCount || 0) + 1;
            concierge.chatLastReconnectAt = now();
            concierge.chatLastReconnectPeer = key;
          }

          six7CancelReconnect(key, true);
          post("P2P_CHAT_READY", { from: key });
          rtcSendHello(key);

          const q = six7PendingDm.get(key);

          if (q && q.length) {
            const rest = [];

            for (const item of q) {
              const sent = rtcSendDC(key, {
                t: "MSG",
                text: item.text,
                sys: item.sys,
                file: item.file || null,
                at: item.at || Date.now(),
              });

              if (sent) {
                post("messageEcho", {
                  to: key,
                  text: item.text,
                  sys: item.sys,
                  file: item.file || null,
                  at: item.at || Date.now(),
                });
              } else {
                rest.push(item);
              }
            }

            if (rest.length) {
              six7PendingDm.set(key, rest.slice(-SIX7_DM_PENDING_MAX));
              concierge.lastErr = "pending_dm_flush_partial";
              emitSnapshot("pending_dm_flush_partial");
            } else {
              six7PendingDm.delete(key);
              concierge.lastErr = null;
              emitSnapshot("pending_dm_flushed_chat_ready");
            }
          }

          emitSnapshot("chat_pipe_ready_from_deferred_hello");
        } else {
          six7CancelReconnect(key, true);
          post("P2P_CHAT_READY", { from: key });
          emitSnapshot("chat_pipe_existing_hello_observed");
        }
      } else {
        if (ent) {
          ent.chatState = "open";
          ent.chatReadyAt = 0;
          ent.chatProof = "dc_open";
          ent.chatFault = "waiting_for_chat_hello";
        }

        rtcSendHello(key);
        emitSnapshot("chat_pipe_open_waiting_for_hello");
      }

      return true;
    }

    if (pc && pc.signalingState !== "stable") {
      if (ent) {
        ent.chatState = "waiting_carrier_stable";
        ent.chatFault = String(pc.signalingState || "not_stable");
      }

      concierge.lastErr =
        "chat_pipe_waiting_for_stable_carrier:" +
        String(reason || "relationship");

      emitSnapshot("chat_pipe_waiting_for_stable_carrier");
      return false;
    }

    if (!six7OwnsChatPipeOffer(key)) {
      const role = String(
        concierge.localRoleCandidate || concierge.role || "peer",
      );

      if (ent) {
        ent.chatState = "waiting_owner_offer";
        ent.chatFault = role;
      }

      concierge.lastErr =
        "chat_pipe_waiting_for_offer_owner:" +
        role +
        ":" +
        String(reason || "relationship");

      emitSnapshot("chat_pipe_waiting_for_offer_owner");
      return false;
    }

    const target = six7GetOrCreateChatPeer(key);
    if (target) {
      target.relationshipCycleId = relationship.cycleId;
      target.chatState = "needed";
      target.chatFault = "none";
    }

    await rtcDial(key, {
      pipe: "chat",
      reason: String(reason || "relationship"),
    });

    return true;
  }

  function six7ReconcileRelationshipStatus(reason = "relationship_status") {
    if (!selfHKey || !videoId || !lastRoster || !lastRoster.size) return false;
    if (!concierge.sysReady) return false;

    if (
      six7PendingRefreshRelationships.size &&
      six7PendingRefreshRoom === String(videoId || "")
    ) {
      let probed = 0;

      for (const peer of Array.from(six7PendingRefreshRelationships)) {
        const key = six7ShortKey(peer);

        if (!key || key === six7ShortKey(selfHKey)) {
          six7PendingRefreshRelationships.delete(peer);
          continue;
        }

        if (six7RelationshipRevoked.has(key)) {
          six7PendingRefreshRelationships.delete(peer);
          continue;
        }

        if (!lastRoster.has(key)) continue;

        const relationship = six7RelationshipGet(key);
        if (!relationship?.cycleId || relationship.endedAt) continue;

        six7SendControl(
          {
            type: "veil_status",
            from: selfHKey,
            to: key,
            videoId,
            state: "continuity_pending",
            cycleId: relationship.cycleId,
          },
          "veil_status_continuity_probe",
        );
        probed++;
      }

      if (!six7PendingRefreshRelationships.size) {
        six7PendingRefreshRoom = "";

        try {
          sessionStorage.removeItem("six7_refresh_relationships_v1");
        } catch {}

        emitSnapshot("relationship_continuity_pending_drained");
      } else if (probed) {
        emitSnapshot("relationship_continuity_waiting_for_peer_proof");
      } else {
        emitSnapshot("relationship_continuity_waiting_for_authority");
      }
    }

    let sent = 0;

    for (const hKey of Array.from(lastRoster || [])) {
      const peer = six7ShortKey(hKey);
      if (!peer || peer === six7ShortKey(selfHKey)) continue;
      if (six7RelationshipRevoked.has(peer)) continue;

      const state = getState(peer);
      if (state !== VEIL.INSIDE && state !== VEIL.ESTABLISHED) continue;

      six7SendControl(
        {
          type: "veil_status",
          from: selfHKey,
          to: peer,
          videoId,
          state,
          cycleId: six7RelationshipGet(peer)?.cycleId || null,
        },
        "veil_status",
      );

      six7RequestChatPipe(peer, "relationship_status_reconcile");

      sent++;
    }

    if (sent) emitSnapshot(String(reason || "relationship_status"));
    return !!sent;
  }

  async function maybeEnterLounge(hKey) {
    if (!selfHKey) return;

    const key = String(hKey || "").toLowerCase();
    if (!key) return;

    const ent = six7ChatPeers.get(key);
    const pc = ent?.pc || null;
    const dc = ent?.dc || null;
    const dcState = String(dc?.readyState || "");

    if (dcState === "open" || dcState === "connecting") {
      emitSnapshot("rtc_dial_skipped_chat_pipe_exists");
      return;
    }

    if (pc && pc.signalingState !== "stable") {
      emitSnapshot("rtc_dial_skipped_carrier_not_stable");
      return;
    }

    if (!lastRoster.has(key)) {
      CORE_DEBUG && console.log("[six7][CORE] waiting for roster →", key);
      loungeWaiters.add(key);
      return;
    }

    loungeWaiters.delete(key);

    await six7EnsureChatPipe(key, "lounge_entry");
  }

  function setState(hKey, next, force = false) {
    CORE_DEBUG && console.log("STATE", hKey, next);

    const key = six7ShortKey(hKey);
    if (!key) return;

    const rec = six7RelationshipGet(key);
    const prev = rec.state;
    if (prev === next && !force) return;

    if (next !== VEIL.OUTSIDE && !rec.cycleId) {
      rec.cycleId = six7RelationshipCycleId(key, "state_transition");
      rec.createdAt = now();
      rec.endedAt = 0;
      rec.endReason = "none";
    }

    rec.state = next;
    rec.updatedAt = now();
    if (next !== VEIL.OUTSIDE) {
      rec.endedAt = 0;
      rec.endReason = "none";
    }
    six7Relationships.set(key, rec);

    try {
      const relationships = Array.from(six7Relationships.values())
        .filter((row) => six7RelationshipIsAccepted(row))
        .map((row) => ({
          hKey: six7ShortKey(row.hKey),
          cycleId: String(row.cycleId || ""),
          inviteCid: String(row.inviteCid || "") || null,
          remoteBootId: String(row.remoteBootId || "") || null,
          membershipEpoch: Number(row.membershipEpoch || 0),
        }))
        .filter((row) => row.hKey && row.cycleId);

      if (videoId && relationships.length) {
        sessionStorage.setItem(
          "six7_refresh_relationships_v1",
          JSON.stringify({
            room: videoId,
            peers: relationships.map((row) => row.hKey),
            relationships,
          }),
        );
      } else {
        sessionStorage.removeItem("six7_refresh_relationships_v1");
      }
    } catch {}

    post("VEIL_STATE_UPDATE", { hKey: key, state: next });
    emitSnapshot("veil_state_" + next);
  }

  function six7ShortKey(hKey) {
    return String(hKey || "")
      .slice(0, 16)
      .toLowerCase();
  }

  function six7IsSysconRole(role) {
    role = String(role || "");
    return role === "syscon1" || role === "syscon2";
  }

  function six7IsAuthoritySysTether(remoteHKey) {
    const key = six7ShortKey(remoteHKey);
    const self = six7ShortKey(selfHKey);
    const one = six7ShortKey(concierge.syscon1);
    const two = six7ShortKey(concierge.syscon2);

    if (!key || !self || !one || !two) return false;

    return (self === one && key === two) || (self === two && key === one);
  }

  function six7OwnsPrimarySysState(remoteHKey) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );

    if (role === "syscon1" || role === "syscon2") {
      return six7IsAuthoritySysTether(key);
    }

    return key === six7ShortKey(six7SysTargetFromCandidates());
  }

  function six7SysBirthRole(remoteHKey) {
    const key = six7ShortKey(remoteHKey);
    const self = six7ShortKey(selfHKey);
    const one = six7ShortKey(concierge.syscon1);
    const two = six7ShortKey(concierge.syscon2);
    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );

    if (!key || !self || key === self) return "illegal";

    const transaction = six7TemporarySysTransactions.get(key);
    if (
      transaction &&
      transaction.state !== "closed" &&
      SIX7_TEMP_SYS_PURPOSES.has(String(transaction.purpose || ""))
    ) {
      return six7ShortKey(transaction.initiator) === self
        ? "opener"
        : "acceptor";
    }

    // Authority tether: syscon1 is the only opener; syscon2 only accepts.
    if (role === "syscon1" && self === one && key === two) return "opener";
    if (role === "syscon2" && self === two && key === one) return "acceptor";

    // Every ordinary peer owns exactly one SYS tether to its authoritative
    // target. No peer may open a SYS edge to another ordinary peer.
    if (role === "peer") {
      const target = six7ShortKey(six7SysTargetFromCandidates());
      return target && key === target ? "opener" : "illegal";
    }

    // Syscon1 accepts every doorway-admitted ordinary peer tether. This is a
    // separate per-peer SYS edge, not the syscon1<->syscon2 authority attempt.
    if (role === "syscon1" && self === one && key !== one && key !== two) {
      const doorwayAdmitted = six7DoorwayAdmittedPeers.has(key);
      const committedMember = six7CommittedMember(key);

      // A continuing committed member may restore its ordinary SYS tether
      // without re-entering through CF. Doorway admission remains required
      // only for a genuinely new room incarnation.
      return doorwayAdmitted || committedMember ? "acceptor" : "illegal";
    }

    // Syscon2 never forms direct SYS tethers with ordinary peers.
    return "illegal";
  }

  function six7SysEdgeAttempt(remoteHKey, create = true) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return null;

    const ent = create ? six7GetOrCreateSysPeer(key) : six7SysPeers.get(key);
    if (!ent) return null;

    if (!ent.attempt && create) {
      ent.attempt = {
        id: six7NewSysTransportAttemptId(key),
        role: six7SysBirthRole(key),
        state: "missing",
        startedAt: 0,
        lastEvent: "created",
        faultReason: "none",
      };
    }

    return ent.attempt || null;
  }

  function six7AuthorityRosterRows(rows = []) {
    const out = [];
    const seen = new Set();

    for (const row of Array.isArray(rows) ? rows : []) {
      const key = six7ShortKey(row?.hKey || row?.from || row);
      if (!key || seen.has(key)) continue;
      seen.add(key);

      /*
       * Authority is membership truth, not an identity-blob carrier.
       * Keep the committed value deterministic and bounded: member key plus
       * the small display handle only. Avatar data, full identity hashes,
       * boot metadata, timestamps, and presentation provenance stay in the
       * presence/identity plane and must never be duplicated into SYS STATUS.
       */
      const presence = six7PresenceBook.get(key) || {};
      const handle = String(
        (row && typeof row === "object" ? row.handle : "") ||
          presence.handle ||
          "",
      ).trim();

      out.push({
        hKey: key,
        from: key,
        ...(handle ? { handle } : {}),
      });
    }

    return out;
  }

  function six7AuthorityRosterKey(rows = []) {
    return six7AuthorityRosterRows(rows)
      .map((row) => row.hKey)
      .sort()
      .join(",");
  }
  function six7TerminalExitCommand(msg, via = "authority_exit") {
    if (!msg || typeof msg !== "object") return false;

    const member = six7ShortKey(msg.from || msg.member);
    const room = String(msg.room || msg.videoId || "");
    const bootId = String(msg.bootId || "");
    const epoch = Number(msg.epoch || concierge.sysconEpoch || 0);
    const at = Number(msg.at || now());

    if (!member || !room || (videoId && room !== String(videoId))) return false;

    const self = six7ShortKey(selfHKey);
    if (self && member === self) {
      const selfReplacement = six7SysReplacementWindows.get(member);
      try {
        if (selfReplacement?.timer) clearTimeout(selfReplacement.timer);
      } catch {}
      six7PendingTerminalExits.delete(member);
      six7SysReplacementWindows.delete(member);
      emitSnapshot("terminal_exit_self_ignored");
      return false;
    }

    const knownBootId = String(six7RemoteBootIds.get(member) || "");
    if (bootId && knownBootId && bootId !== knownBootId) {
      emitSnapshot("stale_terminal_exit_boot_ignored");
      return false;
    }

    const committed = six7LocalCommittedAuthority();
    const committedRows = six7AuthorityRosterRows(committed?.roster);
    const committedKeys = new Set(committedRows.map((row) => row.hKey));

    const pending = six7SysAuthority.pending;
    const pendingRows = six7AuthorityRosterRows(pending?.snapshot?.roster);
    const pendingKeys = new Set(pendingRows.map((row) => row.hKey));
    const pendingBaseKeys = new Set(
      six7AuthorityRosterRows(pending?.baseRoster).map((row) => row.hKey),
    );
    const pendingUncommittedMember = !!(
      pending &&
      pendingKeys.has(member) &&
      !pendingBaseKeys.has(member)
    );

    // A member that exits before its admission/genesis transaction commits was
    // never room truth. Abort that uncommitted transaction without projecting
    // ROOM_EXITED, then restore the committed base (or self-only genesis).
    if (pendingUncommittedMember) {
      const pendingLeader = six7ShortKey(pending?.snapshot?.syscon1);
      const survivors = pendingRows.filter((row) => row.hKey !== member);
      const pendingId = String(pending?.id || "");

      six7SysAuthority.pending = null;
      six7TeardownSysPeer(member, "uncommitted_member_exit");
      six7PendingTerminalExits.delete(member);
      six7SysReplacementWindows.delete(member);

      six7MembershipAuditSet("pending_aborted", {
        peer: member,
        id: pendingId,
        reason: "uncommitted_member_exit",
      });

      if (
        !committed &&
        self &&
        pendingLeader === self &&
        survivors.some((row) => row.hKey === self)
      ) {
        six7StageSyscon1Authority(
          survivors,
          "syscon1_genesis_member_exit_restart",
        );
        six7PublishCommittedAuthority(
          "syscon1_genesis_member_exit_restart_published",
        );
      } else if (committed) {
        six7PublishCommittedAuthority("syscon1_admission_aborted_member_exit");
      }

      emitSnapshot("uncommitted_member_exit_transaction_aborted");
      return true;
    }

    if (committed && !committedKeys.has(member)) {
      const staleReplacement = six7SysReplacementWindows.get(member);
      try {
        if (staleReplacement?.timer) clearTimeout(staleReplacement.timer);
      } catch {}
      six7PendingTerminalExits.delete(member);
      six7SysReplacementWindows.delete(member);
      emitSnapshot("terminal_exit_member_already_absent_cleared");
      return true;
    }

    const replacementOwner = six7SysReplacementWindows.get(member) || {};
    try {
      if (replacementOwner.timer) clearTimeout(replacementOwner.timer);
    } catch {}
    replacementOwner.timer = null;
    replacementOwner.state = "terminal_staged";
    replacementOwner.terminalAt = at;
    replacementOwner.terminalReason = String(
      msg.reason || via || "authority_exit",
    );
    replacementOwner.bootId =
      bootId || knownBootId || replacementOwner.bootId || null;
    six7SysReplacementWindows.set(member, replacementOwner);

    const command = {
      bootId: bootId || knownBootId || null,
      epoch,
      at,
      via: String(via || "authority_exit"),
      reason: String(msg.reason || via || "authority_exit"),
    };
    six7PendingTerminalExits.set(member, command);
    six7RefreshSysCandidates([], "confirmed_terminal_exit");

    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const currentOne = six7ShortKey(concierge.syscon1);

    if (!self || role !== "syscon1" || currentOne !== self || !committed) {
      emitSnapshot("terminal_exit_recorded_waiting_for_syscon1");
      return true;
    }

    const committedTerminalExits = Array.from(
      six7PendingTerminalExits.entries(),
    )
      .filter(([key]) => committedKeys.has(six7ShortKey(key)))
      .map(([key, rec]) => ({
        member: six7ShortKey(key),
        bootId: String(rec?.bootId || "") || null,
        epoch: Number(rec?.epoch || epoch || 0),
        at: Number(rec?.at || at),
        via: String(rec?.via || via || "authority_exit"),
      }));
    const terminalMembers = new Set(
      committedTerminalExits.map((rec) => rec.member).filter(Boolean),
    );
    const nextRoster = committedRows.filter(
      (row) => !terminalMembers.has(row.hKey),
    );

    if (!nextRoster.some((row) => row.hKey === self)) {
      emitSnapshot("terminal_exit_rejected_self_missing_from_survivors");
      return false;
    }

    const committedOne = six7ShortKey(committed.syscon1);
    const committedTwo = six7ShortKey(committed.syscon2);
    const activeLeader = committedOne === self;
    const promotedMirror = !!(
      committedTwo === self && terminalMembers.has(committedOne)
    );
    const oldSpineTerminal = !!(
      terminalMembers.has(committedOne) &&
      (!committedTwo || terminalMembers.has(committedTwo))
    );
    const electedSuccessor = oldSpineTerminal
      ? six7ElectAuthorityCandidate(nextRoster, "", true, true)
      : null;
    const promotedSuccessor = !!(
      !activeLeader &&
      !promotedMirror &&
      currentOne === self &&
      committedKeys.has(self) &&
      oldSpineTerminal &&
      electedSuccessor === self
    );

    if (!activeLeader && !promotedMirror && !promotedSuccessor) {
      emitSnapshot("terminal_exit_recorded_waiting_for_authority_handoff");
      return true;
    }

    const promotion = promotedMirror || promotedSuccessor;
    const stageVia = promotedSuccessor
      ? "syscon1_elected_survivor_terminal_exit"
      : promotedMirror
        ? "syscon1_promoted_terminal_exit"
        : "syscon1_terminal_exit";
    const staged = six7StageSyscon1Authority(
      nextRoster,
      stageVia,
      committed,
      promotion ? { syscon1: self } : null,
      { terminalExits: committedTerminalExits },
    );

    if (!staged) {
      if (promotion) six7ScheduleSysAdmission(stageVia + "_restore_mirror");
      emitSnapshot(stageVia + "_waiting");
      return true;
    }

    six7PublishCommittedAuthority(stageVia + "_published");
    emitSnapshot(stageVia + "_staged");
    return true;
  }

  function six7ResumeTerminalAuthority(reason = "authority_resume") {
    if (!six7PendingTerminalExits.size) return false;

    let touched = false;
    const entries = Array.from(six7PendingTerminalExits.entries()).sort(
      (a, b) =>
        Number(a[1]?.at || 0) - Number(b[1]?.at || 0) ||
        String(a[0]).localeCompare(String(b[0])),
    );

    for (const [member, command] of entries) {
      const beforeSeq = Number(six7SysAuthority.rosterSeq || 0);
      const accepted = six7TerminalExitCommand(
        {
          type: "authority_exit",
          from: member,
          member,
          room: videoId || null,
          videoId: videoId || null,
          bootId: String(command?.bootId || ""),
          epoch: Number(command?.epoch || concierge.sysconEpoch || 0),
          reason: String(command?.reason || reason || "authority_resume"),
          at: Number(command?.at || now()),
        },
        String(command?.via || reason || "authority_resume"),
      );
      touched = touched || accepted;

      if (six7SysAuthority.pending) break;
      if (Number(six7SysAuthority.rosterSeq || 0) === beforeSeq) break;
    }

    if (touched)
      emitSnapshot("terminal_authority_resumed_" + String(reason || "resume"));
    return touched;
  }

  function six7SendAuthorityDepartureCommand(
    reason = "session_kill",
    final = true,
  ) {
    const self = six7ShortKey(selfHKey);
    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const one = six7ShortKey(concierge.syscon1);
    const two = six7ShortKey(concierge.syscon2);
    const target = role === "syscon1" ? two : one;
    const type = final ? "authority_exit" : "authority_detach";

    if (!self || !target || target === self || !six7HasOpenSysPipe(target)) {
      emitSnapshot(type + "_command_no_sys_route");
      return false;
    }

    const command = {
      type,
      from: self,
      member: self,
      to: target,
      room: videoId || null,
      videoId: videoId || null,
      bootId: six7BootId,
      epoch: Number(concierge.sysconEpoch || 0),
      reason: String(reason || (final ? "session_kill" : "document_detach")),
      at: now(),
    };

    const sent = six7SendSysControl(command, type);
    emitSnapshot(sent ? type + "_command_sent" : type + "_command_failed");
    return sent;
  }

  function six7SendTerminalExitCommand(reason = "session_kill") {
    return six7SendAuthorityDepartureCommand(reason, true);
  }

  function six7SendDocumentDetachCommand(reason = "document_detach") {
    return six7SendAuthorityDepartureCommand(reason, false);
  }
  function six7ApplyCommittedAuthority(
    snapshot,
    via = "sys_authority",
    previousRowsOverride = null,
    transaction = null,
  ) {
    if (!snapshot || typeof snapshot !== "object") return false;

    const room = String(snapshot.room || videoId || "");
    const from = six7ShortKey(snapshot.from);
    const one = six7ShortKey(snapshot.syscon1);
    const two = six7ShortKey(snapshot.syscon2);
    const epoch = Number(snapshot.epoch || 0);
    const rosterSeq = Number(snapshot.rosterSeq || 0);
    const roster = six7AuthorityRosterRows(snapshot.roster);
    const self = six7ShortKey(selfHKey);

    if (!room || (videoId && room !== String(videoId))) return false;
    if (!from || !one || from !== one) return false;
    if (String(snapshot.role || "") !== "syscon1") return false;
    if (!epoch || !rosterSeq || !roster.length) return false;
    if (two && two === one) return false;

    const rosterKeys = new Set(roster.map((row) => row.hKey));
    if (!rosterKeys.has(one)) return false;
    if (two && !rosterKeys.has(two)) return false;
    if (self && !rosterKeys.has(self)) return false;

    if (
      transaction &&
      (six7AuthoritySnapshotId(transaction.snapshot) !==
        six7AuthoritySnapshotId(snapshot) ||
        String(transaction.id || "") !==
          six7AuthorityTransactionId(transaction))
    ) {
      six7MembershipAuditSet("apply_rejected", {
        id: transaction?.id,
        reason: "transaction_integrity_mismatch",
      });
      return false;
    }

    const currentEpoch = Number(six7SysAuthority.epoch || 0);
    const currentSeq = Number(six7SysAuthority.rosterSeq || 0);
    const currentOne = six7ShortKey(six7SysAuthority.syscon1);
    const currentTwo = six7ShortKey(six7SysAuthority.syscon2);
    const incomingKey = six7AuthorityRosterKey(roster);
    const currentKey = six7AuthorityRosterKey(six7SysAuthority.roster);

    if (currentEpoch && epoch < currentEpoch) {
      emitSnapshot("sys_authority_older_epoch_ignored");
      return false;
    }

    if (currentOne && one !== currentOne && epoch <= currentEpoch) {
      concierge.sysLastErr =
        "sys_authority_leader_change_without_epoch_advance";
      emitSnapshot("sys_authority_leader_change_without_epoch_advance");
      return false;
    }

    if (epoch === currentEpoch && rosterSeq < currentSeq) {
      emitSnapshot("sys_authority_older_roster_seq_ignored");
      return false;
    }

    if (epoch === currentEpoch && rosterSeq === currentSeq && currentSeq > 0) {
      if (
        incomingKey !== currentKey ||
        one !== currentOne ||
        two !== currentTwo
      ) {
        concierge.sysLastErr = "sys_authority_equal_seq_conflict";
        emitSnapshot("sys_authority_equal_seq_conflict_ignored");
        return false;
      }

      emitSnapshot("sys_authority_duplicate_ignored");
      return true;
    }

    const previousRows = Array.isArray(previousRowsOverride)
      ? six7AuthorityRosterRows(previousRowsOverride)
      : six7AuthorityRosterRows(six7SysAuthority.roster);
    const previousRoster = new Set(previousRows.map((row) => row.hKey));
    const nextRoster = new Set(roster.map((row) => row.hKey));

    if (transaction?.admission?.member) {
      const member = six7ShortKey(transaction.admission.member);
      const bootId = String(transaction.admission.bootId || "");
      const knownBoot = String(six7RemoteBootIds.get(member) || "");

      if (
        !member ||
        previousRoster.has(member) ||
        !nextRoster.has(member) ||
        (bootId && knownBoot && bootId !== knownBoot)
      ) {
        six7MembershipAuditSet("apply_rejected", {
          peer: member,
          id: transaction.id,
          reason: "admission_incarnation_mismatch",
          bootId: bootId ? bootId.slice(0, 8) : "none",
          knownBoot: knownBoot ? knownBoot.slice(0, 8) : "none",
        });
        return false;
      }
    }

    const accepted = {
      at: now(),
      via: String(via || "sys_authority"),
      from,
      syscon1: one,
      syscon2: two || null,
      epoch,
      rosterSeq,
      roster,
    };

    Object.assign(six7SysAuthority, accepted);
    // Accepted SYS authority replaces provisional provider epoch metadata.
    concierge.sysconEpoch = epoch;
    concierge.syscon1 = one;
    concierge.syscon2 = two || null;

    lastRoster = six7MarkRosterAudit(
      "committed_sys_authority_apply",
      roster.map((row) => row.hKey),
      "committed_sys_authority_projection",
    );

    for (const row of roster) {
      six7RememberPresencePeer(row, "six7_sys_authority", {
        peerEvent: false,
        rosterEvent: false,
      });
    }

    for (const newKey of nextRoster) {
      if (newKey === self || previousRoster.has(newKey)) continue;

      const replacement = six7SysReplacementWindows.get(newKey);
      try {
        if (replacement?.timer) clearTimeout(replacement.timer);
      } catch {}

      six7PendingTerminalExits.delete(newKey);
      six7SysReplacementWindows.delete(newKey);
      __six7GoneNotified.delete(newKey);
      __six7MembershipExitApplied.delete(newKey);
      six7RelationshipRevoked.delete(newKey);

      six7MembershipAuditSet("member_admitted", {
        peer: newKey,
        reason: String(
          transaction?.admission?.via || via || "authority_commit",
        ),
        bootId:
          String(transaction?.admission?.bootId || "").slice(0, 8) || "none",
      });
      emitSnapshot("committed_member_admission_applied");
    }

    for (const oldKey of previousRoster) {
      if (oldKey === self || nextRoster.has(oldKey)) continue;

      const replacementOwner = six7SysReplacementWindows.get(oldKey);
      if (replacementOwner) {
        try {
          if (replacementOwner.timer) clearTimeout(replacementOwner.timer);
        } catch {}
        replacementOwner.timer = null;
        replacementOwner.state = "committed";
        replacementOwner.committedAt = now();
        six7SysReplacementWindows.set(oldKey, replacementOwner);
      }

      six7NotifyPeerGone(oldKey, "ROOM_EXITED", now());
      six7PendingTerminalExits.delete(oldKey);
      six7SysReplacementWindows.delete(oldKey);
    }

    for (const terminalKey of Array.from(six7PendingTerminalExits.keys())) {
      const key = six7ShortKey(terminalKey);
      if (!key || nextRoster.has(key)) continue;
      const replacementOwner = six7SysReplacementWindows.get(key);
      try {
        if (replacementOwner?.timer) clearTimeout(replacementOwner.timer);
      } catch {}
      six7PendingTerminalExits.delete(key);
      six7SysReplacementWindows.delete(key);
    }

    six7EmitPresenceRoster("committed_sys_authority_apply");
    return true;
  }
  function six7AuthoritySnapshotId(snapshot) {
    if (!snapshot || typeof snapshot !== "object") return "";

    return [
      String(snapshot.room || ""),
      Number(snapshot.epoch || 0),
      Number(snapshot.rosterSeq || 0),
      six7ShortKey(snapshot.syscon1),
      six7ShortKey(snapshot.syscon2),
      six7AuthorityRosterKey(snapshot.roster),
    ].join(":");
  }

  function six7AuthorityTerminalExits(source) {
    const input = Array.isArray(source?.terminalExits)
      ? source.terminalExits
      : source?.terminalExit
        ? [source.terminalExit]
        : [];
    const byMember = new Map();

    for (const rec of input) {
      const member = six7ShortKey(rec?.member || rec?.from);
      if (!member) continue;

      const normalized = {
        member,
        bootId: String(rec?.bootId || "") || null,
        epoch: Number(rec?.epoch || 0),
        at: Number(rec?.at || 0),
        via: String(rec?.via || "terminal_exit"),
      };
      const current = byMember.get(member);
      if (!current || normalized.at >= current.at) {
        byMember.set(member, normalized);
      }
    }

    return Array.from(byMember.values()).sort((a, b) =>
      a.member.localeCompare(b.member),
    );
  }

  function six7AuthorityTransactionId(pending) {
    if (!pending || typeof pending !== "object") return "";

    const base = pending.baseAuthority || null;
    const terminalKey = six7AuthorityTerminalExits(pending)
      .map((rec) =>
        [rec.member, rec.bootId || "", rec.epoch, rec.at, rec.via].join("~"),
      )
      .join(",");
    const admission = pending.admission || null;
    const leadership = pending.leadership || null;

    return [
      six7AuthoritySnapshotId(pending.snapshot),
      Number(pending.baseEpoch || 0),
      Number(pending.baseRosterSeq || 0),
      six7AuthorityRosterKey(pending.baseRoster),
      six7ShortKey(base?.syscon1),
      six7ShortKey(base?.syscon2),
      terminalKey,
      String(admission?.member || ""),
      String(admission?.bootId || ""),
      String(leadership?.mode || ""),
      six7ShortKey(leadership?.winner),
      six7ShortKey(leadership?.baseOne),
      six7ShortKey(leadership?.baseTwo),
    ].join(":");
  }

  function six7ElectAuthorityCandidate(
    rows,
    excludedMember = "",
    requireOpen = true,
    includeSelf = false,
  ) {
    const excluded = six7ShortKey(excludedMember);
    const self = six7ShortKey(selfHKey);
    const nowAt = now();

    const candidates = six7AuthorityRosterRows(rows)
      .map((row) => six7ShortKey(row?.hKey || row?.from || row))
      .filter(
        (key) =>
          key &&
          key !== excluded &&
          !six7PendingTerminalExits.has(key) &&
          (includeSelf || key !== self),
      )
      .map((key) => {
        const presence = six7PresenceBook.get(key) || {};
        const ent = six7SysPeers.get(key) || null;
        const localCandidate = includeSelf && key === self;
        const open = localCandidate ? true : six7HasOpenSysPipe(key);
        const lastGood = localCandidate
          ? nowAt
          : Number(six7SysLastGoodRx.get(key) || 0);
        const proofEpoch = localCandidate
          ? Math.max(
              Number(six7SysAuthority.epoch || 0),
              Number(concierge.sysconEpoch || 0),
            )
          : Number(ent?.lastProofEpoch || 0);
        const joinedAt =
          Number(
            presence.roomJoinedAt ||
              presence.joinedAt ||
              (localCandidate ? concierge.roomJoinedAt : 0) ||
              presence.t ||
              0,
          ) || Number.MAX_SAFE_INTEGER;
        const health =
          (open ? 2 : 0) + (lastGood && nowAt - lastGood < 5000 ? 1 : 0);

        return { key, proofEpoch, joinedAt, health, lastGood, open };
      })
      .filter((candidate) => !requireOpen || candidate.open)
      .sort(
        (a, b) =>
          b.proofEpoch - a.proofEpoch ||
          a.joinedAt - b.joinedAt ||
          b.health - a.health ||
          b.lastGood - a.lastGood ||
          a.key.localeCompare(b.key),
      );

    return candidates[0]?.key || null;
  }
  let sdkAdminDutySequence = 0;
  function six7AuthorizeSyscon1Duty(key, resume) {
    return host.authorizeSyscon1?.({roomId:videoId, peerId:six7ShortKey(selfHKey),
      leader:six7ShortKey(concierge.syscon1), role:String(concierge.localRoleCandidate || concierge.role || 'peer')}, key, resume) !== false;
  }
  function six7StageSyscon1Authority(
    nextRows,
    via = "syscon1_commit",
    transactionBase = null,
    topologyOverride = null,
    transactionMeta = null,
  ) {
    const existing = new Set(six7AuthorityRosterRows(six7SysAuthority.roster).map(row => row.hKey));
    if (six7AuthorityRosterRows(nextRows).some(row => row.hKey !== six7ShortKey(selfHKey) && !existing.has(row.hKey) && !host.canAdmitPeer(row.hKey))) {
      emitSnapshot('peer_authorization_denied');
      return false;
    }
    if (!six7AuthorizeSyscon1Duty('stage:'+via+':'+String(transactionMeta?.admission?.member || ''), () => {
      if (six7StageSyscon1Authority(nextRows, via, transactionBase, topologyOverride, transactionMeta))
        six7PublishCommittedAuthority('admin_checked_'+via);
    })) return false;
    const self = six7ShortKey(selfHKey);
    let committed =
      transactionBase ||
      six7FreshSysAuthority() ||
      six7LocalCommittedAuthority();

    let requestedRoster = six7AuthorityRosterRows(nextRows);
    if (self && !requestedRoster.some((row) => row.hKey === self)) {
      requestedRoster.unshift(six7LocalPresenceRow());
      requestedRoster = six7AuthorityRosterRows(requestedRoster);
    }

    const requestedKeys = new Set(requestedRoster.map((row) => row.hKey));
    const terminalExits = six7AuthorityTerminalExits(transactionMeta).map(
      (rec) => ({
        ...rec,
        at: Number(rec.at || now()),
        via: String(rec.via || via || "terminal_exit"),
      }),
    );
    const terminalMembers = new Set(terminalExits.map((rec) => rec.member));
    const admission = transactionMeta?.admission
      ? {
          member: six7ShortKey(transactionMeta.admission.member),
          bootId: String(transactionMeta.admission.bootId || "") || null,
          at: Number(transactionMeta.admission.at || now()),
          via: String(
            transactionMeta.admission.via || via || "member_admission",
          ),
        }
      : null;

    const replica = six7SysAuthority.pending;
    if (replica && six7ShortKey(replica.acceptedBy) === self &&
        terminalMembers.has(six7ShortKey(replica.snapshot?.from)) &&
        Number(replica.snapshot?.epoch || 0) === Number(committed?.epoch || 0) &&
        Number(replica.snapshot?.rosterSeq || 0) <= Number(committed?.rosterSeq || 0)) {
      six7SysAuthority.pending = null;
      six7MembershipAuditSet("committed_replica_retired", {peer: replica.snapshot?.from, id: replica.id});
    }
    if (six7SysAuthority.pending) {
      const oldPending = six7SysAuthority.pending;
      const oldMirror = six7ShortKey(
        oldPending.mirror || oldPending.snapshot?.syscon2,
      );
      const preemptTerminalMirror = !!(
        oldMirror &&
        six7PendingTerminalExits.has(oldMirror) &&
        !requestedKeys.has(oldMirror)
      );

      if (!preemptTerminalMirror) {
        six7MembershipAuditSet("stage_blocked", {
          peer: oldMirror,
          id: oldPending.id,
          reason: "pending_already_exists",
          baseEpoch: Number(oldPending.baseEpoch || 0),
          baseSeq: Number(oldPending.baseRosterSeq || 0),
        });
        emitSnapshot("sys_authority_command_held_pending_mirror_acceptance");
        return false;
      }

      committed = oldPending.baseAuthority || {
        at: Number(oldPending.at || now()),
        via: "pending_transaction_base",
        from: six7ShortKey(oldPending.baseAuthority?.from),
        syscon1: six7ShortKey(oldPending.baseAuthority?.syscon1),
        syscon2: six7ShortKey(oldPending.baseAuthority?.syscon2) || null,
        epoch: Number(oldPending.baseEpoch || 0),
        rosterSeq: Number(oldPending.baseRosterSeq || 0),
        roster: six7AuthorityRosterRows(oldPending.baseRoster),
      };

      six7MembershipAuditSet("stage_preempted", {
        peer: oldMirror,
        id: oldPending.id,
        reason: "terminal_pending_mirror_replaced",
      });
      six7SysAuthority.pending = null;
      emitSnapshot("sys_authority_terminal_pending_mirror_replaced");
    }

    const baseRows = six7AuthorityRosterRows(committed?.roster);
    const baseKeys = new Set(baseRows.map((row) => row.hKey));
    const baseOne = six7ShortKey(committed?.syscon1);
    const baseTwo = six7ShortKey(committed?.syscon2);
    const baseEpoch = Number(committed?.epoch || 0);
    const baseSeq = Number(committed?.rosterSeq || 0);
    const genesis = !baseEpoch && !baseSeq && !baseRows.length;

    const overrideOne = six7ShortKey(topologyOverride?.syscon1);
    const overrideTwoSpecified = Object.prototype.hasOwnProperty.call(
      topologyOverride || {},
      "syscon2",
    );
    const overrideTwo = six7ShortKey(topologyOverride?.syscon2);
    const one = six7ShortKey(overrideOne || baseOne || concierge.syscon1);

    if (!self || one !== self) {
      six7MembershipAuditSet("stage_rejected", {
        peer: overrideTwo || baseTwo,
        reason: !self ? "missing_self" : "not_syscon1",
        baseOne: baseOne || "none",
        proposedOne: one || "none",
      });
      return false;
    }

    const leadershipChanged = !!(baseOne && one !== baseOne);
    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const currentOne = six7ShortKey(concierge.syscon1);
    let leadershipMode = genesis
      ? "genesis"
      : leadershipChanged
        ? "unproven"
        : "continuing_leader";

    if (leadershipChanged) {
      const historicalMirrorPromotion = !!(
        role === "syscon1" &&
        currentOne === self &&
        baseTwo === self &&
        terminalMembers.has(baseOne) &&
        !requestedKeys.has(baseOne)
      );
      const oldSpineTerminal = !!(
        terminalMembers.has(baseOne) &&
        !requestedKeys.has(baseOne) &&
        (!baseTwo ||
          (terminalMembers.has(baseTwo) && !requestedKeys.has(baseTwo)))
      );
      const electedSuccessor = oldSpineTerminal
        ? six7ElectAuthorityCandidate(requestedRoster, "", true, true)
        : null;
      const deterministicSurvivorPromotion = !!(
        role === "syscon1" &&
        currentOne === self &&
        baseKeys.has(self) &&
        requestedKeys.has(self) &&
        oldSpineTerminal &&
        electedSuccessor === self
      );

      if (!historicalMirrorPromotion && !deterministicSurvivorPromotion) {
        six7MembershipAuditSet("stage_rejected", {
          peer: baseOne,
          reason: "unproven_leader_promotion",
          role,
          currentOne: currentOne || "none",
          baseOne: baseOne || "none",
          baseTwo: baseTwo || "none",
          elected: electedSuccessor || "none",
          terminal: Array.from(terminalMembers).join(",") || "none",
        });
        return false;
      }

      leadershipMode = historicalMirrorPromotion
        ? "historical_mirror"
        : "deterministic_survivor";
    }

    if (!requestedKeys.has(one)) {
      six7MembershipAuditSet("stage_rejected", {
        peer: one,
        reason: "leader_missing_from_requested_roster",
      });
      return false;
    }

    if (!genesis) {
      const removed = Array.from(baseKeys).filter(
        (key) => !requestedKeys.has(key),
      );
      const added = Array.from(requestedKeys).filter(
        (key) => !baseKeys.has(key),
      );

      const removedKey = removed.slice().sort().join(",");
      const terminalKey = Array.from(terminalMembers).sort().join(",");
      if (removedKey !== terminalKey) {
        six7MembershipAuditSet("stage_rejected", {
          peer: terminalKey || null,
          reason: "untagged_membership_removal",
          removed: removedKey || "none",
          terminal: terminalKey || "none",
        });
        return false;
      }

      if (
        added.length !== (admission?.member ? 1 : 0) ||
        (admission?.member && added[0] !== admission.member)
      ) {
        six7MembershipAuditSet("stage_rejected", {
          peer: admission?.member,
          reason: "untagged_membership_addition",
          added: added.join(",") || "none",
        });
        return false;
      }
    }

    const cfTwo = six7ShortKey(concierge.cfConciergeSyscon2);
    let two = null;

    if (requestedRoster.length > 1) {
      if (
        overrideTwoSpecified &&
        overrideTwo &&
        overrideTwo !== one &&
        requestedKeys.has(overrideTwo) &&
        !six7PendingTerminalExits.has(overrideTwo)
      ) {
        two = overrideTwo;
      } else if (
        baseTwo &&
        baseTwo !== one &&
        requestedKeys.has(baseTwo) &&
        !six7PendingTerminalExits.has(baseTwo)
      ) {
        two = baseTwo;
      } else if (
        genesis &&
        cfTwo &&
        cfTwo !== one &&
        requestedKeys.has(cfTwo)
      ) {
        two = cfTwo;
      } else {
        two = six7ElectAuthorityCandidate(requestedRoster, one, true, false);
      }

      if (!two) {
        six7MembershipAuditSet("stage_blocked", {
          peer: null,
          reason: "waiting_for_healthy_mirror",
          count: requestedRoster.length,
          leader: one,
        });
        emitSnapshot("sys_authority_waiting_for_healthy_mirror");
        return false;
      }
    }

    const candidateEpoch = Math.max(
      committed ? 0 : Number(concierge.cfConciergeEpoch || 0),
      Number(concierge.sysconEpoch || 0),
      baseEpoch + (leadershipChanged ? 1 : 0),
      baseEpoch || 1,
    );
    const epoch = Number(candidateEpoch || 0);
    if (!epoch) return false;

    const nextSnapshot = {
      room: videoId || null,
      from: self,
      role: "syscon1",
      syscon1: self,
      syscon2: two || null,
      epoch,
      rosterSeq: epoch === baseEpoch ? baseSeq + 1 : 1,
      roster: requestedRoster,
    };

    const baseAuthority = genesis
      ? null
      : {
          room: videoId || null,
          from: six7ShortKey(committed?.from || baseOne),
          role: "syscon1",
          syscon1: baseOne,
          syscon2: baseTwo || null,
          epoch: baseEpoch,
          rosterSeq: baseSeq,
          roster: baseRows,
        };

    const pending = {
      id: "",
      via: String(via || "syscon1_commit"),
      baseEpoch,
      baseRosterSeq: baseSeq,
      baseRoster: baseRows,
      baseAuthority,
      terminalExits,
      admission,
      leadership: {
        mode: leadershipMode,
        winner: one,
        baseOne: baseOne || null,
        baseTwo: baseTwo || null,
      },
      mirror: two || null,
      acceptedBy: null,
      at: now(),
      snapshot: nextSnapshot,
    };
    pending.id = six7AuthorityTransactionId(pending);

    if (!pending.id) return false;

    six7SysAuthority.pending = pending;
    six7MembershipAuditSet("stage_created", {
      peer: two,
      id: pending.id,
      via: pending.via,
      baseEpoch,
      baseSeq,
      nextEpoch: epoch,
      nextSeq: Number(nextSnapshot.rosterSeq || 0),
      count: requestedRoster.length,
      terminal: Array.from(terminalMembers).join(",") || "none",
      admission: admission?.member || "none",
    });

    if (!two) {
      const promoted = six7ApplyCommittedAuthority(
        nextSnapshot,
        pending.via + "_solo_commit",
        pending.baseRoster,
        pending,
      );
      six7SysAuthority.pending = null;
      six7MembershipAuditSet(
        promoted ? "solo_committed" : "solo_commit_failed",
        {
          id: pending.id,
          count: requestedRoster.length,
          nextSeq: Number(nextSnapshot.rosterSeq || 0),
        },
      );
      return promoted;
    }

    const mirrorEnt = six7SysPeers.get(two);
    const mirrorDc = mirrorEnt?.sysdc || null;
    const pendingStatus = six7SysStatusPayload("SYS_STATUS");
    let payloadBytes = 0;
    let encodeErr = "none";

    try {
      payloadBytes = new TextEncoder().encode(
        JSON.stringify(pendingStatus),
      ).length;
    } catch (e) {
      encodeErr = String(e?.name || "Error") + ":" + String(e?.message || e);
    }

    const bufferedBefore = Number(mirrorDc?.bufferedAmount || 0);
    const sent = six7SendSysDC(two, pendingStatus);
    const sendErr = String(concierge.sysLastErr || "none");

    six7MembershipAuditSet(sent ? "stage_sent" : "stage_waiting_pipe", {
      peer: two,
      id: pending.id,
      baseEpoch,
      baseSeq,
      nextSeq: Number(nextSnapshot.rosterSeq || 0),
      count: requestedRoster.length,
      pipe: String(mirrorDc?.readyState || "missing"),
      pc: String(mirrorEnt?.pc?.connectionState || "missing"),
      ice: String(mirrorEnt?.pc?.iceConnectionState || "missing"),
      sctp: String(mirrorEnt?.pc?.sctp?.state || "missing"),
      bytes: payloadBytes,
      bufferedBefore,
      bufferedAfter: Number(mirrorDc?.bufferedAmount || 0),
      maxMessage: Number(mirrorEnt?.pc?.sctp?.maxMessageSize || 0),
      sendErr,
      encodeErr,
    });

    emitSnapshot(
      sent
        ? "sys_authority_pending_sent_to_exact_mirror"
        : "sys_authority_pending_waiting_for_exact_mirror_pipe",
    );

    // Staging succeeded even if the exact mirror pipe is still opening.
    // Publication resumes from the existing pending slot when that pipe opens.
    return true;
  }
  function six7AcceptPendingAuthority(msg, transportPeer) {
    const pending = msg?.pendingAuthority;
    const self = six7ShortKey(selfHKey);
    const sender = six7ShortKey(msg?.from || transportPeer);
    const transport = six7ShortKey(transportPeer);

    if (!pending || typeof pending !== "object") return false;

    const pendingId = String(pending.id || "");
    const snapshot = pending.snapshot || null;
    const pendingLeader = six7ShortKey(snapshot?.syscon1);
    const pendingMirror = six7ShortKey(snapshot?.syscon2);

    if (!self || !sender || sender !== transport) {
      six7MembershipAuditSet("mirror_rejected", {
        peer: sender || transport,
        id: pendingId,
        reason: !self ? "missing_self" : "sender_transport_mismatch",
      });
      return false;
    }

    if (sender !== pendingLeader || self !== pendingMirror) {
      six7MembershipAuditSet("mirror_rejected", {
        peer: sender,
        id: pendingId,
        reason:
          sender !== pendingLeader
            ? "sender_not_pending_leader"
            : "self_not_pending_mirror",
        leader: pendingLeader || "none",
        mirror: pendingMirror || "none",
      });
      return false;
    }

    if (pendingId !== six7AuthorityTransactionId(pending)) {
      six7MembershipAuditSet("mirror_rejected", {
        peer: sender,
        id: pendingId,
        reason: "pending_transaction_id_mismatch",
      });
      return false;
    }

    const baseEpoch = Number(pending.baseEpoch || 0);
    const baseSeq = Number(pending.baseRosterSeq || 0);
    const baseRoster = six7AuthorityRosterRows(pending.baseRoster);
    const baseAuthority = pending.baseAuthority || null;
    const baseKeys = new Set(baseRoster.map((row) => row.hKey));
    const nextRoster = six7AuthorityRosterRows(snapshot?.roster);
    const nextKeys = new Set(nextRoster.map((row) => row.hKey));
    const terminalExits = six7AuthorityTerminalExits(pending);
    const terminalMembers = new Set(terminalExits.map((rec) => rec.member));
    const admission = pending.admission || null;
    const leadership = pending.leadership || null;
    const leadershipMode = String(leadership?.mode || "");
    const leadershipWinner = six7ShortKey(leadership?.winner);
    const leadershipBaseOne = six7ShortKey(leadership?.baseOne);
    const leadershipBaseTwo = six7ShortKey(leadership?.baseTwo);
    const genesis = !baseEpoch && !baseSeq && !baseRoster.length;

    if (
      !snapshot ||
      !nextRoster.length ||
      !nextKeys.has(sender) ||
      !nextKeys.has(self)
    ) {
      six7MembershipAuditSet("mirror_rejected", {
        peer: sender,
        id: pendingId,
        reason: "invalid_pending_roster",
      });
      return false;
    }

    if (
      genesis &&
      (leadershipMode !== "genesis" ||
        leadershipWinner !== pendingLeader ||
        leadershipBaseOne ||
        leadershipBaseTwo)
    ) {
      six7MembershipAuditSet("mirror_rejected", {
        peer: sender,
        id: pendingId,
        reason: "invalid_genesis_leadership_witness",
      });
      return false;
    }

    if (!genesis) {
      const baseOne = six7ShortKey(baseAuthority?.syscon1);
      const baseTwo = six7ShortKey(baseAuthority?.syscon2);
      const baseFrom = six7ShortKey(baseAuthority?.from);
      const baseAuthorityRows = six7AuthorityRosterRows(baseAuthority?.roster);

      if (
        !baseAuthority ||
        baseFrom !== baseOne ||
        !baseOne ||
        Number(baseAuthority.epoch || 0) !== baseEpoch ||
        Number(baseAuthority.rosterSeq || 0) !== baseSeq ||
        six7AuthorityRosterKey(baseAuthorityRows) !==
          six7AuthorityRosterKey(baseRoster) ||
        !baseKeys.has(baseOne) ||
        (baseTwo && !baseKeys.has(baseTwo))
      ) {
        six7MembershipAuditSet("mirror_rejected", {
          peer: sender,
          id: pendingId,
          reason: "invalid_transaction_base",
        });
        return false;
      }

      const removed = Array.from(baseKeys).filter((key) => !nextKeys.has(key));
      const added = Array.from(nextKeys).filter((key) => !baseKeys.has(key));
      const terminalKey = Array.from(terminalMembers).sort().join(",");
      const removedKey = removed.slice().sort().join(",");
      const admissionMember = six7ShortKey(admission?.member);

      if (
        removedKey !== terminalKey ||
        added.length !== (admissionMember ? 1 : 0) ||
        (admissionMember && added[0] !== admissionMember)
      ) {
        six7MembershipAuditSet("mirror_rejected", {
          peer: sender,
          id: pendingId,
          reason: "membership_delta_not_owned",
          removed: removed.join(",") || "none",
          added: added.join(",") || "none",
        });
        return false;
      }

      const witnessMatchesBase = !!(
        leadershipWinner === pendingLeader &&
        leadershipBaseOne === baseOne &&
        leadershipBaseTwo === baseTwo
      );
      const legalSameLeader = !!(
        pendingLeader === baseOne &&
        leadershipMode === "continuing_leader" &&
        witnessMatchesBase
      );
      const legalPromotedMirror = !!(
        baseTwo &&
        pendingLeader === baseTwo &&
        terminalMembers.has(baseOne) &&
        !nextKeys.has(baseOne) &&
        leadershipMode === "historical_mirror" &&
        witnessMatchesBase
      );
      const oldSpineTerminal = !!(
        terminalMembers.has(baseOne) &&
        !nextKeys.has(baseOne) &&
        (!baseTwo || (terminalMembers.has(baseTwo) && !nextKeys.has(baseTwo)))
      );
      const legalDeterministicSuccessor = !!(
        oldSpineTerminal &&
        baseKeys.has(pendingLeader) &&
        nextKeys.has(pendingLeader) &&
        leadershipMode === "deterministic_survivor" &&
        witnessMatchesBase
      );

      if (
        !legalSameLeader &&
        !legalPromotedMirror &&
        !legalDeterministicSuccessor
      ) {
        six7MembershipAuditSet("mirror_rejected", {
          peer: sender,
          id: pendingId,
          reason: "unproven_authority_spine_transition",
          baseOne: baseOne || "none",
          baseTwo: baseTwo || "none",
          nextOne: pendingLeader || "none",
          mode: leadershipMode || "none",
          witness: leadershipWinner || "none",
        });
        return false;
      }

      if (
        legalSameLeader &&
        baseTwo &&
        nextKeys.has(baseTwo) &&
        pendingMirror !== baseTwo
      ) {
        six7MembershipAuditSet("mirror_rejected", {
          peer: sender,
          id: pendingId,
          reason: "healthy_committed_mirror_replaced",
          baseTwo,
          nextTwo: pendingMirror,
        });
        return false;
      }
    }

    const local = six7LocalCommittedAuthority();
    const localMatchesBase = !!(
      !genesis &&
      local &&
      Number(local.epoch || 0) === baseEpoch &&
      Number(local.rosterSeq || 0) === baseSeq &&
      six7AuthorityRosterKey(local.roster) ===
        six7AuthorityRosterKey(baseRoster)
    );
    const localIsGenesis = !!(
      genesis &&
      !Number(six7SysAuthority.epoch || 0) &&
      !Number(six7SysAuthority.rosterSeq || 0)
    );
    const freshMirrorAdmission = !!(
      !genesis &&
      !local &&
      !Number(six7SysAuthority.epoch || 0) &&
      !Number(six7SysAuthority.rosterSeq || 0) &&
      six7ShortKey(admission?.member) === self &&
      String(admission?.bootId || "") === String(six7BootId || "") &&
      !baseKeys.has(self) &&
      nextKeys.has(self)
    );

    if (!localMatchesBase && !localIsGenesis && !freshMirrorAdmission) {
      six7MembershipAuditSet("mirror_rejected", {
        peer: sender,
        id: pendingId,
        reason: "base_mismatch",
        baseEpoch,
        baseSeq,
        localEpoch: Number(six7SysAuthority.epoch || 0),
        localSeq: Number(six7SysAuthority.rosterSeq || 0),
      });
      emitSnapshot("sys_authority_pending_base_mismatch");
      return false;
    }

    const snapshotEpoch = Number(snapshot.epoch || 0);
    if (!snapshotEpoch || snapshotEpoch < baseEpoch) {
      six7MembershipAuditSet("mirror_rejected", {
        peer: sender,
        id: pendingId,
        reason: "snapshot_epoch_older_than_base",
        snapshotEpoch,
        baseEpoch,
      });
      return false;
    }

    if (
      !genesis &&
      pendingLeader !== six7ShortKey(baseAuthority?.syscon1) &&
      snapshotEpoch <= baseEpoch
    ) {
      six7MembershipAuditSet("mirror_rejected", {
        peer: sender,
        id: pendingId,
        reason: "leader_change_without_epoch_advance",
        snapshotEpoch,
        baseEpoch,
      });
      return false;
    }

    six7SysAuthority.pending = {
      id: pendingId,
      via: String(pending.via || "sys_authority_pending"),
      baseEpoch,
      baseRosterSeq: baseSeq,
      baseRoster,
      baseAuthority,
      terminalExits,
      admission: admission || null,
      leadership: leadership || null,
      mirror: self,
      acceptedBy: self,
      at: now(),
      snapshot: { ...snapshot, roster: nextRoster },
    };

    six7MembershipAuditSet("mirror_accepted", {
      peer: sender,
      id: pendingId,
      baseEpoch,
      baseSeq,
      nextSeq: Number(snapshot.rosterSeq || 0),
      count: nextRoster.length,
      mode: freshMirrorAdmission
        ? "fresh_admission"
        : genesis
          ? "genesis"
          : "replica",
    });
    emitSnapshot("sys_authority_pending_accepted_by_exact_mirror");
    return true;
  }
  function six7PromotePendingAuthority(acceptedId, transportPeer) {
    if (!six7AuthorizeSyscon1Duty('commit:'+String(acceptedId), () => six7PromotePendingAuthority(acceptedId, transportPeer))) return false;
    const pending = six7SysAuthority.pending;
    const sender = six7ShortKey(transportPeer);
    const accepted = String(acceptedId || "");

    if (!pending && !accepted) return false;
    if (!pending) {
      six7MembershipAuditSet("promote_rejected", {
        peer: sender,
        id: accepted,
        reason: "no_local_pending",
      });
      return false;
    }

    if (!sender || !accepted || accepted !== String(pending.id || "")) {
      if (sender === six7ShortKey(pending.mirror)) {
        six7MembershipAuditSet("promote_rejected", {
          peer: sender,
          id: pending.id,
          reason: !sender
            ? "missing_transport_peer"
            : !accepted
              ? "ack_missing_accepted_id"
              : "accepted_id_mismatch",
        });
      }
      return false;
    }

    if (sender !== six7ShortKey(pending.mirror)) {
      six7MembershipAuditSet("promote_rejected", {
        peer: sender,
        id: pending.id,
        reason: "ack_not_from_exact_mirror",
      });
      return false;
    }

    if (six7ShortKey(pending.snapshot?.syscon1) !== six7ShortKey(selfHKey)) {
      six7MembershipAuditSet("promote_rejected", {
        peer: sender,
        id: pending.id,
        reason: "local_not_pending_leader",
      });
      return false;
    }

    const promoted = six7ApplyCommittedAuthority(
      pending.snapshot,
      pending.via + "_mirror_accepted_commit",
      pending.baseRoster,
      pending,
    );

    if (!promoted) {
      six7MembershipAuditSet("promote_rejected", {
        peer: sender,
        id: pending.id,
        reason: "committed_apply_failed",
      });
      return false;
    }

    const promotedId = pending.id;
    const promotedSeq = Number(pending.snapshot?.rosterSeq || 0);
    const promotedCount = six7AuthorityRosterRows(
      pending.snapshot?.roster,
    ).length;

    six7SysAuthority.pending = null;
    six7PublishCommittedAuthority(
      "sys_authority_committed_after_mirror_accept",
    );
    six7MembershipAuditSet("promoted", {
      peer: sender,
      id: promotedId,
      seq: promotedSeq,
      count: promotedCount,
    });
    emitSnapshot("sys_authority_pending_promoted_to_committed");
    queueMicrotask(sdkResumeAdmissions);
    six7ResumeTerminalAuthority("post_authority_commit");
    return true;
  }
  const sdkDeferredAdmissions=new Map();
  function sdkApplySysAdmission(key,inner) {
    if(String(concierge.localRoleCandidate || concierge.role || "peer")!=="syscon1")return;
    const fromKey=six7ShortKey(inner.from),selfKey=six7ShortKey(selfHKey);
    if (fromKey !== selfKey && !host.canAdmitPeer(fromKey)) {
      sdkDeferredAdmissions.delete(fromKey);
      emitSnapshot('peer_authorization_denied');
      return;
    }
            const committedRoster = six7AuthorityRosterRows(
              six7SysAuthority.roster,
            );
            const alreadyCommitted = committedRoster.some(
              (row) => row.hKey === fromKey,
            );
            const doorwayAdmitted = !!(
              fromKey && six7DoorwayAdmittedPeers.has(fromKey)
            );
            const sourcePipeProven = !!(
              key &&
              six7HasOpenSysPipe(key) &&
              (key === fromKey || six7ShortKey(inner?.forwardedBy) === key)
            );
            const directTransport = !!(key && key === fromKey);
            const explicitAdmission = inner?.admit === true;
            const blocked = six7IsBlocked(fromKey);
            const blockedBy = six7IsBlockedBy(fromKey);
            const goneLatched = __six7GoneNotified.has(fromKey);

            if (!alreadyCommitted && fromKey && fromKey !== selfKey) {
              six7MembershipAuditSet("admit_received", {
                peer: fromKey,
                doorway: doorwayAdmitted,
                pipe: sourcePipeProven,
                direct: directTransport,
                admit: explicitAdmission,
                gone: goneLatched,
                blocked,
                blockedBy,
                deliveredBy: key ? key.slice(0, 8) : "none",
              });
            }

            const incomingBootId = String(
              inner?.bootId || six7PresenceBook.get(fromKey)?.bootId || "",
            );
            const knownBootId = String(six7RemoteBootIds.get(fromKey) || "");
            const exitedIncarnation = __six7MembershipExitApplied.get(fromKey);
            const freshIncarnation = !!(
              !exitedIncarnation ||
              (incomingBootId &&
                incomingBootId !== String(exitedIncarnation?.bootId || ""))
            );
            const transportIncarnationBound = !!(
              incomingBootId &&
              (!knownBootId || knownBootId === incomingBootId)
            );

            /*
             * Doorway visibility grants only bootstrap eligibility. A removed
             * member returns to room truth only through an explicit admission
             * on its direct, proven SYS tether, with a new room-session boot
             * identity. Exit/revocation latches stay closed until the staged
             * authority transaction commits.
             */
            const freshDirectAdmission = !!(
              !alreadyCommitted &&
              fromKey &&
              fromKey !== selfKey &&
              doorwayAdmitted &&
              sourcePipeProven &&
              directTransport &&
              explicitAdmission &&
              freshIncarnation &&
              transportIncarnationBound
            );

            if (freshDirectAdmission && six7SysAuthority.pending) {
              sdkDeferredAdmissions.set(fromKey,{key,inner:structuredClone(inner),entry:six7SysPeers.get(key)});
              emitSnapshot("sys_admission_deferred_pending_commit");
              return;
            }
            if (freshDirectAdmission) {
              sdkDeferredAdmissions.delete(fromKey);
              const presence = six7PresenceBook.get(fromKey) || inner || {};
              const nextRoster = committedRoster.concat({
                hKey: fromKey,
                from: fromKey,
                handle: presence.handle || inner.handle || null,
                bootId: incomingBootId,
                epoch: Number(presence.epoch || inner.epoch || 0),
                roomJoinedAt: Number(
                  presence.roomJoinedAt || inner.roomJoinedAt || now(),
                ),
                hex: presence.hex || inner.hex || null,
                avatar: presence.avatar || inner.avatar || undefined,
                videoId: presence.videoId || inner.videoId || videoId || null,
                seenVia: "six7_sys_authority",
                t: now(),
              });

              if (
                six7CommitSyscon1Roster(
                  nextRoster,
                  "syscon1_member_admission",
                  {
                    admission: {
                      member: fromKey,
                      bootId: incomingBootId,
                      at: now(),
                      via: "syscon1_member_admission",
                    },
                  },
                )
              ) {
                six7RefreshSysCandidates(
                  six7LatchedSysRosterList(),
                  "syscon1_member_admission",
                );
                six7PublishCommittedAuthority(
                  "syscon1_member_admission_published",
                );
                emitSnapshot(
                  "syscon1_member_admission_staged_from_proven_sys_tether",
                );
              }
            } else if (alreadyCommitted) {
              // Presence decorates committed identity only. It never rewrites
              // the immutable authority value or advances rosterSeq.
              six7EmitPresenceRoster("syscon1_presence_metadata");
            } else {
              const rejection = !fromKey
                ? "missing_from"
                : fromKey === selfKey
                  ? "self_presence"
                  : !doorwayAdmitted
                    ? "not_doorway_admitted"
                    : !sourcePipeProven
                      ? "source_pipe_unproven"
                      : !directTransport
                        ? "not_direct_transport"
                        : !explicitAdmission
                          ? "missing_explicit_admission"
                          : !freshIncarnation
                            ? "same_retired_incarnation"
                            : !transportIncarnationBound
                              ? "transport_incarnation_unbound"
                              : "unknown_gate";

              if (fromKey && fromKey !== selfKey) {
                six7MembershipAuditSet("admit_rejected", {
                  peer: fromKey,
                  reason: rejection,
                  doorway: doorwayAdmitted,
                  pipe: sourcePipeProven,
                  direct: directTransport,
                  admit: explicitAdmission,
                  gone: __six7GoneNotified.has(fromKey),
                  blocked,
                  blockedBy,
                });
              }

              emitSnapshot("syscon1_presence_not_committed");
            }

            six7ResumeTerminalAuthority("syscon1_presence_rx");
  }
  function sdkResumeAdmissions() {
    resumeDeferredAdmissions(sdkDeferredAdmissions,{
      hasPending:()=>!!six7SysAuthority.pending,
      currentEntry:key=>six7SysPeers.get(key),
      pipeOpen:six7HasOpenSysPipe,
      apply:sdkApplySysAdmission,
      onRetired:()=>emitSnapshot("sys_deferred_admission_transport_retired")
    });
  }
  function six7CommitSyscon1Roster(
    nextRows,
    via = "syscon1_commit",
    transactionMeta = null,
  ) {
    return six7StageSyscon1Authority(
      nextRows,
      via,
      null,
      null,
      transactionMeta,
    );
  }

  function six7PublishCommittedAuthority(reason = "syscon1_publish") {
    if (!six7AuthorizeSyscon1Duty('publish:'+reason, () => six7PublishCommittedAuthority(reason))) return 0;
    const self = six7ShortKey(selfHKey);
    const pending = six7SysAuthority.pending;
    const pendingLeader = six7ShortKey(pending?.snapshot?.syscon1);
    const committedLeader = six7ShortKey(six7SysAuthority.syscon1);

    // A successor may publish a staged transaction whose base was imported
    // from the mirror without first projecting that historical base into live
    // authority. Therefore publication ownership may come from either the
    // current committed leader or the exact staged snapshot leader.
    if (!self || (committedLeader !== self && pendingLeader !== self)) return 0;

    const status = six7SysStatusPayload("SYS_STATUS");
    const pendingId = String(status?.pendingAuthority?.id || "");
    const publishRows = pendingId
      ? six7AuthorityRosterRows(status.pendingAuthority?.snapshot?.roster)
      : six7AuthorityRosterRows(status.roster);

    if (!publishRows.length || (!pendingId && !status.rosterSeq)) {
      emitSnapshot("syscon1_publish_blocked_no_authority_payload");
      return 0;
    }

    const committedKeys = new Set(publishRows.map((row) => row.hKey));

    let payloadBytes = 0;
    let encodeErr = "none";

    if (pendingId) {
      try {
        payloadBytes = new TextEncoder().encode(JSON.stringify(status)).length;
      } catch (e) {
        encodeErr = String(e?.name || "Error") + ":" + String(e?.message || e);
      }
    }

    let published = 0;

    for (const target of committedKeys) {
      if (!target || target === self) continue;

      const targetEnt = six7SysPeers.get(target);
      const targetDc = targetEnt?.sysdc || null;

      if (!six7HasOpenSysPipe(target)) {
        if (pendingId) {
          six7MembershipAuditSet("publish_skipped_pipe", {
            peer: target,
            id: pendingId,
            pipe: String(targetDc?.readyState || "missing"),
            pc: String(targetEnt?.pc?.connectionState || "missing"),
            ice: String(targetEnt?.pc?.iceConnectionState || "missing"),
            sctp: String(targetEnt?.pc?.sctp?.state || "missing"),
            bytes: payloadBytes,
            buffered: Number(targetDc?.bufferedAmount || 0),
            maxMessage: Number(targetEnt?.pc?.sctp?.maxMessageSize || 0),
            encodeErr,
          });
        }
        continue;
      }

      const bufferedBefore = Number(targetDc?.bufferedAmount || 0);
      const sent = six7SendSysDC(target, status);
      const sendErr = String(concierge.sysLastErr || "none");

      if (pendingId) {
        six7MembershipAuditSet(sent ? "publish_sent" : "publish_send_failed", {
          peer: target,
          id: pendingId,
          pipe: String(targetDc?.readyState || "missing"),
          pc: String(targetEnt?.pc?.connectionState || "missing"),
          ice: String(targetEnt?.pc?.iceConnectionState || "missing"),
          sctp: String(targetEnt?.pc?.sctp?.state || "missing"),
          bytes: payloadBytes,
          bufferedBefore,
          bufferedAfter: Number(targetDc?.bufferedAmount || 0),
          maxMessage: Number(targetEnt?.pc?.sctp?.maxMessageSize || 0),
          sendErr,
          encodeErr,
        });
      }

      if (sent) published++;
    }

    if (!pendingId && ws?.readyState === WebSocket.OPEN && typeof ws.publishAuthority === "function") {
      const publicationId = six7AuthoritySnapshotId(status);
      if (ws.lastAuthorityPublication !== publicationId) {
        try { ws.publishAuthority(status); ws.lastAuthorityPublication = publicationId; }
        catch (error) { concierge.sysLastErr = "doorway_authority_publish_failed:" + String(error?.message || error); }
      }
    }
    emitSnapshot(String(reason || "syscon1_publish") + "_" + published);
    return published;
  }
  function six7RememberSysAuthority(msg, via = "sys_status") {
    const applied = six7ApplyCommittedAuthority(msg, via);

    if (
      applied &&
      six7SysAuthority.pending?.acceptedBy === six7ShortKey(selfHKey) &&
      six7AuthoritySnapshotId(msg) ===
        six7AuthoritySnapshotId(six7SysAuthority.pending?.snapshot)
    ) {
      six7SysAuthority.pending = null;
      emitSnapshot("sys_authority_mirror_pending_cleared_on_commit");
    }

    if (applied) {
      six7GossipRecovery = {
        source: null,
        targets: [],
        index: 0,
        epoch: 0,
        rosterSeq: 0,
        at: 0,
      };
      six7ResumeTerminalAuthority("committed_authority_received");
      emitSnapshot("authority_gossip_recovery_completed_by_committed_sys");
    }
    return applied;
  }
  function six7AdoptMirrorAuthority(msg, transportPeer) {
    if (!msg || typeof msg !== "object") return false;

    const self = six7ShortKey(selfHKey);
    const sender = six7ShortKey(msg.from || transportPeer);
    const currentOne = six7ShortKey(concierge.syscon1);
    const currentTwo = six7ShortKey(concierge.syscon2);
    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const mirror = msg.mirrorAuthority;

    if (!self || role !== "syscon1" || currentOne !== self) return false;
    if (
      !sender ||
      sender !== six7ShortKey(transportPeer) ||
      sender !== currentTwo
    ) {
      return false;
    }
    if (!mirror || typeof mirror !== "object") return false;

    const historicalFrom = six7ShortKey(mirror.from || mirror.syscon1);
    const historicalOne = six7ShortKey(mirror.syscon1);
    const historicalTwo = six7ShortKey(mirror.syscon2);
    const historicalEpoch = Number(mirror.epoch || 0);
    const historicalSeq = Number(mirror.rosterSeq || 0);
    const inherited = six7AuthorityRosterRows(mirror.roster);
    const inheritedKeys = new Set(inherited.map((row) => row.hKey));

    // This path is state recovery only. A promoted historical mirror must use
    // the terminal-exit authority owner instead of importing itself as leader.
    if (
      historicalFrom !== self ||
      historicalOne !== self ||
      historicalTwo !== sender ||
      !historicalEpoch ||
      !historicalSeq ||
      !inherited.length ||
      !inheritedKeys.has(self) ||
      !inheritedKeys.has(sender)
    ) {
      emitSnapshot("syscon1_mirror_recovery_invalid_base");
      return false;
    }

    const local = six7LocalCommittedAuthority();
    if (
      local &&
      (Number(local.epoch || 0) > historicalEpoch ||
        (Number(local.epoch || 0) === historicalEpoch &&
          Number(local.rosterSeq || 0) >= historicalSeq))
    ) {
      return false;
    }

    const recovered = six7ApplyCommittedAuthority(
      {
        room: videoId || null,
        from: self,
        role: "syscon1",
        syscon1: self,
        syscon2: sender,
        epoch: historicalEpoch,
        rosterSeq: historicalSeq,
        roster: inherited,
      },
      "syscon1_exact_mirror_base_recovery",
    );

    if (!recovered) {
      emitSnapshot("syscon1_exact_mirror_base_recovery_failed");
      return false;
    }

    for (const command of Array.isArray(mirror.terminalExits)
      ? mirror.terminalExits
      : []) {
      const member = six7ShortKey(command?.member);
      if (!member || !inheritedKeys.has(member)) continue;

      const incoming = {
        bootId: String(command?.bootId || "") || null,
        epoch: Number(command?.epoch || historicalEpoch),
        at: Number(command?.at || now()),
        via: "mirror_authority_recovery",
        reason: String(command?.via || "mirror_authority_recovery"),
      };
      const current = six7PendingTerminalExits.get(member);
      if (!current || Number(incoming.at || 0) >= Number(current.at || 0)) {
        six7PendingTerminalExits.set(member, incoming);
      }
    }

    // Provider revision counters cannot invalidate committed SYS authority.
    const acceptedEpoch = Number(concierge.sysconEpoch || 0);

    if (acceptedEpoch > historicalEpoch) {
      const staged = six7StageSyscon1Authority(
        inherited,
        "syscon1_exact_mirror_epoch_recovery",
        six7LocalCommittedAuthority(),
        { syscon1: self, syscon2: sender },
      );

      if (!staged) {
        emitSnapshot("syscon1_exact_mirror_epoch_recovery_waiting");
        return false;
      }

      six7PublishCommittedAuthority(
        "syscon1_exact_mirror_epoch_recovery_published",
      );
    } else {
      six7PublishCommittedAuthority("syscon1_exact_mirror_base_recovered");
    }

    six7ResumeTerminalAuthority("mirror_base_recovery");
    emitSnapshot("syscon1_exact_mirror_base_recovered");
    return true;
  }

  function six7LocalCommittedAuthority() {
    const roster = six7AuthorityRosterRows(six7SysAuthority.roster);
    const from = six7ShortKey(six7SysAuthority.from);
    const one = six7ShortKey(six7SysAuthority.syscon1);
    const two = six7ShortKey(six7SysAuthority.syscon2);
    const epoch = Number(six7SysAuthority.epoch || 0);
    const rosterSeq = Number(six7SysAuthority.rosterSeq || 0);
    const keys = new Set(roster.map((row) => row.hKey));

    if (
      !six7SysAuthority.at ||
      !from ||
      !one ||
      from !== one ||
      !epoch ||
      !rosterSeq ||
      !roster.length ||
      !keys.has(one) ||
      (two && (!keys.has(two) || two === one))
    ) {
      return null;
    }

    return {
      at: Number(six7SysAuthority.at || 0),
      via: String(six7SysAuthority.via || "none"),
      from,
      syscon1: one,
      syscon2: two || null,
      epoch,
      rosterSeq,
      roster,
    };
  }

  function six7FreshSysAuthority() {
    if (!six7SysAuthority.at) return null;

    const authorityEpoch = Number(six7SysAuthority.epoch || 0);
    // Provider revision counters cannot invalidate committed SYS authority.
    const acceptedEpoch = Number(concierge.sysconEpoch || 0);

    if (authorityEpoch && acceptedEpoch && authorityEpoch < acceptedEpoch) {
      return null;
    }

    const one = six7ShortKey(six7SysAuthority.syscon1);
    const two = six7ShortKey(six7SysAuthority.syscon2);

    if (!one && !two) return null;

    return {
      syscon1: one || null,
      syscon2: two || null,
      epoch: Number(six7SysAuthority.epoch || 0),
      rosterSeq: Number(six7SysAuthority.rosterSeq || 0),
      roster: Array.isArray(six7SysAuthority.roster)
        ? six7SysAuthority.roster
        : [],
      via: six7SysAuthority.via || "unknown",
      at: Number(six7SysAuthority.at || 0),
    };
  }

  function six7ApplyLocalRoleCandidate(reason = "candidate") {
    const wantedRole = String(concierge.localRoleCandidate || "peer");
    const demotedBy = String(concierge.roleDemotedBy || "");
    const currentEpoch = Number(concierge.sysconEpoch || 0);
    const demotedEpoch = Number(concierge.roleDemotedEpoch || 0);

    if (
      concierge.roleDemoted &&
      demotedEpoch &&
      currentEpoch &&
      demotedEpoch < currentEpoch
    ) {
      concierge.roleDemoted = false;
      concierge.roleDemotedBy = "none";
      concierge.roleDemotedEpoch = 0;
      concierge.roleDemotedAt = 0;
    }

    if (concierge.roleDemoted) {
      const demotedRole = demotedBy.includes("syscon1")
        ? "syscon1"
        : demotedBy.includes("syscon2")
          ? "syscon2"
          : "";

      if (!demotedRole || wantedRole === demotedRole) {
        concierge.role = "peer";
        concierge.localRoleCandidate = "peer";
        concierge.candidateReason = `demoted_latched_${demotedBy || reason}`;
        concierge.localRoleReason = `role_demoted_latched_${demotedBy || reason}`;
        return;
      }

      concierge.roleDemoted = false;
      concierge.roleDemotedBy = "none";
      concierge.roleDemotedEpoch = 0;
      concierge.roleDemotedAt = 0;
    }

    concierge.role = wantedRole;
    concierge.localRoleReason = `role_candidate_${concierge.role}_${reason}`;
  }

  function six7CompareSysconClaims(localClaim, remoteClaim) {
    const le = Number(localClaim?.epoch || 0);
    const re = Number(remoteClaim?.epoch || 0);

    if (re > le) return { winner: "remote", why: "epoch" };
    if (le > re) return { winner: "local", why: "epoch" };

    const lj = Number(localClaim?.joinedAt || 0) || Number.MAX_SAFE_INTEGER;
    const rj = Number(remoteClaim?.joinedAt || 0) || Number.MAX_SAFE_INTEGER;

    if (rj < lj) return { winner: "remote", why: "older" };
    if (lj < rj) return { winner: "local", why: "older" };

    const lh = six7ShortKey(localClaim?.hKey);
    const rh = six7ShortKey(remoteClaim?.hKey);

    if (rh && lh && rh < lh) return { winner: "remote", why: "tie_hkey" };
    return { winner: "local", why: "tie_hkey" };
  }

  function six7ReconcileRoleFromSysStatus(msg, via = "sys_status") {
    if (!msg || typeof msg !== "object") return false;

    const self = six7ShortKey(selfHKey);
    const remote = six7ShortKey(msg.from);
    if (!self || !remote || remote === self) return false;

    const localRole = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const remoteRole = String(msg.role || msg.candidateRole || "peer");

    if (!six7IsSysconRole(localRole)) return false;
    if (!six7IsSysconRole(remoteRole)) return false;
    if (localRole !== remoteRole) return false;

    const result = six7CompareSysconClaims(
      {
        hKey: self,
        role: localRole,
        epoch: Number(concierge.sysconEpoch || 0),
        joinedAt: Number(concierge.roomJoinedAt || 0),
      },
      {
        hKey: remote,
        role: remoteRole,
        epoch: Number(msg.epoch || 0),
        joinedAt: Number(msg.roomJoinedAt || msg.joinedAt || msg.at || 0),
      },
    );

    if (result.winner === "remote") {
      const one = six7ShortKey(msg.syscon1 || remote);
      const two = six7ShortKey(msg.syscon2);

      concierge.role = "peer";
      concierge.localRoleCandidate = "peer";
      concierge.roleDemoted = true;
      concierge.roleDemotedBy = `${localRole}_${result.why}_${via}`;
      concierge.roleDemotedEpoch = Number(
        msg.epoch || concierge.sysconEpoch || 0,
      );
      concierge.roleDemotedAt = now();

      if (one) concierge.syscon1 = one;
      concierge.syscon2 = two && two !== one ? two : null;

      concierge.sysconEpoch = Math.max(
        Number(concierge.sysconEpoch || 0),
        Number(msg.epoch || 0),
      );

      six7RememberSysAuthority(msg, `demotion_${via}`);

      concierge.candidateReason = `demoted_resynced_${concierge.roleDemotedBy}`;
      concierge.localRoleReason = `role_demoted_resynced_${concierge.roleDemotedBy}`;
      concierge.lastErr = null;

      six7RefreshSysReadiness(`role_demoted_${via}`);
      six7ScheduleSysAdmission(`role_demoted_${via}`);
      emitSnapshot(`role_demoted_resynced_${localRole}_${result.why}`);
      return true;
    }

    concierge.role = localRole;
    concierge.localRoleReason = `role_kept_${localRole}_${result.why}_${via}`;
    emitSnapshot(`role_kept_${localRole}_${result.why}`);
    return false;
  }

  function six7CanUseSys(hKey) {
    const key = six7ShortKey(hKey);
    if (!key) return false;

    if (six7RelationshipRevoked.has(key)) return false;
    if (six7IsBlocked(key) || six7IsBlockedBy(key)) return false;

    const st = getState(key);
    return (
      st === VEIL.INVITE_SENT ||
      st === VEIL.INVITE_RECEIVED ||
      st === VEIL.ESTABLISHED ||
      st === VEIL.INSIDE
    );
  }

  function six7CanUseChat(hKey) {
    const key = six7ShortKey(hKey);
    if (!key) return false;
    if (six7RelationshipRevoked.has(key)) return false;
    return getState(key) === VEIL.INSIDE;
  }

  function six7IsSysMessage(sys) {
    return !!sys;
  }

  function six7IsEstablishedLike(hKey) {
    const st = getState(hKey);
    return st === VEIL.ESTABLISHED || st === VEIL.INSIDE;
  }

  function six7IsReconnectPrimary(remoteHKey) {
    const local = six7ShortKey(selfHKey);
    const remote = six7ShortKey(remoteHKey);
    return !!local && !!remote && local < remote;
  }

  function six7PresentReconnect(
    remoteHKey,
    proof = "nonterminal_reconnect",
    observedBootId = "",
  ) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    const reconnect = reconnectingPeers.get(key);
    if (!reconnect || reconnect.presented === true) return false;

    const relationship = six7RelationshipGet(key);
    if (
      six7PendingTerminalExits.has(key) ||
      __six7GoneNotified.has(key) ||
      !six7RelationshipIsAccepted(relationship) ||
      !lastRoster.has(key)
    ) {
      emitSnapshot("chat_reconnect_presentation_blocked_terminal_relationship");
      return false;
    }

    const expectedBootId = String(reconnect.remoteBootId || "");
    const provenBootId = String(observedBootId || "");
    if (expectedBootId && provenBootId && expectedBootId !== provenBootId) {
      emitSnapshot("chat_reconnect_presentation_blocked_new_incarnation");
      return false;
    }

    reconnect.presented = true;
    reconnect.presentedAt = now();
    reconnect.presentationProof = String(proof || "nonterminal_reconnect");
    reconnectingPeers.set(key, reconnect);

    post("P2P_CHAT_LOST", {
      from: key,
      reason: String(reconnect.reason || "chat_transport_lost"),
      at: Date.now(),
    });

    post("INCOMING_MSG", {
      from: key,
      text: "Connection interrupted. Reconnecting…",
      sys: "CONNECTION_RECONNECTING",
      at: Date.now(),
    });

    emitSnapshot(
      "chat_reconnect_presented_after_" +
        String(proof || "nonterminal_reconnect"),
    );
    return true;
  }

  function six7EndFailedChat(remoteHKey, reason = "reconnect_failed") {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    const relationship = six7RelationshipGet(key);
    if (
      six7PendingTerminalExits.has(key) ||
      __six7GoneNotified.has(key) ||
      !six7RelationshipIsAccepted(relationship) ||
      !lastRoster.has(key)
    ) {
      emitSnapshot("chat_attempt_expiry_observed_terminal_relationship");
      return false;
    }

    const ent = six7ChatPeers.get(key);
    if (ent && ent.chatState !== "ready") {
      ent.chatFault = String(reason || "reconnect_failed");
    }

    six7PresentReconnect(key, "nonterminal_deadline");

    concierge.lastErr =
      "chat_attempt_expired_observation:" +
      String(reason || "reconnect_failed");
    emitSnapshot("chat_attempt_expired_relationship_and_successor_preserved");
    return true;
  }

  function six7CancelReconnect(remoteHKey, restored = false) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return;

    six7CancelChatAttemptDeadline(key);

    const reconnect = reconnectingPeers.get(key) || null;
    const wasReconnecting = !!reconnect;
    const wasPresented = reconnect?.presented === true;
    reconnectingPeers.delete(key);

    if (restored) {
      const ent = six7ChatPeers.get(key);
      const chatReady = !!(
        ent?.dc?.readyState === "open" &&
        ent?.chatState === "ready" &&
        ent?.chatProof === "chat_hello" &&
        ent?.chatReadyAt > 0
      );

      const staleReconnectFault =
        /^(chat_reconnect_exhausted|chat_reconnect_failed|carrier_reconnect_|reconnect_|rtc_teardown:chat_reconnect_)/i;

      if (
        chatReady &&
        staleReconnectFault.test(String(concierge.lastErr || ""))
      ) {
        concierge.lastErr = null;
        emitSnapshot("chat_ready_retired_reconnect_fault");
      }
    }

    // A repair that completed before any post-drop liveness proof remains
    // invisible. Only a reconnect that was actually presented gets a matching
    // restoration line.
    if (restored && wasReconnecting && wasPresented) {
      post("INCOMING_MSG", {
        from: key,
        text: "Connection restored.",
        sys: "CONNECTION_RESTORED",
        at: Date.now(),
      });
    }
  }

  function six7CanReconnect(remoteHKey) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;
    if (__six7ExitSent) return false;
    if (__six7GoneNotified.has(key)) return false;
    return six7IsEstablishedLike(key);
  }

  function six7ScheduleReconnect(remoteHKey, reason = "transport_lost") {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    const why = String(reason || "transport_lost").toLowerCase();
    const carrierFault =
      /^(pc|ice)_(failed|closed|disconnected)$/.test(why) ||
      /^(chat_dc_closed|chat_dc_error)$/.test(why);

    if (!carrierFault) {
      concierge.lastErr = "reconnect_blocked_non_carrier:" + why;
      emitSnapshot("reconnect_blocked_non_carrier_" + why);
      return false;
    }

    if (!six7CanReconnect(key)) {
      six7CancelReconnect(key);
      emitSnapshot("carrier_reconnect_blocked_relationship_invalid");
      return false;
    }

    if (reconnectingPeers.has(key)) {
      concierge.lastErr = "carrier_reconnect_successor_fault:" + why;
      emitSnapshot(
        "carrier_reconnect_successor_fault_waiting_for_deadline_" + why,
      );
      return false;
    }

    const relationship = six7RelationshipGet(key);
    const livenessProbe = six7PrepareRelationshipControl(
      {
        type: "veil_status",
        from: selfHKey,
        to: key,
        videoId,
        state: getState(key),
        cycleId: relationship?.cycleId || null,
        reconnectProbe: true,
        bootId: six7BootId,
      },
      "veil_status",
    );

    const reconnect = {
      reason: why,
      startedAt: now(),
      presented: false,
      presentedAt: 0,
      presentationProof: "none",
      probeCid: String(livenessProbe?.cid || ""),
      probeSent: false,
      remoteBootId: String(
        relationship?.remoteBootId || six7RemoteBootIds.get(key) || "",
      ),
    };
    reconnectingPeers.set(key, reconnect);

    if (!six7ChatReconnectDeadlines.has(key)) {
      const deadline = setTimeout(() => {
        six7ChatReconnectDeadlines.delete(key);

        if (!reconnectingPeers.has(key)) return;
        if (!six7CanReconnect(key)) return;

        six7EndFailedChat(key, "reconnect_deadline_observed");
      }, SIX7_CHAT_RECONNECT_MS);

      six7ChatReconnectDeadlines.set(key, deadline);
    }

    // Transport repair starts immediately, but a shell-visible failure is not
    // projected yet. A directed relationship proof from the same incarnation
    // distinguishes a real chat-only interruption from a peer that is already
    // exiting and awaiting committed authority removal.
    reconnect.probeSent = six7SendControl(
      livenessProbe,
      "veil_status_reconnect_probe",
    );
    reconnectingPeers.set(key, reconnect);
    emitSnapshot(
      reconnect.probeSent
        ? "chat_reconnect_waiting_for_peer_liveness_proof"
        : "chat_reconnect_waiting_for_authority_or_deadline",
    );

    if (!six7IsReconnectPrimary(key)) {
      /*
       * Deterministic non-owner path:
       * teardown is already complete; wait for the pair owner’s offer.
       */
      emitSnapshot("carrier_reconnect_waiting_for_pair_owner_" + why);
      return true;
    }

    emitSnapshot("carrier_reconnect_owner_birth_" + why);

    void six7RequestChatPipe(
      key,
      "carrier_reconnect_owner_" + String(why || "transport_lost"),
    );

    return true;
  }

  function six7HandleTransportLoss(remoteHKey, reason = "transport_lost") {
    const key = six7ShortKey(remoteHKey);
    if (!key) return;

    const why = String(reason || "transport_lost").toLowerCase();
    const sysFault = /^(sysdc|sys_pipe|sys_transport|sys)_/.test(why);
    const chatFault = /^(dc|chat|chat_pipe|pc|ice)_/.test(why);

    if (sysFault) {
      if (six7TemporarySysTransactions.has(key)) {
        six7CloseTemporarySysTransaction(key, "transport_fault_" + why);
        emitSnapshot("temporary_sys_transport_fault_ignored_by_room_sys");
        return;
      }

      const gossipTarget = six7ShortKey(
        six7GossipRecovery.targets[six7GossipRecovery.index],
      );

      if (gossipTarget && gossipTarget === key) {
        six7TeardownSysPeer(key, "authority_gossip_target_failed_" + why);

        if (six7ShortKey(concierge.sysReadyPeer) === key) {
          concierge.sysReadyPeer = null;
          concierge.sysReadyAt = 0;
          concierge.sysReadyProof = "none";
          concierge.sysReadyEpoch = 0;
        }

        if (six7ShortKey(concierge.sysPeer) === key) {
          concierge.sysPeer = null;
          concierge.sysPipeState = "faulted";
          concierge.sysLastErr = why;
        }

        six7GossipRecovery.index += 1;
        if (six7GossipRecovery.kind === 'committed_mirror') {
          const failures = Number(six7GossipRecovery.retryCount || 0) + 1;
          six7GossipRecovery.retryCount = failures;
          six7GossipRecovery.nextRetryAt = now() + Math.min(15000, 1000 * 2 ** Math.min(failures - 1, 4));
        }
        six7RefreshSysReadiness("authority_gossip_target_failed");

        const nextTarget = six7ShortKey(
          six7GossipRecovery.targets[six7GossipRecovery.index],
        );

        if (nextTarget) {
          six7ScheduleSysAdmission("authority_gossip_next_syscon");
          emitSnapshot("authority_gossip_next_syscon_requested");
        } else {
          emitSnapshot("authority_gossip_syscons_unreachable");
          six7BeginGossipSuccessorRecovery(
            "authority_gossip_syscons_unreachable",
          );
        }
        return;
      }

      if (__six7GoneNotified.has(key)) {
        six7TeardownSysPeer(key, "sys_loss_peer_gone");
        emitSnapshot("sys_pipe_loss_ignored_peer_gone");
        return;
      }

      if (six7SysReplacementTerminal(key)) {
        six7TeardownSysPeer(key, "sys_loss_terminal_exit_pending");
        six7SysDialing.delete(key);
        six7SysPreflightUnstableRetries.delete(key);
        six7PendingRtcIce.delete(key);
        emitSnapshot("sys_pipe_loss_held_terminal_exit_pending");
        return;
      }

      six7TeardownSysPeer(key, "sys_transport_retired_" + why);

      if (six7ShortKey(concierge.sysReadyPeer) === key) {
        concierge.sysReadyPeer = null;
        concierge.sysReadyAt = 0;
        concierge.sysReadyProof = "none";
        concierge.sysReadyEpoch = 0;
      }

      if (six7ShortKey(concierge.sysPeer) === key) {
        concierge.sysPipeState = "faulted";
        concierge.sysLastErr = why;
      }

      six7RefreshSysReadiness("sys_transport_fault_" + why);

      const role = String(
        concierge.localRoleCandidate || concierge.role || "peer",
      );

      if (role === "peer" || !six7CommittedMember(selfHKey)) {
        six7ScheduleSysAdmission("sys_transport_fault_" + why);
      } else {
        six7ArmSysReplacement(key, why);
        six7MaybeRelinkSysconPair("sys_transport_fault_" + why);
      }

      emitSnapshot("sys_transport_fault_" + why);
      return;
    }

    if (chatFault) {
      const st = getState(key);

      rtcTeardown(key, "chat_transport_retired_" + why);

      if (
        __six7GoneNotified.has(key) ||
        st === VEIL.OUTSIDE ||
        st === VEIL.REMOVED
      ) {
        emitSnapshot("chat_transport_loss_ignored_not_established");
        return;
      }

      concierge.chatDropCount = Number(concierge.chatDropCount || 0) + 1;
      concierge.chatLastDropAt = now();
      concierge.chatLastDropPeer = key;

      six7ScheduleReconnect(key, why);
      emitSnapshot("chat_transport_reconnect_scheduled_" + why);
      return;
    }

    emitSnapshot("transport_loss_ignored_unclassified_" + why);
  }

  async function rtcSend(to, kind, data, opts = {}) {
    if (!selfHKey || !to) {
      CORE_DEBUG &&
        console.log("[RTC BLOCKED]", {
          selfHKey,
          to,
          hasWS: !!ws,
          state: ws?.readyState,
        });
      return false;
    }

    const fromKey = String(selfHKey || "")
      .slice(0, 16)
      .toLowerCase();
    const toKey = String(to || "")
      .slice(0, 16)
      .toLowerCase();

    const pipe =
      opts?.pipe === "sys" || opts?.sysCarrier === true ? "sys" : "chat";

    const payload = {
      videoId,
      from: fromKey,
      to: toKey,
      pipe,
      bootId: six7BootId,
    };

    if (pipe === "sys") {
      const base = six7LocalCommittedAuthority();
      if (kind === "offer" && base && six7ShortKey(base.syscon2) === fromKey &&
          six7PendingTerminalExits.has(six7ShortKey(base.syscon1))) {
        payload.successorProof = {room: videoId, from: fromKey, to: toKey, bootId: six7BootId,
          baseOne: base.syscon1, baseTwo: base.syscon2, baseEpoch: base.epoch,
          baseSeq: base.rosterSeq, baseRoster: six7AuthorityRosterRows(base.roster), terminalLeader: base.syscon1};
      }
      const ent = six7SysPeers.get(toKey);
      const attemptId = String(ent?.attempt?.id || ent?.sysAttemptId || "");

      if (attemptId) payload.sysAttemptId = attemptId;

      const transaction = six7TemporarySysTransactions.get(toKey);
      if (transaction && transaction.state !== "closed") {
        payload.sysTemporary = true;
        payload.sysPurpose = String(transaction.purpose || "");
        payload.sysTxId = String(transaction.id || "");
      }
    }

    const msg =
      kind === "offer"
        ? { type: "rtc_offer", ...payload, sdp: data }
        : kind === "answer"
          ? { type: "rtc_answer", ...payload, sdp: data }
          : kind === "ice"
            ? { type: "rtc_ice", ...payload, cand: data }
            : null;

    if (!msg) return false;

    if (opts && opts.sysCarrier === true) {
      msg.sysCarrier = true;
    }

    if (pipe === "chat") {
      const relationship = six7RelationshipGet(toKey);
      if (
        !six7RelationshipIsAccepted(relationship) ||
        six7PendingTerminalExits.has(toKey) ||
        __six7GoneNotified.has(toKey)
      ) {
        emitSnapshot("chat_rtc_tx_blocked_relationship_cycle");
        return false;
      }
      msg.relationshipCycleId = relationship.cycleId;
      const ent = six7ChatPeers.get(toKey);
      const chatAttemptId = String(ent?.chatAttemptId || "");
      if (chatAttemptId) msg.chatAttemptId = chatAttemptId;
    }

    CORE_DEBUG && console.log("[RTC TX]", msg.type, "→", to);

    // A surviving accepted peer pipe can introduce a replacement SYS edge
    // even when its two endpoints now use different bootstrap providers.
    const recoveryChat=six7ChatPeers.get(toKey);
    if(!six7HasOpenSysPipe(toKey)&&canCarrySysRecovery({packet:msg,room:videoId,self:fromKey,remote:toKey,
      member:!!six7CommittedMember(toKey),terminal:six7PendingTerminalExits.has(toKey),
      accepted:six7RelationshipIsAccepted(six7RelationshipGet(toKey)),ready:recoveryChat?.dc?.readyState==='open'&&recoveryChat?.chatState==='ready'})){
      try{recoveryChat.dc.send(JSON.stringify({t:'SYS_RECOVERY_RTC',packet:msg}));emitSnapshot('sys_recovery_signal_existing_peer_pipe');return true;}catch{}
    }
    const sent = six7SendControl(msg, msg.type);

    if (pipe === "chat") {
      const ent = six7ChatPeers.get(toKey);
      six7ChatSignalAudit("tx", {
        peer: toKey,
        kind: msg.type,
        route: concierge.lastRouteMode || "none",
        signal: ent?.pc?.signalingState || "missing",
        pc: ent?.pc?.connectionState || "missing",
        dc: ent?.dc?.readyState || "missing",
        cycle: msg.relationshipCycleId || "none",
        ok: sent,
        detail: String(concierge.lastRouteWhy || "none"),
      });
    }

    return sent;
  }

  function isDirectMsgForMe(msg) {
    const me = String(selfHKey || "")
      .slice(0, 16)
      .toLowerCase();
    const to = String(msg?.to || "")
      .slice(0, 16)
      .toLowerCase();
    const from = String(msg?.from || "")
      .slice(0, 16)
      .toLowerCase();

    if (!me) return false;
    if (!to || to !== me) return false;
    if (from && from === me) return false;

    return true;
  }

  function six7IsArrayBuffer(x) {
    return x && Object.prototype.toString.call(x) === "[object ArrayBuffer]";
  }

  function rtcSendDC(remoteHKey, obj) {
    const ent = six7ChatPeers.get(
      String(remoteHKey || "")
        .slice(0, 16)
        .toLowerCase(),
    );
    const dc = ent?.dc;
    if (!dc || dc.readyState !== "open") return false;
    try {
      dc.send(JSON.stringify(obj));
      return true;
    } catch {
      return false;
    }
  }

  function rtcWaitDCDrain(dc) {
    return new Promise((resolve) => {
      if (!dc || dc.readyState !== "open") return resolve(false);

      dc.bufferedAmountLowThreshold = 512 * 1024;

      if (dc.bufferedAmount < 2 * 1024 * 1024) return resolve(true);

      const done = () => {
        dc.removeEventListener("bufferedamountlow", done);
        resolve(dc.readyState === "open");
      };

      dc.addEventListener("bufferedamountlow", done, { once: true });
      setTimeout(done, 5000);
    });
  }

  async function rtcSendRawDC(remoteHKey, buf) {
    const ent = six7ChatPeers.get(
      String(remoteHKey || "")
        .slice(0, 16)
        .toLowerCase(),
    );
    const dc = ent?.dc;
    if (!dc || dc.readyState !== "open") return false;

    const ok = await rtcWaitDCDrain(dc);
    if (!ok || dc.readyState !== "open") return false;

    try {
      dc.send(buf);
      return true;
    } catch {
      return false;
    }
  }

  function sysPeerId(hKey) {
    const base = String(hKey || "")
      .slice(0, 16)
      .toLowerCase();
    if (!base) return "";
    return `${base}sys`;
  }

  function postSysIntent(kind, to, extra = {}) {
    const base = String(to || "")
      .slice(0, 16)
      .toLowerCase();
    if (!base) return;

    post("SYS_INTENT", {
      kind: String(kind || ""),
      to: base,
      sysTo: sysPeerId(base),
      at: Date.now(),
      ...extra,
    });
  }

  function rtcHelloPayload() {
    return {
      t: "HELLO",
      at: now(),
      room: videoId || null,
      from:
        String(selfHKey || "")
          .slice(0, 16)
          .toLowerCase() || null,
      handle: selfHandle || null,
      hex: selfHex || null,
      avatar: selfAvatar || null,
    };
  }

  function rtcSendHello(remoteHKey) {
    const payload = rtcHelloPayload();
    CORE_DEBUG && console.log("[six7][RTC] TX HELLO →", remoteHKey, payload);
    rtcSendDC(remoteHKey, payload);
  }

  function six7MarkSysTx(type, remoteHKey = "") {
    const key = six7ShortKey(remoteHKey);

    if (six7OwnsPrimarySysState(key)) {
      concierge.sysPipeState = concierge.sysPipeState || "missing";
      concierge.sysPeer = key;
    }

    concierge.sysLastTxAt = now();
    concierge.sysLastTxType = String(type || "unknown");
  }

  function six7MarkSysRx(type, remoteHKey = "") {
    const key = six7ShortKey(remoteHKey);
    const t = String(type || "unknown");

    const ownsPrimarySysState = six7OwnsPrimarySysState(key);

    if (ownsPrimarySysState) {
      concierge.sysPeer = key;
    }

    concierge.sysLastRxAt = now();
    concierge.sysLastRxType = t;

    const temporary = six7TemporarySysTransactions.get(key);
    if (temporary && temporary.state !== "closed") {
      emitSnapshot("temporary_sys_rx_" + t);
      return;
    }

    if (
      key &&
      (t === "SYS_HELLO" ||
        t === "SYS_PING" ||
        t === "SYS_PONG" ||
        t === "SYS_STATUS" ||
        t === "SYS_STATUS_ACK" ||
        t === "SYS_CONTROL" ||
        t === "SYS_CONTROL_ACK")
    ) {
      six7SysLastGoodRx.set(key, now());

      if (
        ownsPrimarySysState &&
        concierge.sysPipeState !== "ready" &&
        concierge.sysPipeState !== "closed" &&
        concierge.sysPipeState !== "error"
      ) {
        concierge.sysPipeState = "open";
      }
    }
  }

  function six7SendSysDC(remoteHKey, obj) {
    const t = String(obj?.t || '');
    const inner = obj?.msg;
    // Setup and departure packets must flow even while leadership duties are blocked.
    const transportControl = t === 'SYS_CONTROL' &&
      ((inner?.pipe === 'sys' && ['rtc_offer','rtc_answer','rtc_ice'].includes(inner?.type)) ||
       ['authority_exit','authority_detach'].includes(inner?.type));
    if ((t === 'SYS_STATUS' || t === 'SYS_STATUS_ACK' || (t === 'SYS_CONTROL' && !transportControl)) &&
        !six7AuthorizeSyscon1Duty(t+':'+String(remoteHKey)+':'+(++sdkAdminDutySequence), () => six7SendSysDC(remoteHKey,obj))) return false;

    const key = six7ShortKey(remoteHKey);
    if (!key || !obj || typeof obj !== "object") return false;

    const ent = six7SysPeers.get(key);
    const dc = ent?.sysdc;

    const type = String(obj.t || obj.type || "SYS_UNKNOWN");

    if (!six7TemporarySysAllowsEnvelope(key, obj)) {
      concierge.sysLastErr = "temporary_sys_envelope_rejected:" + type;
      emitSnapshot("temporary_sys_envelope_rejected_" + type);
      return false;
    }

    if (!dc || dc.readyState !== "open") {
      concierge.sysLastErr = "sysdc_not_open:" + type;
      concierge.sysPipeState = dc?.readyState || "missing";
      emitSnapshot("sysdc_send_blocked_" + type);
      return false;
    }

    six7MarkSysTx(type, key);

    try {
      dc.send(JSON.stringify(obj));
      emitSnapshot("sysdc_tx_" + type);
      return true;
    } catch (e) {
      concierge.sysLastErr =
        "sysdc_send_failed:" + type + ":" + String(e?.message || e);
      concierge.sysPipeState = "error";
      emitSnapshot("sysdc_send_failed_" + type);
      return false;
    }
  }

  function six7SysHelloPayload() {
    return {
      t: "SYS_HELLO",
      at: now(),
      room: videoId || null,
      from: six7ShortKey(selfHKey) || null,
      bootId: six7BootId,
      role: concierge.role || "peer",
      candidateRole: concierge.localRoleCandidate || "peer",
      epoch: Number(concierge.sysconEpoch || 0),
      roomJoinedAt: Number(concierge.roomJoinedAt || 0),
    };
  }
  function six7SysStatusPayload(type = "SYS_STATUS") {
    const self = six7ShortKey(selfHKey);
    const authority = {
      from: six7ShortKey(six7SysAuthority.from),
      syscon1: six7ShortKey(six7SysAuthority.syscon1),
      syscon2: six7ShortKey(six7SysAuthority.syscon2),
      epoch: Number(six7SysAuthority.epoch || 0),
      rosterSeq: Number(six7SysAuthority.rosterSeq || 0),
      roster: six7AuthorityRosterRows(six7SysAuthority.roster),
    };

    const publishesAuthority = !!(
      self &&
      authority.from === self &&
      authority.syscon1 === self &&
      authority.epoch === Number(concierge.sysconEpoch || 0) &&
      authority.rosterSeq > 0 &&
      authority.roster.length > 0
    );

    const localPending = six7SysAuthority.pending;
    const pendingAuthority =
      localPending && six7ShortKey(localPending?.snapshot?.syscon1) === self
        ? {
            id: String(localPending.id || ""),
            via: String(localPending.via || "syscon1_commit"),
            baseEpoch: Number(localPending.baseEpoch || 0),
            baseRosterSeq: Number(localPending.baseRosterSeq || 0),
            baseRoster: six7AuthorityRosterRows(localPending.baseRoster),
            baseAuthority: localPending.baseAuthority
              ? {
                  ...localPending.baseAuthority,
                  roster: six7AuthorityRosterRows(
                    localPending.baseAuthority?.roster,
                  ),
                }
              : null,
            terminalExits: six7AuthorityTerminalExits(localPending),
            admission: localPending.admission
              ? { ...localPending.admission }
              : null,
            leadership: localPending.leadership
              ? { ...localPending.leadership }
              : null,
            snapshot: {
              ...localPending.snapshot,
              roster: six7AuthorityRosterRows(localPending.snapshot?.roster),
            },
          }
        : null;

    const mirrorAuthority =
      String(concierge.localRoleCandidate || concierge.role || "peer") ===
        "syscon2" &&
      authority.rosterSeq > 0 &&
      authority.roster.length
        ? {
            from: authority.from,
            syscon1: authority.syscon1,
            syscon2: authority.syscon2,
            epoch: authority.epoch,
            rosterSeq: authority.rosterSeq,
            roster: authority.roster,
            terminalExits: Array.from(six7PendingTerminalExits.entries()).map(
              ([member, command]) => ({
                member: six7ShortKey(member),
                bootId: String(command?.bootId || "") || null,
                epoch: Number(command?.epoch || 0),
                at: Number(command?.at || 0),
                via: String(command?.via || "mirror_authority"),
              }),
            ),
          }
        : null;

    return {
      t: String(type || "SYS_STATUS"),
      at: now(),
      room: videoId || null,
      from: self || null,
      bootId: six7BootId,
      role: publishesAuthority
        ? "syscon1"
        : String(concierge.localRoleCandidate || concierge.role || "peer"),
      candidateRole: concierge.localRoleCandidate || "peer",
      epoch: Number(concierge.sysconEpoch || 0),
      sysReady: !!concierge.sysReady,
      route: concierge.route || "bootstrap_ws",
      wouldRoute: concierge.wouldRoute || "bootstrap_ws",
      roomJoinedAt: Number(concierge.roomJoinedAt || 0),
      syscon1: publishesAuthority
        ? authority.syscon1
        : six7ShortKey(concierge.syscon1) || null,
      syscon2: publishesAuthority
        ? authority.syscon2
        : six7ShortKey(concierge.syscon2) || null,
      rosterSeq: publishesAuthority ? authority.rosterSeq : 0,
      roster: publishesAuthority ? authority.roster : [],
      pendingAuthority,
      pendingAcceptedId:
        type === "SYS_STATUS_ACK" && localPending?.acceptedBy === self
          ? String(localPending.id || "")
          : null,
      mirrorAuthority,
    };
  }
  function six7PresencePayload() {
    return {
      type: "presence",
      t: now(),
      videoId,
      from: selfHKey,
      handle: selfHandle,
      bootId: six7BootId,
      epoch: Number(concierge.sysconEpoch || 0),
      roomJoinedAt: Number(concierge.roomJoinedAt || 0),
      hex: selfHex,
      avatar: selfAvatar || undefined,
    };
  }
  function six7LocalPresenceRow() {
    const self = six7ShortKey(selfHKey);

    return {
      hKey: self,
      from: self,
      handle: selfHandle || null,
      bootId: six7BootId,
      epoch: Number(concierge.sysconEpoch || 0),
      roomJoinedAt: Number(concierge.roomJoinedAt || 0),
      hex: selfHex || null,
      avatar: selfAvatar || undefined,
      videoId: videoId || null,
      seenVia: "self",
      t: now(),
    };
  }

  function six7PresenceRowFromPeer(peer, via = "unknown") {
    if (!peer || typeof peer !== "object") return null;

    const hKey = six7ShortKey(peer.hKey || peer.from || peer.hash);
    if (!hKey) return null;

    return {
      ...peer,
      hKey,
      from: hKey,
      videoId: peer.videoId || videoId || null,
      handle: peer.handle || null,
      hex: peer.hex || null,
      avatar: peer.avatar || undefined,
      seenVia: via,
      t: peer.t || peer.at || now(),
    };
  }

  // SIX7 PATCH START: presence-merge-preserve-identity
  function six7MergePresenceRow(oldRow, nextRow, via = "unknown") {
    if (!nextRow) return oldRow || null;
    if (!oldRow) {
      const joinedAt = Number(
        nextRow.roomJoinedAt || nextRow.joinedAt || nextRow.t || now(),
      );
      return {
        ...nextRow,
        roomJoinedAt: joinedAt,
        epoch: Number(nextRow.epoch || 0),
      };
    }

    const oldJoinedAt = Number(
      oldRow.roomJoinedAt || oldRow.joinedAt || oldRow.t || 0,
    );
    const nextJoinedAt = Number(
      nextRow.roomJoinedAt || nextRow.joinedAt || nextRow.t || 0,
    );

    const merged = {
      ...oldRow,
      ...nextRow,
      hKey: nextRow.hKey || oldRow.hKey,
      from: nextRow.from || oldRow.from,
      videoId: nextRow.videoId || oldRow.videoId || videoId || null,
      handle: nextRow.handle || oldRow.handle || null,
      bootId: nextRow.bootId || oldRow.bootId || null,
      epoch: Math.max(Number(oldRow.epoch || 0), Number(nextRow.epoch || 0)),
      roomJoinedAt:
        oldJoinedAt && nextJoinedAt
          ? Math.min(oldJoinedAt, nextJoinedAt)
          : oldJoinedAt || nextJoinedAt || now(),
      hex: nextRow.hex || oldRow.hex || null,
      avatar: nextRow.avatar || oldRow.avatar || undefined,
      seenVia: via || nextRow.seenVia || oldRow.seenVia || "unknown",
      t: Math.max(Number(oldRow.t || 0), Number(nextRow.t || 0), now()),
    };

    return merged;
  }
  // SIX7 PATCH END: presence-merge-preserve-identity

  function six7EmitPresenceRoster(reason = "presence_roster") {
    const self = six7ShortKey(selfHKey);
    if (!self) return false;

    const rows = new Map();

    rows.set(self, six7LocalPresenceRow());

    for (const hKey of Array.from(lastRoster || [])) {
      const key = six7ShortKey(hKey);
      if (!key || key === self) continue;

      rows.set(
        key,
        six7PresenceBook.get(key) || {
          hKey: key,
          from: key,
          videoId: videoId || null,
          handle: null,
          seenVia: "roster",
        },
      );
    }

    const list = Array.from(rows.values()).filter((p) =>
      six7ShortKey(p.hKey || p.from),
    );

    if (!list.length) return false;

    post("PRESENCE_ROSTER", {
      videoId: videoId || null,
      list,
      via: reason,
    });

    six7RefreshSysReadiness(reason);
    six7RefreshSysCandidates(list, reason);

    emitSnapshot(reason);
    return true;
  }

  function six7RememberPresencePeer(peer, via = "unknown", opts = {}) {
    const row = six7PresenceRowFromPeer(peer, via);
    if (!row) return false;

    const hKey = six7ShortKey(row.hKey || row.from);
    if (!hKey) return false;

    // Presence is identity/metadata evidence only.
    // It cannot add room membership or clear a committed terminal removal.
    const oldRow = six7PresenceBook.get(hKey) || null;
    const mergedRow = six7MergePresenceRow(oldRow, row, via);
    six7PresenceBook.set(hKey, mergedRow);

    if (lastRoster.has(hKey)) {
      dialReady = true;
    }

    if (opts.peerEvent !== false && lastRoster.has(hKey)) {
      post("PRESENCE_PEER", {
        videoId: row.videoId || videoId || null,
        peer: mergedRow,
      });
    }

    if (opts.rosterEvent !== false) {
      six7EmitPresenceRoster("presence_" + via);
    }

    return true;
  }

  function six7OwnsDirectedSysRelay() {
    const self = six7ShortKey(selfHKey);
    if (!self) return false;

    const committedOne = six7ShortKey(
      six7FreshSysAuthority()?.syscon1 || six7SysAuthority.syscon1,
    );
    const effectiveOne = six7ShortKey(
      committedOne || concierge.syscon1 || concierge.cfConciergeSyscon1,
    );

    return !!(effectiveOne && self === effectiveOne);
  }

  function six7QueueDirectedSysControl(
    destinationHKey,
    envelope,
    detail = "queued",
  ) {
    const destination = six7ShortKey(destinationHKey);
    const self = six7ShortKey(selfHKey);

    if (!destination || destination === self || !envelope) return false;
    if (__six7GoneNotified.has(destination)) return false;

    let pending = six7PendingSysRelay.get(destination);

    if (!pending) {
      pending = [];
      six7PendingSysRelay.set(destination, pending);
    }

    pending.push({
      envelope: {
        ...envelope,
        relayedBy: self,
        relayedAt: now(),
      },
      at: now(),
    });

    if (pending.length > SIX7_SYS_RELAY_PENDING_MAX) {
      pending.splice(0, pending.length - SIX7_SYS_RELAY_PENDING_MAX);
    }

    six7ChatSignalAudit("relay_held", {
      peer: destination,
      kind: envelope?.kind || envelope?.msg?.type || "unknown",
      route: "directed_sys_pending",
      signal: "none",
      pc: "none",
      dc: "none",
      cycle: envelope?.msg?.relationshipCycleId || "none",
      ok: true,
      detail,
    });

    emitSnapshot("sys_control_directed_queue_held");
    return true;
  }

  function six7HoldDirectedSysRelay(destinationHKey, envelope) {
    const destination = six7ShortKey(destinationHKey);
    const self = six7ShortKey(selfHKey);

    if (!destination || destination === self) return false;

    if (!six7OwnsDirectedSysRelay()) return false;
    if (!lastRoster.has(destination)) return false;
    if (__six7GoneNotified.has(destination)) return false;

    const held = six7QueueDirectedSysControl(
      destination,
      envelope,
      "authoritative_syscon1_handoff",
    );

    if (held) {
      emitSnapshot("sys_control_relay_held_for_peer_owned_tether");
    }

    return held;
  }

  function six7FlushDirectedSysRelay(destinationHKey) {
    const destination = six7ShortKey(destinationHKey);
    if (!destination) return false;
    if (!six7HasOpenSysPipe(destination)) return false;

    const pending = six7PendingSysRelay.get(destination);
    if (!pending?.length) return false;

    six7PendingSysRelay.delete(destination);

    const held = [];

    for (const item of pending) {
      const envelope = item?.envelope;

      if (!envelope || !six7SendSysDC(destination, envelope)) {
        held.push(item);
      }
    }

    if (held.length) {
      six7PendingSysRelay.set(
        destination,
        held.slice(-SIX7_SYS_RELAY_PENDING_MAX),
      );

      concierge.lastErr = "sys_control_relay_flush_failed:" + destination;

      emitSnapshot("sys_control_relay_flush_failed");
      return false;
    }

    concierge.lastErr = null;

    six7ChatSignalAudit("relay_flushed", {
      peer: destination,
      kind: "queued_control",
      route: "directed_sys",
      signal: "none",
      pc: "none",
      dc: "open",
      cycle: "none",
      ok: true,
      detail: "ordered_queue_delivered",
    });

    emitSnapshot("sys_control_relay_flushed");
    return true;
  }

  function six7SendSysControl(msg, type = "") {
    if (!msg || typeof msg !== "object") return false;

    const kind = String(msg.type || type || "unknown");
    const cls = six7ControlClassFor(kind);

    if (cls === "relationship_control" && !six7ShortKey(msg.to)) {
      concierge.lastErr = "sys_control_missing_to:" + kind;
      emitSnapshot("sys_control_missing_to_" + kind);
      return false;
    }

    const destination = six7ShortKey(msg.to || concierge.sysPeer);

    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const ownsDirectedRelay = six7OwnsDirectedSysRelay();

    const self = six7ShortKey(selfHKey);
    const boss = six7ShortKey(concierge.syscon1);
    const secondary = six7ShortKey(concierge.syscon2);
    const currentNeighbor = six7ShortKey(concierge.sysPeer);

    const directDestinationOpen = !!(
      destination &&
      destination !== self &&
      six7HasOpenSysPipe(destination)
    );

    const envelope = {
      t: "SYS_CONTROL",
      at: now(),
      from: self,
      room: videoId || null,
      kind,
      cid: msg.cid || null,
      msg,
    };

    if (
      cls !== "presence" &&
      destination &&
      destination !== self &&
      ownsDirectedRelay &&
      !directDestinationOpen
    ) {
      return six7HoldDirectedSysRelay(destination, envelope);
    }

    const key =
      cls === "presence"
        ? six7ShortKey(msg.to || concierge.sysPeer)
        : directDestinationOpen
          ? destination
          : currentNeighbor || boss;

    if (!key) {
      concierge.lastErr = "sys_control_no_peer:" + kind;
      emitSnapshot("sys_control_no_peer_" + kind);
      return false;
    }

    // A prior directed RTC offer/answer may have been held during the exact
    // syscon mirror handoff window. Flush it before later ICE/control so
    // signaling order is preserved.
    if (
      destination &&
      destination === key &&
      six7PendingSysRelay.get(destination)?.length
    ) {
      const flushed = six7FlushDirectedSysRelay(destination);

      if (!flushed) {
        return six7QueueDirectedSysControl(
          destination,
          envelope,
          "ordered_behind_unflushed_directed_control",
        );
      }
    }

    const sent = six7SendSysDC(key, envelope);
    if (sent) return true;

    // A physically open SYS edge can still reject one send while its SCTP
    // carrier is transitioning. Do not lose directed RTC signaling. Hold the
    // exact envelope; the next successful directed send/open flushes it first.
    if (
      destination &&
      destination === key &&
      cls === "rtc_signal" &&
      (kind === "rtc_offer" || kind === "rtc_answer")
    ) {
      const held = six7QueueDirectedSysControl(
        destination,
        envelope,
        "endpoint_sys_send_failure",
      );

      if (held) {
        concierge.lastErr =
          "directed_rtc_signal_held_after_sys_send_failure:" + kind;
        emitSnapshot("directed_rtc_signal_held_after_sys_send_failure_" + kind);
        return true;
      }
    }

    return false;
  }

  function six7WireSysDC(remoteHKey, dc) {
    const key = six7ShortKey(remoteHKey);
    if (!key || !dc) return;

    try {
      dc.binaryType = "arraybuffer";
    } catch {}

    const ownsPrimarySysState = six7OwnsPrimarySysState(key);

    if (ownsPrimarySysState) {
      concierge.sysPeer = key;
      concierge.sysPipeState = dc.readyState || "connecting";
    }

    emitSnapshot(
      ownsPrimarySysState ? "sysdc_wire_primary" : "sysdc_wire_peer_tether",
    );

    dc.onopen = () => {
      const ent = six7SysPeers.get(key);
      if (!ent || ent.sysdc !== dc) return;
      completeSysBirth(ent);
      sdkSysBirthFailures.delete(key);
      if (ent) {
        ent.sysCarrier = false;
        const edgeAttempt = six7SysEdgeAttempt(key, false);
        if (edgeAttempt) {
          edgeAttempt.state = "open";
          edgeAttempt.lastEvent = "datachannel_open";
          edgeAttempt.faultReason = "none";
        }
      }

      if (six7SysPeers.get(key)?.sysdc !== dc) return;

      post('PHANTOM_ADMIN_LINK_READY', {from:key});
      const temporary = six7TemporarySysTransactions.get(key) || null;
      if (temporary && ent) {
        ent.temporary = true;
        ent.tempSysPurpose = String(temporary.purpose || "");
        ent.tempSysTxId = String(temporary.id || "");
        temporary.state = "open";
        temporary.openedAt = now();
      }

      if (ownsPrimarySysState) {
        concierge.sysPeer = key;
        concierge.sysPipeState = "open";
        concierge.sysLastErr = null;
      }

      if (!temporary) {
        six7SysLastGoodRx.set(key, now());

        if (six7CancelSysReplacement(key, "replacement_restored")) {
          emitSnapshot("sys_peer_replacement_restored");
        }

        six7RefreshSysReadiness("sysdc_open");
        six7FlushQueuedRtcIce(key, "rtc_ice_queue_flush_sysdc_open");
        six7FlushDirectedSysRelay(key);
      }

      const relPending = six7PendingRelationship.get(key);
      if (relPending?.length) {
        six7PendingRelationship.delete(key);

        for (const item of relPending) {
          const relType = String(item?.type || item?.msg?.type || "unknown");
          const relMsg = item?.msg;

          if (
            !relMsg ||
            six7ControlClassFor(relType) !== "relationship_control"
          ) {
            continue;
          }

          if (
            temporary &&
            (relType !== "sys_unblock" ||
              String(relMsg.sysTxId || "") !== String(temporary.id || ""))
          ) {
            let held = six7PendingRelationship.get(key);
            if (!held) {
              held = [];
              six7PendingRelationship.set(key, held);
            }
            held.push(item);
            continue;
          }

          six7MarkControlTx(relType, "six7_sys");

          concierge.routeReason = "sys_route_relationship_control_pending_open";
          concierge.lastRouteAt = now();
          concierge.lastRouteType = relType;
          concierge.lastRouteReason = concierge.routeReason;
          concierge.lastRouteClass = "relationship_control";
          concierge.routeMode = "sys_primary";
          concierge.lastRouteMode = "sys_primary";
          concierge.wouldRoute = "six7_sys";
          concierge.wouldRouteReason =
            "sys_ready_route_relationship_pending_open";

          concierge.lastRelationshipTxAt = now();
          concierge.lastRelationshipTxType = relType;
          concierge.lastRelationshipRoute = "six7_sys";
          concierge.lastRelationshipReason =
            "sys_route_relationship_control_pending_open";

          const ok = six7SendSysControl(relMsg, relType);

          if (ok) {
            concierge.lastErr = null;
            if (temporary && relType === "sys_unblock") {
              temporary.state = "command_sent";
            }
            emitSnapshot("relationship_pending_sys_sent_" + relType);
          } else {
            concierge.lastErr =
              "relationship_pending_sys_send_failed:" + relType;
            emitSnapshot("relationship_pending_sys_send_failed_" + relType);
          }
        }
      }

      emitSnapshot(temporary ? "temporary_sysdc_open" : "sysdc_open");

      if (temporary) {
        return;
      }

      six7ResumeTerminalAuthority("sysdc_open");
      if (six7SysAuthority.pending) {
        six7PublishCommittedAuthority("sysdc_open_pending_authority");
      }

      six7SendSysDC(key, six7SysHelloPayload());
      six7SendSysDC(key, {
        t: "SYS_PING",
        at: now(),
        from: six7ShortKey(selfHKey),
        room: videoId || null,
      });
      six7SendSysDC(key, six7SysStatusPayload("SYS_STATUS"));

      const localAuthority = six7LocalCommittedAuthority();
      const selfKey = six7ShortKey(selfHKey);
      const selfCommitted = six7AuthorityRosterRows(
        localAuthority?.roster,
      ).some((row) => row.hKey === selfKey);
      const presence = six7PresencePayload();

      if (!selfCommitted) {
        presence.to = key;
        presence.admit = true;
        presence.admitReason = "sysdc_open_uncommitted_incarnation";

        const ok = six7SendSysControl(presence, "presence");

        six7MembershipAuditSet(ok ? "admit_sent" : "admit_send_failed", {
          peer: key,
          reason: "sysdc_open_uncommitted_incarnation",
          pipe: String(dc.readyState || "missing"),
          role: concierge.localRoleCandidate || concierge.role || "peer",
        });

        if (ok) {
          six7SysAdmissionLastSent.set(key, now());
          emitSnapshot("sys_admission_presence_sent_open");
        }
      } else {
        six7SendControl(presence, "presence");
      }
    };

    dc.onclose = () => {
      const ent = six7SysPeers.get(key);

      // A retired SYS channel cannot mutate its successor attempt.
      if (!ent || ent.sysdc !== dc) {
        emitSnapshot("stale_sys_pipe_close_ignored");
        return;
      }

      ent.sysCarrier = false;
      const edgeAttempt = six7SysEdgeAttempt(key, false);
      if (edgeAttempt) {
        edgeAttempt.state = "closed";
        edgeAttempt.lastEvent = "datachannel_closed";
        edgeAttempt.faultReason = "sysdc_closed";
      }

      ent.sysdc = null;

      if (ent.temporary === true || six7TemporarySysTransactions.has(key)) {
        six7CloseTemporarySysTransaction(key, "remote_closed");
        return;
      }

      if (__six7GoneNotified.has(key)) {
        emitSnapshot("sysdc_close_ignored_peer_gone");
        return;
      }

      const relPending = six7PendingRelationship.get(key);
      if (relPending?.length) {
        concierge.lastRelationshipRoute = "six7_sys_pending";
        concierge.lastRelationshipReason = "sysdc_closed_pending_held";
        concierge.lastErr = "relationship_pending_held_sysdc_close";
        emitSnapshot("relationship_pending_held_sysdc_close");
      }

      six7HandleTransportLoss(key, "sysdc_closed");
    };

    dc.onerror = () => {
      const ent = six7SysPeers.get(key);

      // A retired SYS channel cannot mutate its successor attempt.
      if (!ent || ent.sysdc !== dc) {
        emitSnapshot("stale_sys_pipe_error_ignored");
        return;
      }

      ent.sysCarrier = false;
      const edgeAttempt = six7SysEdgeAttempt(key, false);
      if (edgeAttempt) {
        edgeAttempt.state = "closed";
        edgeAttempt.lastEvent = "datachannel_closed";
        edgeAttempt.faultReason = "sysdc_closed";
      }

      ent.sysdc = null;

      if (ent.temporary === true || six7TemporarySysTransactions.has(key)) {
        six7CloseTemporarySysTransaction(key, "error");
        return;
      }

      six7HandleTransportLoss(key, "sysdc_error");
    };

    dc.onmessage = (ev) => {
      let msg = null;
      try {
        msg = JSON.parse(String(ev?.data || ""));
      } catch {
        msg = { t: "SYS_RAW", raw: String(ev?.data || "") };
      }

      if (msg?.t === 'PHANTOM_DIST') {
        if (six7SysPeers.get(key)?.sysdc === dc && !six7TemporarySysTransactions.has(key) && JSON.stringify(msg).length <= 32768)
          post('PHANTOM_DIST', {from:key, frame:msg.frame});
        return;
      }
      if (msg?.t === 'PHANTOM_ADMIN') {
        if (six7SysPeers.get(key)?.sysdc === dc && !six7TemporarySysTransactions.has(key) && JSON.stringify(msg).length <= 4096)
          post('PHANTOM_ADMIN', {from:key, record:msg.record});
        return;
      }
      const type = String(msg?.t || msg?.type || "SYS_UNKNOWN");
      const temporaryRx = six7TemporarySysTransactions.get(key) || null;

      if (temporaryRx && type !== "SYS_CONTROL" && type !== "SYS_CONTROL_ACK") {
        emitSnapshot("temporary_sys_nontransaction_packet_rejected_" + type);
        return;
      }

      six7MarkSysRx(type, key);

      if (
        type === "SYS_HELLO" ||
        type === "SYS_STATUS" ||
        type === "SYS_STATUS_ACK"
      ) {
        six7RememberPresencePeer(
          {
            hKey: key,
            from: key,
            bootId: String(msg?.bootId || "") || null,
            epoch: Number(msg?.epoch || 0),
            roomJoinedAt: Number(msg?.roomJoinedAt || msg?.joinedAt || 0),
            videoId: msg?.room || videoId || null,
            t: Number(msg?.at || now()),
          },
          "six7_sys_claim",
          { peerEvent: false, rosterEvent: false },
        );
      }

      if (type === "SYS_BYE") {
        concierge.lastErr = "legacy_sys_bye_ignored";
        emitSnapshot("legacy_sys_bye_ignored");
        return;
      }

      const authorityBearingStatus =
        type === "SYS_STATUS" || type === "SYS_STATUS_ACK";

      if (type === "SYS_HELLO" && msg?.bootId) {
        six7ObserveRemoteIncarnation(key, {
          bootId: String(msg.bootId || ""),
          attemptId: String(six7SysPeers.get(key)?.attempt?.id || ""),
          bindCurrent: true,
          via: "sys_hello",
        });
      }

      if (
        !authorityBearingStatus &&
        (type === "SYS_HELLO" ||
          type === "SYS_PING" ||
          type === "SYS_PONG" ||
          type === "SYS_CONTROL" ||
          type === "SYS_CONTROL_ACK")
      ) {
        six7MarkGoodSysRx(key, type);
        six7RefreshSysReadiness("sys_rx_" + type);
        emitSnapshot("sys_rx_" + type);
      }

      if (type === "SYS_PING") {
        six7SendSysDC(key, {
          t: "SYS_PONG",
          at: now(),
          from: six7ShortKey(selfHKey),
          room: videoId || null,
        });
      } else if (type === "SYS_STATUS") {
        const localRole = String(
          concierge.localRoleCandidate || concierge.role || "peer",
        );

        const remoteRole = String(msg?.role || msg?.candidateRole || "peer");

        const sender = six7ShortKey(msg?.from || key);
        const boss = six7ShortKey(concierge.syscon1);

        const acceptedPending = six7SysAuthority.pending;
        const acceptedCommit = !!(acceptedPending &&
          acceptedPending.acceptedBy === six7ShortKey(selfHKey) &&
          sender === key && sender === six7ShortKey(acceptedPending.snapshot?.syscon1) &&
          six7AuthoritySnapshotId(msg) === six7AuthoritySnapshotId(acceptedPending.snapshot) &&
          String(acceptedPending.id || "") === six7AuthorityTransactionId(acceptedPending));
        const recoveryCommit = localRole === 'peer' && acceptsSuccessorStatus({
          base:{...six7LocalCommittedAuthority(),room:videoId},message:msg,sender:key,self:six7ShortKey(selfHKey),
          recoveryTarget:six7GossipRecovery.targets[six7GossipRecovery.index],
          leaderPipeOpen:six7HasOpenSysPipe(boss)});
        const authorityStatusAllowed = acceptedCommit || recoveryCommit || !!(
          sender &&
          sender === key &&
          sender === boss &&
          (localRole === "peer" || localRole === "syscon2")
        );

        const duplicateSysconClaim = !!(
          six7IsSysconRole(localRole) &&
          six7IsSysconRole(remoteRole) &&
          localRole === remoteRole
        );

        const pendingAuthorityAccepted = six7AcceptPendingAuthority(msg, key);
        const mirrorHandoffAccepted = pendingAuthorityAccepted
          ? false
          : six7AdoptMirrorAuthority(msg, key);

        if (pendingAuthorityAccepted) {
          emitSnapshot("sys_status_pending_authority_accepted");
        } else if (mirrorHandoffAccepted) {
          emitSnapshot("sys_status_mirror_handoff_accepted");
        } else if (duplicateSysconClaim) {
          six7ReconcileRoleFromSysStatus(msg, "SYS_STATUS");
        } else if (
          authorityStatusAllowed &&
          six7RememberSysAuthority(msg, "SYS_STATUS")
        ) {
          six7RefreshSysCandidates(six7LatchedSysRosterList(), "SYS_STATUS");
        }

        six7MarkGoodSysRx(key, "SYS_STATUS");
        six7RefreshSysReadiness("sys_rx_SYS_STATUS");
        emitSnapshot("sys_rx_SYS_STATUS");

        six7SendSysDC(key, {
          ...six7SysStatusPayload("SYS_STATUS_ACK"),
          ackFor: msg?.from || null,
          ackAt: msg?.at || null,
        });
      } else if (type === "SYS_STATUS_ACK") {
        concierge.sysLastRxAt = now();
        concierge.sysLastRxType = "SYS_STATUS_ACK";

        const sender = six7ShortKey(msg?.from || key);
        const boss = six7ShortKey(concierge.syscon1);
        const stampedBoss = six7ShortKey(msg?.syscon1);
        const self = six7ShortKey(selfHKey);

        const incomingEpoch = Number(msg?.epoch || 0);
        const currentEpoch = Number(concierge.sysconEpoch || 0);

        const incomingSeq = Number(msg?.rosterSeq || 0);
        const currentSeq = Number(six7SysAuthority.rosterSeq || 0);

        const roomOk =
          !msg?.room || !videoId || String(msg.room) === String(videoId);

        const pendingAuthorityPromoted = six7PromotePendingAuthority(
          msg?.pendingAcceptedId,
          key,
        );

        const authoritativeRosterAck = !!(
          sender &&
          boss &&
          sender === key &&
          sender === boss &&
          stampedBoss === boss &&
          String(msg?.role || "") === "syscon1" &&
          six7ShortKey(msg?.ackFor) === self &&
          incomingEpoch &&
          incomingEpoch === currentEpoch &&
          incomingSeq >= currentSeq &&
          Array.isArray(msg?.roster) &&
          roomOk
        );

        if (pendingAuthorityPromoted) {
          emitSnapshot("sys_status_ack_pending_authority_promoted");
        } else if (authoritativeRosterAck) {
          // Compatibility only for a committed leader snapshot. The ACK is
          // no longer an independent authority decision path.
          six7RememberSysAuthority(msg, "SYS_STATUS_ACK_ROSTER");
          emitSnapshot("sys_status_ack_authoritative_roster_rx");
        } else {
          emitSnapshot("sys_status_ack_pipe_proof_only");
        }

        if (pendingAuthorityPromoted || authoritativeRosterAck) {
          six7RefreshSysCandidates(
            six7LatchedSysRosterList(),
            "SYS_STATUS_ACK",
          );
        }

        six7MarkGoodSysRx(key, "SYS_STATUS_ACK");
        six7RefreshSysReadiness("sys_rx_SYS_STATUS_ACK");
        emitSnapshot("sys_rx_SYS_STATUS_ACK");
      } else if (type === "SYS_CONTROL") {
        const inner = msg?.msg || null;
        const kind = String(msg?.kind || inner?.type || "unknown");

        const selfKey = six7ShortKey(selfHKey);
        const destination = six7ShortKey(inner?.to);

        const role = String(
          concierge.localRoleCandidate || concierge.role || "peer",
        );

        const temporary = six7TemporarySysTransactions.get(key) || null;
        if (temporary) {
          const allowed = !!(
            inner &&
            destination === selfKey &&
            six7TemporarySysAllowsEnvelope(key, msg)
          );

          if (allowed) {
            six7HandleInboundRelationshipControl(inner, "six7_sys_temporary");
          } else {
            emitSnapshot("temporary_sys_control_rejected");
          }

          six7SendSysDC(key, {
            t: "SYS_CONTROL_ACK",
            at: now(),
            from: six7ShortKey(selfHKey),
            room: videoId || null,
            kind,
            cid: msg?.cid || inner?.cid || null,
            ackFor: msg?.from || null,
            ackAt: msg?.at || null,
          });

          emitSnapshot("temporary_sys_control_rx_" + kind);
          return;
        }

        if (inner && destination && destination !== selfKey) {
          const boss = six7ShortKey(concierge.syscon1);

          let forwarded = false;
          let held = false;

          if (six7HasOpenSysPipe(destination)) {
            forwarded = six7SendSysDC(destination, {
              ...msg,
              relayedBy: selfKey,
              relayedAt: now(),
            });
          } else if (role === "syscon1") {
            held = six7HoldDirectedSysRelay(destination, msg);
          } else if (
            role === "syscon2" &&
            boss &&
            boss !== selfKey &&
            boss !== key &&
            six7HasOpenSysPipe(boss)
          ) {
            forwarded = six7SendSysDC(boss, {
              ...msg,
              relayedBy: selfKey,
              relayedAt: now(),
            });
          }

          concierge.lastErr =
            forwarded || held
              ? null
              : "sys_control_relay_no_legal_next_hop:" + destination;

          emitSnapshot(
            forwarded
              ? "sys_control_relayed_" + kind
              : held
                ? "sys_control_relay_held_" + kind
                : "sys_control_relay_blocked_" + kind,
          );
        } else if (
          (kind === "authority_exit" || kind === "authority_detach") &&
          inner?.from
        ) {
          const commandFrom = six7ShortKey(inner.from);

          // Departure evidence is accepted only from the exact member's owned
          // SYS edge. It is never relayable or forgeable by a third peer.
          const envelopeFrom = six7ShortKey(msg?.from);
          const senderBound = !!(
            commandFrom &&
            envelopeFrom === commandFrom &&
            commandFrom === key
          );

          if (!senderBound) {
            emitSnapshot(kind + "_sender_mismatch_ignored");
          } else if (kind === "authority_exit") {
            six7TerminalExitCommand(inner, "six7_sys_authority_exit");
          } else {
            const incomingBootId = String(inner?.bootId || "");
            const knownBootId = String(
              six7RemoteBootIds.get(commandFrom) || "",
            );

            if (
              incomingBootId &&
              knownBootId &&
              incomingBootId !== knownBootId
            ) {
              emitSnapshot("stale_authority_detach_boot_ignored");
            } else {
              const armed = six7ArmSysReplacement(
                commandFrom,
                "explicit_document_detach",
              );
              const accepted =
                armed || six7SysReplacementWindows.has(commandFrom);
              emitSnapshot(
                accepted
                  ? "authority_detach_replacement_window_owned"
                  : "authority_detach_no_legal_owner",
              );
            }
          }
        } else if (kind === "presence" && inner?.from) {
          const fromKey = six7ShortKey(inner.from);
          const selfKey = six7ShortKey(selfHKey);
          const role = String(
            concierge.localRoleCandidate || concierge.role || "peer",
          );

          six7RememberPresencePeer(inner, "six7_sys", {
            peerEvent: false,
            rosterEvent: false,
          });

          const relState = getState(fromKey);

          if (
            fromKey &&
            fromKey !== selfKey &&
            concierge.sysReady &&
            (relState === VEIL.INSIDE || relState === VEIL.ESTABLISHED)
          ) {
            six7SendControl(
              {
                type: "veil_status",
                from: selfHKey,
                to: fromKey,
                videoId,
                state: relState,
                cycleId: six7RelationshipGet(fromKey)?.cycleId || null,
              },
              "veil_status_presence_rx",
            );

            emitSnapshot("relationship_status_presence_rx");
          }

          if (role === "syscon2") {
            const boss = six7ShortKey(concierge.syscon1);

            if (boss && boss !== selfKey && fromKey && fromKey !== boss) {
              six7SendSysControl(
                {
                  ...inner,
                  type: "presence",
                  from: fromKey,
                  to: boss,
                  videoId: inner.videoId || videoId || null,
                  via: "syscon2_admission_forward",
                  forwardedBy: selfKey,
                  forwardedAt: now(),
                },
                "presence",
              );

              emitSnapshot("syscon2_forwarded_presence_to_syscon1");
            }
          }

          if(role === "syscon1")sdkApplySysAdmission(key,inner);
        } else if (kind === "rtc_offer" && inner) {
          concierge.sysPeer = key;

          concierge.lastRtcRxAt = now();
          concierge.lastRtcRxType = "rtc_offer";
          concierge.lastRtcRxVia = "six7_sys";

          if (!isDirectMsgForMe(inner)) {
            // ignore safely, but still ACK wrapper below
          } else {
            if (inner?.pipe !== "sys" && inner?.sysCarrier !== true) {
              const ent = six7ChatPeers.get(six7ShortKey(inner.from));
              six7ChatSignalAudit("rx_offer", {
                peer: inner.from,
                kind: "rtc_offer",
                route: "six7_sys",
                signal: ent?.pc?.signalingState || "missing",
                pc: ent?.pc?.connectionState || "missing",
                dc: ent?.dc?.readyState || "missing",
                cycle: inner?.relationshipCycleId || "none",
                ok: true,
                detail: "wrapper_direct_for_me",
              });
            }

            rtcOnOffer(inner.from, inner.sdp, {
              pipe:
                inner?.pipe === "sys" || inner?.sysCarrier === true
                  ? "sys"
                  : "chat",
              successorProof: inner.successorProof,
              sysCarrier: inner?.sysCarrier === true,
              bootId: String(inner?.bootId || ""),
              sysAttemptId: String(inner?.sysAttemptId || ""),
              chatAttemptId: String(inner?.chatAttemptId || ""),
              sysTemporary: inner?.sysTemporary === true,
              sysPurpose: String(inner?.sysPurpose || ""),
              sysTxId: String(inner?.sysTxId || ""),
              room: String(inner?.videoId || msg?.room || videoId || ""),
            });
          }
        } else if (kind === "rtc_answer" && inner) {
          concierge.sysPeer = key;

          concierge.lastRtcRxAt = now();
          concierge.lastRtcRxType = "rtc_answer";
          concierge.lastRtcRxVia = "six7_sys";

          if (!isDirectMsgForMe(inner)) {
            // ignore safely, but still ACK wrapper below
          } else {
            if (inner?.pipe !== "sys" && inner?.sysCarrier !== true) {
              const ent = six7ChatPeers.get(six7ShortKey(inner.from));
              six7ChatSignalAudit("rx_answer", {
                peer: inner.from,
                kind: "rtc_answer",
                route: "six7_sys",
                signal: ent?.pc?.signalingState || "missing",
                pc: ent?.pc?.connectionState || "missing",
                dc: ent?.dc?.readyState || "missing",
                cycle: inner?.relationshipCycleId || "none",
                ok: true,
                detail: "wrapper_direct_for_me",
              });
            }

            rtcOnAnswer(inner.from, inner.sdp, {
              pipe:
                inner?.pipe === "sys" || inner?.sysCarrier === true
                  ? "sys"
                  : "chat",
              sysCarrier: inner?.sysCarrier === true,
              bootId: String(inner?.bootId || ""),
              sysAttemptId: String(inner?.sysAttemptId || ""),
              chatAttemptId: String(inner?.chatAttemptId || ""),
              sysTemporary: inner?.sysTemporary === true,
              sysPurpose: String(inner?.sysPurpose || ""),
              sysTxId: String(inner?.sysTxId || ""),
              room: String(inner?.videoId || msg?.room || videoId || ""),
            });
          }
        } else if (kind === "rtc_ice" && inner) {
          concierge.sysPeer = key;

          concierge.lastRtcRxAt = now();
          concierge.lastRtcRxType = "rtc_ice";
          concierge.lastRtcRxVia = "six7_sys";

          concierge.lastRtcIceRxAt = now();
          concierge.lastRtcIceRxVia = "six7_sys";

          if (!isDirectMsgForMe(inner)) {
            // ignore safely, but still ACK wrapper below
          } else if (!six7RememberRtcSignal(inner, "six7_sys")) {
            rtcOnIce(inner.from, inner.cand, {
              pipe:
                inner?.pipe === "sys" || inner?.sysCarrier === true
                  ? "sys"
                  : "chat",
              sysCarrier: inner?.sysCarrier === true,
              bootId: String(inner?.bootId || ""),
              sysAttemptId: String(inner?.sysAttemptId || ""),
              chatAttemptId: String(inner?.chatAttemptId || ""),
              sysTemporary: inner?.sysTemporary === true,
              sysPurpose: String(inner?.sysPurpose || ""),
              sysTxId: String(inner?.sysTxId || ""),
              room: String(inner?.videoId || msg?.room || videoId || ""),
            });
          }
        } else {
          concierge.sysPeer = key;
          six7HandleInboundRelationshipControl(inner, "six7_sys");
        }

        six7SendSysDC(key, {
          t: "SYS_CONTROL_ACK",
          at: now(),
          from: six7ShortKey(selfHKey),
          room: videoId || null,
          kind,
          cid: msg?.cid || inner?.cid || null,
          ackFor: msg?.from || null,
          ackAt: msg?.at || null,
        });
      }

      emitSnapshot("sysdc_rx_" + type);
    };
  }

  function six7GetOrCreateChatPeer(remoteHKey) {
    const k = six7ShortKey(remoteHKey);
    if (!k) return null;

    let ent = six7ChatPeers.get(k);
    if (ent?.pc) return ent;

    const pc = new RTCPeerConnection(RTC_CFG);
    ent = {
      pc,
      dc: null,
      rtcAnswerApplying: false,
      rtcInboundChain: Promise.resolve(),
      pendingRemoteIce: [],
      chatState: "missing",
      chatReadyAt: 0,
      chatProof: "none",
      chatFault: "none",
      chatDropPending: false,
      relationshipCycleId: null,
      chatAttemptId: null,
      chatAttemptState: "idle",
      chatAttemptStartedAt: 0,
      chatAttemptRecoveryCount: 0,
    };
    six7ChatPeers.set(k, ent);

    pc.onicecandidate = (ev) => {
      const c = ev?.candidate;

      if (c) {
        concierge.lastRtcIceCandidateAt = now();
        concierge.lastRtcIceCandidateState = "candidate";
        concierge.lastRtcIceCandidateCount =
          Number(concierge.lastRtcIceCandidateCount || 0) + 1;

        rtcSend(k, "ice", c, { pipe: "chat" });
        return;
      }

      concierge.lastRtcIceCandidateAt = now();
      concierge.lastRtcIceCandidateState = "complete";
      concierge.lastRtcIceCompleteAt = now();
    };

    pc.ondatachannel = (ev) => {
      const ch = ev?.channel || null;
      if (!ch || String(ch.label || "") !== "six7") return;

      ent.dc = ch;
      rtcWireDC(k, ent.dc);
    };

    pc.onsignalingstatechange = () => {
      const current = six7ChatPeers.get(k);
      if (!current || current.pc !== pc || rtcClosing.has(k)) {
        emitSnapshot("stale_chat_signaling_state_ignored");
        return;
      }

      six7ChatSignalAudit("signal_state", {
        peer: k,
        kind: "state",
        route: concierge.lastRouteMode || "none",
        signal: pc.signalingState,
        pc: pc.connectionState,
        dc: current.dc?.readyState || "missing",
        cycle: current.relationshipCycleId || "none",
        ok: true,
        detail: current.chatState || "none",
      });

      if (
        pc.signalingState === "stable" &&
        ent.chatState === "waiting_carrier_stable" &&
        six7OwnsChatPipeOffer(k) &&
        (getState(k) === VEIL.INSIDE || getState(k) === VEIL.ESTABLISHED)
      ) {
        emitSnapshot("chat_carrier_stable_resuming_owner_offer");
        void six7RequestChatPipe(k, "chat_carrier_signaling_stable");
      }
    };

    pc.onconnectionstatechange = () => {
      const current = six7ChatPeers.get(k);
      if (!current || current.pc !== pc || rtcClosing.has(k)) {
        emitSnapshot("stale_chat_pc_state_ignored");
        return;
      }

      const st = pc.connectionState;

      if (
        (st === "connected" || st === "completed") &&
        ent.chatState === "waiting_carrier_stable" &&
        pc.signalingState === "stable" &&
        (getState(k) === VEIL.INSIDE || getState(k) === VEIL.ESTABLISHED)
      ) {
        six7RequestChatPipe(k, "chat_carrier_connected_after_accept");
      }

      if (st === "failed" || st === "closed") {
        ent.chatState = "failed";
        ent.chatReadyAt = 0;
        ent.chatProof = "none";
        ent.chatFault = "pc_" + st;
        six7HandleTransportLoss(k, "pc_" + st);
      }
    };

    pc.oniceconnectionstatechange = () => {
      const current = six7ChatPeers.get(k);
      if (!current || current.pc !== pc || rtcClosing.has(k)) {
        emitSnapshot("stale_chat_ice_state_ignored");
        return;
      }

      const st = pc.iceConnectionState;

      if (
        (st === "connected" || st === "completed") &&
        ent.chatState === "waiting_carrier_stable" &&
        pc.signalingState === "stable" &&
        (getState(k) === VEIL.INSIDE || getState(k) === VEIL.ESTABLISHED)
      ) {
        six7RequestChatPipe(k, "chat_carrier_ice_connected_after_accept");
      }

      if (st === "failed" || st === "closed") {
        ent.chatState = "failed";
        ent.chatReadyAt = 0;
        ent.chatProof = "none";
        ent.chatFault = "ice_" + st;
        six7HandleTransportLoss(k, "ice_" + st);
      }
    };

    return ent;
  }

  function six7GetOrCreateSysPeer(remoteHKey) {
    const k = six7ShortKey(remoteHKey);
    if (!k) return null;

    const transaction = six7TemporarySysTransactions.get(k) || null;
    let ent = six7SysPeers.get(k);
    if (ent?.pc) {
      if (transaction && transaction.state !== "closed") {
        ent.temporary = true;
        ent.tempSysPurpose = String(transaction.purpose || "");
        ent.tempSysTxId = String(transaction.id || "");
      }
      return ent;
    }

    const pc = new RTCPeerConnection(RTC_CFG);
    ent = {
      pc,
      sysdc: null,
      sysAttemptId: null,
      ownerAttemptId: six7OwnsPrimarySysState(k)
        ? String(concierge.sysAttempt?.id || "") || null
        : null,
      remoteBootId: String(six7RemoteBootIds.get(k) || "") || null,
      sysCarrier: false,
      temporary: !!transaction,
      tempSysPurpose: String(transaction?.purpose || ""),
      tempSysTxId: String(transaction?.id || ""),
      rtcAnswerApplying: false,
      rtcInboundChain: Promise.resolve(),
      pendingRemoteIce: [],
      attempt: {
        id: six7NewSysTransportAttemptId(k),
        role: "unknown",
        state: "missing",
        startedAt: 0,
        lastEvent: "created",
        faultReason: "none",
      },
    };
    six7SysPeers.set(k, ent);

    pc.onicecandidate = (ev) => {
      const c = ev?.candidate;

      if (c) {
        concierge.lastRtcIceCandidateAt = now();
        concierge.lastRtcIceCandidateState = "candidate";
        concierge.lastRtcIceCandidateCount =
          Number(concierge.lastRtcIceCandidateCount || 0) + 1;

        rtcSend(k, "ice", c, {
          pipe: "sys",
          sysCarrier: ent.sysdc?.readyState !== "open",
        });
        return;
      }

      concierge.lastRtcIceCandidateAt = now();
      concierge.lastRtcIceCandidateState = "complete";
      concierge.lastRtcIceCompleteAt = now();
    };

    pc.ondatachannel = (ev) => {
      const ch = ev?.channel || null;
      if (!ch || String(ch.label || "") !== "six7-sys") return;

      ent.sysdc = ch;
      const attempt = six7SysEdgeAttempt(k);
      if (attempt) {
        attempt.role = "acceptor";
        attempt.state = "connecting";
        attempt.startedAt ||= now();
        attempt.lastEvent = "datachannel_received";
        attempt.faultReason = "none";
        ent.sysAttemptId = attempt.id;
      }
      if (six7OwnsPrimarySysState(k)) {
        ent.ownerAttemptId = String(concierge.sysAttempt?.id || "") || null;
      }
      ent.sysCarrier = true;
      six7WireSysDC(k, ent.sysdc);
    };

    pc.onconnectionstatechange = () => {
      const current = six7SysPeers.get(k);
      if (!current || current.pc !== pc) {
        emitSnapshot("stale_sys_pc_state_ignored");
        return;
      }

      const st = pc.connectionState;
      if (st === "failed" || st === "closed") {
        if (current.temporary === true || six7TemporarySysTransactions.has(k)) {
          six7CloseTemporarySysTransaction(k, "pc_" + st);
          return;
        }
        six7HandleTransportLoss(k, "sys_transport_pc_" + st);
      }
    };

    pc.oniceconnectionstatechange = () => {
      const current = six7SysPeers.get(k);
      if (!current || current.pc !== pc) {
        emitSnapshot("stale_sys_ice_state_ignored");
        return;
      }

      const st = pc.iceConnectionState;
      if (st === "failed" || st === "closed") {
        if (current.temporary === true || six7TemporarySysTransactions.has(k)) {
          six7CloseTemporarySysTransaction(k, "ice_" + st);
          return;
        }
        six7HandleTransportLoss(k, "sys_transport_ice_" + st);
      }
    };

    return ent;
  }

  // Temporary compatibility alias while SYS creation is moved to six7SysPeers.

  function rtcTeardown(hKey, reason = "unspecified") {
    const key = six7ShortKey(hKey);
    if (!key || rtcClosing.has(key)) return;

    // Teardown is an edge lifecycle transition. Expected retirement reasons
    // must not overwrite current concierge/SYS readiness with stale history.
    rtcClosing.add(key);

    const ent = six7ChatPeers.get(key);

    loungeWaiters.delete(key);
    six7CancelChatAttemptDeadline(key);
    six7ChatPeers.delete(key);
    rtcDialing.delete(key);

    try {
      ent?.dc?.close();
    } catch {}
    try {
      ent?.pc?.close();
    } catch {}

    rtcClosing.delete(key);
  }

  function six7TeardownSysPeer(hKey, reason = "sys_teardown") {
    const key = six7ShortKey(hKey);
    if (!key) return;

    const ent = six7SysPeers.get(key);
    const temporary = six7TemporarySysTransactions.get(key);
    try {
      if (temporary?.deadlineTimer) clearTimeout(temporary.deadlineTimer);
    } catch {}
    six7SysPeers.delete(key);
    six7TemporarySysTransactions.delete(key);
    six7SysDialing.delete(key);
    six7SysPreflightUnstableRetries.delete(key);

    const dc = ent?.sysdc || null;
    const pc = ent?.pc || null;

    if (ent) {
      ent.sysdc = null;
      ent.sysAttemptId = null;
      ent.ownerAttemptId = null;
      ent.sysCarrier = false;
    }

    try {
      dc?.close();
    } catch {}
    try {
      pc?.close();
    } catch {}

    emitSnapshot(String(reason || "sys_teardown"));
  }

  function rtcTeardownAll(reason = "session_teardown") {
    for (const key of Array.from(six7ChatPeers.keys())) {
      rtcTeardown(key, reason);
    }

    for (const key of Array.from(six7SysPeers.keys())) {
      six7TeardownSysPeer(key, "sys_" + String(reason || "session_teardown"));
    }

    loungeWaiters.clear();
    rtcDialing.clear();
    six7SysDialing.clear();
    rtcClosing.clear();
  }

  function rtcWireDC(remoteHKey, dc) {
    if (!dc) return;
    try {
      dc.binaryType = "arraybuffer";
    } catch {}
    dc.onopen = () => {
      const key = six7ShortKey(remoteHKey);
      const ent = key ? six7ChatPeers.get(key) : null;

      if (ent) {
        ent.chatState = "open";
        ent.chatAttemptState = "open";
        ent.chatReadyAt = 0;
        ent.chatProof = "dc_open";
        ent.chatFault = "none";
      }

      loungeWaiters.delete(key);

      rtcInFlight = false;
      //sigCloseSoon();

      const staleChatWait =
        /chat_pipe_waiting|chat pipe waiting|waiting_for_offer_owner/i;

      if (staleChatWait.test(String(concierge.lastErr || ""))) {
        concierge.lastErr = null;
      }

      emitSnapshot("rtc_dc_open");
      rtcSendHello(remoteHKey);
      rtcSendDC(remoteHKey, { t: "PING", at: now() });
      CORE_DEBUG && console.log("[six7][RTC] TX PING →", remoteHKey);

      post("P2P_DIRECT_OPEN", {
        from: selfHKey,
        to: String(remoteHKey || ""),
        videoId: videoId || null,
      });
    };

    dc.onclose = () => {
      const key = six7ShortKey(remoteHKey);
      if (!key) return;

      const ent = six7ChatPeers.get(key);

      // Ignore closure from an old/replaced chat channel.
      if (!ent || ent.dc !== dc) {
        emitSnapshot("stale_chat_pipe_close_ignored");
        return;
      }

      ent.dc = null;
      ent.chatState = "missing";
      ent.chatReadyAt = 0;
      ent.chatProof = "none";
      ent.chatFault = "dc_close";

      if (__six7GoneNotified.has(key)) {
        emitSnapshot("chat_pipe_close_ignored_peer_gone");
        return;
      }

      ent.chatDropPending = true;

      concierge.chatDropCount = Number(concierge.chatDropCount || 0) + 1;
      concierge.chatLastDropAt = now();
      concierge.chatLastDropPeer = key;

      six7ScheduleReconnect(key, "chat_dc_closed");
      emitSnapshot("chat_pipe_closed");
    };

    dc.onerror = () => {
      const key = six7ShortKey(remoteHKey);
      const ent = key ? six7ChatPeers.get(key) : null;

      if (ent) {
        ent.chatState = "lost";
        ent.chatReadyAt = 0;
        ent.chatProof = "none";
        ent.chatFault = "dc_error";
      }

      six7ScheduleReconnect(key, "chat_dc_error");
      emitSnapshot("chat_pipe_lost_dc_error");
    };

    dc.onmessage = async (ev) => {
      let raw = ev?.data;

      if (raw instanceof Blob) {
        try {
          raw = await raw.arrayBuffer();
        } catch {
          return;
        }
      }

      if (six7IsArrayBuffer(raw) && !six7CanUseChat(remoteHKey)) return;
      if (six7IsArrayBuffer(raw)) {
        const ent = six7ChatPeers.get(
          String(remoteHKey || "")
            .slice(0, 16)
            .toLowerCase(),
        );
        const meta = ent?.pendingFileBinMeta || null;
        if (ent) ent.pendingFileBinMeta = null;

        if (meta?.id) {
          postLocal(
            "FILE_BIN_CHUNK",
            {
              from: remoteHKey,
              file: meta,
              bytes: raw,
              at: Date.now(),
            },
            [raw],
          );
        }

        return;
      }

      let msg = null;
      try {
        msg = JSON.parse(String(raw || ""));
      } catch {
        msg = { t: "RAW", raw: String(raw || "") };
      }
      CORE_DEBUG && console.log("[six7][RTC] RX", remoteHKey, msg);

      if(msg?.t==='SYS_RECOVERY_RTC'){
        const key=six7ShortKey(remoteHKey),entry=six7ChatPeers.get(key),packet=msg.packet;
        if(!canCarrySysRecovery({packet,room:videoId,self:key,remote:six7ShortKey(selfHKey),
          member:!!six7CommittedMember(key),terminal:six7PendingTerminalExits.has(key),
          accepted:six7RelationshipIsAccepted(six7RelationshipGet(key)),ready:entry?.dc?.readyState==='open'&&entry?.chatState==='ready',current:entry?.dc===dc}))return;
        const meta={...packet,room:videoId,pipe:'sys',recoveryPeerPipe:true};
        if(packet.type==='rtc_offer')rtcOnOffer(key,packet.sdp,meta);
        else if(packet.type==='rtc_answer')rtcOnAnswer(key,packet.sdp,meta);
        else rtcOnIce(key,packet.cand,meta);
        emitSnapshot('sys_recovery_signal_received_existing_peer_pipe');return;
      }
      if (msg?.t === "FILE_BIN_META" && !six7CanUseChat(remoteHKey)) return;
      if (msg?.t === "FILE_BIN_META" && msg.file?.id) {
        const ent = six7ChatPeers.get(
          String(remoteHKey || "")
            .slice(0, 16)
            .toLowerCase(),
        );
        if (ent) ent.pendingFileBinMeta = msg.file;
        return;
      }

      if (msg?.t === 'PHANTOM_ADMIN') {
        if (six7ChatPeers.get(six7ShortKey(remoteHKey))?.dc === dc && six7CanUseChat(remoteHKey) && JSON.stringify(msg).length <= 4096)
          post('PHANTOM_ADMIN', {from:remoteHKey, record:msg.record});
        return;
      }
      // record last msg for debugging
      bootstrap.lastMsg = {
        rtcFrom: remoteHKey,
        t: now(),
        msgType: msg?.t || "RAW",
        from: msg?.from ? String(msg.from).slice(0, 16).toLowerCase() : null,
        hasText: typeof msg?.text === "string" && msg.text.length > 0,
        textLen: typeof msg?.text === "string" ? msg.text.length : 0,
        sys: msg?.sys || null,
      };

      if (msg?.t === "MSG") {
        if (msg.sys === "BLOCKED_BY") {
          const blockedByKey = six7ShortKey(remoteHKey);
          if (blockedByKey) {
            six7BlockedByPeers.add(blockedByKey);
            six7BlockedPeers.delete(blockedByKey);
          }

          six7CancelReconnect(remoteHKey);
          setState(remoteHKey, VEIL.OUTSIDE);
          rtcTeardown(remoteHKey, "blocked_by_peer");

          post("BLOCKED_BY", { from: remoteHKey, at: msg.at || Date.now() });
          return;
        }

        const isSys = six7IsSysMessage(msg.sys);
        if (!isSys && !six7CanUseChat(remoteHKey)) return;

        if (msg.sys === "ROOM_EXITED") {
          emitSnapshot("legacy_chat_room_exit_ignored");
          return;
        }

        if (msg.sys === "CHAT_ENDED") {
          const key = six7ShortKey(remoteHKey);
          const currentEnt = key ? six7ChatPeers.get(key) : null;

          if (!currentEnt || currentEnt.dc !== dc) {
            emitSnapshot("stale_chat_end_ignored");
            return;
          }

          six7NotifyPeerGone(key, "CHAT_ENDED", msg.at || Date.now());
          return;
        }

        if (msg.sys === "FILE_OFFER" && msg.file?.id) {
          const key = six7ShortKey(remoteHKey);
          if (key) {
            let offers = six7PendingIncomingFileOffers.get(key);
            if (!offers) {
              offers = new Map();
              six7PendingIncomingFileOffers.set(key, offers);
            }

            offers.set(String(msg.file.id), {
              id: String(msg.file.id),
              name: String(msg.file.name || ""),
              size: Number(msg.file.size || 0),
              type: String(msg.file.type || "application/octet-stream"),
            });
          }
        }

        if (
          (msg.sys === "FILE_ACCEPT" || msg.sys === "FILE_DECLINE") &&
          msg.file?.id
        ) {
          const key = six7ShortKey(remoteHKey);
          const offers = key ? six7PendingIncomingFileOffers.get(key) : null;

          if (offers) {
            offers.delete(String(msg.file.id));
            if (!offers.size) six7PendingIncomingFileOffers.delete(key);
          }
        }

        post("INCOMING_MSG", {
          from: remoteHKey,
          text: msg.text || "",
          sys: msg.sys ?? null,
          file: msg.file || null,
          at: msg.at || Date.now(),
        });

        return;
      }

      // HELLO handshake
      if (msg?.t === "HELLO") {
        const ph = String(msg.handle || "").trim() || null;
        const phex = String(msg.hex || "")
          .trim()
          .toLowerCase();
        const pf = String(msg.from || "")
          .trim()
          .toLowerCase();

        // minimal validation: hKey must look like 16-hex
        if (/^[0-9a-f]{16}$/.test(pf)) {
          // reinforce keystore (same maps you already use)
          try {
            if (phex && /^[0-9a-f]{64}$/.test(phex)) ksHexToHKey.set(phex, pf);
            if (ph && ph.startsWith("@")) ksNameToHKey.set(ph, pf);
            ksSave();
          } catch {}

          // tell TOP so peers_core can mark seenVia.direct + set handle
          post("P2P_IDENTITY", {
            from: pf,
            handle: ph,
            hex: phex && phex.length === 64 ? phex : null,
            videoId: videoId || null,
            avatar: msg.avatar || undefined,
            at: now(),
          });

          CORE_DEBUG && console.log("[six7][RTC] HELLO RX", { remoteHKey });

          const key = six7ShortKey(remoteHKey);
          const ent = key ? six7ChatPeers.get(key) : null;

          if (ent && ent.chatProof !== "chat_hello") {
            ent.chatState = "open";
            ent.chatReadyAt = 0;
            ent.chatProof = "hello_identity_only";
            ent.chatFault = six7CanUseChat(key)
              ? "none"
              : "relationship_not_inside";
          }

          if (!six7IsEstablishedLike(key)) {
            emitSnapshot("rtc_hello_identity_only_chat_not_allowed");
            return;
          }

          six7EnsureChatPipe(key, "rtc_hello_rx");
          return;
        }
      }
    };
  }

  function six7HasOpenSysPipe(remoteHKey = "") {
    const key = six7ShortKey(remoteHKey || concierge.sysPeer);
    if (!key) return concierge.sysPipeState === "open";

    const ent = six7SysPeers.get(key);
    return !!(ent?.sysdc && ent.sysdc.readyState === "open");
  }

  function six7SysTargetFromCandidates() {
    const self = six7ShortKey(selfHKey);
    if (!self) return "";

    const one = six7ShortKey(concierge.syscon1);
    const two = six7ShortKey(concierge.syscon2);
    const role = String(concierge.localRoleCandidate || "peer");
    const epoch = Number(concierge.sysconEpoch || 0);

    if (role === "syscon1") {
      return two && two !== self ? two : "";
    }

    if (role === "syscon2") {
      return one && one !== self ? one : "";
    }

    if (role === "peer") {
      if (six7PeerSysCarrier.epoch !== epoch) {
        six7PeerSysCarrier.epoch = epoch;
        six7PeerSysCarrier.target = null;
        six7PeerSysCarrier.primaryRecoveryUsed = false;
        six7PeerSysCarrier.authorityRefreshPending = false;
      }

      if (six7PeerSysCarrier.authorityRefreshPending) {
        return "";
      }

      const recoveryTarget = six7ShortKey(
        six7GossipRecovery.targets[six7GossipRecovery.index],
      );

      if (recoveryTarget && recoveryTarget !== self) {
        return recoveryTarget;
      }

      if (six7GossipRecovery.targets.length) {
        return "";
      }

      const latched = six7ShortKey(six7PeerSysCarrier.target);

      if (latched && latched !== self && (latched === one || latched === two)) {
        return latched;
      }

      if (one && one !== self) return one;
      if (two && two !== self) return two;
    }

    return "";
  }

  function six7MarkGoodSysRx(remoteHKey, type = "SYS_RX") {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    const t = String(type || "SYS_RX");
    const isProof = t === "SYS_STATUS" || t === "SYS_STATUS_ACK";
    const epoch = Number(concierge.sysconEpoch || 0);

    six7SysLastGoodRx.set(key, now());
    const proofEnt = six7SysPeers.get(key);
    if (proofEnt) {
      proofEnt.lastProofEpoch = Number(concierge.sysconEpoch || 0);
      proofEnt.lastProofAt = now();
    }

    const shouldOwnSysPeer = six7OwnsPrimarySysState(key);

    if (shouldOwnSysPeer) {
      concierge.sysPeer = key;
      concierge.sysPipeState = isProof ? "ready" : "open";
    }

    concierge.sysLastErr = null;

    if (isProof && epoch && shouldOwnSysPeer) {
      concierge.sysReadyPeer = key;
      concierge.sysReadyAt = now();
      concierge.sysReadyProof = t;
      concierge.sysReadyEpoch = epoch;
    }

    six7SysPreflightUnstableRetries.delete(key);

    six7RefreshSysReadiness("sys_good_rx_" + String(type || "SYS_RX"));
    emitSnapshot("sys_good_rx_" + String(type || "SYS_RX"));

    const rxKind = String(type || "SYS_RX");

    const proofOrHandshake =
      rxKind === "SYS_HELLO" ||
      rxKind === "SYS_PING" ||
      rxKind === "SYS_PONG" ||
      rxKind === "SYS_STATUS" ||
      rxKind === "SYS_STATUS_ACK";

    const selfCommitted = six7CommittedMember(selfHKey);
    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );

    if (
      proofOrHandshake &&
      !concierge.sysReady &&
      (role === "syscon1" || role === "syscon2")
    ) {
      six7ScheduleSysPreflight("sys_good_rx_" + rxKind);
      six7MaybeRelinkSysconPair("sys_good_rx_" + rxKind);
    }

    if (
      proofOrHandshake &&
      (!selfCommitted || (!concierge.sysReady && role === "peer"))
    ) {
      six7ScheduleSysAdmission("sys_good_rx_" + rxKind);
    }

    return true;
  }

  function six7SysAdmissionTargets() {
    const self = six7ShortKey(selfHKey);
    if (!self) return [];

    const target = six7ShortKey(six7SysTargetFromCandidates());

    if (!target || target === self) return [];

    return [target];
  }

  function six7SendSysAdmissionPresence(
    reason = "sys_admission",
    targetOnly = "",
  ) {
    const targets = targetOnly
      ? [six7ShortKey(targetOnly)].filter(Boolean)
      : six7SysAdmissionTargets();

    if (!targets.length) {
      concierge.sysLastErr = "sys_admission_no_target";
      emitSnapshot("sys_admission_no_target");
      return false;
    }

    let sent = false;

    for (const target of targets) {
      if (six7SendSysAdmissionPresenceTo(target, reason)) {
        sent = true;
      }
    }

    return sent;
  }

  function six7SendSysAdmissionPresenceTo(target, reason = "sys_admission") {
    const self = six7ShortKey(selfHKey);
    target = six7ShortKey(target);

    if (!self || !target || target === self) return false;

    const nowMs = now();
    const last = Number(six7SysAdmissionLastSent.get(target) || 0);

    if (nowMs - last < 2500) {
      emitSnapshot("sys_admission_throttled");
      return false;
    }

    if (!six7HasOpenSysPipe(target)) {
      concierge.sysPeer = target;
      concierge.sysPipeState = "missing";
      concierge.sysLastErr = "sys_admission_waiting_for_pipe_owner";

      six7RefreshSysReadiness("sys_admission_waiting_for_pipe_owner");
      emitSnapshot("sys_admission_waiting_for_pipe_owner");

      six7EnsureSysPipe(
        target,
        "peer_sys_admission_" + String(reason || "sys_admission"),
      );
      return false;
    }

    const msg = six7PresencePayload();
    msg.to = target;

    // The same SYS tether owner carries both committed-member refresh and
    // fresh-incarnation admission. Membership intent is explicit only while
    // self is absent from committed authority; role labels never decide it.
    const selfCommitted = six7CommittedMember(self);
    if (!selfCommitted) {
      msg.admit = true;
      msg.admitReason = String(reason || "sys_admission");
    }

    six7SysAdmissionLastSent.set(target, nowMs);

    const ok = six7SendSysControl(msg, "presence");
    const auditKind = selfCommitted ? "presence" : "admit";

    six7MembershipAuditSet(
      ok ? auditKind + "_sent" : auditKind + "_send_failed",
      {
        peer: target,
        reason: String(reason || "sys_admission"),
        pipe: String(six7SysPeers.get(target)?.sysdc?.readyState || "missing"),
        role: concierge.localRoleCandidate || concierge.role || "peer",
      },
    );

    if (ok) {
      concierge.lastErr = null;
      concierge.sysLastErr = null;
      concierge.lastRouteAt = now();
      concierge.lastRouteType = "presence";
      concierge.lastRouteReason = "sys_admission_presence";
      concierge.lastRouteClass = "presence";
      concierge.routeMode = "sys_primary";
      concierge.lastRouteMode = "sys_primary";
      concierge.route = "six7_sys";
      concierge.wouldRoute = "six7_sys";
      concierge.wouldRouteReason = "sys_admission_presence";
      emitSnapshot("sys_admission_presence_sent");
      return true;
    }

    concierge.sysLastErr = "sys_admission_presence_failed";
    emitSnapshot("sys_admission_presence_failed");
    return false;
  }

  function six7ScheduleSysAdmission(reason = "schedule") {
    const targets = six7SysAdmissionTargets();
    if (!targets.length) return false;

    let touched = false;

    for (const target of targets) {
      const key = six7ShortKey(target);
      if (!key) continue;

      if (six7HasOpenSysPipe(key)) {
        six7SendSysAdmissionPresenceTo(
          key,
          "sys_admission_open_" + String(reason || "schedule"),
        );
        touched = true;
        continue;
      }

      concierge.sysPeer = key;
      concierge.sysLastErr = null;

      void six7EnsureSysPipe(
        key,
        "peer_sys_admission_pipe_" + String(reason || "schedule"),
      );

      touched = true;
      emitSnapshot("sys_admission_birth_requested");
    }

    return touched;
  }

  function six7ScheduleSysPreflightUnstableRetry(
    remoteHKey,
    reason = "pc_not_stable",
  ) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;

    const ent = six7SysPeers.get(key);
    const pc = ent?.pc || null;
    if (!pc) return false;

    const cleanupWaiter = () => {
      try {
        pc.removeEventListener("signalingstatechange", resume);
        pc.removeEventListener("connectionstatechange", resume);
        pc.removeEventListener("iceconnectionstatechange", resume);
      } catch {}

      try {
        pc.__six7SysStableWaiter = false;
      } catch {}
    };

    const resume = () => {
      const currentSysPeer = six7ShortKey(concierge.sysPeer);
      const freshTarget = six7ShortKey(six7SysTargetFromCandidates());

      const stillOwner = key && (currentSysPeer === key || freshTarget === key);

      if (!stillOwner) {
        cleanupWaiter();
        six7SysPreflightUnstableRetries.delete(key);

        concierge.sysLastErr = null;
        six7RefreshSysReadiness("sys_preflight_wait_target_changed");
        emitSnapshot("sys_preflight_wait_target_changed");
        return;
      }

      if (six7HasOpenSysPipe(key)) {
        cleanupWaiter();
        six7SysPreflightUnstableRetries.delete(key);

        concierge.sysPeer = key;
        concierge.sysPipeState = "open";
        concierge.sysLastErr = null;

        six7RefreshSysReadiness("sys_preflight_wait_existing_open");

        if (
          String(concierge.localRoleCandidate || "peer") === "peer" ||
          !six7CommittedMember(selfHKey)
        ) {
          six7SendSysAdmissionPresence("sys_preflight_wait_existing_open", key);
        } else {
          six7RefreshOpenSysPeer("sys_preflight_wait_existing_open");
        }

        emitSnapshot("sys_preflight_wait_existing_open");
        return;
      }

      if (
        pc.signalingState === "closed" ||
        pc.connectionState === "failed" ||
        pc.connectionState === "closed" ||
        pc.iceConnectionState === "failed" ||
        pc.iceConnectionState === "closed"
      ) {
        cleanupWaiter();
        six7SysPreflightUnstableRetries.delete(key);

        concierge.sysPeer = key;
        concierge.sysPipeState = "closed";
        concierge.sysLastErr = "sys_preflight_wait_pc_closed:" + key;

        six7RefreshSysReadiness("sys_preflight_wait_pc_closed");
        emitSnapshot("sys_preflight_wait_pc_closed");

        if (
          String(concierge.localRoleCandidate || "peer") === "peer" ||
          !six7CommittedMember(selfHKey)
        ) {
          six7ScheduleSysAdmission("sys_preflight_wait_pc_closed");
        } else {
          six7ScheduleSysPreflight("sys_preflight_wait_pc_closed");
        }

        return;
      }

      if (pc.signalingState !== "stable") return;

      cleanupWaiter();
      six7SysPreflightUnstableRetries.delete(key);
      concierge.sysLastErr = null;

      const recentGoodSysRx = Number(six7SysLastGoodRx.get(key) || 0);

      if (recentGoodSysRx && now() - recentGoodSysRx < 5000) {
        const ent = six7SysPeers.get(key);
        const sysState = String(ent?.sysdc?.readyState || "");

        if (sysState === "open") {
          concierge.sysPeer = key;
          concierge.sysPipeState =
            concierge.sysReadyPeer === key && concierge.sysReadyProof !== "none"
              ? "ready"
              : "open";
          concierge.sysLastErr = null;

          six7RefreshSysReadiness("sys_preflight_wait_recent_good_rx_open");
          emitSnapshot("sys_preflight_wait_recent_good_rx_open");
          return;
        }

        six7SysLastGoodRx.delete(key);

        if (six7ShortKey(concierge.sysReadyPeer) === key) {
          concierge.sysReadyPeer = null;
          concierge.sysReadyAt = 0;
          concierge.sysReadyProof = "none";
          concierge.sysReadyEpoch = 0;
        }

        concierge.sysPeer = key;
        concierge.sysPipeState =
          sysState === "connecting" ? "connecting" : "missing";
        concierge.sysLastErr = "wait_recent_good_rx_without_open_pipe";

        six7RefreshSysReadiness("wait_recent_good_rx_without_open_pipe");
        emitSnapshot("wait_recent_good_rx_without_open_pipe");

        // no return; fall through to six7EnsureSysPipe below
      }

      six7EnsureSysPipe(
        key,
        "sys_preflight_pc_stable:" + String(reason || "sys_preflight"),
      );
    };

    concierge.sysPeer = key;
    concierge.sysPipeState = "missing";
    concierge.sysLastErr =
      "sys_preflight_waiting_for_pc_stable:" +
      String(pc.signalingState || "unknown");

    six7RefreshSysReadiness("sys_preflight_waiting_for_pc_stable");
    emitSnapshot("sys_preflight_waiting_for_pc_stable");

    resume();

    if (
      pc.signalingState === "stable" ||
      pc.signalingState === "closed" ||
      pc.connectionState === "failed" ||
      pc.connectionState === "closed" ||
      pc.iceConnectionState === "failed" ||
      pc.iceConnectionState === "closed" ||
      six7HasOpenSysPipe(key)
    ) {
      return true;
    }

    if (!pc.__six7SysStableWaiter) {
      try {
        pc.__six7SysStableWaiter = true;
        pc.addEventListener("signalingstatechange", resume);
        pc.addEventListener("connectionstatechange", resume);
        pc.addEventListener("iceconnectionstatechange", resume);
      } catch {
        try {
          pc.__six7SysStableWaiter = false;
        } catch {}
        return false;
      }
    }

    const old = six7SysPreflightUnstableRetries.get(key) || {};
    const attempts = Number(old.attempts || 0) + 1;

    six7SysPreflightUnstableRetries.set(key, {
      attempts,
      at: Date.now(),
      reason: String(reason || "pc_not_stable"),
    });

    if (attempts >= SIX7_SYS_PREFLIGHT_UNSTABLE_MAX) {
      cleanupWaiter();
      six7SysPreflightUnstableRetries.delete(key);

      concierge.sysPeer = key;
      concierge.sysPipeState = "closed";
      concierge.sysLastErr =
        "sys_preflight_unstable_dead:" + String(pc.signalingState || "unknown");

      six7RefreshSysReadiness("sys_preflight_unstable_dead");
      emitSnapshot("sys_preflight_unstable_dead");

      return false;
    }

    return true;
  }

  const sdkSysBirthFailures=new Map();
  const sdkSysBirthFlights=new Map();
  function six7EnsureSysPipe(remoteHKey, reason = "ensure") {
    const key=six7ShortKey(remoteHKey);
    if(!key)return Promise.resolve(false);
    return runSysBirth(sdkSysBirthFlights,key,()=>six7PerformSysBirth(key,reason));
  }
  async function six7PerformSysBirth(remoteHKey, reason = "ensure") {
    const budgetKey=six7ShortKey(remoteHKey);
    if(sdkSysBirthFailures.get(budgetKey)>=2 && !six7HasOpenSysPipe(budgetKey)){emitSnapshot("sys_birth_retry_budget_exhausted");return false;}
    const key = six7ShortKey(remoteHKey);
    if (!key) return false;
    if (!selfHKey || !videoId) return false;

    if (six7SysReplacementTerminal(key)) {
      six7SysDialing.delete(key);
      six7SysPreflightUnstableRetries.delete(key);
      emitSnapshot("sys_birth_blocked_terminal_exit_pending");
      return false;
    }

    const ownsPrimarySysState = six7OwnsPrimarySysState(key);

    const recentGoodSysRx = Number(six7SysLastGoodRx.get(key) || 0);

    if (recentGoodSysRx && now() - recentGoodSysRx < 5000) {
      const ent = six7SysPeers.get(key);
      const sysState = String(ent?.sysdc?.readyState || "");

      if (sysState === "open") {
        if (ownsPrimarySysState) {
          concierge.sysPeer = key;
          concierge.sysPipeState =
            concierge.sysReadyPeer === key && concierge.sysReadyProof !== "none"
              ? "ready"
              : "open";
          concierge.sysLastErr = null;

          six7RefreshSysReadiness("sys_preflight_recent_good_rx_open");
        }

        emitSnapshot("sys_preflight_recent_good_rx_open");
        return true;
      }

      six7SysLastGoodRx.delete(key);

      if (six7ShortKey(concierge.sysReadyPeer) === key) {
        concierge.sysReadyPeer = null;
        concierge.sysReadyAt = 0;
        concierge.sysReadyProof = "none";
        concierge.sysReadyEpoch = 0;
      }

      if (ownsPrimarySysState) {
        concierge.sysPeer = key;
        concierge.sysPipeState =
          sysState === "connecting" ? "connecting" : "missing";
        concierge.sysLastErr = "recent_good_rx_without_open_pipe";

        six7RefreshSysReadiness("recent_good_rx_without_open_pipe");
      }

      emitSnapshot("recent_good_rx_without_open_pipe");

      // Do not return. Fall through to the real owner/acceptor logic below.
    }

    const birthRole = six7SysBirthRole(key);
    const ownsSysDial = birthRole === "opener";

    if (birthRole === "illegal") {
      concierge.sysLastErr =
        "sys_pipe_illegal_edge:" +
        String(concierge.localRoleCandidate || concierge.role || "peer") +
        ":" +
        key +
        ":" +
        String(reason || "ensure");
      emitSnapshot("sys_pipe_illegal_edge_rejected");
      return false;
    }

    if (!ownsSysDial) {
      const ent = six7SysPeers.get(key);
      const dc = ent?.sysdc || null;

      if (ownsPrimarySysState) {
        concierge.sysPeer = key;
      }

      if (dc && (dc.readyState === "open" || dc.readyState === "connecting")) {
        if (ownsPrimarySysState) {
          concierge.sysPipeState = dc.readyState;
          concierge.sysLastErr = null;
          six7RefreshSysReadiness(
            "sys_preflight_acceptor_observing_" + dc.readyState,
          );
        }

        emitSnapshot("sys_preflight_acceptor_observing_" + dc.readyState);
        return dc.readyState === "open";
      }

      if (ownsPrimarySysState) {
        concierge.sysPipeState = "missing";
        concierge.sysLastErr = "sys_preflight_acceptor_waiting_for_owner";
        six7RefreshSysReadiness("sys_preflight_acceptor_waiting_for_owner");
      }

      emitSnapshot("sys_preflight_acceptor_waiting_for_owner");
      return false;
    }

    let birthEntry=null;
    const failOwnedSysBirth = (faultReason, detail = "") => {
      const fault = String(faultReason || "sys_birth_failed");
      const current = six7SysPeers.get(key);
      if(birthEntry && current!==birthEntry){emitSnapshot("sys_birth_stale_completion_ignored");return false;}

      // Retire this failed birth before reporting it. Any resulting callbacks
      // are stale and cannot mutate the replacement attempt.
      if (current) {
        const failedDc = current.sysdc || null;
        const failedPc = current.pc || null;

        current.sysdc = null;
        current.sysAttemptId = null;
        current.ownerAttemptId = null;
        current.sysCarrier = false;

        six7SysPeers.delete(key);

        try {
          failedDc?.close();
        } catch {}

        try {
          failedPc?.close();
        } catch {}
      }

      six7SysDialing.delete(key);
      six7SysPreflightUnstableRetries.delete(key);

      if (detail) {
        concierge.sysLastErr = `${fault}:${String(detail)}`;
        emitSnapshot(fault + "_detail");
      }

      six7HandleTransportLoss(key, fault);
      return false;
    };

    if (six7HasOpenSysPipe(key)) {
      six7SysPreflightUnstableRetries.delete(key);

      if (ownsPrimarySysState) {
        concierge.sysPeer = key;
        concierge.sysPipeState = "open";
        concierge.sysLastErr = null;

        six7RefreshSysReadiness("sys_preflight_already_open");
      }

      if (
        String(concierge.localRoleCandidate || "peer") === "peer" ||
        !six7CommittedMember(selfHKey)
      ) {
        six7SendSysAdmissionPresence("sys_preflight_already_open", key);
      } else {
        six7RefreshOpenSysPeer("sys_preflight_already_open");
      }

      emitSnapshot("sys_preflight_already_open");
      return true;
    }

    if (six7SysDialing.has(key)) {
      emitSnapshot("sys_pipe_owner_already_dialing");
      return false;
    }

    six7SysDialing.add(key);

    try {
      if (ownsPrimarySysState) {
        concierge.sysPeer = key;
      }

      const ent = six7GetOrCreateSysPeer(key);
      birthEntry=ent;

      if (ent && ownsPrimarySysState) {
        ent.ownerAttemptId = String(concierge.sysAttempt?.id || "") || null;
      }

      if (!ent?.pc) {
        return failOwnedSysBirth("sys_birth_missing_pc");
      }

      ent.sysCarrier = true;

      const pc = ent.pc;
      const dc = ent.sysdc || null;
      const pcState = String(pc.signalingState || "");

      if (dc && dc.readyState === "open") {
        six7SysPreflightUnstableRetries.delete(key);

        if (ownsPrimarySysState) {
          concierge.sysPeer = key;
          concierge.sysPipeState = "open";
          concierge.sysLastErr = null;

          six7RefreshSysReadiness("sys_preflight_existing_dc_open");
        }

        if (
          String(concierge.localRoleCandidate || "peer") === "peer" ||
          !six7CommittedMember(selfHKey)
        ) {
          six7SendSysAdmissionPresence("sys_preflight_existing_dc_open", key);
        } else {
          six7RefreshOpenSysPeer("sys_preflight_existing_dc_open");
        }

        emitSnapshot("sys_preflight_existing_dc_open");
        return true;
      }

      let createdSysDc = false;

      if (pcState && pcState !== "stable") {
        const sysState = String(ent.sysdc?.readyState || "");

        if (sysState === "connecting") {
          const transportDead =
            pc.connectionState === "failed" ||
            pc.connectionState === "closed" ||
            pc.iceConnectionState === "failed" ||
            pc.iceConnectionState === "closed";

          const legalReadyPipe = !!(
            concierge.sysReady &&
            six7ShortKey(concierge.sysPeer) === key &&
            six7ShortKey(concierge.sysReadyPeer) === key &&
            six7HasOpenSysPipe(key)
          );

          if (ownsSysDial && transportDead && !legalReadyPipe) {
            return failOwnedSysBirth("sys_transport_birth_failed");
          }

          if (ownsSysDial && transportDead && legalReadyPipe) {
            concierge.sysLastErr =
              "transport_dead_ignored_legal_ready_sys_pipe";
            six7RefreshSysReadiness(
              "transport_dead_ignored_legal_ready_sys_pipe",
            );
            emitSnapshot("transport_dead_ignored_legal_ready_sys_pipe");
            return true;
          }

          if (ownsPrimarySysState) {
            concierge.sysPeer = key;
            concierge.sysPipeState = "connecting";
            concierge.sysLastErr =
              "sys_preflight_existing_dc_connecting_waiting";

            six7RefreshSysReadiness(
              "sys_preflight_existing_dc_connecting_waiting",
            );
          }

          emitSnapshot("sys_preflight_existing_dc_connecting_waiting");

          six7ScheduleSysPreflightUnstableRetry(
            key,
            "sys_preflight_existing_dc_connecting",
          );

          return false;
        }

        if (sysState === "open") {
          six7SysPreflightUnstableRetries.delete(key);

          if (ownsPrimarySysState) {
            concierge.sysPeer = key;
            concierge.sysPipeState = "open";
            concierge.sysLastErr = null;

            six7RefreshSysReadiness("sys_preflight_existing_dc_open");
          }

          if (
            String(concierge.localRoleCandidate || "peer") === "peer" ||
            !six7CommittedMember(selfHKey)
          ) {
            six7SendSysAdmissionPresence("sys_preflight_existing_dc_open", key);
          } else {
            six7RefreshOpenSysPeer("sys_preflight_existing_dc_open");
          }

          emitSnapshot("sys_preflight_existing_dc_open");
          return true;
        }

        if (ownsPrimarySysState) {
          concierge.sysPeer = key;
          concierge.sysPipeState = "missing";
          concierge.sysLastErr =
            "sys_preflight_waiting_for_pc_stable:" + pcState;

          six7RefreshSysReadiness("sys_preflight_waiting_for_pc_stable");
        }

        emitSnapshot("sys_preflight_waiting_for_pc_stable");

        six7ScheduleSysPreflightUnstableRetry(key, String(reason || "ensure"));
        return false;
      }

      if (!ent.sysdc || ent.sysdc.readyState === "closed") {
        ent.sdkSysBirthCompleted = false;
        ent.sysdc = pc.createDataChannel("six7-sys", { ordered: true });

        const edgeAttempt = six7SysEdgeAttempt(key);
        if (edgeAttempt) {
          edgeAttempt.role = "opener";
          edgeAttempt.state = "creating";
          edgeAttempt.startedAt ||= now();
          edgeAttempt.lastEvent = "datachannel_created";
          edgeAttempt.faultReason = "none";
          ent.sysAttemptId = edgeAttempt.id;
        }

        six7WireSysDC(key, ent.sysdc);
        createdSysDc = true;
      }

      if (!createdSysDc && ent.sysdc?.readyState === "connecting") {
        const disposition=sysBirthDisposition(pc,ent.sysdc);
        if(disposition==='failed')return failOwnedSysBirth("sys_transport_birth_failed");
        if(disposition==='waiting') {
          if(ownsPrimarySysState){concierge.sysPipeState="connecting";concierge.sysLastErr=null;}
          six7RefreshSysReadiness("sys_birth_waiting_for_channel_open");
          emitSnapshot("sys_birth_waiting_for_channel_open");
          return false;
        }
      }

      const offer = await pc.createOffer();
      if(!isCurrentSysBirth(six7SysPeers,key,ent))return false;
      await pc.setLocalDescription(offer);
      if(!isCurrentSysBirth(six7SysPeers,key,ent))return false;

      const edgeAttempt = six7SysEdgeAttempt(key);
      if (edgeAttempt) {
        edgeAttempt.role = "opener";
        edgeAttempt.state = "offer_sent";
        edgeAttempt.startedAt ||= now();
        edgeAttempt.lastEvent = "local_offer_set";
        edgeAttempt.faultReason = "none";
        ent.sysAttemptId = edgeAttempt.id;
      }

      const sent = await rtcSend(key, "offer", pc.localDescription, {
        pipe: "sys",
        sysCarrier: true,
      });

      if(!isCurrentSysBirth(six7SysPeers,key,ent))return false;
      if (!sent) {
        return failOwnedSysBirth("sys_birth_offer_send_failed");
      }

      if (ownsPrimarySysState) {
        concierge.sysPeer = key;
        concierge.sysPipeState = "connecting";
        concierge.sysLastErr = null;

        six7RefreshSysReadiness("sys_preflight_offer_sent");
      }

      // A bounded response deadline detects silence, never elects authority.
      const sentEntry=ent;
      armSysBirthDeadline(sentEntry,{schedule:setTimeout,cancel:clearTimeout},SIX7_SYS_REPLACEMENT_MS,()=>{
        if(!isCurrentSysBirth(six7SysPeers,key,sentEntry))return;
        if(six7HasOpenSysPipe(key)){sdkSysBirthFailures.delete(key);return;}
        // A committed member can arrive before its historical mirror promotes.
        // Silence exhausts this attempt, not the member's recovery route forever.
        if(six7GossipRecovery.kind === 'committed_mirror' &&
            six7ShortKey(six7GossipRecovery.targets[six7GossipRecovery.index]) === key){
          six7HandleTransportLoss(key,"sys_birth_response_deadline");return;
        }
        const failures=(sdkSysBirthFailures.get(key)||0)+1;
        sdkSysBirthFailures.set(key,failures);
        if(failures<2){six7HandleTransportLoss(key,"sys_birth_response_deadline");return;}
        six7TeardownSysPeer(key,"sys_birth_response_deadline_exhausted");
        if(six7ShortKey(concierge.sysPeer)===key){concierge.sysPipeState="faulted";concierge.sysReady=false;concierge.sysLastErr="sys_birth_response_deadline_exhausted";concierge.sysAttempt.state="faulted";concierge.sysAttempt.faultReason="sys_birth_response_deadline_exhausted";}
        post("ADMISSION_FAILED",{peer:key,reason:"sys_birth_response_deadline_exhausted",attempts:failures});
        emitSnapshot("sys_birth_response_deadline_exhausted");
      });
      six7SysPreflightUnstableRetries.delete(key);
      emitSnapshot("sys_preflight_offer_sent");

      return true;
    } catch (e) {
      return failOwnedSysBirth("sys_birth_exception", String(e?.message || e));
    } finally {
      if(!birthEntry || !six7SysPeers.has(key) || six7SysPeers.get(key)===birthEntry)six7SysDialing.delete(key);
    }
  }

  function six7ScheduleSysPreflight(reason = "schedule") {
    const role = String(concierge.localRoleCandidate || "peer");

    if (role !== "syscon1" && role !== "syscon2") {
      return false;
    }

    const target = six7ShortKey(six7SysTargetFromCandidates());
    if (!target) return false;

    if (six7SysReplacementTerminal(target)) {
      six7SysDialing.delete(target);
      six7SysPreflightUnstableRetries.delete(target);
      emitSnapshot("sys_preflight_blocked_terminal_exit_pending");
      return false;
    }

    const ent = six7SysPeers.get(target);
    const dcState = String(ent?.sysdc?.readyState || "");

    if (dcState === "open" || dcState === "connecting") {
      concierge.sysPeer = target;
      concierge.sysPipeState = dcState;

      six7RefreshSysReadiness(
        "sys_preflight_observe_" + dcState + "_" + String(reason || "schedule"),
      );
      emitSnapshot("sys_preflight_observe_" + dcState);
      return true;
    }

    if (role === "syscon2") {
      /*
       * Syscon2 is the acceptor for the current syscon1→syscon2 pair.
       * It does not become an opener because time passed.
       */
      concierge.sysPeer = target;
      concierge.sysPipeState = "missing";
      concierge.sysLastErr = null;

      six7RefreshSysReadiness(
        "sys_preflight_acceptor_wait_" + String(reason || "schedule"),
      );
      emitSnapshot("sys_preflight_acceptor_wait");
      return false;
    }

    /*
     * Syscon1 is the deterministic pair opener.
     * Birth immediately from the authority/target event that called us.
     */
    return six7EnsureSysPipe(
      target,
      "syscon1_primary_" + String(reason || "schedule"),
    );
  }

  async function rtcDial(remoteHKey, opts = {}) {
    const key = six7ShortKey(remoteHKey);
    if (!key) return;

    const self = six7ShortKey(selfHKey);
    if (!self || key === self) return;

    const requestedPipe = String(opts?.pipe || "chat");
    const dialReason = String(opts?.reason || requestedPipe || "rtc_dial");

    if (requestedPipe !== "chat") {
      return six7EnsureSysPipe(key, dialReason);
    }

    if (!six7OwnsChatPipeOffer(key)) {
      concierge.lastErr = "chat_pipe_not_offer_owner:" + dialReason;
      emitSnapshot("chat_pipe_not_offer_owner");
      return;
    }

    if (rtcDialing.has(key)) return;

    const live = six7ChatPeers.get(key);

    if (live?.dc?.readyState === "closed") {
      six7ChatPeers.delete(key);
    }

    if (live?.dc?.readyState === "open") {
      return;
    }

    if (live?.dc?.readyState === "connecting") {
      if (live.chatAttemptId && !six7ChatReconnectDeadlines.has(key)) {
        six7ArmChatAttemptDeadline(
          key,
          live.chatAttemptId,
          "observe_existing_connecting",
        );
      }
      return;
    }

    if (live?.pc?.signalingState && live.pc.signalingState !== "stable") {
      return;
    }

    try {
      rtcDialing.add(key);
      rtcInFlight = true;

      const ent = six7GetOrCreateChatPeer(key);
      if (!ent?.pc) return;
      const pc = ent.pc;

      if (ent.dc?.readyState === "open") {
        return;
      }

      if (ent.dc?.readyState === "connecting") {
        if (ent.chatAttemptId && !six7ChatReconnectDeadlines.has(key)) {
          six7ArmChatAttemptDeadline(
            key,
            ent.chatAttemptId,
            "observe_existing_connecting",
          );
        }
        return;
      }

      if (ent.dc) {
        try {
          ent.dc.close();
        } catch {}

        ent.dc = null;
        ent.chatState = "missing";
        ent.chatReadyAt = 0;
        ent.chatProof = "none";
        ent.chatFault = "stale_dc_cleared_before_dial";
      }

      ent.chatAttemptId = six7NewChatAttemptId(key);
      ent.chatAttemptState = "creating";
      ent.chatAttemptStartedAt = now();
      ent.chatState = "creating";
      ent.chatFault = "none";
      ent.chatProof = "owner_created_dc";

      ent.dc = pc.createDataChannel("six7", { ordered: true });
      rtcWireDC(key, ent.dc);
      emitSnapshot("chat_pipe_owner_created_dc");

      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);

      const offerSent = await rtcSend(key, "offer", pc.localDescription, {
        pipe: "chat",
      });

      ent.chatAttemptState = offerSent ? "awaiting_answer" : "signal_held";
      six7ArmChatAttemptDeadline(key, ent.chatAttemptId, dialReason);
      emitSnapshot("chat_pipe_offer_sent");
    } catch (e) {
      const ent = six7ChatPeers.get(key);
      if (ent?.chatAttemptId) {
        ent.chatAttemptState = "signal_failed";
        ent.chatFault = "offer_failed";
        six7ArmChatAttemptDeadline(key, ent.chatAttemptId, "offer_failed");
      }
      concierge.lastErr = "chat_pipe_offer_failed:" + String(e?.message || e);
      emitSnapshot("chat_pipe_offer_failed");
    } finally {
      rtcDialing.delete(key);
    }
  }

  function six7RtcPipeFromMeta(meta = {}) {
    return meta?.pipe === "sys" || meta?.sysCarrier === true ? "sys" : "chat";
  }

  function six7RtcEdgeForInbound(fromHKey, meta = {}) {
    const key = six7ShortKey(fromHKey);
    if (!key) return null;

    const pipe = six7RtcPipeFromMeta(meta);
    const ent =
      pipe === "sys"
        ? six7GetOrCreateSysPeer(key)
        : six7GetOrCreateChatPeer(key);

    if (!ent) return null;
    if (!ent.rtcInboundChain) ent.rtcInboundChain = Promise.resolve();
    if (!Array.isArray(ent.pendingRemoteIce)) ent.pendingRemoteIce = [];

    return { key, pipe, ent };
  }

  function six7QueueInboundRtc(fromHKey, meta, kind, apply) {
    const edge = six7RtcEdgeForInbound(fromHKey, meta);
    if (!edge) return Promise.resolve(false);

    const { key, pipe, ent } = edge;
    const label = String(kind || "signal");

    ent.rtcInboundChain = Promise.resolve(ent.rtcInboundChain)
      .catch(() => {})
      .then(async () => {
        if(pipe === "sys") {
          const birth=sdkSysBirthFlights.get(key);
          if(birth)await birth.catch(()=>{});
        }
        const current =
          pipe === "sys" ? six7SysPeers.get(key) : six7ChatPeers.get(key);

        if (!current || current !== ent || !ent.pc) {
          emitSnapshot(pipe + "_rtc_inbound_stale_" + label);
          return false;
        }

        return await apply();
      })
      .catch((e) => {
        concierge.lastErr =
          pipe + "_rtc_inbound_" + label + "_failed:" + String(e?.message || e);
        emitSnapshot(pipe + "_rtc_inbound_" + label + "_failed");
        return false;
      });

    return ent.rtcInboundChain;
  }

  async function six7FlushInboundRtcIce(
    fromHKey,
    pipe,
    ent,
    reason = "remote_description_applied",
  ) {
    const key = six7ShortKey(fromHKey);
    if (!key || !ent?.pc || !ent.pc.remoteDescription) return false;

    const pending = Array.isArray(ent.pendingRemoteIce)
      ? ent.pendingRemoteIce.splice(0)
      : [];

    if (!pending.length) return true;

    for (const cand of pending) {
      try {
        await ent.pc.addIceCandidate(cand);
      } catch (e) {
        concierge.lastErr =
          pipe + "_rtc_ice_apply_failed:" + String(e?.message || e);
        emitSnapshot(pipe + "_rtc_ice_apply_failed");
        return false;
      }
    }

    emitSnapshot(
      pipe +
        "_rtc_ice_flushed_" +
        pending.length +
        "_" +
        String(reason || "remote_description_applied"),
    );
    return true;
  }

  function rtcOnOffer(fromHKey, offer, meta = {}) {
    if (!host.canAdmitPeer(six7ShortKey(fromHKey))) return;
    return six7QueueInboundRtc(fromHKey, meta, "offer", () =>
      six7ApplyRtcOffer(fromHKey, offer, meta),
    );
  }

  function rtcOnAnswer(fromHKey, answer, meta = {}) {
    if (!host.canAdmitPeer(six7ShortKey(fromHKey))) return;
    return six7QueueInboundRtc(fromHKey, meta, "answer", () =>
      six7ApplyRtcAnswer(fromHKey, answer, meta),
    );
  }

  function rtcOnIce(fromHKey, cand, meta = {}) {
    if (!host.canAdmitPeer(six7ShortKey(fromHKey))) return;
    return six7QueueInboundRtc(fromHKey, meta, "ice", () =>
      six7ApplyRtcIce(fromHKey, cand, meta),
    );
  }

  async function six7ApplyRtcOffer(fromHKey, offer, meta = {}) {
    const fromKey = six7ShortKey(fromHKey);
    const selfKey = six7ShortKey(selfHKey);
    if (!fromKey || fromKey === selfKey) return;

    const pipe = six7RtcPipeFromMeta(meta);

    let sysBirthRole = "";

    if (pipe === "sys") {
      if (six7SysReplacementTerminal(fromKey)) {
        concierge.lastErr =
          "sys_offer_rejected_terminal_exit_pending:" + fromKey;
        emitSnapshot("sys_offer_rejected_terminal_exit_pending");
        return;
      }

      if (meta?.sysTemporary === true) {
        if (!six7BindInboundTemporarySysTransaction(fromKey, meta)) {
          concierge.lastErr =
            "sys_offer_rejected_invalid_temporary_transaction:" + fromKey;
          return;
        }
      }

      sysBirthRole = six7SysBirthRole(fromKey);
      const base = six7LocalCommittedAuthority();
      if (sysBirthRole !== "acceptor" && meta.sysTemporary !== true &&
          acceptsSuccessorHandshake({proof: meta.successorProof, base, sender: fromKey,
            self: selfKey, room: videoId, bootId: String(meta.bootId || ""),
            leaderPipeOpen: base ? six7HasOpenSysPipe(base.syscon1) : true})) {
        sysBirthRole = "acceptor";
        emitSnapshot("historical_mirror_recovery_pipe_accepted");
      }

      if (sysBirthRole !== "acceptor") {
        concierge.lastErr =
          "sys_offer_rejected_non_owner:" + sysBirthRole + ":" + fromKey;
        emitSnapshot("sys_offer_rejected_non_owner");
        return;
      }
    }

    if(pipe==='sys') {
      // Validate the offer owner before an incoming attempt can retire a pipe.
      six7ObserveRemoteIncarnation(fromKey,{bootId:String(meta.bootId||''),attemptId:String(meta.sysAttemptId||''),signalType:'rtc_offer',via:'rtc_offer',bindCurrent:meta.recoveryPeerPipe===true});
      const incoming=String(meta.sysAttemptId||''),current=six7SysPeers.get(fromKey);
      if(incoming && current?.sysAttemptId && incoming!==current.sysAttemptId && !six7HasOpenSysPipe(fromKey)) {
        six7TeardownSysPeer(fromKey,"sys_attempt_superseded_by_remote_offer");
        emitSnapshot("sys_attempt_superseded_by_remote_offer");
      }
    }

    if (pipe === "chat") {
      const incomingAttemptId = String(meta?.chatAttemptId || "");
      const current = six7ChatPeers.get(fromKey);
      const currentAttemptId = String(current?.chatAttemptId || "");

      if (
        incomingAttemptId &&
        currentAttemptId &&
        incomingAttemptId !== currentAttemptId
      ) {
        rtcTeardown(fromKey, "chat_attempt_superseded_by_remote_offer");
        emitSnapshot("chat_attempt_superseded_by_remote_offer");
      }
    }

    const ent =
      pipe === "sys"
        ? six7GetOrCreateSysPeer(fromKey)
        : six7GetOrCreateChatPeer(fromKey);
    if (!ent?.pc) return;

    if (pipe === "chat") {
      const incomingAttemptId = String(meta?.chatAttemptId || "");
      if (incomingAttemptId) ent.chatAttemptId = incomingAttemptId;
      ent.chatAttemptState = "offer_received";
      ent.chatAttemptStartedAt ||= now();
    }

    if (pipe === "sys") {
      ent.sysCarrier = true;
      const edgeAttempt = six7SysEdgeAttempt(fromKey);
      if (edgeAttempt) {
        const incomingAttemptId = String(meta?.sysAttemptId || "");
        if (incomingAttemptId) edgeAttempt.id = incomingAttemptId;
        edgeAttempt.role = "acceptor";
        edgeAttempt.state = "offer_received";
        edgeAttempt.startedAt ||= now();
        edgeAttempt.lastEvent = "remote_offer_received";
        edgeAttempt.faultReason = "none";
        ent.sysAttemptId = edgeAttempt.id;
      }
      ent.remoteBootId =
        String(meta?.bootId || six7RemoteBootIds.get(fromKey) || "") || null;
      if (six7OwnsPrimarySysState(fromKey)) {
        ent.ownerAttemptId = String(concierge.sysAttempt?.id || "") || null;
      }
    }

    const pc = ent.pc;

    if (pc.signalingState !== "stable") {
      if (pipe === "sys") {
        concierge.lastErr =
          "sys_offer_rejected_acceptor_not_stable:" + pc.signalingState;
        const edgeAttempt = six7SysEdgeAttempt(fromKey, false);
        if (edgeAttempt) {
          edgeAttempt.state = "faulted";
          edgeAttempt.lastEvent = "acceptor_not_stable";
          edgeAttempt.faultReason = String(pc.signalingState || "unknown");
        }
        emitSnapshot("sys_offer_rejected_acceptor_not_stable");
        return;
      }

      const remoteWins = fromKey < selfKey;

      if (!remoteWins) {
        concierge.lastErr =
          pipe + "_offer_glare_kept_local:" + pc.signalingState;
        emitSnapshot(pipe + "_offer_glare_kept_local");
        return;
      }

      try {
        await pc.setLocalDescription({ type: "rollback" });
        concierge.lastErr = null;
        emitSnapshot(pipe + "_offer_glare_rollback_accept_remote");
      } catch (e) {
        concierge.lastErr =
          pipe + "_offer_glare_rollback_failed:" + String(e?.message || e);
        emitSnapshot(pipe + "_offer_glare_rollback_failed");
        return;
      }
    }

    await pc.setRemoteDescription(offer);
    await six7FlushInboundRtcIce(
      fromKey,
      pipe,
      ent,
      "offer_remote_description",
    );
    if (pc.signalingState !== "have-remote-offer") return;

    const answer = await pc.createAnswer();
    if (pc.signalingState !== "have-remote-offer") return;

    try {
      await pc.setLocalDescription(answer);
    } catch {
      return;
    }

    await rtcSend(fromKey, "answer", pc.localDescription, {
      pipe,
      sysCarrier: pipe === "sys" && meta?.sysCarrier === true,
    });

    if (pipe === "sys") {
      const edgeAttempt = six7SysEdgeAttempt(fromKey, false);
      if (edgeAttempt) {
        edgeAttempt.state = "connecting";
        edgeAttempt.lastEvent = "answer_sent";
        edgeAttempt.faultReason = "none";
      }
    }

    emitSnapshot(pipe + "_rtc_answer_sent");

    if (
      pipe === "chat" &&
      pc.signalingState === "stable" &&
      ent.chatState === "waiting_carrier_stable" &&
      (getState(fromKey) === VEIL.INSIDE ||
        getState(fromKey) === VEIL.ESTABLISHED)
    ) {
      six7RequestChatPipe(fromKey, "chat_carrier_stable_after_answer_sent");
    }
  }

  async function six7ApplyRtcAnswer(fromHKey, answer, meta = {}) {
    const key = six7ShortKey(fromHKey);
    if (!key) return;

    const pipe = six7RtcPipeFromMeta(meta);
    const ent = pipe === "sys" ? six7SysPeers.get(key) : six7ChatPeers.get(key);
    if (!ent?.pc) return;

    const pc = ent.pc;

    if (pipe === "chat") {
      const incomingAttemptId = String(meta?.chatAttemptId || "");
      const currentAttemptId = String(ent?.chatAttemptId || "");
      if (
        incomingAttemptId &&
        currentAttemptId &&
        incomingAttemptId !== currentAttemptId
      ) {
        emitSnapshot("chat_rtc_answer_ignored_stale_attempt");
        return;
      }
    }

    if (pipe === "sys") {
      const incomingAttemptId = String(meta?.sysAttemptId || "");
      const currentAttemptId = String(
        ent?.attempt?.id || ent?.sysAttemptId || "",
      );

      if (
        incomingAttemptId &&
        currentAttemptId &&
        incomingAttemptId !== currentAttemptId
      ) {
        emitSnapshot("sys_rtc_answer_ignored_stale_transport_attempt");
        return;
      }

      if (ent.temporary === true) {
        const transaction = six7TemporarySysTransactions.get(key);
        if (
          meta?.sysTemporary !== true ||
          !transaction ||
          String(meta?.sysPurpose || "") !==
            String(transaction.purpose || "") ||
          String(meta?.sysTxId || "") !== String(transaction.id || "")
        ) {
          emitSnapshot("temporary_sys_answer_ignored_transaction_mismatch");
          return;
        }
      }

      six7ObserveRemoteIncarnation(key, {
        bootId: String(meta?.bootId || ""),
        attemptId: incomingAttemptId || currentAttemptId,
        bindCurrent: true,
        via: "rtc_answer",
      });
    }

    if (ent.rtcAnswerApplying || pc.signalingState !== "have-local-offer") {
      emitSnapshot(pipe + "_rtc_answer_ignored_not_expected");
      return;
    }

    ent.rtcAnswerApplying = true;

    try {
      await pc.setRemoteDescription(answer);
      await six7FlushInboundRtcIce(key, pipe, ent, "answer_remote_description");
      emitSnapshot(pipe + "_rtc_answer_applied");
    } catch (e) {
      concierge.lastErr =
        pipe +
        "_rtc_answer_rejected:" +
        String(pc.signalingState || "unknown") +
        ":" +
        String(e?.message || e);
      emitSnapshot(pipe + "_rtc_answer_rejected");
      return;
    } finally {
      ent.rtcAnswerApplying = false;
    }

    if (pc.signalingState !== "stable") return;

    if (pipe === "chat") {
      ent.chatAttemptState = "connecting";
      if (
        ent.chatState === "waiting_carrier_stable" &&
        (getState(key) === VEIL.INSIDE || getState(key) === VEIL.ESTABLISHED)
      ) {
        six7RequestChatPipe(key, "chat_carrier_stable_after_accept");
      }
      return;
    }

    const sysState = String(ent.sysdc?.readyState || "");
    const edgeAttempt = six7SysEdgeAttempt(key, false);
    if (edgeAttempt) {
      edgeAttempt.state = sysState === "open" ? "open" : "connecting";
      edgeAttempt.lastEvent = "answer_applied";
      edgeAttempt.faultReason = "none";
    }

    if (!six7OwnsPrimarySysState(key)) {
      emitSnapshot("sys_peer_tether_answer_applied");
      return;
    }

    if (!sysState || sysState === "closed") {
      concierge.sysLastErr = "sys_answer_applied_pipe_missing";
      six7RefreshSysReadiness("sys_answer_applied_pipe_missing");
    } else if (sysState === "connecting") {
      concierge.sysPeer = key;
      concierge.sysPipeState = "connecting";
      six7RefreshSysReadiness("sys_answer_applied_connecting");
    } else if (sysState === "open") {
      concierge.sysPeer = key;
      concierge.sysPipeState = "open";
      six7RefreshOpenSysPeer("sys_answer_applied_open");
    }
  }

  async function six7ApplyRtcIce(fromHKey, cand, meta = {}) {
    const key = six7ShortKey(fromHKey);
    if (!key) return;

    const pipe = six7RtcPipeFromMeta(meta);
    const ent = pipe === "sys" ? six7SysPeers.get(key) : six7ChatPeers.get(key);
    if (!ent?.pc) return;

    if (pipe === "chat") {
      const incomingAttemptId = String(meta?.chatAttemptId || "");
      const currentAttemptId = String(ent?.chatAttemptId || "");
      if (
        incomingAttemptId &&
        currentAttemptId &&
        incomingAttemptId !== currentAttemptId
      ) {
        emitSnapshot("chat_rtc_ice_ignored_stale_attempt");
        return false;
      }
    }

    if (pipe === "sys") {
      const incomingAttemptId = String(meta?.sysAttemptId || "");
      const currentAttemptId = String(
        ent?.attempt?.id || ent?.sysAttemptId || "",
      );

      if (
        incomingAttemptId &&
        currentAttemptId &&
        incomingAttemptId !== currentAttemptId
      ) {
        emitSnapshot("sys_rtc_ice_ignored_stale_transport_attempt");
        return false;
      }

      if (ent.temporary === true) {
        const transaction = six7TemporarySysTransactions.get(key);
        if (
          meta?.sysTemporary !== true ||
          !transaction ||
          String(meta?.sysPurpose || "") !==
            String(transaction.purpose || "") ||
          String(meta?.sysTxId || "") !== String(transaction.id || "")
        ) {
          emitSnapshot("temporary_sys_ice_ignored_transaction_mismatch");
          return false;
        }
      }
    }

    if (!cand) return false;

    if (!ent.pc.remoteDescription) {
      ent.pendingRemoteIce.push(cand);

      if (ent.pendingRemoteIce.length > SIX7_RTC_ICE_QUEUE_MAX) {
        ent.pendingRemoteIce.splice(
          0,
          ent.pendingRemoteIce.length - SIX7_RTC_ICE_QUEUE_MAX,
        );
      }

      emitSnapshot(pipe + "_rtc_ice_latched_waiting_remote_description");
      return true;
    }

    try {
      await ent.pc.addIceCandidate(cand);
      emitSnapshot(pipe + "_rtc_ice_applied");
      return true;
    } catch (e) {
      concierge.lastErr =
        pipe + "_rtc_ice_apply_failed:" + String(e?.message || e);
      emitSnapshot(pipe + "_rtc_ice_apply_failed");
      return false;
    }
  }

  function post(type, payload) {
    try { six7CountEvent("post:" + String(type || "unknown")); } catch {}
    host.emit(type, payload);
  }
  function postLocal(type, payload) { host.emit(type, payload); }
  post("CORE_ALIVE", { loaded: true, early: true, ts: now() });

  function snapshot(reason) {
    return {
      t: now(),
      reason: reason || null,
      eventCounts: six7EventCountRows(20),

      chatPairs: Array.from(six7ChatPeers.entries())
        .map(([hKey, ent]) => ({
          hKey: six7ShortKey(hKey),
          handle: String(
            six7PresenceBook.get(six7ShortKey(hKey))?.handle || "",
          ),
          relationship: getState(six7ShortKey(hKey)),
          offerOwner: six7OwnsChatPipeOffer(six7ShortKey(hKey)),

          pcState: String(ent?.pc?.connectionState || "missing"),
          iceState: String(ent?.pc?.iceConnectionState || "missing"),
          signalingState: String(ent?.pc?.signalingState || "missing"),

          dcState: String(ent?.dc?.readyState || "missing"),
          dcLabel: String(ent?.dc?.label || "none"),

          chatState: String(ent?.chatState || "missing"),
          chatProof: String(ent?.chatProof || "none"),
          chatReadyAt: Number(ent?.chatReadyAt || 0),
          chatFault: String(ent?.chatFault || "none"),

          reconnecting: reconnectingPeers.has(six7ShortKey(hKey)),
          pendingDm: Number(six7PendingDm.get(six7ShortKey(hKey))?.length || 0),
        }))
        .filter((row) => row.hKey),

      enabled: !!enabled,
      self: { handle: selfHandle, hex: selfHex, hKey: selfHKey },
      ctx: {
        parentHref: ctx?.parentHref || null,
        iframeHref: ctx?.iframeHref || null,
        pathname: ctx?.pathname || null,
        search: ctx?.search || null,
      },
      videoId: videoId || null,
      origin: origin || null,
      tofu: {
        pinned: !!tofu.pinned,
        origin: tofu.origin || null,
        hKey: tofu.hKey || null,
        firstSeenAt: tofu.firstSeenAt || null,
        mismatch: !!tofu.mismatch,
        reason: tofu.reason || null,
      },
      bootstrap: {
        url: bootstrap.url || null,
        state: bootstrap.state,
        ok: !!bootstrap.ok,
        openAt: bootstrap.openAt,
        closeAt: bootstrap.closeAt,
        lastErr: bootstrap.lastErr || null,
        lastMsg: bootstrap.lastMsg || null,

        lastCloseCode: bootstrap.lastCloseCode,
        lastCloseReason: bootstrap.lastCloseReason || null,
        lastCloseClean: bootstrap.lastCloseClean,
        lastCloseClass: bootstrap.lastCloseClass || null,

        recoveryStartedAt: bootstrap.recoveryStartedAt,
        recoveredAt: bootstrap.recoveredAt,
        recoveryMs: bootstrap.recoveryMs,
        recoveryAttemptId: bootstrap.recoveryAttemptId,

        doorwayAttemptId: Number(bootstrap.doorwayAttemptId || 0),
        doorwayDeadlinePhase: bootstrap.doorwayDeadlinePhase || "none",
        doorwayDeadlineAt: Number(bootstrap.doorwayDeadlineAt || 0),
        doorwayDeadlineObservedAt: Number(
          bootstrap.doorwayDeadlineObservedAt || 0,
        ),
        doorwayDeadlineObservedBy:
          bootstrap.doorwayDeadlineObservedBy || "none",

        doorwayState: bootstrap.doorwayState || "none",
        doorwayReason: bootstrap.doorwayReason || "none",
        doorwayFault: bootstrap.doorwayFault || "none",
        doorwayAt: bootstrap.doorwayAt || 0,
        doorwayConnectAt: bootstrap.doorwayConnectAt || 0,
        doorwayOpenAt: bootstrap.doorwayOpenAt || 0,
        doorwayJoinedAt: bootstrap.doorwayJoinedAt || 0,
        doorwayRosteredAt: bootstrap.doorwayRosteredAt || 0,
        doorwayReadyAt: bootstrap.doorwayReadyAt || 0,
        doorwayClosedAt: bootstrap.doorwayClosedAt || 0,
        doorwayFaultAt: bootstrap.doorwayFaultAt || 0,
      },

      rosterAudit: {
        source: six7RosterAudit.source || "none",
        count: Number(six7RosterAudit.count || 0),
        added: six7RosterAudit.added || "none",
        removed: six7RosterAudit.removed || "none",
        pruneReason: six7RosterAudit.pruneReason || "none",
        at: Number(six7RosterAudit.at || 0),
      },

      temporarySys: Array.from(six7TemporarySysTransactions.values()).map(
        (rec) => ({
          id: String(rec?.id || ""),
          purpose: String(rec?.purpose || "none"),
          peer: six7ShortKey(rec?.peer),
          initiator: six7ShortKey(rec?.initiator),
          state: String(rec?.state || "unknown"),
          inbound: rec?.inbound === true,
          createdAt: Number(rec?.createdAt || 0),
          openedAt: Number(rec?.openedAt || 0),
          ackAt: Number(rec?.ackAt || 0),
          deadlineAt: Number(rec?.deadlineAt || 0),
        }),
      ),

      gossipRecovery: {
        source: six7ShortKey(six7GossipRecovery.source) || null,
        targets: Array.isArray(six7GossipRecovery.targets)
          ? six7GossipRecovery.targets.map(six7ShortKey).filter(Boolean)
          : [],
        index: Number(six7GossipRecovery.index || 0),
        epoch: Number(six7GossipRecovery.epoch || 0),
        rosterSeq: Number(six7GossipRecovery.rosterSeq || 0),
        at: Number(six7GossipRecovery.at || 0),
        state: String(six7GossipRecovery.state || "idle"),
        successor: six7ShortKey(six7GossipRecovery.successor) || null,
        exhaustedAt: Number(six7GossipRecovery.exhaustedAt || 0),
        kind: String(six7GossipRecovery.kind || 'gossip'),
        retryCount: Number(six7GossipRecovery.retryCount || 0),
        nextRetryAt: Number(six7GossipRecovery.nextRetryAt || 0),
      },

      concierge: {
        mode: concierge.mode,
        role: concierge.role,
        route: concierge.route,
        routeReason: concierge.routeReason,
        wouldRoute: concierge.wouldRoute,
        wouldRouteReason: concierge.wouldRouteReason,
        routeMode: concierge.routeMode,
        lastRouteMode: concierge.lastRouteMode,
        lastRouteAt: concierge.lastRouteAt,
        lastRouteClass: concierge.lastRouteClass,
        lastRouteType: concierge.lastRouteType,
        lastRouteReason: concierge.lastRouteReason,

        sysconEpoch: concierge.sysconEpoch,
        roomJoinedAt: concierge.roomJoinedAt,

        syscon1: concierge.syscon1,
        syscon2: concierge.syscon2,
        syscon1Label: concierge.syscon1Label,
        syscon2Label: concierge.syscon2Label,
        localRoleCandidate: concierge.localRoleCandidate,
        candidateReason: concierge.candidateReason,
        lastCandidateAt: concierge.lastCandidateAt,

        cfConciergeState: concierge.cfConciergeState,
        cfConciergeAt: concierge.cfConciergeAt,
        cfConciergeSyscon1: concierge.cfConciergeSyscon1,
        cfConciergeSyscon2: concierge.cfConciergeSyscon2,
        cfConciergeEpoch: concierge.cfConciergeEpoch,

        cfHits: {
          total: Number(bootstrap.cfHits?.total || 0),
          doorway: Number(bootstrap.cfHits?.doorway || 0),
          authority: Number(bootstrap.cfHits?.authority || 0),
          carrier: Number(bootstrap.cfHits?.carrier || 0),
          fallback: Number(bootstrap.cfHits?.fallback || 0),
          dirty: Number(bootstrap.cfHits?.dirty || 0),
          lastDir: bootstrap.cfHits?.lastDir || "none",
          lastType: bootstrap.cfHits?.lastType || "none",
          lastBucket: bootstrap.cfHits?.lastBucket || "none",
          lastReason: bootstrap.cfHits?.lastReason || "none",
          lastAt: Number(bootstrap.cfHits?.lastAt || 0),
        },

        sysEligible: concierge.sysEligible,
        sysReady: concierge.sysReady,
        sysUnavailableReason: concierge.sysUnavailableReason,

        sysAttempt: concierge.sysAttempt
          ? {
              id: concierge.sysAttempt.id || "",
              epoch: Number(concierge.sysAttempt.epoch || 0),
              role: concierge.sysAttempt.role || "peer",
              target: concierge.sysAttempt.target || null,
              state: concierge.sysAttempt.state || "idle",
              startedAt: Number(concierge.sysAttempt.startedAt || 0),
              lastEvent: concierge.sysAttempt.lastEvent || "none",
              faultReason: concierge.sysAttempt.faultReason || "none",
              recoveryCount: Number(concierge.sysAttempt.recoveryCount || 0),
            }
          : null,
        localRoleReason: concierge.localRoleReason,
        lastSysRefreshAt: concierge.lastSysRefreshAt,
        lastSysRefreshReason: concierge.lastSysRefreshReason,

        lastRelationshipTxAt: concierge.lastRelationshipTxAt,
        lastRelationshipTxType: concierge.lastRelationshipTxType,
        lastRelationshipRoute: concierge.lastRelationshipRoute,
        lastRelationshipReason: concierge.lastRelationshipReason,
        lastRelationshipRxAt: concierge.lastRelationshipRxAt,
        lastRelationshipRxType: concierge.lastRelationshipRxType,
        lastRelationshipRxVia: concierge.lastRelationshipRxVia,

        lastRtcTxAt: concierge.lastRtcTxAt,
        lastRtcTxType: concierge.lastRtcTxType,
        lastRtcTxRoute: concierge.lastRtcTxRoute,
        lastRtcTxMode: concierge.lastRtcTxMode,
        lastRtcTxReason: concierge.lastRtcTxReason,
        lastRtcWouldRoute: concierge.lastRtcWouldRoute,
        lastRtcWouldReason: concierge.lastRtcWouldReason,
        lastRtcRxAt: concierge.lastRtcRxAt,
        lastRtcRxType: concierge.lastRtcRxType,
        lastRtcRxVia: concierge.lastRtcRxVia,

        sysPipeState: concierge.sysPipeState,
        sysPeer: concierge.sysPeer,
        sysReadyPeer: concierge.sysReadyPeer,
        sysReadyAt: concierge.sysReadyAt,
        sysReadyProof: concierge.sysReadyProof,
        sysLastTxAt: concierge.sysLastTxAt,
        sysLastTxType: concierge.sysLastTxType,
        sysLastRxAt: concierge.sysLastRxAt,
        sysLastRxType: concierge.sysLastRxType,
        sysLastErr:
          [concierge.sysLastErr, six7MembershipAuditText()]
            .filter(Boolean)
            .join(" | ") || null,
        membershipAudit: {
          at: Number(six7MembershipAudit.at || 0),
          stage: six7MembershipAudit.stage || "idle",
          peer: six7MembershipAudit.peer || null,
          id: six7MembershipAudit.id || "none",
          detail: six7MembershipAudit.detail || "none",
          topology: six7MembershipAuditTopology(),
          trace: six7MembershipAuditTrace.slice(-10).map((row) => ({
            at: Number(row.at || 0),
            stage: String(row.stage || "unknown"),
            peer: row.peer || null,
            id: String(row.id || "none"),
            detail: String(row.detail || "none"),
          })),
          chatSignalTrace: six7ChatSignalTrace
            .slice(-12)
            .map((row) => ({ ...row })),
        },

        lastRtcIceCandidateAt: concierge.lastRtcIceCandidateAt,
        lastRtcIceCandidateState: concierge.lastRtcIceCandidateState,
        lastRtcIceCandidateCount: concierge.lastRtcIceCandidateCount,
        lastRtcIceCompleteAt: concierge.lastRtcIceCompleteAt,

        lastRtcIceTxAt: concierge.lastRtcIceTxAt,
        lastRtcIceTxRoute: concierge.lastRtcIceTxRoute,
        lastRtcIceTxMode: concierge.lastRtcIceTxMode,
        lastRtcIceTxReason: concierge.lastRtcIceTxReason,

        lastRtcIceRxAt: concierge.lastRtcIceRxAt,
        lastRtcIceRxVia: concierge.lastRtcIceRxVia,

        lastTxAt: concierge.lastTxAt,
        lastTxType: concierge.lastTxType,
        lastRxAt: concierge.lastRxAt,
        lastRxType: concierge.lastRxType,
        lastErr: concierge.lastErr,
      },
    };
  }

  const six7EventCounts = new Map();
  const SIX7_EVENT_COUNTS_MAX = 80;

  function six7CountEvent(name) {
    const key = String(name || "unknown").slice(0, 80);
    if (!key) return;

    const old = six7EventCounts.get(key) || { count: 0, at: 0 };

    six7EventCounts.set(key, {
      count: Number(old.count || 0) + 1,
      at: now(),
    });

    if (six7EventCounts.size > SIX7_EVENT_COUNTS_MAX) {
      const rows = Array.from(six7EventCounts.entries()).sort(
        (a, b) => Number(a[1]?.at || 0) - Number(b[1]?.at || 0),
      );

      while (
        six7EventCounts.size > Math.floor(SIX7_EVENT_COUNTS_MAX * 0.75) &&
        rows.length
      ) {
        six7EventCounts.delete(rows.shift()[0]);
      }
    }
  }

  function six7EventCountRows(limit = 20) {
    return Array.from(six7EventCounts.entries())
      .sort((a, b) => Number(b[1]?.count || 0) - Number(a[1]?.count || 0))
      .slice(0, limit)
      .map(([name, rec]) => ({
        name,
        count: Number(rec?.count || 0),
        at: Number(rec?.at || 0),
      }));
  }

  let six7LastSnapshotSig = "";
  let six7LastSnapshotReason = "none";

  function six7SnapshotSignature() {
    const rosterCount = lastRoster instanceof Set ? lastRoster.size : 0;

    return [
      String(enabled ? 1 : 0),
      String(videoId || ""),
      String(selfHKey ? 1 : 0),

      String(bootstrap.state || "none"),
      String(bootstrap.ok ? 1 : 0),

      String(rosterCount),
      String(concierge.role || "peer"),
      String(concierge.localRoleCandidate || "peer"),
      String(concierge.sysconEpoch || 0),
      String(concierge.sysPipeState || "missing"),
      String(concierge.sysPeer || "none"),
      String(concierge.sysReady ? 1 : 0),
      String(concierge.sysUnavailableReason || "none"),
      String(concierge.sysReadyPeer || "none"),
      String(concierge.sysReadyEpoch || 0),

      String(concierge.lastRelationshipTxType || "none"),
      String(concierge.lastRelationshipRxType || "none"),
      String(concierge.lastRtcTxType || "none"),
      String(concierge.lastRtcTxRoute || "none"),
      String(concierge.lastRtcRxType || "none"),
      String(concierge.lastRtcRxVia || "none"),

      String(concierge.lastErr || "none"),
      String(concierge.sysLastErr || "none"),
      String(six7MembershipAudit.stage || "idle"),
      String(six7MembershipAudit.peer || "none"),
      String(six7MembershipAudit.id || "none"),
      String(six7MembershipAudit.detail || "none"),
    ].join("|");
  }

  function emitSnapshot(reason) {
    try {
      const why = String(reason || "snapshot");
      six7CountEvent("snapshot:" + why);

      const sig = six7SnapshotSignature();

      if (sig === six7LastSnapshotSig && why === six7LastSnapshotReason) {
        return;
      }

      if (
        sig === six7LastSnapshotSig &&
        (why.includes("rtc_ice") ||
          why.includes("ice_candidate") ||
          why.includes("sysdc_rx_SYS_CONTROL_ACK") ||
          why.includes("sysdc_rx_SYS_STATUS_ACK"))
      ) {
        six7LastSnapshotReason = why;
        return;
      }

      six7LastSnapshotSig = sig;
      six7LastSnapshotReason = why;

      post("SNAPSHOT", snapshot(why));
    } catch {}
  }

  function normalizeHandle(h) {
    const s = String(h || "").trim();
    if (!s) return null;
    if (!host.allowDisplayNames && !s.startsWith("@")) return null;
    return s;
  }

  async function sha256Hex(str) {
    const enc = new TextEncoder();
    const buf = enc.encode(str);
    const hash = await crypto.subtle.digest("SHA-256", buf);
    const bytes = new Uint8Array(hash);
    let out = "";
    for (let i = 0; i < bytes.length; i++)
      out += bytes[i].toString(16).padStart(2, "0");
    return out;
  }

  function ksSerialize() {
    return {
      v: 1,
      self: ks.self || null,
      hexToHKey: Array.from(ksHexToHKey.entries()),
      nameToHKey: Array.from(ksNameToHKey.entries()),
    };
  }

  function ksLoad() {
    try {
      const raw = localStorage.getItem(KEYSTORE_KEY);
      if (!raw) return;
      const obj = JSON.parse(raw);
      if (!obj || obj.v !== 1) return;

      ks.self = obj.self || null;

      ksHexToHKey = new Map(Array.isArray(obj.hexToHKey) ? obj.hexToHKey : []);
      ksNameToHKey = new Map(
        Array.isArray(obj.nameToHKey) ? obj.nameToHKey : [],
      );

      ks.hexToHKey = Array.from(ksHexToHKey.entries());
      ks.nameToHKey = Array.from(ksNameToHKey.entries());
    } catch {}
  }

  function ksSave() {
    try {
      const snap = ksSerialize();
      localStorage.setItem(KEYSTORE_KEY, JSON.stringify(snap));
      ks.hexToHKey = snap.hexToHKey;
      ks.nameToHKey = snap.nameToHKey;
    } catch {}
  }

  function ksTouchSelf(reason) {
    if (!selfHandle || !selfHex || !selfHKey) return;

    const t = now();
    const firstSeenAt = ks.self?.firstSeenAt ?? t;

    ks.self = {
      handle: selfHandle,
      hex: selfHex,
      hKey: selfHKey,
      firstSeenAt,
      lastSeenAt: t,
      reason: reason || null,
    };

    ksHexToHKey.set(selfHex, selfHKey);
    ksNameToHKey.set(selfHandle, selfHKey);

    ksSave();
  }

  function computeOrigin() { return host.origin; }
  function resolveVideoId() { return ctx?.roomId || null; }

  function loadTOFU() {
    try {
      const o = localStorage.getItem(TOFU_ORIGIN_KEY);
      let h = localStorage.getItem(TOFU_HKEY_KEY);
      const t = localStorage.getItem(TOFU_FIRST_KEY);

      // Legacy migration: old builds stored full 64-hex here.
      // If it matches our current selfHex, migrate to short hKey (16) once.
      if (h && h.length === 64 && selfHex && selfHKey && h === selfHex) {
        try {
          localStorage.setItem(TOFU_HKEY_KEY, selfHKey);
        } catch {}
        h = selfHKey;
      }

      if (o && h) {
        tofu.pinned = true;
        tofu.origin = o;
        tofu.hKey = h;
        tofu.firstSeenAt = t ? Number(t) : null;
      }
    } catch {}
  }

  function applyTOFU() {
    loadTOFU();
    if (!origin || !selfHKey) return;

    if (!tofu.pinned) {
      tofu.pinned = true;
      tofu.origin = origin;
      tofu.hKey = selfHKey;
      tofu.firstSeenAt = now();
      tofu.mismatch = false;
      tofu.reason = null;

      try {
        localStorage.setItem(TOFU_ORIGIN_KEY, tofu.origin);
        localStorage.setItem(TOFU_HKEY_KEY, tofu.hKey);
        localStorage.setItem(TOFU_FIRST_KEY, String(tofu.firstSeenAt));
      } catch {}

      return;
    }

    // do NOT overwrite; only flag mismatch
    if (tofu.origin !== origin) {
      tofu.mismatch = true;
      tofu.reason = "origin_changed";
      return;
    }
    if (tofu.hKey !== selfHKey) {
      tofu.mismatch = true;
      tofu.reason = "self_changed";
      return;
    }

    tofu.mismatch = false;
    tofu.reason = null;
  }

  function wsSetDoorwayState(state, reason = "doorway") {
    const s = String(state || "idle");
    const r = String(reason || s);
    const t = now();

    bootstrap.doorwayState = s;
    bootstrap.doorwayReason = r;
    bootstrap.doorwayAt = t;

    if (s === "connecting") {
      bootstrap.doorwayConnectAt = t;
      bootstrap.doorwayOpenAt = 0;
      bootstrap.doorwayJoinedAt = 0;
      bootstrap.doorwayRosteredAt = 0;
      bootstrap.doorwayReadyAt = 0;
      bootstrap.doorwayClosedAt = 0;
      bootstrap.doorwayFault = "none";
      bootstrap.doorwayFaultAt = 0;

      concierge.roomJoinedAt = 0;
    } else if (s === "open") {
      bootstrap.doorwayOpenAt = t;
    } else if (s === "joined") {
      bootstrap.doorwayJoinedAt = t;
      bootstrap.doorwayFault = "none";
      bootstrap.doorwayFaultAt = 0;

      concierge.roomJoinedAt = t;
    } else if (s === "rostered") {
      bootstrap.doorwayRosteredAt = t;
      bootstrap.doorwayFault = "none";
    } else if (s === "ready") {
      bootstrap.doorwayReadyAt = t;
      bootstrap.doorwayFault = "none";
      bootstrap.doorwayDeadlinePhase = "none";
      bootstrap.doorwayDeadlineAt = 0;
      bootstrap.doorwayAttemptId = 0;
      wsClearConnectWatchdog();
    } else if (s === "closed") {
      bootstrap.doorwayClosedAt = t;
    } else if (s === "faulted") {
      bootstrap.doorwayFault = r;
      bootstrap.doorwayFaultAt = t;
    }

    emitSnapshot("doorway_" + s + "_" + r);
  }

  function wsClearConnectWatchdog() {
    try {
      if (wsConnectWatchdog) {
        clearTimeout(wsConnectWatchdog);
        clearInterval(wsConnectWatchdog);
      }
    } catch {}
    wsConnectWatchdog = null;
  }

  function wsAdvanceDoorwayRecovery(attemptId, failure = {}) {
    const recoveryLegal =
      !six7SessionKilling && enabled && !!selfHKey && !!videoId;

    if (!recoveryLegal) return false;

    const reason = String(failure.reason || "doorway_failure");

    if (!wsDoorwayRecovering) {
      wsDoorwayRecovering = true;
      wsDoorwayBirthFailures = 1;
      wsDoorwayRecoveryExhausted = false;

      bootstrap.recoveryStartedAt = now();
      bootstrap.recoveredAt = null;
      bootstrap.recoveryMs = null;
      bootstrap.recoveryAttemptId = Number(bootstrap.attempts || 0) + 1;

      emitSnapshot("doorway_rebirth_started_" + reason);
      wsOpenBootstrap(bootstrap.url);
      return true;
    }

    wsDoorwayBirthFailures += 1;

    if (wsDoorwayRecoveryExhausted) return false;

    wsDoorwayRecoveryExhausted = true;

    post("DOORWAY_RECOVERY_EXHAUSTED", {
      videoId,
      failures: wsDoorwayBirthFailures,
      attemptId,
      reason,
      closeCode: failure.closeCode ?? null,
      closeReason: failure.closeDetail || null,
      closeClean:
        typeof failure.closeClean === "boolean" ? failure.closeClean : null,
      closeClass: failure.closeClass || reason,
      recoveryStartedAt: bootstrap.recoveryStartedAt,
      recoveryAttemptId: bootstrap.recoveryAttemptId,
      at: now(),
    });

    return true;
  }

  function wsFailOwnedDoorwayAttempt(sock, attemptId, failure = {}) {
    const ownsAttempt = !!(
      attemptId &&
      attemptId === Number(bootstrap.attempts || 0) &&
      (!sock || ws === sock)
    );

    if (!ownsAttempt) return false;

    wsClearConnectWatchdog();

    if (!sock || ws === sock) ws = null;

    bootstrap.doorwayAttemptId = 0;
    bootstrap.doorwayDeadlinePhase = "none";
    bootstrap.doorwayDeadlineAt = 0;

    const reason = String(failure.reason || "doorway_failure");
    const closeCode =
      typeof failure.closeCode === "number" ? failure.closeCode : null;
    const closeDetail = String(failure.closeDetail || "");
    const closeClean =
      typeof failure.closeClean === "boolean" ? failure.closeClean : null;
    const closeClass = String(failure.closeClass || reason);

    bootstrap.state = String(failure.state || "failed");
    bootstrap.ok = false;
    bootstrap.closeAt = now();
    bootstrap.lastCloseCode = closeCode;
    bootstrap.lastCloseReason = closeDetail || null;
    bootstrap.lastCloseClean = closeClean;
    bootstrap.lastCloseClass = closeClass;
    bootstrap.lastErr = String(
      failure.lastErr ||
        `ws_failure:${closeCode ?? "none"}:${closeDetail || reason}:${closeClass}`,
    );

    wsSetDoorwayState("faulted", reason);
    emitSnapshot(String(failure.snapshot || "ws_failure_" + closeClass));

    if (sock && failure.closeSocket !== false) {
      try {
        sock.close(
          typeof failure.localCloseCode === "number"
            ? failure.localCloseCode
            : 4000,
          String(failure.localCloseReason || "six7_doorway_failure"),
        );
      } catch {}
    }

    wsAdvanceDoorwayRecovery(attemptId, {
      reason,
      closeCode,
      closeDetail,
      closeClean,
      closeClass,
    });

    return true;
  }

  function wsObserveDoorwayDeadline(source = "watchdog") {
    const sock = ws;
    const attemptId = Number(bootstrap.doorwayAttemptId || 0);
    const deadlineAt = Number(bootstrap.doorwayDeadlineAt || 0);
    const phase = String(bootstrap.doorwayDeadlinePhase || "none");

    const ownsAttempt = !!(
      sock &&
      attemptId &&
      attemptId === Number(bootstrap.attempts || 0) &&
      ws === sock
    );

    if (!ownsAttempt || !deadlineAt || phase === "none") return false;
    if (now() < deadlineAt) return false;

    const phaseStillPending =
      phase === "connect"
        ? bootstrap.state === "connecting"
        : phase === "ready"
          ? bootstrap.state === "open" &&
            String(bootstrap.doorwayState || "") !== "ready"
          : false;

    if (!phaseStillPending) {
      wsClearConnectWatchdog();
      return false;
    }

    bootstrap.doorwayDeadlineObservedAt = now();
    bootstrap.doorwayDeadlineObservedBy = String(source || "watchdog");

    const connectPhase = phase === "connect";
    const reason = connectPhase
      ? "connect_deadline_exceeded"
      : "ready_deadline_exceeded";
    const closeCode = connectPhase ? 4001 : 4002;
    const closeClass = connectPhase ? "connect_deadline" : "ready_deadline";
    const closeDetail = connectPhase
      ? "six7_connect_deadline"
      : "six7_ready_deadline";

    return wsFailOwnedDoorwayAttempt(sock, attemptId, {
      state: "failed",
      reason,
      closeCode,
      closeDetail,
      closeClean: false,
      closeClass,
      lastErr: reason,
      snapshot: "ws_" + reason,
      localCloseCode: closeCode,
      localCloseReason: closeDetail,
    });
  }

  function wsArmDoorwayDeadline(sock, attemptId, phase) {
    const p = String(phase || "connect");
    const deadlineMs =
      p === "ready" ? SIX7_WS_READY_DEADLINE_MS : SIX7_WS_CONNECT_DEADLINE_MS;

    wsClearConnectWatchdog();

    bootstrap.doorwayAttemptId = Number(attemptId || 0);
    bootstrap.doorwayDeadlinePhase = p;
    bootstrap.doorwayDeadlineAt = now() + deadlineMs;

    wsConnectWatchdog = setInterval(() => {
      if (ws !== sock || attemptId !== Number(bootstrap.attempts || 0)) {
        wsClearConnectWatchdog();
        return;
      }

      wsObserveDoorwayDeadline("watchdog_tick");
    }, SIX7_WS_WATCH_TICK_MS);
  }

  function wsClose(reason) {
    wsClearConnectWatchdog();

    wsDoorwayRecovering = false;
    bootstrap.doorwayAttemptId = 0;
    bootstrap.doorwayDeadlinePhase = "none";
    bootstrap.doorwayDeadlineAt = 0;

    try {
      ws?.close();
    } catch {}

    ws = null;
    bootstrap.state = "closed";
    bootstrap.ok = false;
    bootstrap.closeAt = now();

    wsSetDoorwayState("closed", reason || "ws_closed");

    if (reason) {
      bootstrap.lastErr = String(reason);
    }

    emitSnapshot(reason || "ws_closed");
  }

  function wsOpenBootstrap(url) {
    if (six7SessionKilling) return;

    if (ws && ws.readyState === WebSocket.OPEN) return;

    if (ws && ws.readyState === WebSocket.CONNECTING) {
      bootstrap.lastErr = "ws_connecting_owned_attempt";
      emitSnapshot("ws_connecting_owned_attempt");
      return;
    }

    if (!enabled) return;
    if (!selfHKey) return;
    if (!videoId) return;

    const selfKey = String(selfHKey || "")
      .slice(0, 16)
      .toLowerCase();

    bootstrap.url = `${DEFAULT_BOOTSTRAP_WS}ws?v=${encodeURIComponent(videoId)}&to=${encodeURIComponent(selfKey)}`;

    bootstrap.state = "connecting";
    bootstrap.ok = false;
    bootstrap.connectAt = now();
    bootstrap.openAt = null;
    wsSetDoorwayState(
      "connecting",
      wsDoorwayRecovering ? "doorway_rebirth_connecting" : "ws_connecting",
    );
    bootstrap.attempts = Number(bootstrap.attempts || 0) + 1;
    const attemptId = Number(bootstrap.attempts || 0);

    bootstrap.lastErr = null;
    bootstrap.lastMsg = null;

    // Preserve the close evidence while the owned rebirth is running.
    if (!wsDoorwayRecovering) {
      bootstrap.lastCloseCode = null;
      bootstrap.lastCloseReason = null;
      bootstrap.lastCloseClean = null;
      bootstrap.lastCloseClass = null;

      bootstrap.recoveryStartedAt = null;
      bootstrap.recoveredAt = null;
      bootstrap.recoveryMs = null;
      bootstrap.recoveryAttemptId = null;
    }

    bootstrap.lastSendType = "none";
    bootstrap.lastSendAt = null;
    bootstrap.lastSendOk = null;
    emitSnapshot("ws_connecting");

    let sock = null;

    try {
      sock = new WebSocket(bootstrap.url);
      ws = sock;
    } catch (e) {
      ws = null;
      wsFailOwnedDoorwayAttempt(null, attemptId, {
        state: "failed",
        reason: "ctor_failed",
        closeClass: "ctor_failed",
        lastErr: "ctor_failed: " + String(e?.message || e),
        snapshot: "ws_ctor_failed",
        closeSocket: false,
      });
      return;
    }

    wsArmDoorwayDeadline(sock, attemptId, "connect");

    ws.onopen = () => {
      if (attemptId !== Number(bootstrap.attempts || 0) || ws !== sock) return;
      if (wsObserveDoorwayDeadline("ws_open")) return;

      bootstrap.state = "open";
      bootstrap.ok = false;
      bootstrap.openAt = now();
      bootstrap.lastErr = null;
      wsSetDoorwayState(
        "open",
        wsDoorwayRecovering ? "doorway_rebirth_open" : "ws_open",
      );

      emitSnapshot(wsDoorwayRecovering ? "doorway_rebirth_open" : "ws_open");
      wsArmDoorwayDeadline(sock, attemptId, "ready");

      const connectMsg = {
        type: "concierge_connect",
        videoId,
        from: selfHKey,
        handle: selfHandle,
        bootId: six7BootId,
        t: now(),
      };

      bootstrap.lastSendType = "concierge_connect";
      bootstrap.lastSendAt = now();
      bootstrap.lastSendOk = false;

      const ok = six7SendControl(connectMsg, "concierge_connect");
      bootstrap.lastSendOk = !!ok;

      if (!ok) {
        bootstrap.lastErr = "concierge_connect_send_failed";
        emitSnapshot("concierge_connect_send_failed");
        return;
      }

      post("brainReady", {
        booted: true,
        handle: selfHandle,
        selfHKey,
        videoId,
        tofuOk: !tofu.mismatch,
        ts: now(),
      });
    };

    ws.onmessage = (ev) => {
      if (attemptId !== Number(bootstrap.attempts || 0) || ws !== sock) return;
      if (wsObserveDoorwayDeadline("ws_message")) return;

      let msg = null;

      try {
        msg = JSON.parse(ev.data);
      } catch {
        msg = { raw: String(ev.data || "") };
      }

      bootstrap.lastMsg = msg
        ? {
            type: msg.type || null,
            from: msg.from ? String(msg.from).slice(0, 16).toLowerCase() : null,
            to: msg.to ? String(msg.to).slice(0, 16).toLowerCase() : null,
            videoId: msg.videoId || null,
            t: msg.t || null,
            listCount: Array.isArray(msg.list) ? msg.list.length : null,
          }
        : null;

      if (msg && msg.type) {
        six7MarkControlRx(msg.type);

        let cfBucket = "";

        if (
          msg.type === "concierge_route" ||
          msg.type === "concierge_bootstrap_needed" ||
          msg.type === "roster"
        ) {
          cfBucket = "doorway";
        } else if (msg.type === "concierge_update") {
          cfBucket = "authority";
        } else if (
          (msg.type === "rtc_offer" ||
            msg.type === "rtc_answer" ||
            msg.type === "rtc_ice") &&
          msg.sysCarrier === true
        ) {
          cfBucket = "carrier";
        } else if (
          concierge.sysReady &&
          (msg.type === "rtc_offer" ||
            msg.type === "rtc_answer" ||
            msg.type === "rtc_ice" ||
            msg.type === "veil_invite" ||
            msg.type === "veil_accept" ||
            msg.type === "veil_deny")
        ) {
          cfBucket = "dirty";
        }

        if (cfBucket) {
          bootstrap.cfHits.total++;
          bootstrap.cfHits[cfBucket]++;
          bootstrap.cfHits.lastDir = "rx";
          bootstrap.cfHits.lastType = String(msg.type || "unknown");
          bootstrap.cfHits.lastBucket = cfBucket;
          bootstrap.cfHits.lastReason =
            cfBucket === "carrier"
              ? "sys_carrier_rx"
              : String(msg.type || cfBucket);
          bootstrap.cfHits.lastAt = now();
        }
      }

      if (msg && six7IsRtcSignalType(msg.type)) {
        concierge.lastRtcRxAt = now();
        concierge.lastRtcRxType = String(msg.type || "unknown");
        concierge.lastRtcRxVia = "bootstrap_ws";
      }

      if (msg && msg.type === "status_request") {
        six7SendControl(
          {
            type: "status_response",
            t: now(),
            selfHKey,
            enabled,
          },
          "status_response",
        );

        return;
      }

      if (
        msg &&
        (msg.type === "concierge_route" ||
          msg.type === "concierge_bootstrap_needed" ||
          msg.type === "concierge_update")
      ) {
        bootstrap.ok = true;

        if (bootstrap.doorwayState !== "ready") {
          wsSetDoorwayState("joined", msg.type);
        }

        const acceptedDoorwayRoster = six7ApplyConciergeRoute(msg, msg.type);

        const provedDoorwayRoster = !!(
          Array.isArray(msg.list) &&
          msg.list.some((row) => {
            const key = six7ShortKey(typeof row === "string" ? row : row?.hKey);

            return key && key === six7ShortKey(selfHKey);
          })
        );

        if (
          (acceptedDoorwayRoster || provedDoorwayRoster) &&
          lastRoster &&
          lastRoster.size > 0
        ) {
          wsSetDoorwayState("ready", msg.type);

          if (wsDoorwayRecovering) {
            bootstrap.recoveredAt = now();
            bootstrap.recoveryMs = bootstrap.recoveryStartedAt
              ? Math.max(0, bootstrap.recoveredAt - bootstrap.recoveryStartedAt)
              : null;

            wsDoorwayRecovering = false;
            wsDoorwayBirthFailures = 0;
            wsDoorwayRecoveryExhausted = false;

            emitSnapshot("doorway_rebirth_recovered");

            post("DOORWAY_FAULT_RECOVERED", {
              videoId,
              closeCode: bootstrap.lastCloseCode,
              closeReason: bootstrap.lastCloseReason || null,
              closeClean: bootstrap.lastCloseClean,
              closeClass: bootstrap.lastCloseClass || "unknown_close",
              recoveryMs: bootstrap.recoveryMs,
              recoveredAt: bootstrap.recoveredAt,
            });
          }
        } else if (bootstrap.doorwayState === "ready") {
          bootstrap.doorwayReason = String(msg.type || "concierge_update");
          bootstrap.doorwayAt = now();
          emitSnapshot(
            "doorway_ready_" + String(msg.type || "concierge_update"),
          );
        }

        return;
      }

      if (msg && msg.type === "roster" && Array.isArray(msg.list)) {
        bootstrap.ok = true;
        wsSetDoorwayState("rostered", "roster");
        wsSetDoorwayState("ready", "roster");
        bootstrap.state = "open";

        if (wsDoorwayRecovering) {
          bootstrap.recoveredAt = now();
          bootstrap.recoveryMs = bootstrap.recoveryStartedAt
            ? Math.max(0, bootstrap.recoveredAt - bootstrap.recoveryStartedAt)
            : null;

          wsDoorwayRecovering = false;
          wsDoorwayBirthFailures = 0;
          wsDoorwayRecoveryExhausted = false;

          emitSnapshot("doorway_rebirth_recovered");

          post("DOORWAY_FAULT_RECOVERED", {
            videoId,
            closeCode: bootstrap.lastCloseCode,
            closeReason: bootstrap.lastCloseReason || null,
            closeClean: bootstrap.lastCloseClean,
            closeClass: bootstrap.lastCloseClass || "unknown_close",
            recoveryMs: bootstrap.recoveryMs,
            recoveredAt: bootstrap.recoveredAt,
          });
        }

        const nextRosterKeys = new Set(
          msg.list
            .map((p) => (typeof p === "string" ? p : p?.hKey))
            .filter(Boolean)
            .map((k) => six7ShortKey(k)),
        );

        const sysAuthority = six7LocalCommittedAuthority();
        const sysOwnsRoster = !!(concierge.sysReady || sysAuthority);

        if (!sysOwnsRoster) {
          for (const oldKey of Array.from(lastRoster || [])) {
            const gone = six7ShortKey(oldKey);
            if (!gone || gone === six7ShortKey(selfHKey)) continue;
            if (nextRosterKeys.has(gone)) continue;

            emitSnapshot("bootstrap_roster_peer_temporarily_absent");
          }

          lastRoster = six7MarkRosterAudit(
            "bootstrap_roster",
            Array.from(nextRosterKeys),
            "bootstrap_roster_replace",
          );
        } else {
          emitSnapshot("bootstrap_roster_observed_sys_owns_roster");
        }

        for (const p of msg.list) {
          const row =
            typeof p === "string"
              ? { hKey: p, from: p, videoId: msg.videoId || videoId || null }
              : p;

          six7RememberPresencePeer(row, "bootstrap_roster", {
            peerEvent: false,
            rosterEvent: false,
          });
        }

        dialReady = true;

        if (sysOwnsRoster) {
          six7EmitPresenceRoster("bootstrap_roster_observed_sys_owns_roster");
        } else {
          post("PRESENCE_ROSTER", {
            videoId: msg.videoId || videoId || null,
            list: msg.list,
          });
        }

        six7RefreshSysReadiness("roster_rx");
        six7RefreshSysCandidates(msg.list, "roster_rx");
        six7ReconcileRelationshipStatus("relationship_status_roster_rx");

        if (!concierge.sysReady) {
          if (
            concierge.localRoleCandidate === "syscon1" ||
            concierge.localRoleCandidate === "syscon2"
          ) {
            six7ScheduleSysPreflight("roster_rx");
          }

          if (
            concierge.localRoleCandidate === "peer" ||
            !six7CommittedMember(selfHKey)
          ) {
            six7ScheduleSysAdmission("roster_rx");
          }
        }

        for (const key of Array.from(loungeWaiters)) {
          if (lastRoster.has(key)) {
            loungeWaiters.delete(key);
            maybeEnterLounge(key);
          }
        }

        emitSnapshot("roster_rx");
        return;
      }

      if (msg && msg.type === "rtc_undeliverable") {
        const target = six7ShortKey(msg.target);
        const attemptTarget = six7ShortKey(concierge.sysAttempt?.target);
        const role = String(concierge.localRoleCandidate || "peer");

        if (target && six7SysReplacementTerminal(target)) {
          six7TeardownSysPeer(
            target,
            "rtc_undeliverable_terminal_exit_pending",
          );
          six7SysDialing.delete(target);
          six7SysPreflightUnstableRetries.delete(target);
          six7PendingRtcIce.delete(target);
          emitSnapshot("rtc_undeliverable_held_terminal_exit_pending");
          return;
        }

        if (
          target &&
          msg.signalType === "rtc_offer" &&
          msg.sysCarrier === true &&
          six7TemporarySysTransactions.has(target)
        ) {
          six7CloseTemporarySysTransaction(target, "rtc_undeliverable");
          emitSnapshot("temporary_sys_rtc_undeliverable");
          return;
        }

        const ownsFailedBirth = !!(
          msg.signalType === "rtc_offer" &&
          msg.sysCarrier === true &&
          role === "syscon1" &&
          target &&
          target === six7ShortKey(concierge.syscon2) &&
          target === attemptTarget &&
          Number(concierge.sysAttempt?.recoveryCount || 0) === 1
        );

        if (!ownsFailedBirth) {
          emitSnapshot("rtc_undeliverable_ignored_stale_attempt");
          return;
        }

        const ent = six7SysPeers.get(target);

        if (ent) {
          const failedDc = ent.sysdc || null;
          const failedPc = ent.pc || null;

          ent.sysdc = null;
          ent.sysAttemptId = null;
          ent.ownerAttemptId = null;
          ent.sysCarrier = false;

          six7SysPeers.delete(target);

          try {
            failedDc?.close();
          } catch {}

          try {
            failedPc?.close();
          } catch {}
        }

        six7SysDialing.delete(target);
        six7SysPreflightUnstableRetries.delete(target);

        if (!six7CompleteFailedSysReplacement(target, "rtc_undeliverable")) {
          six7HandleTransportLoss(target, "sys_transport_undeliverable");
        }
        return;
      }

      if (
        msg &&
        (msg.type === "rtc_offer" ||
          msg.type === "rtc_answer" ||
          msg.type === "rtc_ice")
      ) {
        const pipe =
          msg?.pipe === "sys" || msg?.sysCarrier === true ? "sys" : "chat";
        if (pipe === "chat") {
          const from = six7ShortKey(msg.from);
          const relationship = six7RelationshipGet(from);
          const incomingCycle = String(msg.relationshipCycleId || "");
          if (
            !from ||
            six7PendingTerminalExits.has(from) ||
            __six7GoneNotified.has(from) ||
            !six7RelationshipIsAccepted(relationship) ||
            !incomingCycle ||
            incomingCycle !== String(relationship.cycleId || "")
          ) {
            emitSnapshot("chat_rtc_rx_ignored_wrong_relationship_cycle");
            return;
          }
        }
      }

      if (msg && msg.type === "rtc_offer") {
        if (!isDirectMsgForMe(msg)) return;

        const pipe =
          msg?.pipe === "sys" || msg?.sysCarrier === true ? "sys" : "chat";

        rtcOnOffer(msg.from, msg.sdp, {
          pipe,
          successorProof: msg.successorProof,
          sysCarrier: msg.sysCarrier === true,
          bootId: String(msg.bootId || ""),
          sysAttemptId: String(msg.sysAttemptId || ""),
          chatAttemptId: String(msg.chatAttemptId || ""),
          sysTemporary: msg.sysTemporary === true,
          sysPurpose: String(msg.sysPurpose || ""),
          sysTxId: String(msg.sysTxId || ""),
          room: String(msg.videoId || videoId || ""),
        });
        return;
      }

      if (msg && msg.type === "rtc_answer") {
        if (!isDirectMsgForMe(msg)) return;
        rtcOnAnswer(msg.from, msg.sdp, {
          pipe:
            msg?.pipe === "sys" || msg?.sysCarrier === true ? "sys" : "chat",
          sysCarrier: msg?.sysCarrier === true,
          bootId: String(msg.bootId || ""),
          sysAttemptId: String(msg.sysAttemptId || ""),
          chatAttemptId: String(msg.chatAttemptId || ""),
          sysTemporary: msg.sysTemporary === true,
          sysPurpose: String(msg.sysPurpose || ""),
          sysTxId: String(msg.sysTxId || ""),
          room: String(msg.videoId || videoId || ""),
        });
        return;
      }

      if (msg && msg.type === "rtc_ice") {
        if (!isDirectMsgForMe(msg)) return;

        const from = six7ShortKey(msg.from);
        const ent = from ? six7SysPeers.get(from) : null;
        const sysState = String(ent?.sysdc?.readyState || "");
        const sysCarrier = !!(
          msg.sysCarrier === true || ent?.sysCarrier === true
        );

        let iceVia = "bootstrap_ws";

        if (msg.sysCarrier === true && sysState !== "open") {
          iceVia = "bootstrap_ws_sys_carrier_birth";
        } else if (msg.sysCarrier === true && sysState === "open") {
          iceVia = "bootstrap_ws_late_sys_carrier_birth";
        } else if (ent?.sysCarrier === true && sysState !== "open") {
          iceVia = "bootstrap_ws_local_carrier_birth";
        } else if (concierge.sysReady) {
          iceVia = "bootstrap_ws_dirty_after_sys_ready";
        } else {
          iceVia = "bootstrap_ws_pre_sys";
        }

        concierge.lastRtcIceRxAt = now();
        concierge.lastRtcIceRxVia = iceVia;
        concierge.lastRtcRxAt = now();
        concierge.lastRtcRxType = "rtc_ice";
        concierge.lastRtcRxVia = iceVia;

        emitSnapshot("rtc_ice_rx_" + iceVia);

        if (!six7RememberRtcSignal(msg, iceVia)) {
          rtcOnIce(msg.from, msg.cand, {
            pipe:
              msg?.pipe === "sys" || msg?.sysCarrier === true ? "sys" : "chat",
            sysCarrier: msg?.sysCarrier === true,
            bootId: String(msg.bootId || ""),
            sysAttemptId: String(msg.sysAttemptId || ""),
            chatAttemptId: String(msg.chatAttemptId || ""),
            sysTemporary: msg.sysTemporary === true,
            sysPurpose: String(msg.sysPurpose || ""),
            sysTxId: String(msg.sysTxId || ""),
            room: String(msg.videoId || videoId || ""),
          });
        }

        return;
      }

      // Phase 4 unified relationship fallback path.
      // Handles veil_invite / veil_accept / veil_deny / sys_block / sys_unblock / sys_unblock_ack.
      if (six7HandleInboundRelationshipControl(msg, "bootstrap_ws")) {
        return;
      }

      if (msg && msg.type === "presence" && msg.from) {
        six7RememberPresencePeer(msg, "bootstrap_ws");
        return;
      }

      if (msg && (msg.type === "anchor" || msg.type === "bootstrap_anchor")) {
        bootstrap.ok = true;
        emitSnapshot("bootstrap_anchor");
        return;
      }
    };

    ws.onerror = () => {
      if (attemptId !== Number(bootstrap.attempts || 0) || ws !== sock) return;

      wsFailOwnedDoorwayAttempt(sock, attemptId, {
        state: "failed",
        reason: "ws_error",
        closeCode: 4000,
        closeDetail: "six7_ws_error",
        closeClean: false,
        closeClass: "local_ws_error_close",
        lastErr: "ws_error",
        snapshot: "ws_error",
        localCloseCode: 4000,
        localCloseReason: "six7_ws_error",
      });
    };

    ws.onclose = (ev) => {
      if (attemptId !== Number(bootstrap.attempts || 0) || ws !== sock) return;

      const closeCode = typeof ev?.code === "number" ? ev.code : null;
      const closeDetail = String(ev?.reason || "");
      const closeClean = typeof ev?.wasClean === "boolean" ? ev.wasClean : null;

      let closeClass = "unknown_close";

      if (closeCode === 4000 && closeDetail === "doorway_replaced") {
        closeClass = "doorway_replaced";
      } else if (closeCode === 4000 && closeDetail === "six7_ws_error") {
        closeClass = "local_ws_error_close";
      } else if (
        closeCode === 4001 &&
        closeDetail === "six7_connect_deadline"
      ) {
        closeClass = "connect_deadline";
      } else if (closeCode === 4002 && closeDetail === "six7_ready_deadline") {
        closeClass = "ready_deadline";
      } else if (closeCode === 1000) {
        closeClass = closeClean ? "normal_clean_close" : "normal_unclean_close";
      } else if (closeCode === 1006) {
        closeClass = "abnormal_transport_close";
      } else if (closeClean === true) {
        closeClass = "remote_clean_close";
      } else if (closeClean === false) {
        closeClass = "remote_unclean_close";
      }

      const closedAfterReady = !!bootstrap.doorwayReadyAt;
      const priorDoorwayFault = String(bootstrap.doorwayFault || "none");
      const closeReason =
        priorDoorwayFault !== "none"
          ? priorDoorwayFault
          : closedAfterReady
            ? "closed_after_ready"
            : "closed_before_ready";

      wsFailOwnedDoorwayAttempt(sock, attemptId, {
        state: "closed",
        reason: closeReason,
        closeCode,
        closeDetail,
        closeClean,
        closeClass,
        lastErr:
          "ws_close:" +
          String(closeCode ?? "none") +
          ":" +
          String(closeDetail || "no_reason") +
          ":" +
          closeClass,
        snapshot: "ws_onclose_" + closeClass,
        closeSocket: false,
      });
    };
  }

  // Browser wake events are only observers. The stored attempt identity and
  // absolute deadline remain the sole owners of expiry.
  window.addEventListener(
    "online",
    () => wsObserveDoorwayDeadline("window_online"),
    { passive: true },
  );

  window.addEventListener(
    "pageshow",
    () => wsObserveDoorwayDeadline("window_pageshow"),
    { passive: true },
  );

  document.addEventListener(
    "visibilitychange",
    () => {
      if (document.visibilityState === "visible") {
        wsObserveDoorwayDeadline("document_visible");
      }
    },
    { passive: true },
  );

  window.__six7_core_listener_ready = true;
  post("CORE_LISTENER_READY", { ts: now() });

  let __six7ExitSent = false;
  const __six7GoneNotified = new Set();
  // Membership exit has its own idempotence latch. The committed authority
  // consequence is the only owner allowed to project room-exit teardown.
  const __six7MembershipExitApplied = new Map(); // hKey -> { bootId, epoch, at, reason }

  function six7NotifyPeerGone(hKey, sys = "ROOM_EXITED", at = Date.now()) {
    const key = String(hKey || "")
      .slice(0, 16)
      .toLowerCase();

    if (!key) return false;

    const membershipExit = String(sys || "ROOM_EXITED") !== "CHAT_ENDED";

    if (membershipExit) {
      if (__six7MembershipExitApplied.has(key)) return false;
      const terminal = six7PendingTerminalExits.get(key) || null;
      __six7MembershipExitApplied.set(key, {
        bootId:
          String(terminal?.bootId || six7RemoteBootIds.get(key) || "") || null,
        epoch: Number(
          terminal?.epoch ||
            six7SysAuthority.epoch ||
            concierge.sysconEpoch ||
            0,
        ),
        at: Number(at || Date.now()),
        reason: String(sys || "ROOM_EXITED"),
      });
      __six7GoneNotified.add(key);
    } else {
      if (__six7GoneNotified.has(key)) return false;
      __six7GoneNotified.add(key);
    }

    // Relationship/chat teardown may already have run. Committed membership
    // removal still owns one final, idempotent consequence projection.
    six7RelationshipEnd(key, String(sys || "peer_gone"), at, true);
    six7CancelReconnect(key);
    const reconnectDeadline = six7ChatReconnectDeadlines.get(key);
    if (reconnectDeadline) clearTimeout(reconnectDeadline);
    six7ChatReconnectDeadlines.delete(key);

    const ent = six7ChatPeers.get(key);

    try {
      ent?.dc?.close();
    } catch {}

    if (ent) {
      ent.dc = null;
      ent.chatState = "missing";
      ent.chatReadyAt = 0;
      ent.chatProof = "none";
      ent.chatFault = String(sys || "peer_gone");
    }

    loungeWaiters.delete(key);
    six7PendingRelationship.delete(key);
    six7PendingSysRelay.delete(key);
    six7PendingDm.delete(key);
    six7PendingIncomingFileOffers.delete(key);
    reconnectingPeers.delete(key);

    six7RelationshipRevoked.add(key);
    setState(key, VEIL.OUTSIDE, true);

    const uiSys = sys === "CHAT_ENDED" ? "CHAT_ENDED" : "ROOM_EXITED";

    // Committed membership removal projects exactly one terminal system line.
    // The shell owns presentation; core follows with the existing edge teardown.
    post("INCOMING_MSG", {
      from: key,
      text: "",
      sys: uiSys,
      at,
    });

    // ROOM_EXITED is the single shell-facing membership consequence.
    // Existing teardown follows it; no second presentation owner may erase it.
    emitSnapshot("peer_room_exit_projected");

    // Chat teardown is always legal. SYS/membership projection belongs only
    // to committed room removal, never to a local CHAT_ENDED relationship close.
    try {
      rtcTeardown(key, membershipExit ? "room_exited" : "chat_ended");
      if (membershipExit) {
        six7TeardownSysPeer(key, "sys_room_exited");
      }
    } catch {}

    if (membershipExit) {
      try {
        six7PresenceBook.delete(key);
        six7PendingRelationship.delete(key);
        six7PendingRtcIce.delete(key);
        six7SysAdmissionLastSent.delete(key);
        six7SysPreflightUnstableRetries.delete(key);
        six7SysDialing.delete(key);
      } catch {}

      if (
        six7ShortKey(concierge.sysPeer) === key ||
        six7ShortKey(concierge.sysReadyPeer) === key ||
        six7ShortKey(concierge.sysAttempt?.target) === key
      ) {
        concierge.sysPeer = null;
        concierge.sysPipeState = "missing";
        concierge.sysReadyPeer = null;
        concierge.sysReadyAt = 0;
        concierge.sysReadyProof = "none";
        concierge.sysReadyEpoch = 0;
        concierge.sysLastErr = null;

        if (concierge.sysAttempt) {
          concierge.sysAttempt.target = null;
          concierge.sysAttempt.state = "idle";
          concierge.sysAttempt.lastEvent = "peer_gone_dead_session";
          concierge.sysAttempt.faultReason = "none";
        }
      }

      if (six7ShortKey(concierge.syscon1) === key) concierge.syscon1 = null;
      if (six7ShortKey(concierge.syscon2) === key) concierge.syscon2 = null;

      six7RefreshSysCandidates(
        six7LatchedSysRosterList(),
        "peer_gone_committed_projection",
      );
      six7RefreshSysReadiness("peer_gone_dead_session");
      six7EmitPresenceRoster("peer_gone_dead_session");
    }

    return true;
  }

  function six7CancelPendingVeilInvites(reason = "leaving") {
    try {
      for (const [hKey, relationship] of Array.from(
        six7Relationships.entries(),
      )) {
        const st = String(relationship?.state || VEIL.OUTSIDE);
        if (st !== VEIL.INVITE_SENT && st !== VEIL.INVITE_RECEIVED) continue;

        six7SendControl(
          {
            type: "veil_deny",
            from: selfHKey,
            to: hKey,
            videoId,
            reason,
          },
          "veil_deny",
        );

        setState(hKey, VEIL.OUTSIDE);
        try {
          rtcTeardown(hKey, "pending_invite_cancelled");
        } catch {}
      }
    } catch {}
  }

  function six7DeclinePendingIncomingFileOffers(
    reason = "session_kill",
    at = Date.now(),
  ) {
    try {
      for (const [to, offers] of Array.from(
        six7PendingIncomingFileOffers.entries(),
      )) {
        const key = six7ShortKey(to);
        if (!key || !offers || !offers.size) continue;

        for (const file of Array.from(offers.values())) {
          try {
            rtcSendDC(key, {
              t: "MSG",
              text: "",
              sys: "FILE_DECLINE",
              file: {
                id: String(file.id || ""),
                name: String(file.name || ""),
                size: Number(file.size || 0),
                type: String(file.type || "application/octet-stream"),
              },
              reason,
              at,
            });
          } catch {}
        }

        six7PendingIncomingFileOffers.delete(key);
      }
    } catch {}
  }

  function six7SurrenderRoomSession(txId = null) {
    if (__six7ExitSent) {
      post("SIX7_SESSION_SUSPENDED", {
        txId,
        room: videoId || null,
        alreadySuspended: true,
        at: Date.now(),
      });
      return;
    }

    __six7ExitSent = true;
    six7SessionKilling = true;
    enabled = false;

    const retiredRoom = videoId || null;
    const at = Date.now();

    six7DeclinePendingIncomingFileOffers("session_suspend", at);
    six7SendTerminalExitCommand("session_suspend");

    six7Relationships.clear();
    six7RelationshipRevoked.clear();
    six7PendingRelationship.clear();
    six7PendingSysRelay.clear();
    six7PendingDm.clear();
    six7SeenControlCids.clear();

    rtcTeardownAll("session_suspend");
    wsClose("session_suspend");

    six7RetireRoomAuthority("session_suspend");

    bootOnce = false;

    post("SIX7_SESSION_SUSPENDED", {
      txId,
      room: retiredRoom,
      at,
    });
  }

  function six7SendRoomExitedAndTeardown(finalDeparture = true) {
    if (__six7ExitSent) return;
    __six7ExitSent = true;

    six7SessionKilling = true;
    enabled = false;

    const at = Date.now();

    const exitReason = finalDeparture ? "session_kill" : "document_detach";

    six7CancelPendingVeilInvites(exitReason);
    six7DeclinePendingIncomingFileOffers(exitReason, at);
    if (finalDeparture) {
      six7SendTerminalExitCommand(exitReason);
    } else {
      six7SendDocumentDetachCommand(exitReason);
    }

    six7Relationships.clear();
    six7RelationshipRevoked.clear();
    six7PendingRelationship.clear();
    six7PendingSysRelay.clear();
    six7PendingDm.clear();
    six7PendingRtcIce.clear();
    six7SeenControlCids.clear();
    six7SeenRtcSignals.clear();

    rtcTeardownAll(exitReason);
    wsClose(exitReason);

    six7RetireRoomAuthority(exitReason);

    bootOnce = false;

    wsDoorwayRecovering = false;
    wsDoorwayBirthFailures = 0;
    wsDoorwayRecoveryExhausted = false;

    emitSnapshot(finalDeparture ? "session_killed" : "session_detached");
  }

  function six7RetireRoomAuthority(reason = "room_retired") {
    lastRoster = new Set();
    six7PresenceBook.clear();
    six7RemoteBootIds.clear();
    six7RemoteEndpointIds.clear();
    six7DoorwayAdmittedPeers.clear();
    six7PendingTerminalExits.clear();
    for (const rec of six7TemporarySysTransactions.values()) {
      try {
        if (rec?.deadlineTimer) clearTimeout(rec.deadlineTimer);
      } catch {}
    }
    six7TemporarySysTransactions.clear();
    six7AuthorityGossipHints.clear();
    six7GossipRecovery = {
      source: null,
      targets: [],
      index: 0,
      epoch: 0,
      rosterSeq: 0,
      at: 0,
    };
    for (const rec of six7SysReplacementWindows.values()) {
      try {
        clearTimeout(rec?.timer);
      } catch {}
    }
    six7SysReplacementWindows.clear();
    reconnectingPeers.clear();
    __six7GoneNotified.clear();
    __six7MembershipExitApplied.clear();

    concierge = JSON.parse(JSON.stringify(SIX7_EMPTY_CONCIERGE));

    Object.assign(six7SysAuthority, {
      at: 0,
      via: "none",
      from: null,
      syscon1: null,
      syscon2: null,
      epoch: 0,
      rosterSeq: 0,
      roster: [],
      pending: null,
    });

    Object.assign(six7PeerSysCarrier, {
      epoch: 0,
      target: null,
      primaryRecoveryUsed: false,
      authorityRefreshPending: false,
    });

    six7RosterAudit = {
      source: String(reason || "room_retired"),
      count: 0,
      added: "none",
      removed: "none",
      pruneReason: "room_retired",
      at: now(),
    };

    videoId = null;
  }

  function six7LifecycleCommandMatchesCurrentRoom(payload = {}) {
    const commandRoom = String(payload?.room || payload?.videoId || "").trim();

    const currentRoom = String(videoId || "").trim();

    // Preserve compatibility for non-room-tagged commands.
    if (!commandRoom || !currentRoom) return true;

    return commandRoom === currentRoom;
  }

  // ---- Inbound CORE_* ----
  window.addEventListener("message", (ev) => {
    const d = ev?.data;
    if (!d || d.six7 !== 1) return;

    wsObserveDoorwayDeadline("core_message");

    // Debug ping
    if (d.type === "PING") {
      post("PONG", { t: now() });
      return;
    }

    if (d.type === "SIX7_SESSION_SUSPEND") {
      if (!six7LifecycleCommandMatchesCurrentRoom(d.payload)) {
        emitSnapshot("stale_room_session_suspend_ignored");
        return;
      }

      six7SurrenderRoomSession(d.payload?.txId || null);
      return;
    }

    if (d.type === "SIX7_SESSION_RESET") {
      if (!six7LifecycleCommandMatchesCurrentRoom(d.payload)) {
        emitSnapshot("stale_room_session_reset_ignored");
        return;
      }

      enabled = false;

      wsClose("session_reset");
      rtcTeardownAll("session_reset");

      six7Relationships.clear();
      six7RelationshipRevoked.clear();
      six7PendingRelationship.clear();
      six7PendingSysRelay.clear();
      six7PendingDm.clear();

      six7RetireRoomAuthority("session_reset");

      bootOnce = false;
      __six7ExitSent = false;
      six7SessionKilling = false;

      wsDoorwayRecovering = false;
      wsDoorwayBirthFailures = 0;
      wsDoorwayRecoveryExhausted = false;

      emitSnapshot("session_reset");
      return;
    }

    if (d.type === "SIX7_SESSION_DETACH") {
      if (!six7LifecycleCommandMatchesCurrentRoom(d.payload)) {
        emitSnapshot("stale_room_session_detach_ignored");
        return;
      }

      six7SendRoomExitedAndTeardown(false);
      return;
    }

    if (d.type === "SIX7_SESSION_KILL") {
      if (!six7LifecycleCommandMatchesCurrentRoom(d.payload)) {
        emitSnapshot("stale_room_session_kill_ignored");
        return;
      }

      six7SendRoomExitedAndTeardown(true);
      return;
    }

    if (d.type === "VEIL_SHELL_READY") {
      const requestedRoom = String(d.payload?.room || "").trim();

      if (
        requestedRoom &&
        videoId &&
        requestedRoom !== String(videoId || "").trim()
      ) {
        return;
      }

      for (const [hKey, ent] of six7ChatPeers.entries()) {
        const key = six7ShortKey(hKey);
        if (!key || !ent) continue;

        const chatReady =
          ent.dc?.readyState === "open" &&
          ent.chatState === "ready" &&
          ent.chatProof === "chat_hello" &&
          ent.chatReadyAt > 0 &&
          six7CanUseChat(key);

        if (!chatReady) continue;

        post("P2P_CHAT_READY", { from: key });
      }

      emitSnapshot("shell_ready_chat_truth_replayed");
      return;
    }

    if (d.type === "VEIL_INVITE") {
      const to = String(d.payload?.hKey || "").slice(0, 16);
      if (!to) return;
      if (
        !six7CommittedMember(to) ||
        six7PendingTerminalExits.has(to) ||
        __six7MembershipExitApplied.has(to) ||
        six7IsBlocked(to) ||
        six7IsBlockedBy(to)
      ) {
        emitSnapshot("local_invite_blocked_nonmember_or_terminal");
        return;
      }

      const state = getState(to);
      if (state !== VEIL.OUTSIDE && state !== VEIL.ESTABLISHED) return;

      if (state === VEIL.ESTABLISHED) {
        return;
      }

      six7RelationshipRevoked.delete(to);
      __six7GoneNotified.delete(to);

      const invite = six7PrepareRelationshipControl(
        {
          type: "veil_invite",
          from: selfHKey,
          to,
          videoId,
        },
        "veil_invite",
      );

      const relationship = six7RelationshipBegin(to, VEIL.INVITE_SENT, {
        cycleId: invite.cid,
        inviteCid: invite.cid,
        origin: "local_invite",
      });
      invite.cycleId = relationship.cycleId;
      setState(to, VEIL.INVITE_SENT, true);
      six7SendControl(invite, "veil_invite");

      return;
    }

    if (d.type === "VEIL_ACCEPT") {
      const from = String(d.payload?.from || "").slice(0, 16);
      if (!from) return;
      if (six7IsBlocked(from) || six7IsBlockedBy(from)) return;

      const relationship = six7RelationshipGet(from);
      if (
        relationship.state !== VEIL.INVITE_RECEIVED ||
        relationship.endedAt ||
        !relationship.cycleId
      )
        return;

      six7RelationshipRevoked.delete(from);
      relationship.state = VEIL.INSIDE;
      relationship.updatedAt = now();
      relationship.endedAt = 0;
      relationship.endReason = "none";
      six7Relationships.set(from, relationship);
      setState(from, VEIL.INSIDE);

      six7SendControl(
        {
          type: "veil_accept",
          from: selfHKey,
          to: from,
          videoId,
          cycleId: relationship.cycleId,
          replyTo: relationship.inviteCid || null,
        },
        "veil_accept",
      );

      six7RequestChatPipe(from, "relationship_accept");
      return;
    }

    if (d.type === "VEIL_DENY") {
      const h = String(d.payload?.hKey || "").slice(0, 16);
      if (!h) return;

      const relationship = six7RelationshipGet(h);
      if (getState(h) !== VEIL.INSIDE) {
        six7RelationshipEnd(h, "local_deny", now(), false);
        setState(h, VEIL.OUTSIDE, true);
        try {
          rtcTeardown(h, "relationship_denied_local");
        } catch {}
      }

      six7SendControl(
        {
          type: "veil_deny",
          from: selfHKey,
          to: h,
          videoId,
          cycleId: relationship?.cycleId || null,
          replyTo: relationship?.inviteCid || null,
        },
        "veil_deny",
      );

      return;
    }

    if (d.type === "VEIL_REMOVE") {
      const h = String(d.payload?.hKey || "").slice(0, 16);
      if (!h) return;

      const relationship = six7RelationshipGet(h);
      if (getState(h) !== VEIL.INSIDE) {
        six7RelationshipEnd(h, "local_deny", now(), false);
        setState(h, VEIL.OUTSIDE, true);
        try {
          rtcTeardown(h, "relationship_denied_local");
        } catch {}
      }

      six7SendControl(
        {
          type: "veil_deny",
          from: selfHKey,
          to: h,
          videoId,
          cycleId: relationship?.cycleId || null,
          replyTo: relationship?.inviteCid || null,
        },
        "veil_deny",
      );

      return;
    }

    if (d.type === "TRY_DIAL") {
      const hKey = String(d.payload?.hKey || "").slice(0, 16);
      if (!hKey) return;
      six7RequestChatPipe(hKey, "user_open_dm");
      return;
    }

    if (d.type === "BLOCK_PEER") {
      const to = String(d.payload?.to || "").slice(0, 16);
      if (!to) return;

      // Block is a relationship action. Use the existing direct chat edge
      // while it still exists; fall back to the room SYS relay only when the
      // direct relationship carrier is unavailable.
      const directBlockSent = rtcSendDC(to, {
        t: "MSG",
        text: "",
        sys: "BLOCKED_BY",
        at: Date.now(),
      });

      const blockSent =
        directBlockSent ||
        six7SendControl(
          {
            type: "sys_block",
            from: selfHKey,
            to,
            sysFrom: sysPeerId(selfHKey),
            sysTo: sysPeerId(to),
            videoId,
            t: now(),
          },
          "sys_block",
        );

      if (!blockSent) {
        emitSnapshot("relationship_block_notification_unroutable");
      }

      six7BlockedPeers.add(to);
      six7BlockedByPeers.delete(to);
      six7RelationshipEnd(to, "blocked_local", now(), true);

      six7CancelReconnect(to);
      loungeWaiters.delete(to);
      six7PendingRelationship.delete(to);
      six7PendingDm.delete(to);

      six7PendingRtcIce.delete(to);

      setState(to, VEIL.OUTSIDE, true);
      rtcTeardown(to, "blocked_local");
      post("BLOCKED_LOCAL", { to, at: Date.now() });
      return;
    }

    if (d.type === "UNBLOCK_PEER") {
      const to = String(d.payload?.to || "").slice(0, 16);
      if (!to) return;

      six7BlockedPeers.delete(to);
      six7BlockedByPeers.delete(to);
      six7RelationshipRevoked.delete(to);
      if (
        six7CommittedMember(to) &&
        !six7PendingTerminalExits.has(to) &&
        !__six7MembershipExitApplied.has(to)
      ) {
        __six7GoneNotified.delete(to);
      }
      six7RelationshipEnd(to, "unblocked_local", now(), false);
      setState(to, VEIL.OUTSIDE, true);

      six7SendControl(
        {
          type: "sys_unblock",
          from: selfHKey,
          to,
          sysFrom: sysPeerId(selfHKey),
          sysTo: sysPeerId(to),
          videoId,
          t: now(),
        },
        "sys_unblock",
      );

      post("UNBLOCK_LOCAL", { to, at: Date.now() });
      return;
    }

    if (d.type === "SYS_SEND") {
      const to = String(d.payload?.to || "").slice(0, 16);
      const kind = String(d.payload?.kind || "");
      if (!to || !kind) return;

      postSysIntent(kind, to, { payload: d.payload || {} });

      if (kind === "UNBLOCK") {
        six7SendControl(
          {
            type: "sys_unblock",
            from: selfHKey,
            to,
            sysFrom: sysPeerId(selfHKey),
            sysTo: sysPeerId(to),
            videoId,
            t: now(),
          },
          "sys_unblock",
        );
      }

      return;
    }

    if (d.type === "SEND_FILE_BIN_CHUNK") {
      (async () => {
        const to = String(d.payload?.to || "").slice(0, 16);
        const file = d.payload?.file || null;
        const bytes = d.payload?.bytes || null;

        if (!six7CanUseChat(to)) {
          postLocal("FILE_BIN_CHUNK_SENT", {
            ok: false,
            to,
            file,
            error: "Chat not allowed",
          });
          return;
        }

        if (!to || !file?.id || !six7IsArrayBuffer(bytes)) {
          postLocal("FILE_BIN_CHUNK_SENT", {
            ok: false,
            to,
            file,
            error: "Invalid binary chunk",
          });
          return;
        }

        const channel = six7ChatPeers.get(six7ShortKey(to))?.dc;
        const drained = await rtcWaitDCDrain(channel);
        if (!drained || channel !== six7ChatPeers.get(six7ShortKey(to))?.dc || !six7CanUseChat(to) || host.canSendFile?.(file) === false) {
          postLocal('FILE_BIN_CHUNK_SENT', {ok:false,to,file,error:'File channel unavailable'});
          return;
        }
        const metaOk = rtcSendDC(to, {
          t: "FILE_BIN_META",
          file: {
            id: file.id,
            name: file.name,
            size: file.size,
            type: file.type || "application/octet-stream",
            index: file.index,
            total: file.total,
            bytes: file.bytes,
          },
          at: Date.now(),
        });

        let binOk = false;
        if (metaOk && channel.readyState === 'open') {
          try { channel.send(bytes); binOk = true; } catch {}
        }

        postLocal("FILE_BIN_CHUNK_SENT", {
          ok: !!binOk,
          to,
          file: {
            id: file.id,
            index: file.index,
            total: file.total,
          },
          error: binOk ? null : "DataChannel send failed",
        });
      })();

      return;
    }

    if (d.type === "VEIL_TYPING") {
      const to = six7ShortKey(d.payload?.to);
      const active = d.payload?.active === true;

      if (!to) return;

      const relationship = six7RelationshipGet(to);

      if (
        six7IsBlocked(to) ||
        six7IsBlockedBy(to) ||
        !six7CommittedMember(to) ||
        !six7RelationshipIsAccepted(relationship) ||
        !relationship.cycleId
      ) {
        return;
      }

      six7SendControl(
        {
          type: "veil_typing",
          from: selfHKey,
          to,
          videoId,
          cycleId: relationship.cycleId,
          active,
          t: now(),
        },
        "veil_typing",
      );

      return;
    }

    if (d.type === 'PHANTOM_DIST_RECOVER_SYS') {
      const base = six7LocalCommittedAuthority();
      const self = six7ShortKey(selfHKey);
      const one = six7ShortKey(base?.syscon1);
      const two = six7ShortKey(base?.syscon2);
      // Use only the historical committed mirror, never an untrusted doorway
      // roster or distribution table. Existing SYS proof/admission rules apply.
      if (String(concierge.localRoleCandidate || 'peer') !== 'peer' || !base ||
          !one || !two || two === self || one === self || six7HasOpenSysPipe(one) ||
          !six7CommittedMember(self) || six7PendingTerminalExits.has(two)) return;
      let retryCount = 0;
      if(six7GossipRecovery.targets.length){
        // Leave active attempts and other gossip/election recovery untouched.
        // Retry only the same exhausted mirror route from this committed base.
        const r=six7GossipRecovery;
        if(r.kind!=='committed_mirror'||r.targets.length!==1||r.targets[0]!==two||
            r.index<r.targets.length||r.epoch!==Number(base.epoch)||
            r.rosterSeq!==Number(base.rosterSeq)||now()<Number(r.nextRetryAt||0))return;
        retryCount=Number(r.retryCount||0);
      }
      six7GossipRecovery = {source:self, targets:[two], index:0,kind:'committed_mirror',retryCount,nextRetryAt:0,
        epoch:Number(base.epoch), rosterSeq:Number(base.rosterSeq), at:now()};
      six7ScheduleSysAdmission('distribution_restore_committed_mirror');
      emitSnapshot('distribution_restore_committed_mirror');
      return;
    }

    // Distribution owns its protocol and transports outside the roster engine.
    // This command only carries bounded control frames over existing SYS links.
    if (d.type === 'PHANTOM_DIST_SEND') {
      const packet = {t:'PHANTOM_DIST', frame:d.payload?.frame};
      if (JSON.stringify(packet).length > 32768) return;
      const requested = d.payload?.to ? six7ShortKey(d.payload.to) : null;
      for (const [key, ent] of six7SysPeers) {
        if ((requested && key !== requested) || ent.sysdc?.readyState !== 'open' || six7TemporarySysTransactions.has(key)) continue;
        try { ent.sysdc.send(JSON.stringify(packet)); } catch {}
      }
      return;
    }

    if (d.type === 'PHANTOM_ADMIN_SEND') {
      const packet = {t:'PHANTOM_ADMIN', record:d.payload?.record};
      if (JSON.stringify(packet).length > 4096) return;
      const requested = d.payload?.to ? six7ShortKey(d.payload.to) : null;
      const sent = new Set();
      for (const [key, ent] of six7SysPeers) {
        if ((requested && key !== requested) || ent.sysdc?.readyState !== 'open' || six7TemporarySysTransactions.has(key)) continue;
        try { ent.sysdc.send(JSON.stringify(packet)); sent.add(key); } catch {}
      }
      for (const [key, ent] of six7ChatPeers) {
        if ((requested && key !== requested) || sent.has(key) || !six7CanUseChat(key) || ent.dc?.readyState !== 'open') continue;
        try { ent.dc.send(JSON.stringify(packet)); sent.add(key); } catch {}
      }
      post('PHANTOM_ADMIN_SENT', {operationId:packet.record?.operationId, peers:[...sent]});
      return;
    }

    if (d.type === "SEND_DM") {
      const to = String(d.payload?.to || "").slice(0, 16);
      const text = String(d.payload?.text || "");
      const sys = d.payload?.sys ?? null;

      if (!to) return;
      if (!text.trim() && !sys) return;

      if (
        (sys === "FILE_ACCEPT" || sys === "FILE_DECLINE") &&
        d.payload?.file?.id
      ) {
        const key = six7ShortKey(to);
        const offers = key ? six7PendingIncomingFileOffers.get(key) : null;

        if (offers) {
          offers.delete(String(d.payload.file.id));
          if (!offers.size) six7PendingIncomingFileOffers.delete(key);
        }
      }

      if (!six7IsSysMessage(sys) && !six7CanUseChat(to)) {
        CORE_DEBUG &&
          console.log("[six7][CORE] SEND_DM blocked; chat not allowed →", to);

        concierge.lastErr = "dm_send_blocked_chat_not_allowed";
        emitSnapshot("dm_send_blocked_chat_not_allowed");
        return;
      }

      const chatEnt = six7ChatPeers.get(to);
      const chatDcState = String(chatEnt?.dc?.readyState || "missing");

      let ok = false;

      if (
        chatDcState === "open" &&
        chatEnt?.chatState === "ready" &&
        chatEnt?.chatProof === "chat_hello" &&
        chatEnt?.chatReadyAt > 0 &&
        six7CanUseChat(to)
      ) {
        ok = rtcSendDC(to, {
          t: "MSG",
          text,
          sys,
          file: d.payload?.file || null,
          at: Date.now(),
        });
      }

      if (!ok) {
        const st = getState(to);
        const canRecoverChat = !!(
          st === VEIL.ESTABLISHED ||
          st === VEIL.INSIDE ||
          st === VEIL.INVITE_RECEIVED ||
          st === VEIL.INVITE_SENT
        );

        if (canRecoverChat) {
          let q = six7PendingDm.get(to);
          if (!q) {
            q = [];
            six7PendingDm.set(to, q);
          }

          q.push({
            text,
            sys,
            file: d.payload?.file || null,
            at: Date.now(),
          });

          if (q.length > SIX7_DM_PENDING_MAX) {
            q.splice(0, q.length - SIX7_DM_PENDING_MAX);
          }

          concierge.lastErr = "dm_queued_waiting_for_chat_pipe:" + chatDcState;
          emitSnapshot("dm_queued_waiting_for_chat_pipe_" + chatDcState);

          if (chatEnt) {
            chatEnt.chatState = "lost";
            chatEnt.chatReadyAt = 0;
            chatEnt.chatProof = "none";
            chatEnt.chatFault =
              "dm_queued_waiting_for_chat_pipe:" + chatDcState;
          }
        } else {
          concierge.lastErr = "dm_send_blocked_relationship_not_active";
          emitSnapshot("dm_send_blocked_relationship_not_active");
        }
      }

      CORE_DEBUG && console.log("[six7][CORE] SEND_DM →", to, "ok=", ok);
      if (ok) {
        post("messageEcho", {
          to,
          text,
          sys,
          file: d.payload?.file || null,
          at: Date.now(),
        });

        if (sys === "CHAT_ENDED") {
          __six7GoneNotified.add(to);
          six7CancelReconnect(to);
          rtcTeardown(to);
        }
      }
      return;
    }

    if (d.type === "CORE_CTX") {
      const previousRoom = String(videoId || "");

      ctx = d.payload || null;
      origin = computeOrigin();

      const nextRoom = String(resolveVideoId() || "");

      if (previousRoom && nextRoom && previousRoom !== nextRoom) {
        // A room identity change retires the previous room through the existing
        // lifecycle owner before the reused core claims the new room.
        six7SendRoomExitedAndTeardown(true);
      }

      videoId = nextRoom || null;

      try {
        const saved = JSON.parse(
          sessionStorage.getItem("six7_refresh_relationships_v1") || "null",
        );

        const sameRoom =
          !!videoId && !!saved?.room && String(saved.room) === String(videoId);

        if (sameRoom && Array.isArray(saved?.peers)) {
          six7PendingRefreshRelationships.clear();
          six7PendingRefreshRoom = String(videoId || "");

          const savedRecords = new Map(
            (Array.isArray(saved.relationships) ? saved.relationships : [])
              .map((row) => [six7ShortKey(row?.hKey), row])
              .filter(([key]) => !!key),
          );

          for (const peer of saved.peers) {
            const key = six7ShortKey(peer);
            if (!key) continue;

            const row = savedRecords.get(key) || {};
            const cycleId = String(row.cycleId || "");
            if (!cycleId) continue;

            const relationship = six7RelationshipGet(key);
            relationship.state = VEIL.OUTSIDE;
            relationship.cycleId = cycleId;
            relationship.inviteCid = String(row.inviteCid || "") || null;
            relationship.localBootId = six7BootId;
            relationship.remoteBootId =
              String(row.remoteBootId || six7RemoteBootIds.get(key) || "") ||
              null;
            relationship.membershipEpoch = Number(row.membershipEpoch || 0);
            relationship.createdAt = now();
            relationship.updatedAt = now();
            relationship.endedAt = 0;
            relationship.endReason = "continuity_pending";
            six7Relationships.set(key, relationship);
            six7PendingRefreshRelationships.add(key);
          }

          emitSnapshot("relationship_continuity_staged_for_authority");
        } else {
          six7PendingRefreshRelationships.clear();
          six7PendingRefreshRoom = "";
          sessionStorage.removeItem("six7_refresh_relationships_v1");
        }
      } catch {
        try {
          sessionStorage.removeItem("six7_refresh_relationships_v1");
        } catch {}
      }

      applyTOFU();
      emitSnapshot("CORE_CTX");
      return;
    }

    if (d.type === "CORE_SET_SELF") {
      (async () => {
        const h = normalizeHandle(d.payload?.handle);
        if (!h) {
          selfHandle = null;
          selfHex = null;
          selfHKey = null;
          post("IDENTITY_READY", {
            handle: null,
            hex: null,
            hKey: null,
            videoId,
            origin,
          });
          emitSnapshot("CORE_SET_SELF_invalid");
          return;
        }
        const previousHandle = selfHandle;
        const previousHKey = selfHKey;

        selfHandle = h;

        selfAvatar = host.avatar || null;

        // --- PATCH C1: accept precomputed hKey if provided ---
        const providedHKey = String(d.payload?.hKey || "")
          .trim()
          .toLowerCase();

        if (/^[0-9a-f]{64}$/.test(providedHKey)) {
          // full SHA-256 hex was provided
          selfHex = providedHKey;
          selfHKey = providedHKey.slice(0, 16);
        } else {
          // fallback to hashing
          try {
            selfHex = await sha256Hex(selfHandle);
            selfHKey = selfHex ? selfHex.slice(0, 16) : null;
          } catch (e) {
            selfHex = null;
            selfHKey = null;
            bootstrap.lastErr = "hash_failed: " + String(e?.message || e);
            emitSnapshot("CORE_SET_SELF_hash_failed");
            return;
          }
        }

        ksTouchSelf("CORE_SET_SELF");

        if (selfHKey && enabled) {
          if (!videoId) videoId = resolveVideoId();
          origin = computeOrigin();
          applyTOFU();

          if (!bootOnce) {
            six7BootId = crypto.randomUUID();
            bootOnce = true;

            emitSnapshot("CORE_BOOTSTRAP");
          }

          const identityChanged =
            !!previousHKey &&
            (previousHKey !== selfHKey || previousHandle !== selfHandle);

          const doorwayMissing =
            !ws ||
            (ws.readyState !== WebSocket.OPEN &&
              ws.readyState !== WebSocket.CONNECTING);

          if (identityChanged) {
            wsClose("identity_changed");
            wsOpenBootstrap();
          } else if (doorwayMissing) {
            wsOpenBootstrap();
          }
        }
        // ---- LOCAL identity event for hobo ----
        try {
          if (selfHandle && selfHKey) {
            window.dispatchEvent(
              new CustomEvent("six7_identity_ready", {
                detail: { handle: selfHandle, hKey: selfHKey },
              }),
            );
          }
        } catch {}

        // --- PATCH B3: enriched identity snapshot to TOP ---
        post("IDENTITY_UPDATE", {
          handle: selfHandle,
          hex: selfHex,
          hKey: selfHKey,

          // core context injection (needed for stability + TOFU + HUD)
          videoId,
          origin,

          keystore: ksSerialize(),
        });

        origin = computeOrigin();
        if (!videoId) videoId = resolveVideoId();
        applyTOFU();
        emitSnapshot("CORE_SET_SELF");

        // Do not mark full brain boot from CORE_SET_SELF.
        // Identity is ready here, but CORE_BOOTSTRAP owns bootOnce + brainReady.
        if (selfHKey) {
          post("IDENTITY_READY", {
            handle: selfHandle,
            hex: selfHex,
            hKey: selfHKey,
            videoId,
            origin,
            via: "CORE_SET_SELF",
          });
        }
      })();
      return;
    }

    if (d.type === "CORE_SET_ENABLED") {
      enabled = !!d.payload?.enabled;
      emitSnapshot("CORE_SET_ENABLED");
      return;
    }

    if (d.type === "CORE_BOOTSTRAP") {
      // A reused suspended core must regain lifecycle authority before
      // deciding whether bootstrap work was already claimed.
      __six7ExitSent = false;
      six7SessionKilling = false;

      if (bootOnce) {
        const doorwayMissing =
          !ws ||
          (ws.readyState !== WebSocket.OPEN &&
            ws.readyState !== WebSocket.CONNECTING);

        if (enabled && selfHKey && videoId && doorwayMissing) {
          wsOpenBootstrap();
        }

        return;
      }

      CORE_DEBUG &&
        console.log("[six7][CORE] CORE_BOOTSTRAP HIT", {
          bootOnce,
          selfHandle,
          selfHKey,
          enabled,
          videoId,
        });

      if (!videoId) videoId = resolveVideoId();
      origin = computeOrigin();
      applyTOFU();

      if (!selfHKey) {
        CORE_DEBUG &&
          console.debug("[six7][CORE] CORE_BOOTSTRAP waiting for identity");
        return;
      }

      six7BootId = crypto.randomUUID();
      bootOnce = true;

      const url =
        d.payload?.url ||
        `${DEFAULT_BOOTSTRAP_WS}discover?v=${encodeURIComponent(videoId)}`;

      emitSnapshot("CORE_BOOTSTRAP");

      wsOpenBootstrap(url);
      return;
    }

    // ======================================================
    // P2P WebRTC (Signaling from TOP/BG)
    // ======================================================

    // RTC_DIAL disabled; TRY_DIAL remains the only public dial request path

    // Inbound signaling from BG → TOP → brain
    // d.type: "RTC_SIGNAL" payload: { from, kind, data, room, to }
    if (d.type === "RTC_SIGNAL") {
      (async () => {
        try {
          const p = d.payload || {};
          const from = String(p.from || "");
          const kind = String(p.kind || "");
          const data = p.data;

          if (!from || !kind) return;
          if (six7SessionKilling) return;

          if (kind === "offer") {
            await rtcOnOffer(from, data);
            return;
          }
          if (kind === "answer") {
            await rtcOnAnswer(from, data);
            return;
          }
          if (kind === "ice") {
            await rtcOnIce(from, data);
            return;
          }
        } catch (e) {
          bootstrap.lastErr = "rtc_signal_failed: " + String(e?.message || e);
          emitSnapshot("rtc_signal_failed");
        }
      })();
      return;
    }

    // ignore everything else by design
  });

  // ======================================================
  // SPRING #1 — Chat Scan Integration Hook
  // ======================================================

  const peerTable = new Map(); // reserved for future core-owned peer state only

  ksLoad();
  CORE_DEBUG && console.log(TAG, "boot", location.href);
  emitSnapshot("core_boot");

  return {
    command(type, payload = {}) {
      window.dispatchEvent({ type: "message", data: { six7: 1, type, payload } });
    },
    snapshot() { return snapshot("sdk_read"); },
    dispose() {
      six7SendRoomExitedAndTeardown(true);
      wsClose("sdk_dispose");
      rtcTeardownAll("sdk_dispose");
    }
  };
}
