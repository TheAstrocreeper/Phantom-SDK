import {waitForDataChannelCapacity} from './data-channel-capacity.js';
import {createSequentialSignaling} from './sequential-signaling.js';
import {createSysOutageClock} from './sys-outage-clock.js';
import {createIceTrace} from './ice-trace.js';
import {createRtcDiagnostics} from './rtc-diagnostics.js';
import {COMPATIBILITY_WIRE} from './compatibility.js';
import {acceptsSuccessorStatus} from './successor-status.js';
import {createSignalingBridge} from './signaling-bridge.js';
import { canCarrySysRecovery } from './recovery-signaling.js';
import { resumeDeferredAdmissions } from './deferred-admission.js';
import { sysBirthDisposition, isCurrentSysBirth, runSysBirth } from './sys-birth.js';
import {acceptsBootstrapRouteHint} from './bootstrap-route-hint.js';
import {createDeferredSysOffers} from './deferred-sys-offers.js';
import { acceptsSuccessorHandshake } from './successor-handshake.js';
/** Peer networking engine: membership, leadership, signaling, and data channels. */
export function createNetworkEngine(host) {
  const canUsePeer=peer=>{try{return host.canUsePeer?host.canUsePeer(peer)===true:true;}catch{return false;}};
  const { window, document, localStorage, sessionStorage, location,
    WebSocket, RTCPeerConnection, crypto, setTimeout, clearTimeout,
    setInterval, clearInterval } = host;

  "use strict";
  const CORE_DEBUG = false;
  const sysOutageClock = createSysOutageClock({now:()=>now()});
  const iceTrace=createIceTrace({emit:detail=>post('ICE_TRACE',detail)});
  function traceIce(stage,peer,pipe,candidate,meta={},pc){
    const key=phantomShortKey(peer),ent=pipe==='sys'?phantomSysPeers.get(key):phantomChatPeers.get(key);
    iceTrace.record(stage,candidate,{self:phantomShortKey(selfHKey),peer:key,pipe,connectionId:rtcDiagnostics.connectionId(pc??ent?.pc),attemptId:meta.sysAttemptId??meta.chatAttemptId??ent?.sysAttemptId??ent?.chatAttemptId??ent?.attempt?.id??null,bootId:meta.bootId??null,...(meta.error?{error:{name:String(meta.error.name??'').slice(0,128),message:String(meta.error.message??'').slice(0,512)}}:{}),...(meta.route?{route:meta.route}:{}),...(meta.submitted!==undefined?{submitted:meta.submitted}:{})});
  }
  const rtcDiagnostics = createRtcDiagnostics({now:()=>Date.now(),emit:detail=>post("RTC_DIAGNOSTICS",detail)});

  const TAG = "[phantom][CORE]";

  // ---- Config ----
  const DEFAULT_BOOTSTRAP_WS = host.doorwayUrl;
  const TOFU_ORIGIN_KEY = "phantom_tofu_origin";
  const TOFU_HKEY_KEY = "phantom_tofu_hkey";
  const TOFU_FIRST_KEY = "phantom_tofu_firstSeenAt";
  const KEYSTORE_KEY = "phantom_keystore_v1";

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
  let phantomBootId = crypto.randomUUID();
  const phantomRemoteBootIds = new Map();
  const phantomRemoteEndpointIds = new Map();
  let phantomSysTransportAttemptSeq = 0;
  const phantomDoorwayAdmittedPeers = new Set();
  const sdkDeferredOffers=createDeferredSysOffers({setTimeout,clearTimeout,emit:detail=>post(detail.kind==='successor'?'SYS_SUCCESSOR_OFFER':'SYS_HANDOFF_OFFER',detail)});
  let sdkBootstrapTarget = null;
  const sdkBootstrapUnanswered = new Set();
  // Terminal membership commands are authority evidence waiting for the
  // current or successor syscon1 to commit the corresponding removal.
  const phantomPendingTerminalExits = new Map(); // hKey -> { bootId, epoch, at, via }
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
  const PHANTOM_WS_CONNECT_DEADLINE_MS = 10000;
  const PHANTOM_WS_READY_DEADLINE_MS = 10000;
  const PHANTOM_WS_WATCH_TICK_MS = 1000;

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
  const PHANTOM_EMPTY_CONCIERGE = JSON.parse(JSON.stringify(concierge));

  const phantomSeenRtcSignals = new Map();
  const PHANTOM_RTC_SIGNAL_MAX = 500;

  const phantomPendingRtcIce = new Map();
  const PHANTOM_RTC_ICE_QUEUE_MAX = 24;

  const phantomPendingRelationship = new Map(); // hKey -> [{ msg, type, at }]
  const PHANTOM_REL_PENDING_MAX = 12;
  const phantomPendingSysRelay = new Map(); // destination hKey -> SYS_CONTROL envelopes
  const PHANTOM_SYS_RELAY_PENDING_MAX = 24;
  const phantomPendingDm = new Map(); // hKey -> [{ text, sys, file, at }]
  const PHANTOM_DM_PENDING_MAX = 20;
  const phantomPendingRefreshRelationships = new Set();
  let phantomPendingRefreshRoom = "";
  const phantomPendingIncomingFileOffers = new Map(); // hKey -> Map(fileId -> file)

  // Temporary SYS is an existing SYS transport in transaction mode. It is
  // used only when no room SYS route exists, carries one allowlisted control
  // transaction, then collapses after acknowledgment.
  const phantomTemporarySysTransactions = new Map(); // hKey -> transaction
  const PHANTOM_TEMP_SYS_PURPOSES = new Set(["relationship_unblock"]);

  // Gossip is never authority. It is only a remembered route hint used to
  // find the accepted syscons and request normal SYS authority again.
  const phantomAuthorityGossipHints = new Map(); // source hKey -> accepted snapshot hint
  let phantomGossipRecovery = {
    source: null,
    targets: [],
    index: 0,
    epoch: 0,
    rosterSeq: 0,
    at: 0,
  };

  function phantomRtcSignalKey(msg, via = "") {
    if (!msg || typeof msg !== "object") return "";

    const type = String(msg.type || "");
    const from = phantomShortKey(msg.from);
    const to = phantomShortKey(msg.to);

    if (!phantomIsRtcSignalType(type) || !from || !to) return "";

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

  function phantomRememberRtcSignal(msg, via = "") {
    const key = phantomRtcSignalKey(msg, via);
    if (!key) return false;

    if (phantomSeenRtcSignals.has(key)) {
      concierge.lastErr =
        "rtc_duplicate_ignored:" + String(msg?.type || "unknown");
      emitSnapshot("rtc_duplicate_ignored_" + String(msg?.type || "unknown"));
      return true;
    }

    phantomSeenRtcSignals.set(key, now());

    if (phantomSeenRtcSignals.size > PHANTOM_RTC_SIGNAL_MAX) {
      const rows = Array.from(phantomSeenRtcSignals.entries()).sort(
        (a, b) => Number(a[1] || 0) - Number(b[1] || 0),
      );

      while (
        phantomSeenRtcSignals.size > Math.floor(PHANTOM_RTC_SIGNAL_MAX * 0.75) &&
        rows.length
      ) {
        phantomSeenRtcSignals.delete(rows.shift()[0]);
      }
    }

    return false;
  }

  function phantomIsRtcSignalType(type) {
    type = String(type || "");
    return type === "rtc_offer" || type === "rtc_answer" || type === "rtc_ice";
  }

  function phantomMarkControlTx(type, route = concierge.route) {
    concierge.lastTxAt = now();
    concierge.lastTxType = String(type || "unknown");
    concierge.route = String(route || concierge.route || "bootstrap_ws");
  }

  function phantomMarkControlRx(type) {
    concierge.lastRxAt = now();
    concierge.lastRxType = String(type || "unknown");
  }

  function phantomControlClassFor(type) {
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

  function phantomControlClassMode(cls, type = "") {
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

  function phantomNewSysTransportAttemptId(remoteHKey) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return "";

    phantomSysTransportAttemptSeq += 1;

    return [phantomBootId, key, phantomSysTransportAttemptSeq].join(":");
  }

  const networkFirstSignaling=createSequentialSignaling({setTimeout,clearTimeout,now:()=>now(),status:detail=>post('SYS_SIGNAL_ROUTE',detail)});
  const signalingPeerKeys=new Map();
  function bindSignalingPeerKey(key,hex){if(!phantomCommittedMember(key)||typeof hex!=='string'||!/^[0-9a-f]{64}$/.test(hex)||hex.slice(0,16)!==key)return false;const previous=signalingPeerKeys.get(key);if(previous&&previous!==hex)return false;if(!previous&&signalingPeerKeys.size>=256)return false;signalingPeerKeys.set(key,hex);return true;}
  const signalingBridge=createSignalingBridge({self:()=>phantomShortKey(selfHKey),room:()=>videoId,authentication:host.recoverySignaling,bindIdentity:bindSignalingPeerKey,
    member:key=>{if(phantomPendingTerminalExits.has(key)||__phantomGoneNotified.has(key)||host.canAdmitPeer(key)!==true)return null;if(!phantomCommittedMember(key))return null;const presence=phantomPresenceBook.get(key)||{},direct=phantomChatPeers.get(key);if(direct?.dc?.readyState==='open'&&direct.chatState==='ready'&&phantomRelationshipIsAccepted(phantomRelationshipGet(key)))bindSignalingPeerKey(key,direct.signalingPublicKey);return {hKey:key,hex:key===phantomShortKey(selfHKey)?selfHex:signalingPeerKeys.get(key),bootId:key===phantomShortKey(selfHKey)?phantomBootId:phantomRemoteBootIds.get(key)||presence.bootId};},
    peers:()=>[...phantomChatPeers].filter(([key,entry])=>phantomCommittedMember(key)&&!phantomPendingTerminalExits.has(key)&&!__phantomGoneNotified.has(key)&&host.canAdmitPeer(key)===true&&phantomRelationshipIsAccepted(phantomRelationshipGet(key))&&entry.dc?.readyState==='open'&&entry.chatState==='ready').map(([key])=>key),
    send:(key,envelope)=>{try{phantomChatPeers.get(key).dc.send(JSON.stringify(envelope));return true;}catch{return false;}},
    deliver:packet=>{const meta={...packet,room:videoId,pipe:'sys',recoveryPeerPipe:true};if(packet.type==='rtc_offer')rtcOnOffer(packet.from,packet.sdp,meta);else if(packet.type==='rtc_answer')rtcOnAnswer(packet.from,packet.sdp,meta);else rtcOnIce(packet.from,packet.cand,meta);},
    status:detail=>host.emit('SYS_SIGNAL_BRIDGE',detail)
  });

  function phantomCommittedMember(remoteHKey) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;

    return phantomAuthorityRosterRows(phantomLocalCommittedAuthority()?.roster).some(
      (row) => row.hKey === key,
    );
  }

  function phantomRetireRemoteIncarnationTransport(
    remoteHKey,
    reason = "remote_incarnation_changed",
  ) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;

    const oldChatEnt = phantomChatPeers.get(key);
    const oldSysEnt = phantomSysPeers.get(key);

    phantomCancelReconnect(key);
    reconnectingPeers.delete(key);

    const reconnectDeadline = phantomChatReconnectDeadlines.get(key);
    if (reconnectDeadline) clearTimeout(reconnectDeadline);
    phantomChatReconnectDeadlines.delete(key);

    rtcClosing.add(key);
    phantomChatPeers.delete(key);
    phantomSysPeers.delete(key);
    rtcDialing.delete(key);
    phantomSysDialing.delete(key);
    phantomSysPreflightUnstableRetries.delete(key);
    phantomPendingRtcIce.delete(key);
    phantomSysLastGoodRx.delete(key);
    phantomSysAdmissionLastSent.delete(key);

    if (oldChatEnt) {
      oldChatEnt.dc = null;
      try {
        rtcDiagnostics.retire(oldChatEnt.pc,reason);
        oldChatEnt.pc?.close();
      } catch {}
    }

    if (oldSysEnt) {
      oldSysEnt.sysdc = null;
      oldSysEnt.sysAttemptId = null;
      oldSysEnt.ownerAttemptId = null;
      oldSysEnt.sysCarrier = false;
      try {
        rtcDiagnostics.retire(oldSysEnt.pc,reason);
        oldSysEnt.pc?.close();
      } catch {}
    }

    rtcClosing.delete(key);

    if (phantomShortKey(concierge.sysReadyPeer) === key) {
      concierge.sysReady = false;
      concierge.sysReadyPeer = null;
      concierge.sysReadyAt = 0;
      concierge.sysReadyProof = "none";
      concierge.sysReadyEpoch = 0;
    }

    if (
      phantomShortKey(concierge.sysPeer) === key ||
      phantomShortKey(phantomSysTargetFromCandidates()) === key
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

  function phantomObserveRemoteIncarnation(remoteHKey, observation = {}) {
    const key = phantomShortKey(remoteHKey);
    const self = phantomShortKey(selfHKey);
    if (!key || key === self) return { changed: false, committed: false };

    const incomingBootId = String(observation.bootId || "");
    const incomingEndpointId = String(observation.endpointId || "");
    const incomingAttemptId = String(observation.attemptId || "");
    const signalType = String(observation.signalType || "");
    const bindCurrent = observation.bindCurrent === true;
    const via = String(observation.via || "remote_incarnation");

    const previousBootId = String(phantomRemoteBootIds.get(key) || "");
    const previousEndpointId = String(phantomRemoteEndpointIds.get(key) || "");
    const currentEnt = phantomSysPeers.get(key);
    const currentAttemptId = String(
      currentEnt?.attempt?.id || currentEnt?.sysAttemptId || "",
    );
    const currentPipeOpen = phantomHasOpenSysPipe(key);

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

    if (incomingBootId) phantomRemoteBootIds.set(key, incomingBootId);
    if (incomingEndpointId) {
      phantomRemoteEndpointIds.set(key, incomingEndpointId);
    }

    const changed = bootChanged || endpointChanged || offerAttemptChanged;
    if(bootChanged||endpointChanged){sdkDeferredOffers.drop(key,'incarnation_changed');}
    const committed = phantomCommittedMember(key);

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
        phantomSupersedeSysReplacementForCommittedRefresh(
          key,
          previousBootId || previousEndpointId || currentAttemptId,
          incomingBootId || incomingEndpointId || incomingAttemptId,
        );
      }

      emitSnapshot("remote_incarnation_bound_current_transport_" + via);
      return { changed: true, committed };
    }

    if (committed) {
      phantomSupersedeSysReplacementForCommittedRefresh(
        key,
        previousBootId || previousEndpointId || currentAttemptId,
        incomingBootId || incomingEndpointId || incomingAttemptId,
      );

      const relationship = phantomRelationships.get(key);
      if (relationship && !relationship.endedAt) {
        relationship.remoteBootId = incomingBootId || null;
        relationship.updatedAt = now();
        phantomRelationships.set(key, relationship);
      }

      emitSnapshot("committed_member_remote_incarnation_transition");
    } else if (bootChanged || endpointChanged) {
      // A new transport for an absent member is admission evidence only.
      // Membership-exit latches remain closed until a committed authority
      // snapshot explicitly adds this new incarnation.
      emitSnapshot("fresh_incarnation_waiting_for_committed_admission");
    }

    phantomRetireRemoteIncarnationTransport(key, "remote_incarnation_" + via);

    return { changed: true, committed };
  }

  function phantomRefreshSysReadiness(reason = "refresh") {
    const wasSysReady = concierge.sysReady === true;

    const hasRoom = !!videoId;
    const hasSelf = !!selfHKey;
    const hasRoster = !!lastRoster && lastRoster.size > 0;

    const readyPeer = phantomShortKey(concierge.sysReadyPeer);
    const expectedPeer = phantomShortKey(phantomSysTargetFromCandidates());

    // SYS truth is derived from the legal concierge target's actual datachannel.
    const targetPeer = expectedPeer;

    const roleNow = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const epochNow = Number(concierge.sysconEpoch || 0);

    const cfSyscon1 = phantomShortKey(concierge.cfConciergeSyscon1);
    const cfSyscon2 = phantomShortKey(concierge.cfConciergeSyscon2);

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
      const oldTarget = phantomShortKey(concierge.sysAttempt.target);

      const oldEnt = oldTarget ? phantomSysPeers.get(oldTarget) : null;
      const oldSysDc = oldEnt?.sysdc || null;

      concierge.sysAttempt.state = "superseded";
      concierge.sysAttempt.lastEvent = "authority_or_target_changed";

      phantomSysDialing.delete(oldTarget);
      phantomSysPreflightUnstableRetries.delete(oldTarget);

      /*
       * A SYS attempt is superseded only by a real authority/role/target change.
       * CF endpoint replacement is doorway metadata and never retires the
       * established peer carrier.
       */
      if (oldEnt && oldTarget === targetPeer) {
        oldEnt.ownerAttemptId = attemptId;
        emitSnapshot("sys_birth_preserved_same_target");
      } else if (oldEnt && roleNow === "syscon1" &&
          phantomSysBirthRole(oldTarget) === "acceptor") {
        // Changing the primary mirror does not retire a legal inbound tether.
        // Admission frames may still be in flight on that candidate transport.
        oldEnt.ownerAttemptId = null;
        emitSnapshot("sys_birth_preserved_peer_tether");
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

    const ent = targetPeer ? phantomSysPeers.get(targetPeer) : null;
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
        const provenTarget = phantomShortKey(targetPeer);
        const primary = phantomShortKey(concierge.syscon1);

        phantomPeerSysCarrier.target = provenTarget || null;

        if (provenTarget && provenTarget === primary) {
          phantomPeerSysCarrier.primaryRecoveryUsed = false;
        }
      }

      // Diagnostic normalization only.
      // Once SYS is proven ready, stale pending/waiting labels are no longer useful.
      const staleSysWait =
        /phantom_sys_pending|waiting_for_sys_pipe|waiting_for_target_sys_pipe|waiting_for_proof|sys_pipe_not_open|sys_pipe_connecting/i;

      if (staleSysWait.test(String(concierge.route || ""))) {
        concierge.route = "phantom_sys";
        concierge.routeReason = "sys_ready";
        concierge.wouldRoute = "phantom_sys";
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

      if (targetPeer && phantomPendingRtcIce.has(targetPeer)) {
        phantomFlushQueuedRtcIce(targetPeer, "sys_ready_flush_queued_ice");
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
      phantomReconcileRelationshipStatus("relationship_status_sys_ready");
    }

    phantomRefreshSysFallbackIndicator();
  }

  function phantomRefreshSysCandidates(rosterList = [], reason = "refresh") {
    const self = phantomShortKey(selfHKey);

    const sysAuthForRoster = phantomLocalCommittedAuthority();
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
      const hKey = phantomShortKey(
        typeof row === "string" ? row : row?.hKey || row?.from,
      );
      if (!hKey || phantomPendingTerminalExits.has(hKey)) return null;

      const old = phantomPresenceBook.get(hKey) || {};

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

    const cfOne = phantomShortKey(concierge.cfConciergeSyscon1);
    const cfTwo = phantomShortKey(concierge.cfConciergeSyscon2);

    if (cfOne) pushCandidateRow({ hKey: cfOne }, "cf_syscon1");
    if (cfTwo && cfTwo !== cfOne)
      pushCandidateRow({ hKey: cfTwo }, "cf_syscon2");

    const authOne = phantomShortKey(sysAuthForRoster?.syscon1);
    const authTwo = phantomShortKey(sysAuthForRoster?.syscon2);

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
    const byKey = new Map(unique.map((x) => [phantomShortKey(x.hKey), x]));

    let one = null;
    let two = null;
    let candidateSource = "hkey_fallback";

    const sysAuth = phantomFreshSysAuthority();

    if (sysAuth) {
      const a = phantomShortKey(sysAuth.syscon1);
      const b = phantomShortKey(sysAuth.syscon2);

      if (a && byKey.has(a)) one = byKey.get(a);
      if (b && b !== a && byKey.has(b)) two = byKey.get(b);

      if (a && phantomPendingTerminalExits.has(a) && b && byKey.has(b)) {
        one = byKey.get(b);
        two = unique.find(row => row.hKey !== b) || null;
        candidateSource = "syscon2_promoted_primary";
      } else if (one || two) {
        candidateSource = "sys_authority";
      }
    }

    const currentSelf = phantomShortKey(selfHKey);
    const oldOne = phantomShortKey(concierge.syscon1);
    const oldTwo = phantomShortKey(concierge.syscon2);

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
        const k = phantomShortKey(x?.hKey);
        return k && k !== currentSelf;
      });

      two = next || null;
      candidateSource = "syscon2_promoted_primary";
    }

    if (!one && !two) {
      const a = phantomShortKey(concierge.cfConciergeSyscon1);
      const b = concierge.bootstrapAdmission === true ? "" : phantomShortKey(concierge.cfConciergeSyscon2);

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
      cleanReason === "presence_phantom_sys"
    ) {
      cleanReason = "boss_identity";
    }

    concierge.candidateReason = unique.length
      ? `${candidateSource}_${cleanReason}`
      : "no_roster_candidates";

    concierge.lastCandidateAt = now();

    phantomApplyLocalRoleCandidate(reason);
  }

  function phantomApplyConciergeRoute(msg, reason = "concierge_route") {
    if (!msg || typeof msg !== "object") return false;

    const advertisedEpoch = Number(msg.epoch || 0);
    const committedEpoch = Number(phantomLocalCommittedAuthority()?.epoch || 0);
    const incomingEpoch = committedEpoch || advertisedEpoch;
    const currentEpoch = Number(concierge.sysconEpoch || 0);

    if (incomingEpoch && currentEpoch && incomingEpoch < currentEpoch) {
      emitSnapshot("cf_concierge_older_epoch_ignored");
      return false;
    }

    const authorityReply = String(reason || msg.type || "");

    if (
      phantomPeerSysCarrier.authorityRefreshPending === true &&
      authorityReply === "concierge_update" &&
      Array.isArray(msg.list) &&
      incomingEpoch &&
      (!currentEpoch || incomingEpoch >= currentEpoch)
    ) {
      phantomPeerSysCarrier.authorityRefreshPending = false;
    }

    // Candidate admission affects bootstrap direction only; committed SYS truth wins.
    concierge.bootstrapAdmission = msg.bootstrapAdmission === true &&
      phantomShortKey(msg.syscon2) === phantomShortKey(selfHKey) &&
      phantomShortKey(msg.syscon1) !== phantomShortKey(selfHKey) &&
      !phantomLocalCommittedAuthority();
    concierge.cfConciergeState = String(reason || msg.type || "unknown");
    concierge.cfConciergeAt = now();
    concierge.cfConciergeSyscon1 = phantomShortKey(msg.syscon1);
    concierge.cfConciergeSyscon2 = phantomShortKey(msg.syscon2);

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
      const hKey = phantomShortKey(row?.hKey || row?.from || row);
      if (!hKey || seen.has(hKey)) return;

      const old = phantomPresenceBook.get(hKey) || {};

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

    const one = phantomShortKey(msg.syscon1);
    const two = phantomShortKey(msg.syscon2);

    const cfRowsByKey = new Map();

    if (Array.isArray(msg.list)) {
      const admittedNow = new Set();

      for (const p of msg.list) {
        const k = phantomShortKey(p?.hKey || p?.from || p);
        if (!k) continue;

        cfRowsByKey.set(k, p);
        admittedNow.add(k);
      }

      // Doorway admission is bootstrap eligibility, not room authority.
      // Keep it separate from the SYS-stamped roster so syscon1 can accept
      // a newly arrived plain peer's owned SYS tether before that peer has
      // been incorporated into the next SYS authority publication.
      phantomDoorwayAdmittedPeers.clear();
      for (const k of admittedNow) phantomDoorwayAdmittedPeers.add(k);
    }

    const self = phantomShortKey(selfHKey);
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

      phantomObserveRemoteIncarnation(remoteKey, {
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
      concierge.sysReady || phantomLocalCommittedAuthority()
    );

    const cfAllowedKeys = new Set();

    if (one) cfAllowedKeys.add(one);
    if (two) cfAllowedKeys.add(two);

    if (Array.isArray(msg.list)) {
      for (const p of msg.list) {
        const k = phantomShortKey(p?.hKey || p?.from || p);
        if (k) cfAllowedKeys.add(k);
      }
    }

    const committedKeys = new Set(
      (phantomLocalCommittedAuthority()?.roster || Array.from(lastRoster || []))
        .map((row) => phantomShortKey(row?.hKey || row?.from || row))
        .filter(Boolean),
    );

    const doorwayRemovesCommitted = Array.from(committedKeys).some(
      (key) => key && !cfAllowedKeys.has(key),
    );

    const committedAuthority = phantomLocalCommittedAuthority();

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

    const latchedSysList = sysOwnsRoster ? phantomLatchedSysRosterList() : [];

    const cfSelfOnly = !!(
      cfHasAuthoritativeList &&
      Array.isArray(msg.list) &&
      msg.list.length <= 1 &&
      one &&
      cfAllowedKeys.size <= 1
    );

    const merged = new Map();

    for (const p of latchedSysList) {
      const hKey = phantomShortKey(p?.hKey || p?.from);
      if (!hKey) continue;

      merged.set(hKey, {
        hKey,
        handle: p.handle || "",
        seenVia: p.seenVia || "phantom_sys",
      });
    }

    for (const p of list) {
      const hKey = phantomShortKey(p?.hKey || p?.from);
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
      const allowed = new Set(effectiveList.map((p) => phantomShortKey(p.hKey)));

      for (const [hKey, row] of Array.from(phantomPresenceBook.entries())) {
        const key = phantomShortKey(hKey || row?.hKey || row?.from);

        if (!key || key === phantomShortKey(selfHKey)) continue;
        if (allowed.has(key)) continue;

        const via = String(row?.seenVia || row?.via || "");

        if (via === "phantom_sys" && sysOwnsRoster) {
          continue;
        }

        if (
          via === "phantom_sys" ||
          via === "cf_anchor" ||
          via === "cf_anchor_roster" ||
          via === "cf_anchor_syscon"
        ) {
          phantomPresenceBook.delete(hKey);
        }
      }
    }

    const nextConciergeKeys = new Set(
      effectiveList
        .map((p) => p?.hKey || p?.from)
        .filter(Boolean)
        .map((k) => phantomShortKey(k)),
    );

    for (const oldKey of Array.from(lastRoster || [])) {
      const gone = phantomShortKey(oldKey);

      if (!gone || gone === phantomShortKey(selfHKey)) continue;
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
      lastRoster = phantomMarkRosterAudit(
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
            phantomPresenceBook.get(one) || {
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
            phantomPresenceBook.get(two) || {
              hKey: two,
              from: two,
              handle: two === self ? selfHandle || null : null,
              videoId: videoId || null,
            },
        );
      }

      if (
        phantomCommitSyscon1Roster(
          initialAuthorityRows,
          "syscon1_initial_room_birth",
        )
      ) {
        phantomPublishCommittedAuthority("syscon1_initial_room_birth_published");
      } else {
        emitSnapshot("syscon1_initial_room_birth_commit_failed");
      }
    }

    for (const p of effectiveList) {
      phantomRememberPresencePeer(
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

    phantomRefreshSysCandidates(effectiveList, reason);

    /*
     * If both historical authority relays disappear, a committed survivor may
     * be elected into the current syscon1 slot. It still cannot rewrite room
     * truth from the doorway roster. Instead it opens the existing replacement
     * observation window for each missing historical spine member; only proven
     * SYS loss plus doorway absence may become terminal authority evidence.
     */
    const handoffBase = phantomLocalCommittedAuthority();
    const handoffRole = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    if (
      handoffBase &&
      self &&
      handoffRole === "syscon1" &&
      phantomShortKey(concierge.syscon1) === self &&
      phantomShortKey(handoffBase.syscon1) !== self &&
      phantomAuthorityRosterRows(handoffBase.roster).some(
        (row) => row.hKey === self,
      )
    ) {
      for (const oldSpineKey of [
        phantomShortKey(handoffBase.syscon1),
        phantomShortKey(handoffBase.syscon2),
      ]) {
        if (
          !oldSpineKey ||
          oldSpineKey === self ||
          (host.doorwayPresence === 'live-sockets' && phantomDoorwayAdmittedPeers.has(oldSpineKey)) ||
          phantomHasOpenSysPipe(oldSpineKey) ||
          phantomPendingTerminalExits.has(oldSpineKey) ||
          phantomSysReplacementWindows.has(oldSpineKey)
        ) {
          continue;
        }
        phantomArmSysReplacement(
          oldSpineKey,
          "elected_successor_missing_historical_spine",
        );
      }
    }

    for (const [remoteKey, rec] of Array.from(
      phantomSysReplacementWindows.entries(),
    )) {
      if (
        String(rec?.state || "") === "awaiting_doorway_absence" &&
        !(host.doorwayPresence === 'live-sockets' && phantomDoorwayAdmittedPeers.has(remoteKey)) &&
        !phantomHasOpenSysPipe(remoteKey)
      ) {
        phantomCompleteFailedSysReplacement(
          remoteKey,
          "doorway_absence_after_sys_loss",
        );
      }
    }

    phantomResumeTerminalAuthority("concierge_role_refresh");

    const epochBase = phantomLocalCommittedAuthority();
    const acceptedEpoch = Number(concierge.sysconEpoch || 0);
    const epochOwner = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );

    // A doorway epoch change is topology metadata, not membership truth. The
    // continuing leader advances the exact committed value through the same
    // mirror-accepted authority transaction, even when the roster is unchanged.
    if (
      !phantomSysAuthority.pending &&
      !phantomPendingTerminalExits.size &&
      epochBase &&
      self &&
      epochOwner === "syscon1" &&
      phantomShortKey(concierge.syscon1) === self &&
      phantomShortKey(epochBase.syscon1) === self &&
      acceptedEpoch > Number(epochBase.epoch || 0)
    ) {
      if (
        phantomStageSyscon1Authority(
          epochBase.roster,
          "syscon1_epoch_advance",
          epochBase,
          {
            syscon1: self,
            syscon2: phantomShortKey(epochBase.syscon2) || null,
          },
        )
      ) {
        phantomPublishCommittedAuthority("syscon1_epoch_advance_published");
      }
    }

    phantomRefreshSysReadiness(
      "concierge_owner_resolve_" + String(reason || "route"),
    );

    phantomReconcileRelationshipStatus(
      "relationship_status_" + String(reason || "concierge"),
    );

    const ownerRole = String(concierge.localRoleCandidate || "peer");
    const authorityHeld =
      phantomPeerSysCarrier.authorityRefreshPending === true &&
      String(reason || msg.type || "") !== "concierge_update";

    if (authorityHeld) {
      emitSnapshot("concierge_reconcile_waiting_for_authority");
    } else if (ownerRole === "syscon1") {
      phantomMaybeRelinkSysconPair("concierge_owner_" + String(reason || "route"));
    } else if (ownerRole === "syscon2") {
      phantomMaybeRelinkSysconPair("concierge_owner_" + String(reason || "route"));
      phantomScheduleSysPreflight(
        "concierge_owner_syscon2_" + String(reason || "route"),
      );
    } else {
      phantomScheduleSysAdmission("concierge_owner_" + String(reason || "route"));
    }

    if (ownerRole !== "peer" && !phantomCommittedMember(self)) {
      phantomScheduleSysAdmission(
        "concierge_uncommitted_role_" + String(reason || "route"),
      );
    }

    phantomEmitPresenceRoster("concierge_anchor");

    if (cfSelfOnly && doorwayMembershipAdvance) {
      emitSnapshot("doorway_self_only_held_for_committed_authority");
    }

    emitSnapshot("concierge_owner_" + ownerRole);
    return true;
  }

  function phantomMaybeRelinkSysconPair(reason = "syscon_relink") {
    const self = phantomShortKey(selfHKey);
    if (!self) return false;

    const role = String(concierge.localRoleCandidate || "peer");

    // Syscon pair pipe has one opener:
    // syscon1 dials syscon2.
    // syscon2 observes/accepts unless it has promoted to syscon1.
    if (role !== "syscon1") {
      const target = phantomShortKey(
        concierge.sysPeer || phantomSysTargetFromCandidates(),
      );
      if (!target || target === self) return false;

      const ent = phantomSysPeers.get(target);
      const dc = ent?.sysdc;

      if (dc && (dc.readyState === "open" || dc.readyState === "connecting")) {
        concierge.sysPeer = target;
        concierge.sysPipeState = dc.readyState;
        phantomRefreshSysReadiness("syscon_relink_observe_" + dc.readyState);
        emitSnapshot("syscon_relink_observe_" + dc.readyState);
        return true;
      }

      phantomRefreshSysReadiness("syscon_relink_observe_no_pipe");
      emitSnapshot("syscon_relink_observe_no_pipe");
      return false;
    }

    const target = phantomShortKey(concierge.syscon2);
    if (!target || target === self) return false;

    // Accepted terminal membership evidence preempts SYS rebirth while the
    // authority transaction is pending. The committed-removal owner decides
    // the final roster and teardown; transport cannot reopen this target.
    if (phantomSysReplacementTerminal(target)) {
      phantomSysDialing.delete(target);
      phantomSysPreflightUnstableRetries.delete(target);
      emitSnapshot("syscon_relink_blocked_terminal_exit_pending");
      return false;
    }

    const ent = phantomSysPeers.get(target);
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
        phantomRefreshSysReadiness("syscon_relink_primary_already_open");
        phantomRefreshOpenSysPeer("syscon_relink_primary_open");
        return true;
      }

      phantomRefreshSysReadiness("syscon_relink_primary_already_connecting");
      emitSnapshot("syscon_relink_primary_already_connecting");
      return true;
    }

    concierge.sysPeer = target;
    concierge.sysLastErr = null;

    phantomRefreshSysReadiness(
      "syscon_relink_primary_request_" + String(reason || "relink"),
    );
    emitSnapshot("syscon_relink_primary_request_" + String(reason || "relink"));

    return phantomEnsureSysPipe(
      target,
      "syscon1_pair_owner_" + String(reason || "relink"),
    );
  }

  function phantomRefreshOpenSysPeer(reason = "sys_refresh") {
    const self = phantomShortKey(selfHKey);

    const expected = phantomShortKey(phantomSysTargetFromCandidates());

    const candidates = [
      expected,
      concierge.sysPeer,
      concierge.syscon1,
      concierge.syscon2,
    ]
      .map(phantomShortKey)
      .filter((x) => x && x !== self);

    for (const hKey of Array.from(lastRoster || [])) {
      const key = phantomShortKey(hKey);
      if (key && key !== self && key !== expected) candidates.push(key);
    }

    let target = "";

    for (const key of [...new Set(candidates)]) {
      if (phantomHasOpenSysPipe(key)) {
        target = key;
        break;
      }
    }

    if (!target) {
      phantomRefreshSysReadiness(reason + "_no_open_sys_peer");
      emitSnapshot(reason + "_no_open_sys_peer");
      return false;
    }

    concierge.sysPeer = target;
    concierge.sysPipeState = "open";
    concierge.sysLastErr = null;

    const old = phantomPresenceBook.get(target) || {};
    phantomRememberPresencePeer(
      {
        ...old,
        hKey: target,
        from: target,
        videoId: videoId || null,
        t: now(),
      },
      "phantom_sys",
      {
        peerEvent: false,
        rosterEvent: false,
      },
    );

    phantomRefreshSysReadiness(reason);

    const readyTarget = phantomShortKey(concierge.sysReadyPeer);
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
      phantomSendSysDC(target, phantomSysStatusPayload("SYS_STATUS"));
      emitSnapshot("sys_status_sent_open_pipe_waiting_for_proof");
    }

    const msg = phantomPresencePayload();
    msg.to = target;
    phantomSendSysControl(msg, "presence");

    phantomEmitPresenceRoster("presence_phantom_sys");
    phantomRefreshSysReadiness(reason);
    emitSnapshot(reason);
    return true;
  }

  function phantomControlRouteFor(msg) {
    const type = String(msg?.type || "unknown");
    if(type==='rtc_route_hint')return {type,cls:'rtc_signal',mode:'bootstrap',route:'bootstrap_ws',
      reason:'bootstrap_route_hint',wouldRoute:'bootstrap_ws',wouldRouteReason:'bootstrap_route_hint'};
    const cls = phantomControlClassFor(type);
    const mode = phantomControlClassMode(cls, type);
    const target = phantomShortKey(msg?.to || concierge.sysPeer);

    const isRtcSignal =
      type === "rtc_offer" || type === "rtc_answer" || type === "rtc_ice";

    const isSysCarrier = !!(msg?.sysCarrier === true && isRtcSignal);

    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );

    const self = phantomShortKey(selfHKey);
    const boss = phantomShortKey(concierge.syscon1);
    const secondary = phantomShortKey(concierge.syscon2);
    const currentNeighbor = phantomShortKey(concierge.sysPeer);

    const targetSysOpen = target ? phantomHasOpenSysPipe(target) : false;

    const nextHop = targetSysOpen
      ? target
      : role === "syscon1"
        ? secondary
        : currentNeighbor || boss;

    const nextHopSysOpen = !!(
      nextHop &&
      nextHop !== self &&
      phantomHasOpenSysPipe(nextHop)
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
      !!phantomShortKey(concierge.sysPeer) &&
      phantomHasOpenSysPipe(concierge.sysPeer);

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
      route = "phantom_sys";
      reason = "sys_carrier_ignored_target_sys_ready";
      wouldRoute = "phantom_sys";
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
      wouldRoute = "phantom_sys";

      if (!concierge.sysEligible) {
        route = "phantom_sys_pending";
        reason =
          cls === "rtc_signal"
            ? `rtc_sys_pending_ineligible_${concierge.sysUnavailableReason || "unknown"}`
            : `sys_pending_ineligible_${concierge.sysUnavailableReason || "unknown"}`;
        wouldRouteReason = reason;
      } else if (cls === "relationship_control" && !target) {
        route = "phantom_sys_pending";
        reason = "relationship_missing_target";
        wouldRouteReason = reason;
      } else if (
        cls === "relationship_control" &&
        !nextHopSysOpen &&
        !syscon1CanHoldDirected
      ) {
        route = "phantom_sys_pending";
        reason = "relationship_waiting_for_sys_next_hop";
        wouldRouteReason = reason;
      } else if (cls !== "relationship_control" && !concierge.sysReady) {
        route = "phantom_sys_pending";
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
        route = "phantom_sys_pending";
        reason = `rtc_${type}_waiting_for_sys_next_hop`;
        wouldRouteReason = reason;
      } else if (
        nextHopSysOpen ||
        syscon1CanHoldDirected ||
        (cls === "presence" && ownerSysOpen)
      ) {
        route = "phantom_sys";
        reason =
          cls === "relationship_control"
            ? "peer_sys_route_relationship_control"
            : `sys_route_${cls}`;
        wouldRouteReason =
          cls === "relationship_control"
            ? "peer_sys_ready_route_relationship_control"
            : `sys_ready_route_${cls}`;
      } else {
        route = "phantom_sys_pending";
        reason = `sys_pending_${cls}`;
        wouldRouteReason = reason;
      }
    } else if (mode === "observe") {
      route = "phantom_sys_pending";
      reason = `observe_hold_${cls}`;
      wouldRoute = "phantom_sys";
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

  function phantomMaybeDelayRelationshipFallback(msg, type, routeReason) {
    if (!msg || typeof msg !== "object") return false;
    if (phantomControlClassFor(type) !== "relationship_control") return false;
    const target = phantomShortKey(
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
    const attemptTarget = phantomShortKey(attempt.target || concierge.sysPeer);

    if (attemptTarget && attemptTarget !== target) {
      return false;
    }

    if (phantomOwnsPrimarySysState(target)) {
      concierge.sysPeer = target;
    }

    const ent = phantomSysPeers.get(target);
    const dcState = String(ent?.sysdc?.readyState || "");

    if (dcState !== "open" && dcState !== "connecting") {
      emitSnapshot("relationship_pending_wait_no_birth_" + type);
    }

    concierge.lastErr = "relationship_waiting_for_sysdc:" + type;
    concierge.lastRelationshipTxAt = now();
    concierge.lastRelationshipTxType = type;
    concierge.lastRelationshipRoute = "phantom_sys_pending";
    concierge.lastRelationshipReason = "waiting_for_target_sysdc";

    emitSnapshot("relationship_waiting_for_sysdc_" + type);

    let pending = phantomPendingRelationship.get(target);

    if (!pending) {
      pending = [];
      phantomPendingRelationship.set(target, pending);
    }

    pending.push({
      msg: { ...msg },
      type,
      at: now(),
    });

    if (pending.length > PHANTOM_REL_PENDING_MAX) {
      pending.splice(0, pending.length - PHANTOM_REL_PENDING_MAX);
    }

    emitSnapshot("relationship_pending_for_sysdc_" + type);

    return true;
  }

  function phantomSendQueuedRtcIceFallbackWs(msg, why = "ice_queue_fallback_ws") {
    const type = "rtc_ice";

    bootstrap.cfHits.total++;
    bootstrap.cfHits.fallback++;
    bootstrap.cfHits.lastDir = "tx";
    bootstrap.cfHits.lastType = type;
    bootstrap.cfHits.lastBucket = "fallback";
    bootstrap.cfHits.lastReason = String(why || "ice_queue_fallback_ws");
    bootstrap.cfHits.lastAt = now();

    phantomMarkControlTx(type, "bootstrap_ws");

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

  function phantomFlushQueuedRtcIce(remoteHKey, reason = "rtc_ice_queue_flush") {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;

    const rec = phantomPendingRtcIce.get(key);
    if (!rec) return false;

    phantomPendingRtcIce.delete(key);

    const items = Array.isArray(rec.items) ? rec.items : [];
    if (!items.length) return false;

    if (!phantomHasOpenSysPipe(key)) {
      const ent = phantomSysPeers.get(key);
      const sysState = String(ent?.sysdc?.readyState || "");

      if (sysState === "connecting" || phantomSysDialing.has(key)) {
        const current = phantomShortKey(concierge.sysPeer);
        const readyPeer = phantomShortKey(concierge.sysReadyPeer);
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

      phantomPendingRtcIce.set(key, {
        items,
      });

      concierge.lastRtcIceTxAt = now();
      concierge.lastRtcIceTxRoute = "phantom_sys_pending";
      concierge.lastRtcIceTxMode = "sys_primary";
      concierge.lastRtcIceTxReason = "rtc_ice_queue_waiting_for_sys_pipe";

      concierge.lastRtcTxAt = now();
      concierge.lastRtcTxType = "rtc_ice";
      concierge.lastRtcTxRoute = "phantom_sys_pending";
      concierge.lastRtcTxMode = "sys_primary";
      concierge.lastRtcTxReason = "rtc_ice_queue_waiting_for_sys_pipe";

      concierge.lastErr = "rtc_ice_queue_waiting_for_sys_pipe";

      phantomRefreshSysReadiness("rtc_ice_queue_waiting_for_sys_pipe");
      emitSnapshot("rtc_ice_queue_waiting_for_sys_pipe");
      return true;
    }

    const held = [];

    for (const item of items) {
      phantomMarkControlTx("rtc_ice", "phantom_sys");

      concierge.routeReason = "sys_route_rtc_signal_queued";
      concierge.lastRouteAt = now();
      concierge.lastRouteType = "rtc_ice";
      concierge.lastRouteReason = concierge.routeReason;
      concierge.lastRouteClass = "rtc_signal";
      concierge.routeMode = "sys_primary";
      concierge.lastRouteMode = "sys_primary";
      concierge.wouldRoute = "phantom_sys";
      concierge.wouldRouteReason = "sys_ready_route_rtc_signal_queued";

      concierge.lastRtcTxAt = now();
      concierge.lastRtcTxType = "rtc_ice";
      concierge.lastRtcTxRoute = "phantom_sys";
      concierge.lastRtcTxMode = "sys_primary";
      concierge.lastRtcTxReason = concierge.routeReason;

      concierge.lastRtcIceTxAt = now();
      concierge.lastRtcIceTxRoute = "phantom_sys";
      concierge.lastRtcIceTxMode = "sys_primary";
      concierge.lastRtcIceTxReason = concierge.routeReason;

      const ok = phantomSendSysControl(item.msg, "rtc_ice");

      if (ok) {
        concierge.lastErr = null;
      } else {
        held.push(item);
      }
    }

    if (held.length) {
      phantomPendingRtcIce.set(key, {
        items: held.slice(-PHANTOM_RTC_ICE_QUEUE_MAX),
      });

      concierge.lastErr = "rtc_ice_queue_sys_send_failed_held";
      emitSnapshot("rtc_ice_queue_sys_send_failed_held");
      return false;
    }

    emitSnapshot(String(reason || "rtc_ice_queue_flush"));
    return true;
  }

  function phantomMaybeQueueRtcIceFallback(msg, type, routeReason) {
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

    const target = phantomShortKey(msg.to || concierge.sysPeer);
    if (!target) return false;

    if (phantomHasOpenSysPipe(target)) return false;

    phantomSendSysAdmissionPresence("rtc_ice_queue", target);

    const ent = phantomSysPeers.get(target);
    const sysState = String(ent?.sysdc?.readyState || "");

    if (sysState === "connecting") {
      const current = phantomShortKey(concierge.sysPeer);
      const readyPeer = phantomShortKey(concierge.sysReadyPeer);
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
        phantomRefreshSysReadiness("rtc_ice_queue_target_sys_connecting");
      } else {
        emitSnapshot("rtc_ice_queue_target_sys_connecting_observed");
      }
    } else if (!sysState || sysState === "closed") {
      emitSnapshot("rtc_ice_queue_target_pipe_missing");
    }

    let rec = phantomPendingRtcIce.get(target);

    if (!rec) {
      rec = {
        items: [],
      };
      phantomPendingRtcIce.set(target, rec);
    }

    rec.items.push({
      msg: { ...msg },
      at: now(),
    });

    if (rec.items.length > PHANTOM_RTC_ICE_QUEUE_MAX) {
      rec.items.splice(0, rec.items.length - PHANTOM_RTC_ICE_QUEUE_MAX);
    }

    concierge.lastRtcIceTxAt = now();
    concierge.lastRtcIceTxRoute = "phantom_sys_pending";
    concierge.lastRtcIceTxMode = "sys_primary";
    concierge.lastRtcIceTxReason = "rtc_ice_waiting_for_target_sys_pipe";

    concierge.lastRtcTxAt = now();
    concierge.lastRtcTxType = "rtc_ice";
    concierge.lastRtcTxRoute = "phantom_sys_pending";
    concierge.lastRtcTxMode = "sys_primary";
    concierge.lastRtcTxReason = "rtc_ice_waiting_for_target_sys_pipe";

    concierge.lastErr = "rtc_ice_waiting_for_sys_pipe";

    emitSnapshot("rtc_ice_queued_for_sys");
    return true;
  }

  function phantomSendControl(msg, reason = "") {
    if (!msg || typeof msg !== "object") return false;

    const decision = phantomControlRouteFor(msg);
    const type = String(decision.type || msg.type || reason || "unknown");
    const route = String(decision.route || "bootstrap_ws");

    if (route === "bootstrap_ws") {
      const cfReason = String(decision.reason || reason || route);
      let cfBucket = "fallback";

      const cfTarget = phantomShortKey(msg?.to || concierge.sysPeer);
      const cfIsRtcSignal =
        type === "rtc_offer" || type === "rtc_answer" || type === "rtc_ice";
      const cfTargetSysOpen = cfTarget ? phantomHasOpenSysPipe(cfTarget) : false;
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
      phantomPrepareRelationshipControl(msg, type);
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

    phantomMarkControlTx(type, route);

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

    if (route === "phantom_sys_pending") {
      // Directed chat signaling can enter the pending route before
      // phantomSendSysControl is called at all. Preserve the whole sequence here,
      // not just ICE, so offer/answer cannot disappear or be overtaken.
      if (decision.cls === "rtc_signal" && msg?.sysCarrier !== true) {
        const target = phantomShortKey(msg.to || concierge.sysPeer);

        if (target) {
          const envelope = {
            t: "SYS_CONTROL",
            at: now(),
            from: phantomShortKey(selfHKey),
            room: videoId || null,
            kind: type,
            cid: msg.cid || null,
            msg,
          };

          const held = phantomQueueDirectedSysControl(
            target,
            envelope,
            "phantom_sys_pending_" + type,
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
        return phantomMaybeQueueRtcIceFallback(msg, type, concierge.routeReason);
      }

      if (decision.cls === "relationship_control") {
        const target = phantomShortKey(msg.to || concierge.sysPeer);

        if (target) {
          const normalSysTarget = phantomShortKey(phantomSysTargetFromCandidates());
          const temporaryUnblock = !!(
            type === "sys_unblock" &&
            !normalSysTarget &&
            !phantomHasOpenSysPipe(concierge.sysPeer)
          );
          const queuePeer = phantomShortKey(
            (type === "sys_unblock" || type === "sys_unblock_ack") &&
              normalSysTarget
              ? normalSysTarget
              : target,
          );

          if (temporaryUnblock) {
            msg.sysTxId = String(msg.sysTxId || msg.cid || "") || null;
            msg.recoveryProbe = true;
          }

          let q = phantomPendingRelationship.get(queuePeer);
          if (!q) {
            q = [];
            phantomPendingRelationship.set(queuePeer, q);
          }

          q.push({ msg: { ...msg }, type, at: now() });

          if (q.length > PHANTOM_REL_PENDING_MAX) {
            q.splice(0, q.length - PHANTOM_REL_PENDING_MAX);
          }

          if (type === "sys_unblock") {
            if (temporaryUnblock) {
              phantomStartTemporarySysTransaction(target, msg);
            } else {
              phantomScheduleSysAdmission("sys_unblock_waiting_for_room_sys");
            }
          }

          emitSnapshot(
            "relationship_queued_waiting_for_existing_sys_pipe_" + type,
          );

          concierge.lastRelationshipRoute = "phantom_sys_pending";
          concierge.lastRelationshipReason = concierge.routeReason;
          concierge.lastErr = "relationship_waiting_for_peer_sys_pipe:" + type;
          emitSnapshot("relationship_queued_for_sys_" + type);
          return true;
        }
      }

      if (decision.cls === "presence") {
        const target = phantomShortKey(msg.to || concierge.sysPeer);

        if (target) {
          const ent = phantomSysPeers.get(target);
          const sysState = String(ent?.sysdc?.readyState || "");

          if (sysState === "connecting" || phantomSysDialing.has(target)) {
            phantomRefreshSysReadiness("presence_waiting_for_sys_pipe");
            emitSnapshot("presence_waiting_for_sys_pipe");
          } else {
            emitSnapshot("presence_waiting_for_sys_pipe_no_birth");
          }
        } else {
          phantomScheduleSysAdmission("presence_waiting_for_sys_target");
        }

        concierge.lastErr = "presence_waiting_for_sys_pipe";
        return false;
      }

      concierge.lastErr = "sys_primary_waiting_for_pipe:" + type;
      emitSnapshot("sys_primary_waiting_for_pipe_" + type);
      return false;
    }

    if (route === "phantom_sys") {
      const ok = phantomSendSysControl(msg, type);
      if (ok) return true;

      concierge.lastErr = "sys_route_failed_no_ws_fallback:" + type;

      if (decision.cls === "relationship_control") {
        const target = phantomShortKey(msg.to || concierge.sysPeer);

        if (type === "veil_typing") {
          concierge.lastErr = "typing_dropped_no_open_sys_route";
          emitSnapshot("typing_dropped_no_open_sys_route");
          return false;
        }

        if (target) {
          const normalSysTarget = phantomShortKey(phantomSysTargetFromCandidates());
          const temporaryUnblock = !!(
            type === "sys_unblock" &&
            !normalSysTarget &&
            !phantomHasOpenSysPipe(concierge.sysPeer)
          );
          const queuePeer = phantomShortKey(
            (type === "sys_unblock" || type === "sys_unblock_ack") &&
              normalSysTarget
              ? normalSysTarget
              : target,
          );

          if (temporaryUnblock) {
            msg.sysTxId = String(msg.sysTxId || msg.cid || "") || null;
            msg.recoveryProbe = true;
          }

          let q = phantomPendingRelationship.get(queuePeer);
          if (!q) {
            q = [];
            phantomPendingRelationship.set(queuePeer, q);
          }

          q.push({ msg: { ...msg }, type, at: now() });

          if (q.length > PHANTOM_REL_PENDING_MAX) {
            q.splice(0, q.length - PHANTOM_REL_PENDING_MAX);
          }

          if (type === "sys_unblock") {
            if (temporaryUnblock) {
              phantomStartTemporarySysTransaction(target, msg);
            } else {
              phantomScheduleSysAdmission("sys_unblock_requeued_room_sys");
            }
          }

          if (type === "veil_invite") {
            emitSnapshot("relationship_sys_send_failed_requeue_no_birth");
          } else {
            emitSnapshot("relationship_requeued_no_birth_" + type);
          }

          concierge.lastRelationshipRoute = "phantom_sys_pending";
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
  let phantomSessionKilling = false;
  let lastRosterRequestAt = 0;
  const ROSTER_MIN_INTERVAL = 15000; // 15s
  let lastRoster = new Set();
  const phantomPresenceBook = new Map(); // hKey -> latest presence-ish peer row

  let phantomRosterAudit = {
    source: "none",
    count: 0,
    added: "none",
    removed: "none",
    pruneReason: "none",
    at: 0,
  };

  function phantomMarkRosterAudit(source, nextKeys, pruneReason = "none") {
    const prev = lastRoster instanceof Set ? lastRoster : new Set();
    const next = new Set(
      Array.from(nextKeys || [])
        .map(phantomShortKey)
        .filter(Boolean),
    );

    const added = Array.from(next).find((k) => !prev.has(k)) || "none";
    const removed = Array.from(prev).find((k) => !next.has(k)) || "none";

    phantomRosterAudit = {
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

  function phantomLatchedSysRosterList() {
    const self = phantomShortKey(selfHKey);
    const authority = phantomLocalCommittedAuthority();

    const source =
      authority && Array.isArray(authority.roster) ? authority.roster : [];

    const rows = [];

    for (const row of source) {
      const key = phantomShortKey(row?.hKey || row?.from);
      if (!key) continue;

      const presence = phantomPresenceBook.get(key) || {};

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
  // Peer WebRTC data channels owned by the engine.
  // Signaling crosses the host interface through:
  //   post("RTC_SEND", {to, kind, data})
  // and inbound:
  //   d.type === "RTC_SIGNAL" with payload {from, kind, data}
  // ======================================================

  // NOTE: STUN is optional. With empty iceServers, some NAT pairs won't connect.
  const RTC_CFG = host.rtcConfiguration;

  // remoteHKey -> { pc, dc }
  const phantomSysPeers = new Map();
  const phantomChatPeers = new Map();

  // ======================================================
  // CONCIERGE / SYS preflight
  // Establish the system channel before invite, accept, or block traffic.
  // ======================================================

  const phantomSysDialing = new Set();
  const phantomSysPreflightUnstableRetries = new Map();
  const PHANTOM_SYS_PREFLIGHT_UNSTABLE_MAX = 6;
  const phantomSysAdmissionLastSent = new Map();
  const phantomSysLastGoodRx = new Map();

  const phantomPeerSysCarrier = {
    epoch: 0,
    target: null,
    primaryRecoveryUsed: false,
    authorityRefreshPending: false,
  };

  let phantomLastSysFallbackIndicator = "unknown";

  function phantomRefreshSysFallbackIndicator() {
    if (phantomLastSysFallbackIndicator === "off") return;

    phantomLastSysFallbackIndicator = "off";

    post("SYS_FALLBACK_STATE", {
      state: "off",
      epoch: Number(concierge.sysconEpoch || 0),
      target: null,
      syscon2: null,
      at: now(),
    });
  }

  const phantomSysAuthority = {
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
  const phantomMembershipAudit = {
    at: 0,
    stage: "idle",
    peer: null,
    id: "none",
    detail: "none",
  };

  function phantomMembershipAuditToken(value) {
    const text = String(value || "");
    if (!text) return "none";

    let hash = 2166136261;
    for (let i = 0; i < text.length; i++) {
      hash ^= text.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }

    return (hash >>> 0).toString(16).padStart(8, "0");
  }

  function phantomMembershipAuditSet(stage, fields = {}) {
    const peer = phantomShortKey(fields.peer || fields.from || fields.transport);
    const id = fields.id ? phantomMembershipAuditToken(fields.id) : "none";
    const topology = phantomMembershipAuditTopology();

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

    phantomMembershipAudit.at = at;
    phantomMembershipAudit.stage = String(stage || "unknown");
    phantomMembershipAudit.peer = peer || null;
    phantomMembershipAudit.id = id;
    phantomMembershipAudit.detail = detail || "none";

    phantomMembershipAuditTrace.push({
      at,
      stage: phantomMembershipAudit.stage,
      peer: phantomMembershipAudit.peer,
      id: phantomMembershipAudit.id,
      detail: phantomMembershipAudit.detail,
    });

    if (phantomMembershipAuditTrace.length > PHANTOM_MEMBERSHIP_AUDIT_TRACE_MAX) {
      phantomMembershipAuditTrace.splice(
        0,
        phantomMembershipAuditTrace.length - PHANTOM_MEMBERSHIP_AUDIT_TRACE_MAX,
      );
    }

    emitSnapshot("membership_audit_" + phantomMembershipAudit.stage);
  }

  function phantomMembershipAuditText() {
    if (!phantomMembershipAudit.at) return "";

    const peer = phantomMembershipAudit.peer
      ? phantomMembershipAudit.peer.slice(0, 8)
      : "none";
    const trace = phantomMembershipAuditTraceText();

    return [
      "MEMAUDIT",
      phantomMembershipAudit.stage,
      `peer=${peer}`,
      `id=${phantomMembershipAudit.id || "none"}`,
      phantomMembershipAudit.detail || "none",
      trace ? `TRACE[${trace}]` : "",
      phantomChatSignalAuditText()
        ? `CHATTRACE[${phantomChatSignalAuditText()}]`
        : "",
    ]
      .filter(Boolean)
      .join(" ");
  }

  const phantomMembershipAuditTrace = [];
  const PHANTOM_MEMBERSHIP_AUDIT_TRACE_MAX = 16;
  const phantomChatSignalTrace = [];
  const PHANTOM_CHAT_SIGNAL_TRACE_MAX = 20;

  function phantomChatSignalAudit(stage, fields = {}) {
    const row = {
      at: now(),
      stage: String(stage || "unknown"),
      peer:
        phantomShortKey(fields.peer || fields.to || fields.from)?.slice(0, 8) ||
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

    phantomChatSignalTrace.push(row);

    if (phantomChatSignalTrace.length > PHANTOM_CHAT_SIGNAL_TRACE_MAX) {
      phantomChatSignalTrace.splice(
        0,
        phantomChatSignalTrace.length - PHANTOM_CHAT_SIGNAL_TRACE_MAX,
      );
    }

    emitSnapshot("chat_signal_audit_" + row.stage);
  }

  function phantomChatSignalAuditText() {
    if (!phantomChatSignalTrace.length) return "";

    return phantomChatSignalTrace
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

  function phantomMembershipAuditTopology() {
    const self = phantomShortKey(selfHKey);
    const committedRows = phantomAuthorityRosterRows(phantomSysAuthority.roster);
    const pending = phantomSysAuthority.pending;
    const pendingRows = phantomAuthorityRosterRows(pending?.snapshot?.roster);

    return {
      self: self ? self.slice(0, 8) : "none",
      role: String(concierge.localRoleCandidate || concierge.role || "peer"),
      epoch: Number(concierge.sysconEpoch || 0),
      cfEpoch: Number(concierge.cfConciergeEpoch || 0),

      committedFrom: phantomShortKey(phantomSysAuthority.from)?.slice(0, 8) || "none",
      committedOne:
        phantomShortKey(phantomSysAuthority.syscon1)?.slice(0, 8) || "none",
      committedTwo:
        phantomShortKey(phantomSysAuthority.syscon2)?.slice(0, 8) || "none",
      committedEpoch: Number(phantomSysAuthority.epoch || 0),
      committedSeq: Number(phantomSysAuthority.rosterSeq || 0),
      committedCount: committedRows.length,
      committedKeys: committedRows.map((row) => row.hKey.slice(0, 8)).join(","),

      pendingId: pending?.id ? phantomMembershipAuditToken(pending.id) : "none",
      pendingMirror:
        phantomShortKey(pending?.mirror || pending?.snapshot?.syscon2)?.slice(
          0,
          8,
        ) || "none",
      pendingEpoch: Number(pending?.snapshot?.epoch || 0),
      pendingBaseEpoch: Number(pending?.baseEpoch || 0),
      pendingBaseSeq: Number(pending?.baseRosterSeq || 0),
      pendingNextSeq: Number(pending?.snapshot?.rosterSeq || 0),
      pendingCount: pendingRows.length,
      pendingKeys: pendingRows.map((row) => row.hKey.slice(0, 8)).join(","),

      cfOne: phantomShortKey(concierge.cfConciergeSyscon1)?.slice(0, 8) || "none",
      cfTwo: phantomShortKey(concierge.cfConciergeSyscon2)?.slice(0, 8) || "none",

      terminalKeys: Array.from(phantomPendingTerminalExits.keys())
        .map((key) => phantomShortKey(key)?.slice(0, 8))
        .filter(Boolean)
        .join(","),

      sysTarget:
        phantomShortKey(concierge.sysAttempt?.target)?.slice(0, 8) || "none",
      sysAttempt: String(concierge.sysAttempt?.state || "none"),
      sysPipe: String(concierge.sysPipeState || "none"),
    };
  }

  function phantomMembershipAuditTraceText() {
    if (!phantomMembershipAuditTrace.length) return "";

    return phantomMembershipAuditTrace
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

  const PHANTOM_CHAT_RECONNECT_MS = 20000;
  const phantomChatReconnectDeadlines = new Map();
  let phantomChatAttemptSeq = 0;

  function phantomNewChatAttemptId(remoteHKey) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return "";
    phantomChatAttemptSeq += 1;
    return [phantomBootId, key, phantomChatAttemptSeq].join(":");
  }

  function phantomCancelChatAttemptDeadline(remoteHKey) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return;
    const timer = phantomChatReconnectDeadlines.get(key);
    if (timer) clearTimeout(timer);
    phantomChatReconnectDeadlines.delete(key);
  }

  function phantomArmChatAttemptDeadline(
    remoteHKey,
    attemptId,
    reason = "chat_birth",
  ) {
    const key = phantomShortKey(remoteHKey);
    if (!key || !attemptId) return false;

    phantomCancelChatAttemptDeadline(key);

    const timer = setTimeout(() => {
      phantomChatReconnectDeadlines.delete(key);

      const ent = phantomChatPeers.get(key);
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

      const relationship = phantomRelationshipGet(key);
      if (
        !phantomRelationshipIsAccepted(relationship) ||
        !lastRoster.has(key) ||
        phantomPendingTerminalExits.has(key) ||
        __phantomGoneNotified.has(key)
      ) {
        emitSnapshot("chat_attempt_deadline_observed_terminal_relationship");
        return;
      }

      if (!phantomOwnsChatPipeOffer(key)) {
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

      const successor = phantomGetOrCreateChatPeer(key);
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
    }, PHANTOM_CHAT_RECONNECT_MS);

    phantomChatReconnectDeadlines.set(key, timer);
    return true;
  }

  const PHANTOM_SYS_REPLACEMENT_MS = 10000;
  const phantomSysReplacementWindows = new Map(); // hKey -> { state, timer, attemptId, bootId, startedAt, terminalAt, committedAt }

  function phantomSysReplacementState(remoteHKey) {
    const key = phantomShortKey(remoteHKey);
    return key
      ? String(phantomSysReplacementWindows.get(key)?.state || "healthy")
      : "healthy";
  }

  function phantomSysReplacementTerminal(remoteHKey) {
    const state = phantomSysReplacementState(remoteHKey);
    return state === "terminal_staged" || state === "committed";
  }

  function phantomSupersedeSysReplacementForCommittedRefresh(
    remoteHKey,
    previousGeneration,
    incomingGeneration,
  ) {
    const key = phantomShortKey(remoteHKey);
    if (
      !key ||
      !previousGeneration ||
      !incomingGeneration ||
      previousGeneration === incomingGeneration
    ) {
      return false;
    }

    const committed = phantomLocalCommittedAuthority();
    const committedKeys = new Set(
      phantomAuthorityRosterRows(committed?.roster).map((row) => row.hKey),
    );
    if (!committedKeys.has(key)) return false;

    const rec = phantomSysReplacementWindows.get(key);
    if (
      rec &&
      (String(rec.state || "") === "replacing" ||
        String(rec.state || "") === "awaiting_doorway_absence")
    ) {
      try {
        if (rec.timer) clearTimeout(rec.timer);
      } catch {}
      phantomSysReplacementWindows.delete(key);
    }

    phantomSysDialing.delete(key);
    phantomSysPreflightUnstableRetries.delete(key);
    phantomPendingRtcIce.delete(key);

    if (phantomShortKey(concierge.sysAttempt?.target) === key) {
      concierge.sysAttempt.recoveryCount = 0;
      concierge.sysAttempt.state = "connecting";
      concierge.sysAttempt.lastEvent = "committed_member_new_boot";
      concierge.sysAttempt.faultReason = "none";
    }

    emitSnapshot("sys_replacement_superseded_committed_refresh");
    return true;
  }
  function phantomCancelSysReplacement(
    remoteHKey,
    reason = "replacement_cancelled",
  ) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;

    const rec = phantomSysReplacementWindows.get(key);
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

    phantomSysReplacementWindows.delete(key);

    if (phantomShortKey(concierge.sysAttempt?.target) === key) {
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
  function phantomCompleteFailedSysReplacement(
    remoteHKey,
    reason = "replacement_failed",
  ) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;

    const rec = phantomSysReplacementWindows.get(key);
    const state = String(rec?.state || "");
    if (
      !rec ||
      (state !== "replacing" && state !== "awaiting_doorway_absence")
    ) {
      return false;
    }

    const self = phantomShortKey(selfHKey);
    const committed = phantomLocalCommittedAuthority();
    const committedKeys = new Set(
      phantomAuthorityRosterRows(committed?.roster).map((row) => row.hKey),
    );
    const mode = String(rec.mode || "");
    const leaderOwnsMember = !!(
      mode === "leader_observes_member" &&
      self &&
      phantomShortKey(committed?.syscon1) === self &&
      committedKeys.has(key)
    );
    const mirrorOwnsLeaderLoss = !!(
      mode === "mirror_observes_leader" &&
      self &&
      phantomShortKey(committed?.syscon2) === self &&
      phantomShortKey(committed?.syscon1) === key &&
      committedKeys.has(key)
    );
    const currentRole = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const currentOne = phantomShortKey(concierge.syscon1);
    const successorOwnsSpineLoss = !!(
      mode === "successor_observes_spine" &&
      self &&
      currentRole === "syscon1" &&
      currentOne === self &&
      phantomShortKey(committed?.syscon1) !== self &&
      committedKeys.has(self) &&
      committedKeys.has(key) &&
      (phantomShortKey(committed?.syscon1) === key ||
        phantomShortKey(committed?.syscon2) === key)
    );

    const restored = phantomHasOpenSysPipe(key);
    const knownBoot = String(phantomRemoteBootIds.get(key) || "");
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
      __phantomGoneNotified.has(key) ||
      (!leaderOwnsMember && !mirrorOwnsLeaderLoss && !successorOwnsSpineLoss)
    ) {
      phantomCancelSysReplacement(
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
    if (elapsed < PHANTOM_SYS_REPLACEMENT_MS) {
      // Timers can wake before the wall-clock deadline. Keep the same recovery
      // window alive until its full grace period has elapsed.
      if (rec.timer) clearTimeout(rec.timer);
      rec.timer = setTimeout(() => {
        phantomCompleteFailedSysReplacement(key, "replacement_window_expired");
      }, PHANTOM_SYS_REPLACEMENT_MS - elapsed);
      post("SYS_RECOVERY_TIMER", {peer:key, action:"rearmed", elapsedMs:elapsed,
        remainingMs:PHANTOM_SYS_REPLACEMENT_MS-elapsed});
      return false;
    }

    // Doorway visibility is corroborating liveness only. It never mutates
    // membership; it merely prevents a transient SYS fault from becoming a
    // terminal command while the same incarnation still owns a live socket.
    if (host.doorwayPresence === 'live-sockets' && phantomDoorwayAdmittedPeers.has(key)) {
      try {
        if (rec.timer) clearTimeout(rec.timer);
      } catch {}
      rec.timer = null;
      rec.state = "awaiting_doorway_absence";
      rec.awaitingDoorwayAt = now();
      phantomSysReplacementWindows.set(key, rec);
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
    phantomSysReplacementWindows.set(key, rec);

    if (phantomShortKey(concierge.sysAttempt?.target) === key) {
      concierge.sysAttempt.state = "terminal";
      concierge.sysAttempt.lastEvent = "replacement_failed_terminal";
      concierge.sysAttempt.faultReason = String(reason || "replacement_failed");
      concierge.sysAttempt.recoveryCount = 1;
    }

    phantomTeardownSysPeer(
      key,
      "sys_replacement_terminal_" + String(reason || "failed"),
    );

    const terminal = phantomTerminalExitCommand(
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

    const accepted = terminal || phantomPendingTerminalExits.has(key);
    if (accepted) {
      phantomSysDialing.delete(key);
      phantomSysPreflightUnstableRetries.delete(key);
      phantomPendingRtcIce.delete(key);
    }

    emitSnapshot(
      accepted
        ? "sys_replacement_terminal_exit_recorded"
        : "sys_replacement_terminal_exit_failed",
    );
    return accepted;
  }
  function phantomArmSysReplacement(remoteHKey, reason = "sys_transport_lost", outageStartedAt = null) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;

    const currentState = phantomSysReplacementState(key);
    if (
      currentState === "replacing" ||
      currentState === "awaiting_doorway_absence" ||
      currentState === "terminal_staged" ||
      currentState === "committed"
    ) {
      emitSnapshot("sys_replacement_arm_ignored_state_" + currentState);
      return false;
    }

    const self = phantomShortKey(selfHKey);
    const committed = phantomLocalCommittedAuthority();
    const committedKeys = new Set(
      phantomAuthorityRosterRows(committed?.roster).map((row) => row.hKey),
    );
    const committedOne = phantomShortKey(committed?.syscon1);
    const committedTwo = phantomShortKey(committed?.syscon2);

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
    const currentOne = phantomShortKey(concierge.syscon1);
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
      phantomSysPeers.get(key)?.attempt?.id || concierge.sysAttempt?.id || "",
    );
    const bootId = String(phantomRemoteBootIds.get(key) || "");
    const observedAt = now();
    const startedAt = Number.isFinite(outageStartedAt) && outageStartedAt <= observedAt
      ? outageStartedAt : observedAt;
    const remainingMs = Math.max(0, PHANTOM_SYS_REPLACEMENT_MS - (observedAt - startedAt));
    const timer = setTimeout(() => {
      phantomCompleteFailedSysReplacement(key, "replacement_window_expired");
    }, remainingMs);
    post("SYS_RECOVERY_WINDOW", {peer:key, mode, reason, observedAt, startedAt,
      outageMs:observedAt-startedAt, remainingMs, attemptId, bootId});

    phantomSysReplacementWindows.set(key, {
      state: "replacing",
      mode,
      timer,
      attemptId,
      bootId,
      startedAt,
      reason: String(reason || "sys_transport_lost"),
    });

    if (phantomShortKey(concierge.sysAttempt?.target) === key) {
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
  const phantomRelationships = new Map(); // hKey -> relationship record

  const phantomRelationshipRevoked = new Set();

  function phantomRelationshipCycleId(peer, seed = "cycle") {
    const self = phantomShortKey(selfHKey) || "self";
    const remote = phantomShortKey(peer) || "peer";
    return `${[self, remote].sort().join(":")}:${String(seed || "cycle")}:${now()}:${Math.random().toString(16).slice(2)}`;
  }

  function phantomRelationshipGet(hKey) {
    const key = phantomShortKey(hKey);
    if (!key) return null;

    let rec = phantomRelationships.get(key);
    if (!rec) {
      rec = {
        hKey: key,
        state: VEIL.OUTSIDE,
        cycleId: null,
        inviteCid: null,
        localBootId: phantomBootId,
        remoteBootId: String(phantomRemoteBootIds.get(key) || "") || null,
        membershipEpoch: Number(concierge.sysconEpoch || 0),
        createdAt: 0,
        updatedAt: now(),
        endedAt: 0,
        endReason: "none",
      };
      phantomRelationships.set(key, rec);
    }
    return rec;
  }

  function phantomRelationshipIsAccepted(rec) {
    return (
      !!rec &&
      !rec.endedAt &&
      (rec.state === VEIL.INSIDE || rec.state === VEIL.ESTABLISHED)
    );
  }

  function phantomRelationshipBegin(hKey, state, options = {}) {
    const key = phantomShortKey(hKey);
    if (!key) return null;

    const at = Number(options.at || now());
    const rec = phantomRelationshipGet(key);
    rec.state = String(state || VEIL.OUTSIDE);
    rec.cycleId = String(
      options.cycleId ||
        phantomRelationshipCycleId(key, options.origin || state || "cycle"),
    );
    rec.inviteCid = String(options.inviteCid || "") || null;
    rec.localBootId = phantomBootId;
    rec.remoteBootId =
      String(options.remoteBootId || phantomRemoteBootIds.get(key) || "") || null;
    rec.membershipEpoch = Number(
      options.membershipEpoch || concierge.sysconEpoch || 0,
    );
    rec.createdAt = at;
    rec.updatedAt = at;
    rec.endedAt = 0;
    rec.endReason = "none";
    phantomRelationships.set(key, rec);
    return rec;
  }

  function phantomRelationshipEnd(
    hKey,
    reason = "ended",
    at = now(),
    revoke = true,
  ) {
    const key = phantomShortKey(hKey);
    if (!key) return null;

    const rec = phantomRelationshipGet(key);
    rec.state = VEIL.OUTSIDE;
    rec.updatedAt = Number(at || now());
    rec.endedAt = rec.updatedAt;
    rec.endReason = String(reason || "ended");
    phantomRelationships.set(key, rec);

    if (revoke) phantomRelationshipRevoked.add(key);
    return rec;
  }

  function phantomRelationshipMatches(msg, rec) {
    if (!rec || !rec.cycleId) return false;
    const cycleId = String(msg?.cycleId || "");
    if (cycleId && cycleId === String(rec.cycleId)) return true;

    const replyTo = String(msg?.replyTo || "");
    if (replyTo && rec.inviteCid && replyTo === String(rec.inviteCid))
      return true;

    return false;
  }

  const phantomBlockedPeers = new Set(); // peers I blocked
  const phantomBlockedByPeers = new Set(); // peers who blocked me

  const phantomSeenControlCids = new Map();
  const PHANTOM_CONTROL_CID_MAX = 300;

  function phantomNewControlCid(type) {
    const me = phantomShortKey(selfHKey) || "self";
    return `${me}:${String(type || "control")}:${now()}:${Math.random().toString(16).slice(2)}`;
  }

  function phantomPrepareRelationshipControl(msg, type) {
    if (!msg || typeof msg !== "object") return msg;

    msg.type = String(msg.type || type || "unknown");
    msg.from = phantomShortKey(msg.from || selfHKey);
    msg.to = phantomShortKey(msg.to);
    msg.videoId = msg.videoId || videoId || null;
    msg.cid = msg.cid || phantomNewControlCid(msg.type);
    msg.sentAt = msg.sentAt || now();

    const relationship = phantomRelationships.get(msg.to);
    if (relationship?.cycleId && !msg.cycleId) {
      msg.cycleId = relationship.cycleId;
    }

    if (msg.type === "sys_unblock" || msg.type === "sys_unblock_ack") {
      msg.sysTxId = String(msg.sysTxId || msg.ackFor || msg.cid || "") || null;
    }

    return msg;
  }

  function phantomAuthorityGossipSnapshot() {
    const authority = phantomLocalCommittedAuthority();
    if (!authority) return null;

    return {
      gossip: true,
      room: String(authority.room || videoId || ""),
      from: phantomShortKey(authority.from || authority.syscon1),
      role: "syscon1",
      syscon1: phantomShortKey(authority.syscon1),
      syscon2: phantomShortKey(authority.syscon2) || null,
      epoch: Number(authority.epoch || 0),
      rosterSeq: Number(authority.rosterSeq || 0),
      roster: phantomAuthorityRosterRows(authority.roster),
      acceptedAt: Number(authority.at || 0),
      relayedAt: now(),
    };
  }

  function phantomRememberAuthorityGossip(sourceHKey, hint) {
    const source = phantomShortKey(sourceHKey);
    const self = phantomShortKey(selfHKey);
    if (!source || !self || !hint || hint.gossip !== true) return false;

    const room = String(hint.room || "");
    const one = phantomShortKey(hint.syscon1);
    const two = phantomShortKey(hint.syscon2);
    const from = phantomShortKey(hint.from || one);
    const epoch = Number(hint.epoch || 0);
    const rosterSeq = Number(hint.rosterSeq || 0);
    const roster = phantomAuthorityRosterRows(hint.roster);
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

    phantomAuthorityGossipHints.set(source, normalized);

    // Local committed authority remains the only source of truth. Gossip
    // contributes route hints only; it never replaces the committed value.
    const localAuthority = phantomLocalCommittedAuthority();
    if (localAuthority) {
      phantomRefreshSysCandidates(
        phantomLatchedSysRosterList(),
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
      ? phantomShortKey(localAuthority?.syscon1)
      : one;
    const routeTwo = localIsAtLeastAsFresh
      ? phantomShortKey(localAuthority?.syscon2)
      : two;

    const targets = [routeOne, routeTwo]
      .map(phantomShortKey)
      .filter(
        (key, index, rows) =>
          key && key !== self && rows.indexOf(key) === index,
      );

    phantomGossipRecovery = {
      source,
      targets,
      index: 0,
      epoch,
      rosterSeq,
      at: now(),
    };

    if (targets.length) {
      phantomScheduleSysAdmission("authority_gossip_recovery_target");
      emitSnapshot("authority_gossip_recovery_started");
    } else {
      emitSnapshot("authority_gossip_no_remote_syscon_target");
    }

    return true;
  }

  function phantomBeginGossipSuccessorRecovery(
    reason = "authority_gossip_syscons_unreachable",
  ) {
    const self = phantomShortKey(selfHKey);
    const authority = phantomLocalCommittedAuthority();
    if (!self || !authority) {
      emitSnapshot("authority_gossip_successor_missing_committed_base");
      return false;
    }

    const oldOne = phantomShortKey(authority.syscon1);
    const oldTwo = phantomShortKey(authority.syscon2);
    const oldSpine = [oldOne, oldTwo].filter(Boolean);
    const attempted = new Set(
      (phantomGossipRecovery.targets || []).map(phantomShortKey).filter(Boolean),
    );

    if (oldSpine.some((key) => key !== self && !attempted.has(key))) {
      emitSnapshot("authority_gossip_successor_spine_not_exhausted");
      return false;
    }

    const survivors = phantomAuthorityRosterRows(authority.roster).filter(
      (row) => row.hKey !== oldOne && row.hKey !== oldTwo,
    );
    const winner = phantomElectAuthorityCandidate(survivors, "", false, true);

    if (!winner || winner !== self) {
      phantomGossipRecovery.successor = winner || null;
      phantomGossipRecovery.state = "waiting_for_elected_survivor";
      emitSnapshot("authority_gossip_waiting_for_elected_survivor");
      return false;
    }

    const mirror = phantomElectAuthorityCandidate(survivors, self, false, false);

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
    phantomApplyLocalRoleCandidate("authority_gossip_provisional_successor");

    phantomGossipRecovery.successor = self;
    phantomGossipRecovery.state = "replacement_proof";
    phantomGossipRecovery.exhaustedAt = now();

    let armed = false;
    for (const oldKey of oldSpine) {
      if (!oldKey || oldKey === self) continue;
      if (
        phantomArmSysReplacement(oldKey, "authority_gossip_syscon_unreachable")
      ) {
        armed = true;
      }
    }

    phantomRefreshSysReadiness("authority_gossip_provisional_successor");
    emitSnapshot(
      armed
        ? "authority_gossip_successor_replacement_proof_started"
        : "authority_gossip_successor_replacement_proof_not_armed",
    );
    return armed;
  }

  function phantomStartTemporarySysTransaction(targetHKey, msg) {
    const target = phantomShortKey(targetHKey);
    const self = phantomShortKey(selfHKey);
    const purpose = "relationship_unblock";
    const id = String(msg?.sysTxId || msg?.cid || "");

    if (
      !target ||
      !self ||
      target === self ||
      !id ||
      !PHANTOM_TEMP_SYS_PURPOSES.has(purpose) ||
      !phantomCommittedMember(self) ||
      !phantomCommittedMember(target) ||
      phantomPendingTerminalExits.has(target) ||
      __phantomMembershipExitApplied.has(target)
    ) {
      emitSnapshot("temporary_sys_transaction_start_rejected");
      return false;
    }

    const current = phantomTemporarySysTransactions.get(target);
    if (current && current.id !== id && current.state !== "closed") {
      emitSnapshot("temporary_sys_transaction_busy");
      return false;
    }

    const createdAt = now();
    const deadlineAt = createdAt + PHANTOM_SYS_REPLACEMENT_MS;
    const deadlineTimer = setTimeout(() => {
      const active = phantomTemporarySysTransactions.get(target);
      if (
        !active ||
        String(active.id || "") !== id ||
        active.state === "closed" ||
        now() < Number(active.deadlineAt || 0)
      ) {
        return;
      }
      phantomCloseTemporarySysTransaction(target, "transaction_deadline");
    }, PHANTOM_SYS_REPLACEMENT_MS + 25);

    phantomTemporarySysTransactions.set(target, {
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

    const ent = phantomSysPeers.get(target);
    if (ent) {
      ent.temporary = true;
      ent.tempSysPurpose = purpose;
      ent.tempSysTxId = id;
    }

    void phantomEnsureSysPipe(target, "temporary_sys_" + purpose);
    emitSnapshot("temporary_sys_transaction_birth_requested");
    return true;
  }

  function phantomBindInboundTemporarySysTransaction(fromHKey, meta = {}) {
    const from = phantomShortKey(fromHKey);
    const self = phantomShortKey(selfHKey);
    const purpose = String(meta?.sysPurpose || "");
    const id = String(meta?.sysTxId || "");
    const room = String(meta?.room || videoId || "");

    if (meta?.sysTemporary !== true) return false;

    const relationship = phantomRelationshipGet(from);
    const expectedUnblock = !!(
      phantomBlockedByPeers.has(from) ||
      phantomRelationshipRevoked.has(from) ||
      String(relationship?.endReason || "") === "blocked_by_peer"
    );

    if (
      !from ||
      !self ||
      from === self ||
      !id ||
      !PHANTOM_TEMP_SYS_PURPOSES.has(purpose) ||
      purpose !== "relationship_unblock" ||
      !expectedUnblock ||
      !phantomCommittedMember(self) ||
      !phantomCommittedMember(from) ||
      phantomPendingTerminalExits.has(from) ||
      __phantomMembershipExitApplied.has(from) ||
      !room ||
      (videoId && room !== String(videoId))
    ) {
      emitSnapshot("temporary_sys_transaction_offer_rejected");
      return false;
    }

    const current = phantomTemporarySysTransactions.get(from);
    if (current && current.id !== id && current.state !== "closed") {
      emitSnapshot("temporary_sys_transaction_offer_conflict");
      return false;
    }

    const existing = phantomSysPeers.get(from);
    if (existing && !existing.temporary && phantomHasOpenSysPipe(from)) {
      emitSnapshot("temporary_sys_transaction_rejected_normal_pipe_open");
      return false;
    }

    if (existing && !existing.temporary) {
      phantomTeardownSysPeer(from, "temporary_sys_retires_stale_edge");
    }

    const createdAt = now();
    const deadlineAt = createdAt + PHANTOM_SYS_REPLACEMENT_MS;
    const deadlineTimer = setTimeout(() => {
      const active = phantomTemporarySysTransactions.get(from);
      if (
        !active ||
        String(active.id || "") !== id ||
        active.state === "closed" ||
        now() < Number(active.deadlineAt || 0)
      ) {
        return;
      }
      phantomCloseTemporarySysTransaction(from, "transaction_deadline");
    }, PHANTOM_SYS_REPLACEMENT_MS + 25);

    phantomTemporarySysTransactions.set(from, {
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

  function phantomCloseTemporarySysTransaction(remoteHKey, reason = "complete") {
    const key = phantomShortKey(remoteHKey);
    const rec = phantomTemporarySysTransactions.get(key);
    if (!key || !rec) return false;

    rec.state = "closed";
    rec.closedAt = now();
    rec.closeReason = String(reason || "complete");
    try {
      if (rec.deadlineTimer) clearTimeout(rec.deadlineTimer);
    } catch {}
    rec.deadlineTimer = null;
    phantomTemporarySysTransactions.delete(key);

    const pending = phantomPendingRelationship.get(key);
    if (pending?.length) {
      const kept = pending.filter(
        (item) => String(item?.msg?.sysTxId || "") !== String(rec.id || ""),
      );
      if (kept.length) phantomPendingRelationship.set(key, kept);
      else phantomPendingRelationship.delete(key);
    }

    const ent = phantomSysPeers.get(key);
    if (
      ent?.temporary === true &&
      String(ent.tempSysTxId || "") === String(rec.id || "")
    ) {
      phantomTeardownSysPeer(
        key,
        "temporary_sys_transaction_" + String(reason || "complete"),
      );
    }

    emitSnapshot(
      "temporary_sys_transaction_closed_" + String(reason || "complete"),
    );
    return true;
  }

  function phantomTemporarySysAllowsEnvelope(remoteHKey, envelope) {
    const key = phantomShortKey(remoteHKey);
    const rec = phantomTemporarySysTransactions.get(key);
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

  function phantomControlSeenKey(msg, via = "") {
    const cid = String(msg?.cid || "");
    if (!cid) return "";
    return [
      String(msg?.type || "unknown"),
      phantomShortKey(msg?.from),
      phantomShortKey(msg?.to),
      cid,
    ].join("|");
  }

  function phantomRememberControlCid(msg, via = "") {
    const key = phantomControlSeenKey(msg, via);
    if (!key) return false;

    if (phantomSeenControlCids.has(key)) {
      concierge.lastErr =
        "control_duplicate_ignored:" + String(msg?.type || "unknown");
      emitSnapshot(
        "control_duplicate_ignored_" + String(msg?.type || "unknown"),
      );
      return true;
    }

    phantomSeenControlCids.set(key, now());

    if (phantomSeenControlCids.size > PHANTOM_CONTROL_CID_MAX) {
      const rows = Array.from(phantomSeenControlCids.entries()).sort(
        (a, b) => Number(a[1] || 0) - Number(b[1] || 0),
      );

      while (
        phantomSeenControlCids.size > Math.floor(PHANTOM_CONTROL_CID_MAX * 0.75) &&
        rows.length
      ) {
        phantomSeenControlCids.delete(rows.shift()[0]);
      }
    }

    return false;
  }

  function phantomHandleInboundRelationshipControl(msg, via = "unknown") {
    if (!msg || typeof msg !== "object") return false;

    const type = String(msg.type || "");
    if (phantomControlClassFor(type) !== "relationship_control") return false;
    if (!isDirectMsgForMe(msg)) return true;

    const from = phantomShortKey(msg.from);
    if (!from) return true;

    if (phantomRememberControlCid(msg, via)) return true;

    phantomMarkControlRx(type);
    concierge.lastRelationshipRxAt = now();
    concierge.lastRelationshipRxType = type;
    concierge.lastRelationshipRxVia = String(via || "unknown");
    concierge.lastRxType = type;
    concierge.lastRxAt = now();

    const pendingExit = phantomPendingTerminalExits.has(from);
    const committedMember = !!(
      lastRoster.has(from) && phantomCommittedMember(from)
    );
    const membershipGone = !!(
      pendingExit ||
      __phantomMembershipExitApplied.has(from) ||
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

    if (via === "phantom_sys_temporary") {
      const transaction = phantomTemporarySysTransactions.get(from);
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
      if (via === "phantom_sys") {
        const ent = phantomSysPeers.get(from);
        const sysState = String(ent?.sysdc?.readyState || "");
        concierge.sysPeer = from;
        if (sysState === "open" || sysState === "connecting") {
          phantomRefreshSysReadiness("relationship_rx_invite_observe_" + sysState);
          emitSnapshot("relationship_rx_invite_observe_" + sysState);
        }
      }

      if (phantomIsBlocked(from) || phantomIsBlockedBy(from)) {
        phantomRelationshipEnd(from, "invite_blocked", now(), true);
        setState(from, VEIL.OUTSIDE, true);
        phantomSendControl(
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
      __phantomGoneNotified.delete(from);
      phantomRelationshipRevoked.delete(from);

      const current = phantomRelationshipGet(from);
      const incomingCycle = String(
        msg.cycleId ||
          msg.cid ||
          phantomRelationshipCycleId(from, "remote_invite"),
      );
      const incomingInviteCid = String(msg.cid || "") || null;

      if (phantomRelationshipIsAccepted(current)) {
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
        phantomRelationships.set(from, current);
        phantomRelationshipRevoked.delete(from);
        setState(from, VEIL.INSIDE);
        phantomSendControl(
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
        phantomRequestChatPipe(from, "relationship_simultaneous_accept");
        return true;
      }

      phantomRelationshipRevoked.delete(from);
      phantomCancelReconnect(from);
      rtcTeardown(from, "relationship_new_cycle");
      phantomRelationshipBegin(from, VEIL.INVITE_RECEIVED, {
        cycleId: incomingCycle,
        inviteCid: incomingInviteCid,
        origin: "remote_invite",
      });
      setState(from, VEIL.INVITE_RECEIVED, true);
      post("VEIL_INVITE_RECEIVED", { from });
      if(host.messaging==='automatic')queueMicrotask(phantomReconcileMessaging);
      return true;
    }

    if (type === "veil_accept") {
      const current = phantomRelationshipGet(from);
      if (phantomIsBlocked(from) || phantomIsBlockedBy(from)) {
        phantomRelationshipEnd(from, "accept_blocked", now(), true);
        setState(from, VEIL.OUTSIDE, true);
        return true;
      }

      if (
        current.state !== VEIL.INVITE_SENT ||
        current.endedAt ||
        !phantomRelationshipMatches(msg, current)
      ) {
        concierge.lastErr = "veil_accept_ignored_wrong_relationship_cycle";
        emitSnapshot("veil_accept_ignored_wrong_relationship_cycle");
        return true;
      }

      phantomRelationshipRevoked.delete(from);
      current.state = VEIL.INSIDE;
      current.updatedAt = now();
      current.endedAt = 0;
      current.endReason = "none";
      phantomRelationships.set(from, current);
      setState(from, VEIL.INSIDE);
      post("VEIL_ACCEPTED", { from });
      phantomRequestChatPipe(from, "relationship_accept");
      return true;
    }

    if (type === "veil_deny") {
      const current = phantomRelationshipGet(from);
      if (
        !current.cycleId ||
        !msg.cycleId ||
        phantomRelationshipMatches(msg, current)
      ) {
        phantomRelationshipEnd(from, "remote_deny", now(), false);
        setState(from, VEIL.OUTSIDE, true);
        phantomCancelReconnect(from);
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
      const current = phantomRelationshipGet(from);

      if (
        via !== "phantom_sys" ||
        phantomIsBlocked(from) ||
        phantomIsBlockedBy(from) ||
        !phantomRelationshipIsAccepted(current) ||
        !phantomRelationshipMatches(msg, current)
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
      let current = phantomRelationshipGet(from);
      if (phantomIsBlocked(from) || phantomIsBlockedBy(from)) return true;
      if (
        !(lastRoster instanceof Set && lastRoster.has(from)) ||
        via !== "phantom_sys"
      )
        return true;

      const incomingCycleId = String(msg?.cycleId || "");
      const incomingState = String(msg?.state || "");
      const incomingAccepted =
        incomingState === VEIL.INSIDE || incomingState === VEIL.ESTABLISHED;

      const continuityPending = !!(
        phantomPendingRefreshRoom === String(videoId || "") &&
        phantomPendingRefreshRelationships.has(from)
      );

      if (
        !phantomRelationshipRevoked.has(from) &&
        !phantomRelationshipIsAccepted(current) &&
        !current?.cycleId &&
        incomingAccepted &&
        incomingCycleId
      ) {
        current = phantomRelationshipBegin(from, VEIL.ESTABLISHED, {
          cycleId: incomingCycleId,
          remoteBootId:
            String(msg?.bootId || phantomRemoteBootIds.get(from) || "") || null,
          membershipEpoch: Number(concierge.sysconEpoch || 0),
          origin: "surviving_peer_continuity_proof",
        });

        phantomPendingRefreshRelationships.delete(from);
        if (!phantomPendingRefreshRelationships.size) {
          phantomPendingRefreshRoom = "";
          try {
            sessionStorage.removeItem("phantom_refresh_relationships_v1");
          } catch {}
        }

        emitSnapshot("relationship_continuity_adopted_from_surviving_peer");
      } else if (continuityPending) {
        if (
          phantomRelationshipRevoked.has(from) ||
          !current?.cycleId ||
          current.endedAt ||
          !incomingAccepted ||
          !phantomRelationshipMatches(msg, current)
        ) {
          emitSnapshot("veil_status_ignored_wrong_continuity_cycle");
          return true;
        }

        phantomRelationshipRevoked.delete(from);
        setState(from, VEIL.ESTABLISHED, true);
        phantomPendingRefreshRelationships.delete(from);

        if (!phantomPendingRefreshRelationships.size) {
          phantomPendingRefreshRoom = "";
          try {
            sessionStorage.removeItem("phantom_refresh_relationships_v1");
          } catch {}
        }

        emitSnapshot("relationship_continuity_peer_validated");
      } else {
        if (
          phantomRelationshipRevoked.has(from) ||
          !phantomRelationshipIsAccepted(current)
        ) {
          emitSnapshot("veil_status_ignored_no_active_relationship_cycle");
          return true;
        }
        if (!phantomRelationshipMatches(msg, current)) {
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
        phantomPresentReconnect(
          from,
          "veil_status_reply",
          String(msg?.bootId || ""),
        );
      }

      if (!msg.replyTo) {
        phantomSendControl(
          {
            type: "veil_status",
            from: selfHKey,
            to: from,
            videoId,
            state: getState(from),
            cycleId: current.cycleId,
            replyTo: msg.cid || null,
            reconnectProbe: msg?.reconnectProbe === true,
            bootId: phantomBootId,
          },
          "veil_status",
        );
      }

      phantomRequestChatPipe(from, "relationship_status_reconcile");
      return true;
    }

    if (type === "sys_block") {
      phantomBlockedByPeers.add(from);
      phantomBlockedPeers.delete(from);
      phantomRelationshipEnd(from, "blocked_by_peer", now(), true);
      phantomCancelReconnect(from);
      loungeWaiters.delete(from);
      phantomPendingRelationship.delete(from);
      phantomPendingDm.delete(from);
      phantomPendingRtcIce.delete(from);
      setState(from, VEIL.OUTSIDE, true);
      rtcTeardown(from, "blocked_by_peer");
      post("BLOCKED_BY", { from, at: msg.t || msg.sentAt || Date.now() });
      return true;
    }

    if (type === "sys_unblock") {
      phantomBlockedByPeers.delete(from);
      phantomBlockedPeers.delete(from);
      phantomRelationshipRevoked.delete(from);
      if (
        phantomCommittedMember(from) &&
        !phantomPendingTerminalExits.has(from) &&
        !__phantomMembershipExitApplied.has(from)
      ) {
        __phantomGoneNotified.delete(from);
      }
      phantomRelationshipEnd(from, "unblocked", now(), false);
      setState(from, VEIL.OUTSIDE, true);
      post("UNBLOCKED_BY", { from, at: msg.t || msg.sentAt || Date.now() });

      const transaction = phantomTemporarySysTransactions.get(from);
      if (transaction) {
        transaction.state = "command_received";
      }

      const ackSent = phantomSendControl(
        {
          type: "sys_unblock_ack",
          from: selfHKey,
          to: from,
          videoId,
          ackFor: msg.cid || null,
          sysTxId: msg.sysTxId || msg.cid || null,
          authorityHint:
            msg.recoveryProbe === true || via === "phantom_sys_temporary"
              ? phantomAuthorityGossipSnapshot()
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
      phantomBlockedPeers.delete(from);
      phantomBlockedByPeers.delete(from);
      phantomRelationshipRevoked.delete(from);
      if (
        phantomCommittedMember(from) &&
        !phantomPendingTerminalExits.has(from) &&
        !__phantomMembershipExitApplied.has(from)
      ) {
        __phantomGoneNotified.delete(from);
      }
      phantomRelationshipEnd(from, "unblocked_ack", now(), false);
      setState(from, VEIL.OUTSIDE, true);

      if (msg.authorityHint) {
        phantomRememberAuthorityGossip(from, msg.authorityHint);
      }

      post("SYS_UNBLOCK_ACK", {
        from,
        ackFor: msg.ackFor || null,
        at: msg.t || msg.sentAt || Date.now(),
      });

      const transaction = phantomTemporarySysTransactions.get(from);
      if (
        transaction &&
        String(msg.sysTxId || "") === String(transaction.id || "")
      ) {
        transaction.state = "acknowledged";
        transaction.ackAt = now();
        phantomCloseTemporarySysTransaction(from, "acknowledged");
      }

      return true;
    }

    return true;
  }

  function phantomBlockKey(hKey) {
    return String(hKey || "")
      .slice(0, 16)
      .toLowerCase();
  }

  function phantomIsBlocked(hKey) {
    const key = phantomBlockKey(hKey);
    return !!key && phantomBlockedPeers.has(key);
  }

  function phantomIsBlockedBy(hKey) {
    const key = phantomBlockKey(hKey);
    return !!key && phantomBlockedByPeers.has(key);
  }

  function getState(hKey) {
    return phantomRelationshipGet(hKey)?.state || VEIL.OUTSIDE;
  }

  function phantomRequestChatPipe(hKey, reason = "relationship") {
    if (!selfHKey) return false;

    const key = phantomShortKey(hKey);
    if (!key) return false;

    if (
      phantomRelationshipRevoked.has(key) ||
      phantomPendingTerminalExits.has(key) ||
      __phantomGoneNotified.has(key)
    ) {
      concierge.lastErr =
        "chat_pipe_blocked_relationship_terminal:" +
        String(reason || "relationship");
      emitSnapshot("chat_pipe_blocked_relationship_terminal");
      return false;
    }

    const relationship = phantomRelationshipGet(key);
    const st = relationship?.state || VEIL.OUTSIDE;

    if (
      !phantomRelationshipIsAccepted(relationship) ||
      (st !== VEIL.ESTABLISHED && st !== VEIL.INSIDE)
    ) {
      concierge.lastErr =
        "chat_pipe_blocked_until_accept:" + String(reason || "relationship");
      emitSnapshot("chat_pipe_blocked_until_accept");
      return false;
    }

    return phantomEnsureChatPipe(key, reason);
  }

  function phantomOwnsChatPipeOffer(hKey) {
    const key = phantomShortKey(hKey);
    const self = phantomShortKey(selfHKey);
    if (!key || !self || key === self) return false;

    // Chat pipe ownership is per accepted peer pair.
    // SYS/relay role only carries signaling; it must not decide chat birth.
    return self < key;
  }

  async function phantomEnsureChatPipe(hKey, reason = "relationship") {
    if (!selfHKey) return false;

    const key = phantomShortKey(hKey);
    if (!key) return false;

    const relationship = phantomRelationshipGet(key);
    if (
      !phantomRelationshipIsAccepted(relationship) ||
      phantomPendingTerminalExits.has(key) ||
      __phantomGoneNotified.has(key)
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

    let ent = phantomChatPeers.get(key);
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
          phantomCancelChatAttemptDeadline(key);
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

          phantomCancelReconnect(key, true);
          post("P2P_CHAT_READY", { from: key });
          rtcSendHello(key);

          const q = phantomPendingDm.get(key);

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
              phantomPendingDm.set(key, rest.slice(-PHANTOM_DM_PENDING_MAX));
              concierge.lastErr = "pending_dm_flush_partial";
              emitSnapshot("pending_dm_flush_partial");
            } else {
              phantomPendingDm.delete(key);
              concierge.lastErr = null;
              emitSnapshot("pending_dm_flushed_chat_ready");
            }
          }

          emitSnapshot("chat_pipe_ready_from_deferred_hello");
        } else {
          phantomCancelReconnect(key, true);
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

    if (!phantomOwnsChatPipeOffer(key)) {
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

    const target = phantomGetOrCreateChatPeer(key);
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

  function phantomReconcileRelationshipStatus(reason = "relationship_status") {
    if (!selfHKey || !videoId || !lastRoster || !lastRoster.size) return false;
    if (!concierge.sysReady) return false;

    if (
      phantomPendingRefreshRelationships.size &&
      phantomPendingRefreshRoom === String(videoId || "")
    ) {
      let probed = 0;

      for (const peer of Array.from(phantomPendingRefreshRelationships)) {
        const key = phantomShortKey(peer);

        if (!key || key === phantomShortKey(selfHKey)) {
          phantomPendingRefreshRelationships.delete(peer);
          continue;
        }

        if (phantomRelationshipRevoked.has(key)) {
          phantomPendingRefreshRelationships.delete(peer);
          continue;
        }

        if (!lastRoster.has(key)) continue;

        const relationship = phantomRelationshipGet(key);
        if (!relationship?.cycleId || relationship.endedAt) continue;

        phantomSendControl(
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

      if (!phantomPendingRefreshRelationships.size) {
        phantomPendingRefreshRoom = "";

        try {
          sessionStorage.removeItem("phantom_refresh_relationships_v1");
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
      const peer = phantomShortKey(hKey);
      if (!peer || peer === phantomShortKey(selfHKey)) continue;
      if (phantomRelationshipRevoked.has(peer)) continue;

      const state = getState(peer);
      if (state !== VEIL.INSIDE && state !== VEIL.ESTABLISHED) continue;

      phantomSendControl(
        {
          type: "veil_status",
          from: selfHKey,
          to: peer,
          videoId,
          state,
          cycleId: phantomRelationshipGet(peer)?.cycleId || null,
        },
        "veil_status",
      );

      phantomRequestChatPipe(peer, "relationship_status_reconcile");

      sent++;
    }

    if (sent) emitSnapshot(String(reason || "relationship_status"));
    return !!sent;
  }

  async function maybeEnterLounge(hKey) {
    if (!selfHKey) return;

    const key = String(hKey || "").toLowerCase();
    if (!key) return;

    const ent = phantomChatPeers.get(key);
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
      CORE_DEBUG && console.log("[phantom][CORE] waiting for roster →", key);
      loungeWaiters.add(key);
      return;
    }

    loungeWaiters.delete(key);

    await phantomEnsureChatPipe(key, "lounge_entry");
  }

  function setState(hKey, next, force = false) {
    CORE_DEBUG && console.log("STATE", hKey, next);

    const key = phantomShortKey(hKey);
    if (!key) return;

    const rec = phantomRelationshipGet(key);
    const prev = rec.state;
    if (prev === next && !force) return;

    if (next !== VEIL.OUTSIDE && !rec.cycleId) {
      rec.cycleId = phantomRelationshipCycleId(key, "state_transition");
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
    phantomRelationships.set(key, rec);

    try {
      const relationships = Array.from(phantomRelationships.values())
        .filter((row) => phantomRelationshipIsAccepted(row))
        .map((row) => ({
          hKey: phantomShortKey(row.hKey),
          cycleId: String(row.cycleId || ""),
          inviteCid: String(row.inviteCid || "") || null,
          remoteBootId: String(row.remoteBootId || "") || null,
          membershipEpoch: Number(row.membershipEpoch || 0),
        }))
        .filter((row) => row.hKey && row.cycleId);

      if (videoId && relationships.length) {
        sessionStorage.setItem(
          "phantom_refresh_relationships_v1",
          JSON.stringify({
            room: videoId,
            peers: relationships.map((row) => row.hKey),
            relationships,
          }),
        );
      } else {
        sessionStorage.removeItem("phantom_refresh_relationships_v1");
      }
    } catch {}

    post("VEIL_STATE_UPDATE", { hKey: key, state: next });
    emitSnapshot("veil_state_" + next);
  }

  function phantomShortKey(hKey) {
    return String(hKey || "")
      .slice(0, 16)
      .toLowerCase();
  }

  function phantomIsSysconRole(role) {
    role = String(role || "");
    return role === "syscon1" || role === "syscon2";
  }

  function phantomIsAuthoritySysTether(remoteHKey) {
    const key = phantomShortKey(remoteHKey);
    const self = phantomShortKey(selfHKey);
    const one = phantomShortKey(concierge.syscon1);
    const two = phantomShortKey(concierge.syscon2);

    if (!key || !self || !one || !two) return false;

    return (self === one && key === two) || (self === two && key === one);
  }

  function phantomOwnsPrimarySysState(remoteHKey) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;

    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );

    if (role === "syscon1" || role === "syscon2") {
      return phantomIsAuthoritySysTether(key);
    }

    return key === phantomShortKey(phantomSysTargetFromCandidates());
  }

  function phantomSysBirthRole(remoteHKey) {
    const key = phantomShortKey(remoteHKey);
    const self = phantomShortKey(selfHKey);
    const one = phantomShortKey(concierge.syscon1);
    const two = phantomShortKey(concierge.syscon2);
    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );

    if (!key || !self || key === self) return "illegal";

    const transaction = phantomTemporarySysTransactions.get(key);
    if (
      transaction &&
      transaction.state !== "closed" &&
      PHANTOM_TEMP_SYS_PURPOSES.has(String(transaction.purpose || ""))
    ) {
      return phantomShortKey(transaction.initiator) === self
        ? "opener"
        : "acceptor";
    }

    // Authority tether: syscon1 is the only opener; syscon2 only accepts.
    if (role === "syscon1" && self === one && key === two) return "opener";
    if (role === "syscon2" && self === two && key === one) return "acceptor";

    // A candidate mirror has not committed authority yet, but its bootstrap
    // edge must still have one opener. The advertised leader owns this edge;
    // treating the candidate as an ordinary peer would make both ends offer.
    // This selects transport direction only; it does not admit or promote self.
    if (
      concierge.bootstrapAdmission === true &&
      self === phantomShortKey(concierge.cfConciergeSyscon2) &&
      key === phantomShortKey(concierge.cfConciergeSyscon1)
    ) return "acceptor";

    // Every ordinary peer owns exactly one SYS tether to its authoritative
    // target. No peer may open a SYS edge to another ordinary peer.
    if (role === "peer") {
      const target = phantomShortKey(phantomSysTargetFromCandidates());
      return target && key === target ? "opener" : "illegal";
    }

    // Syscon1 accepts every doorway-admitted ordinary peer tether. This is a
    // separate per-peer SYS edge, not the syscon1<->syscon2 authority attempt.
    if (role === "syscon1" && self === one && key !== one && key !== two) {
      const doorwayAdmitted = phantomDoorwayAdmittedPeers.has(key);
      const committedMember = phantomCommittedMember(key);

      // A continuing committed member may restore its ordinary SYS tether
      // without re-entering through CF. Doorway admission remains required
      // only for a genuinely new room incarnation.
      return doorwayAdmitted || committedMember ? "acceptor" : "illegal";
    }

    // Syscon2 never forms direct SYS tethers with ordinary peers.
    return "illegal";
  }

  function phantomSysEdgeAttempt(remoteHKey, create = true) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return null;

    const ent = create ? phantomGetOrCreateSysPeer(key) : phantomSysPeers.get(key);
    if (!ent) return null;

    if (!ent.attempt && create) {
      ent.attempt = {
        id: phantomNewSysTransportAttemptId(key),
        role: phantomSysBirthRole(key),
        state: "missing",
        startedAt: 0,
        lastEvent: "created",
        faultReason: "none",
      };
    }

    return ent.attempt || null;
  }

  function phantomAuthorityRosterRows(rows = []) {
    const out = [];
    const seen = new Set();

    for (const row of Array.isArray(rows) ? rows : []) {
      const key = phantomShortKey(row?.hKey || row?.from || row);
      if (!key || seen.has(key)) continue;
      seen.add(key);

      /*
       * Authority is membership truth, not an identity-blob carrier.
       * Keep the committed value deterministic and bounded: member key plus
       * the small display handle only. Avatar data, full identity hashes,
       * boot metadata, timestamps, and presentation provenance stay in the
       * presence/identity plane and must never be duplicated into SYS STATUS.
       */
      const presence = phantomPresenceBook.get(key) || {};
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

  function phantomAuthorityRosterKey(rows = []) {
    return phantomAuthorityRosterRows(rows)
      .map((row) => row.hKey)
      .sort()
      .join(",");
  }
  function phantomTerminalExitCommand(msg, via = "authority_exit") {
    if (!msg || typeof msg !== "object") return false;

    const member = phantomShortKey(msg.from || msg.member);
    const room = String(msg.room || msg.videoId || "");
    const bootId = String(msg.bootId || "");
    const epoch = Number(msg.epoch || concierge.sysconEpoch || 0);
    const at = Number(msg.at || now());

    if (!member || !room || (videoId && room !== String(videoId))) return false;

    const self = phantomShortKey(selfHKey);
    if (self && member === self) {
      const selfReplacement = phantomSysReplacementWindows.get(member);
      try {
        if (selfReplacement?.timer) clearTimeout(selfReplacement.timer);
      } catch {}
      phantomPendingTerminalExits.delete(member);
      phantomSysReplacementWindows.delete(member);
      emitSnapshot("terminal_exit_self_ignored");
      return false;
    }

    const knownBootId = String(phantomRemoteBootIds.get(member) || "");
    if (bootId && knownBootId && bootId !== knownBootId) {
      emitSnapshot("stale_terminal_exit_boot_ignored");
      return false;
    }

    const committed = phantomLocalCommittedAuthority();
    const committedRows = phantomAuthorityRosterRows(committed?.roster);
    const committedKeys = new Set(committedRows.map((row) => row.hKey));

    const pending = phantomSysAuthority.pending;
    const pendingRows = phantomAuthorityRosterRows(pending?.snapshot?.roster);
    const pendingKeys = new Set(pendingRows.map((row) => row.hKey));
    const pendingBaseKeys = new Set(
      phantomAuthorityRosterRows(pending?.baseRoster).map((row) => row.hKey),
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
      const pendingLeader = phantomShortKey(pending?.snapshot?.syscon1);
      const survivors = pendingRows.filter((row) => row.hKey !== member);
      const pendingId = String(pending?.id || "");

      phantomSysAuthority.pending = null;
      phantomTeardownSysPeer(member, "uncommitted_member_exit");
      phantomPendingTerminalExits.delete(member);
      phantomSysReplacementWindows.delete(member);

      phantomMembershipAuditSet("pending_aborted", {
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
        phantomStageSyscon1Authority(
          survivors,
          "syscon1_genesis_member_exit_restart",
        );
        phantomPublishCommittedAuthority(
          "syscon1_genesis_member_exit_restart_published",
        );
      } else if (committed) {
        phantomPublishCommittedAuthority("syscon1_admission_aborted_member_exit");
      }

      emitSnapshot("uncommitted_member_exit_transaction_aborted");
      return true;
    }

    if (committed && !committedKeys.has(member)) {
      const staleReplacement = phantomSysReplacementWindows.get(member);
      try {
        if (staleReplacement?.timer) clearTimeout(staleReplacement.timer);
      } catch {}
      phantomPendingTerminalExits.delete(member);
      phantomSysReplacementWindows.delete(member);
      emitSnapshot("terminal_exit_member_already_absent_cleared");
      return true;
    }

    const replacementOwner = phantomSysReplacementWindows.get(member) || {};
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
    phantomSysReplacementWindows.set(member, replacementOwner);

    const command = {
      bootId: bootId || knownBootId || null,
      epoch,
      at,
      via: String(via || "authority_exit"),
      reason: String(msg.reason || via || "authority_exit"),
    };
    phantomPendingTerminalExits.set(member, command);
    phantomRefreshSysCandidates([], "confirmed_terminal_exit");

    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const currentOne = phantomShortKey(concierge.syscon1);

    if (!self || role !== "syscon1" || currentOne !== self || !committed) {
      emitSnapshot("terminal_exit_recorded_waiting_for_syscon1");
      return true;
    }

    const committedTerminalExits = Array.from(
      phantomPendingTerminalExits.entries(),
    )
      .filter(([key]) => committedKeys.has(phantomShortKey(key)))
      .map(([key, rec]) => ({
        member: phantomShortKey(key),
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

    const committedOne = phantomShortKey(committed.syscon1);
    const committedTwo = phantomShortKey(committed.syscon2);
    const activeLeader = committedOne === self;
    const promotedMirror = !!(
      committedTwo === self && terminalMembers.has(committedOne)
    );
    const oldSpineTerminal = !!(
      terminalMembers.has(committedOne) &&
      (!committedTwo || terminalMembers.has(committedTwo))
    );
    const electedSuccessor = oldSpineTerminal
      ? phantomElectAuthorityCandidate(nextRoster, "", true, true)
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
    const staged = phantomStageSyscon1Authority(
      nextRoster,
      stageVia,
      committed,
      promotion ? { syscon1: self } : null,
      { terminalExits: committedTerminalExits },
    );

    if (!staged) {
      if (promotion) phantomScheduleSysAdmission(stageVia + "_restore_mirror");
      emitSnapshot(stageVia + "_waiting");
      return true;
    }

    phantomPublishCommittedAuthority(stageVia + "_published");
    emitSnapshot(stageVia + "_staged");
    return true;
  }

  function phantomResumeTerminalAuthority(reason = "authority_resume") {
    if (!phantomPendingTerminalExits.size) return false;

    let touched = false;
    const entries = Array.from(phantomPendingTerminalExits.entries()).sort(
      (a, b) =>
        Number(a[1]?.at || 0) - Number(b[1]?.at || 0) ||
        String(a[0]).localeCompare(String(b[0])),
    );

    for (const [member, command] of entries) {
      const beforeSeq = Number(phantomSysAuthority.rosterSeq || 0);
      const accepted = phantomTerminalExitCommand(
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

      if (phantomSysAuthority.pending) break;
      if (Number(phantomSysAuthority.rosterSeq || 0) === beforeSeq) break;
    }

    if (touched)
      emitSnapshot("terminal_authority_resumed_" + String(reason || "resume"));
    return touched;
  }

  function phantomSendAuthorityDepartureCommand(
    reason = "session_kill",
    final = true,
  ) {
    const self = phantomShortKey(selfHKey);
    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const one = phantomShortKey(concierge.syscon1);
    const two = phantomShortKey(concierge.syscon2);
    const target = role === "syscon1" ? two : one;
    const type = final ? "authority_exit" : "authority_detach";

    if (!self || !target || target === self || !phantomHasOpenSysPipe(target)) {
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
      bootId: phantomBootId,
      epoch: Number(concierge.sysconEpoch || 0),
      reason: String(reason || (final ? "session_kill" : "document_detach")),
      at: now(),
    };

    const sent = phantomSendSysControl(command, type);
    emitSnapshot(sent ? type + "_command_sent" : type + "_command_failed");
    return sent;
  }

  function phantomSendTerminalExitCommand(reason = "session_kill") {
    return phantomSendAuthorityDepartureCommand(reason, true);
  }

  function phantomSendDocumentDetachCommand(reason = "document_detach") {
    return phantomSendAuthorityDepartureCommand(reason, false);
  }
  function phantomApplyCommittedAuthority(
    snapshot,
    via = "sys_authority",
    previousRowsOverride = null,
    transaction = null,
  ) {
    if (!snapshot || typeof snapshot !== "object") return false;

    const room = String(snapshot.room || videoId || "");
    const from = phantomShortKey(snapshot.from);
    const one = phantomShortKey(snapshot.syscon1);
    const two = phantomShortKey(snapshot.syscon2);
    const epoch = Number(snapshot.epoch || 0);
    const rosterSeq = Number(snapshot.rosterSeq || 0);
    const roster = phantomAuthorityRosterRows(snapshot.roster);
    const self = phantomShortKey(selfHKey);

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
      (phantomAuthoritySnapshotId(transaction.snapshot) !==
        phantomAuthoritySnapshotId(snapshot) ||
        String(transaction.id || "") !==
          phantomAuthorityTransactionId(transaction))
    ) {
      phantomMembershipAuditSet("apply_rejected", {
        id: transaction?.id,
        reason: "transaction_integrity_mismatch",
      });
      return false;
    }

    const currentEpoch = Number(phantomSysAuthority.epoch || 0);
    const currentSeq = Number(phantomSysAuthority.rosterSeq || 0);
    const currentOne = phantomShortKey(phantomSysAuthority.syscon1);
    const currentTwo = phantomShortKey(phantomSysAuthority.syscon2);
    const incomingKey = phantomAuthorityRosterKey(roster);
    const currentKey = phantomAuthorityRosterKey(phantomSysAuthority.roster);

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
      ? phantomAuthorityRosterRows(previousRowsOverride)
      : phantomAuthorityRosterRows(phantomSysAuthority.roster);
    const previousRoster = new Set(previousRows.map((row) => row.hKey));
    const nextRoster = new Set(roster.map((row) => row.hKey));

    if (transaction?.admission?.member) {
      const member = phantomShortKey(transaction.admission.member);
      const bootId = String(transaction.admission.bootId || "");
      const knownBoot = String(phantomRemoteBootIds.get(member) || "");

      if (
        !member ||
        previousRoster.has(member) ||
        !nextRoster.has(member) ||
        (bootId && knownBoot && bootId !== knownBoot)
      ) {
        phantomMembershipAuditSet("apply_rejected", {
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

    Object.assign(phantomSysAuthority, accepted);
    // Accepted SYS authority replaces provisional provider epoch metadata.
    concierge.sysconEpoch = epoch;
    concierge.syscon1 = one;
    concierge.syscon2 = two || null;

    lastRoster = phantomMarkRosterAudit(
      "committed_sys_authority_apply",
      roster.map((row) => row.hKey),
      "committed_sys_authority_projection",
    );

    for (const row of roster) {
      phantomRememberPresencePeer(row, "phantom_sys_authority", {
        peerEvent: false,
        rosterEvent: false,
      });
    }

    for (const newKey of nextRoster) {
      if (newKey === self || previousRoster.has(newKey)) continue;

      const replacement = phantomSysReplacementWindows.get(newKey);
      try {
        if (replacement?.timer) clearTimeout(replacement.timer);
      } catch {}

      const exited = __phantomMembershipExitApplied.get(newKey);
      const admittedBoot = String(phantomRemoteBootIds.get(newKey) || "");
      const relationship = phantomRelationships.get(newKey);
      if (host.messaging === "automatic" && relationship?.endReason === "ROOM_EXITED" &&
          exited?.bootId && admittedBoot && admittedBoot !== exited.bootId) {
        // A newly admitted incarnation starts a new automatic messaging cycle.
        // Explicit chat ends and block state retain their existing owners.
        phantomRelationships.delete(newKey);
      }
      phantomPendingTerminalExits.delete(newKey);
      phantomSysReplacementWindows.delete(newKey);
      __phantomGoneNotified.delete(newKey);
      __phantomMembershipExitApplied.delete(newKey);
      phantomRelationshipRevoked.delete(newKey);

      phantomMembershipAuditSet("member_admitted", {
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

      const replacementOwner = phantomSysReplacementWindows.get(oldKey);
      if (replacementOwner) {
        try {
          if (replacementOwner.timer) clearTimeout(replacementOwner.timer);
        } catch {}
        replacementOwner.timer = null;
        replacementOwner.state = "committed";
        replacementOwner.committedAt = now();
        phantomSysReplacementWindows.set(oldKey, replacementOwner);
      }

      phantomNotifyPeerGone(oldKey, "ROOM_EXITED", now());
      phantomPendingTerminalExits.delete(oldKey);
      phantomSysReplacementWindows.delete(oldKey);
    }

    for (const terminalKey of Array.from(phantomPendingTerminalExits.keys())) {
      const key = phantomShortKey(terminalKey);
      if (!key || nextRoster.has(key)) continue;
      const replacementOwner = phantomSysReplacementWindows.get(key);
      try {
        if (replacementOwner?.timer) clearTimeout(replacementOwner.timer);
      } catch {}
      phantomPendingTerminalExits.delete(key);
      phantomSysReplacementWindows.delete(key);
    }

    phantomEmitPresenceRoster("committed_sys_authority_apply");
    queueMicrotask(sdkResumeDeferredOffers);
    return true;
  }
  function phantomAuthoritySnapshotId(snapshot) {
    if (!snapshot || typeof snapshot !== "object") return "";

    return [
      String(snapshot.room || ""),
      Number(snapshot.epoch || 0),
      Number(snapshot.rosterSeq || 0),
      phantomShortKey(snapshot.syscon1),
      phantomShortKey(snapshot.syscon2),
      phantomAuthorityRosterKey(snapshot.roster),
    ].join(":");
  }

  function phantomAuthorityTerminalExits(source) {
    const input = Array.isArray(source?.terminalExits)
      ? source.terminalExits
      : source?.terminalExit
        ? [source.terminalExit]
        : [];
    const byMember = new Map();

    for (const rec of input) {
      const member = phantomShortKey(rec?.member || rec?.from);
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

  function phantomAuthorityTransactionId(pending) {
    if (!pending || typeof pending !== "object") return "";

    const base = pending.baseAuthority || null;
    const terminalKey = phantomAuthorityTerminalExits(pending)
      .map((rec) =>
        [rec.member, rec.bootId || "", rec.epoch, rec.at, rec.via].join("~"),
      )
      .join(",");
    const admission = pending.admission || null;
    const leadership = pending.leadership || null;

    return [
      phantomAuthoritySnapshotId(pending.snapshot),
      Number(pending.baseEpoch || 0),
      Number(pending.baseRosterSeq || 0),
      phantomAuthorityRosterKey(pending.baseRoster),
      phantomShortKey(base?.syscon1),
      phantomShortKey(base?.syscon2),
      terminalKey,
      String(admission?.member || ""),
      String(admission?.bootId || ""),
      String(leadership?.mode || ""),
      phantomShortKey(leadership?.winner),
      phantomShortKey(leadership?.baseOne),
      phantomShortKey(leadership?.baseTwo),
    ].join(":");
  }

  function phantomElectAuthorityCandidate(
    rows,
    excludedMember = "",
    requireOpen = true,
    includeSelf = false,
  ) {
    const excluded = phantomShortKey(excludedMember);
    const self = phantomShortKey(selfHKey);
    const nowAt = now();

    const candidates = phantomAuthorityRosterRows(rows)
      .map((row) => phantomShortKey(row?.hKey || row?.from || row))
      .filter(
        (key) =>
          key &&
          key !== excluded &&
          !phantomPendingTerminalExits.has(key) &&
          (includeSelf || key !== self),
      )
      .map((key) => {
        const presence = phantomPresenceBook.get(key) || {};
        const ent = phantomSysPeers.get(key) || null;
        const localCandidate = includeSelf && key === self;
        const open = localCandidate ? true : phantomHasOpenSysPipe(key);
        const lastGood = localCandidate
          ? nowAt
          : Number(phantomSysLastGoodRx.get(key) || 0);
        const proofEpoch = localCandidate
          ? Math.max(
              Number(phantomSysAuthority.epoch || 0),
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
  function phantomAuthorizeSyscon1Duty(key, resume) {
    return host.authorizeSyscon1?.({roomId:videoId, peerId:phantomShortKey(selfHKey),
      leader:phantomShortKey(concierge.syscon1), role:String(concierge.localRoleCandidate || concierge.role || 'peer')}, key, resume) !== false;
  }
  function phantomStageSyscon1Authority(
    nextRows,
    via = "syscon1_commit",
    transactionBase = null,
    topologyOverride = null,
    transactionMeta = null,
  ) {
    const existing = new Set(phantomAuthorityRosterRows(phantomSysAuthority.roster).map(row => row.hKey));
    if (phantomAuthorityRosterRows(nextRows).some(row => row.hKey !== phantomShortKey(selfHKey) && !existing.has(row.hKey) && !host.canAdmitPeer(row.hKey))) {
      emitSnapshot('peer_authorization_denied');
      return false;
    }
    if (!phantomAuthorizeSyscon1Duty('stage:'+via+':'+String(transactionMeta?.admission?.member || ''), () => {
      if (phantomStageSyscon1Authority(nextRows, via, transactionBase, topologyOverride, transactionMeta))
        phantomPublishCommittedAuthority('admin_checked_'+via);
    })) return false;
    const self = phantomShortKey(selfHKey);
    let committed =
      transactionBase ||
      phantomFreshSysAuthority() ||
      phantomLocalCommittedAuthority();

    let requestedRoster = phantomAuthorityRosterRows(nextRows);
    if (self && !requestedRoster.some((row) => row.hKey === self)) {
      requestedRoster.unshift(phantomLocalPresenceRow());
      requestedRoster = phantomAuthorityRosterRows(requestedRoster);
    }

    const requestedKeys = new Set(requestedRoster.map((row) => row.hKey));
    const terminalExits = phantomAuthorityTerminalExits(transactionMeta).map(
      (rec) => ({
        ...rec,
        at: Number(rec.at || now()),
        via: String(rec.via || via || "terminal_exit"),
      }),
    );
    const terminalMembers = new Set(terminalExits.map((rec) => rec.member));
    const admission = transactionMeta?.admission
      ? {
          member: phantomShortKey(transactionMeta.admission.member),
          bootId: String(transactionMeta.admission.bootId || "") || null,
          at: Number(transactionMeta.admission.at || now()),
          via: String(
            transactionMeta.admission.via || via || "member_admission",
          ),
        }
      : null;

    const replica = phantomSysAuthority.pending;
    if (replica && phantomShortKey(replica.acceptedBy) === self &&
        terminalMembers.has(phantomShortKey(replica.snapshot?.from)) &&
        Number(replica.snapshot?.epoch || 0) === Number(committed?.epoch || 0) &&
        Number(replica.snapshot?.rosterSeq || 0) <= Number(committed?.rosterSeq || 0)) {
      phantomSysAuthority.pending = null;
      phantomMembershipAuditSet("committed_replica_retired", {peer: replica.snapshot?.from, id: replica.id});
    }
    if (phantomSysAuthority.pending) {
      const oldPending = phantomSysAuthority.pending;
      const oldMirror = phantomShortKey(
        oldPending.mirror || oldPending.snapshot?.syscon2,
      );
      const preemptTerminalMirror = !!(
        oldMirror &&
        phantomPendingTerminalExits.has(oldMirror) &&
        !requestedKeys.has(oldMirror)
      );

      if (!preemptTerminalMirror) {
        phantomMembershipAuditSet("stage_blocked", {
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
        from: phantomShortKey(oldPending.baseAuthority?.from),
        syscon1: phantomShortKey(oldPending.baseAuthority?.syscon1),
        syscon2: phantomShortKey(oldPending.baseAuthority?.syscon2) || null,
        epoch: Number(oldPending.baseEpoch || 0),
        rosterSeq: Number(oldPending.baseRosterSeq || 0),
        roster: phantomAuthorityRosterRows(oldPending.baseRoster),
      };

      phantomMembershipAuditSet("stage_preempted", {
        peer: oldMirror,
        id: oldPending.id,
        reason: "terminal_pending_mirror_replaced",
      });
      phantomSysAuthority.pending = null;
      emitSnapshot("sys_authority_terminal_pending_mirror_replaced");
    }

    const baseRows = phantomAuthorityRosterRows(committed?.roster);
    const baseKeys = new Set(baseRows.map((row) => row.hKey));
    const baseOne = phantomShortKey(committed?.syscon1);
    const baseTwo = phantomShortKey(committed?.syscon2);
    const baseEpoch = Number(committed?.epoch || 0);
    const baseSeq = Number(committed?.rosterSeq || 0);
    const genesis = !baseEpoch && !baseSeq && !baseRows.length;

    const overrideOne = phantomShortKey(topologyOverride?.syscon1);
    const overrideTwoSpecified = Object.prototype.hasOwnProperty.call(
      topologyOverride || {},
      "syscon2",
    );
    const overrideTwo = phantomShortKey(topologyOverride?.syscon2);
    const one = phantomShortKey(overrideOne || baseOne || concierge.syscon1);

    if (!self || one !== self) {
      phantomMembershipAuditSet("stage_rejected", {
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
    const currentOne = phantomShortKey(concierge.syscon1);
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
        ? phantomElectAuthorityCandidate(requestedRoster, "", true, true)
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
        phantomMembershipAuditSet("stage_rejected", {
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
      phantomMembershipAuditSet("stage_rejected", {
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
        phantomMembershipAuditSet("stage_rejected", {
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
        phantomMembershipAuditSet("stage_rejected", {
          peer: admission?.member,
          reason: "untagged_membership_addition",
          added: added.join(",") || "none",
        });
        return false;
      }
    }

    const cfTwo = phantomShortKey(concierge.cfConciergeSyscon2);
    let two = null;

    if (requestedRoster.length > 1) {
      if (
        overrideTwoSpecified &&
        overrideTwo &&
        overrideTwo !== one &&
        requestedKeys.has(overrideTwo) &&
        !phantomPendingTerminalExits.has(overrideTwo)
      ) {
        two = overrideTwo;
      } else if (
        baseTwo &&
        baseTwo !== one &&
        requestedKeys.has(baseTwo) &&
        !phantomPendingTerminalExits.has(baseTwo)
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
        two = phantomElectAuthorityCandidate(requestedRoster, one, true, false);
      }

      if (!two) {
        phantomMembershipAuditSet("stage_blocked", {
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
          from: phantomShortKey(committed?.from || baseOne),
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
    pending.id = phantomAuthorityTransactionId(pending);

    if (!pending.id) return false;

    phantomSysAuthority.pending = pending;
    phantomMembershipAuditSet("stage_created", {
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
      const promoted = phantomApplyCommittedAuthority(
        nextSnapshot,
        pending.via + "_solo_commit",
        pending.baseRoster,
        pending,
      );
      phantomSysAuthority.pending = null;
      phantomMembershipAuditSet(
        promoted ? "solo_committed" : "solo_commit_failed",
        {
          id: pending.id,
          count: requestedRoster.length,
          nextSeq: Number(nextSnapshot.rosterSeq || 0),
        },
      );
      return promoted;
    }

    const mirrorEnt = phantomSysPeers.get(two);
    const mirrorDc = mirrorEnt?.sysdc || null;
    const pendingStatus = phantomSysStatusPayload("SYS_STATUS");
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
    const sent = phantomSendSysDC(two, pendingStatus);
    const sendErr = String(concierge.sysLastErr || "none");

    phantomMembershipAuditSet(sent ? "stage_sent" : "stage_waiting_pipe", {
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
  function phantomAcceptPendingAuthority(msg, transportPeer) {
    const pending = msg?.pendingAuthority;
    const self = phantomShortKey(selfHKey);
    const sender = phantomShortKey(msg?.from || transportPeer);
    const transport = phantomShortKey(transportPeer);

    if (!pending || typeof pending !== "object") return false;

    const pendingId = String(pending.id || "");
    const snapshot = pending.snapshot || null;
    const pendingLeader = phantomShortKey(snapshot?.syscon1);
    const pendingMirror = phantomShortKey(snapshot?.syscon2);

    if (!self || !sender || sender !== transport) {
      phantomMembershipAuditSet("mirror_rejected", {
        peer: sender || transport,
        id: pendingId,
        reason: !self ? "missing_self" : "sender_transport_mismatch",
      });
      return false;
    }

    if (sender !== pendingLeader || self !== pendingMirror) {
      phantomMembershipAuditSet("mirror_rejected", {
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

    if (pendingId !== phantomAuthorityTransactionId(pending)) {
      phantomMembershipAuditSet("mirror_rejected", {
        peer: sender,
        id: pendingId,
        reason: "pending_transaction_id_mismatch",
      });
      return false;
    }

    const baseEpoch = Number(pending.baseEpoch || 0);
    const baseSeq = Number(pending.baseRosterSeq || 0);
    const baseRoster = phantomAuthorityRosterRows(pending.baseRoster);
    const baseAuthority = pending.baseAuthority || null;
    const baseKeys = new Set(baseRoster.map((row) => row.hKey));
    const nextRoster = phantomAuthorityRosterRows(snapshot?.roster);
    const nextKeys = new Set(nextRoster.map((row) => row.hKey));
    const terminalExits = phantomAuthorityTerminalExits(pending);
    const terminalMembers = new Set(terminalExits.map((rec) => rec.member));
    const admission = pending.admission || null;
    const leadership = pending.leadership || null;
    const leadershipMode = String(leadership?.mode || "");
    const leadershipWinner = phantomShortKey(leadership?.winner);
    const leadershipBaseOne = phantomShortKey(leadership?.baseOne);
    const leadershipBaseTwo = phantomShortKey(leadership?.baseTwo);
    const genesis = !baseEpoch && !baseSeq && !baseRoster.length;

    if (
      !snapshot ||
      !nextRoster.length ||
      !nextKeys.has(sender) ||
      !nextKeys.has(self)
    ) {
      phantomMembershipAuditSet("mirror_rejected", {
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
      phantomMembershipAuditSet("mirror_rejected", {
        peer: sender,
        id: pendingId,
        reason: "invalid_genesis_leadership_witness",
      });
      return false;
    }

    if (!genesis) {
      const baseOne = phantomShortKey(baseAuthority?.syscon1);
      const baseTwo = phantomShortKey(baseAuthority?.syscon2);
      const baseFrom = phantomShortKey(baseAuthority?.from);
      const baseAuthorityRows = phantomAuthorityRosterRows(baseAuthority?.roster);

      if (
        !baseAuthority ||
        baseFrom !== baseOne ||
        !baseOne ||
        Number(baseAuthority.epoch || 0) !== baseEpoch ||
        Number(baseAuthority.rosterSeq || 0) !== baseSeq ||
        phantomAuthorityRosterKey(baseAuthorityRows) !==
          phantomAuthorityRosterKey(baseRoster) ||
        !baseKeys.has(baseOne) ||
        (baseTwo && !baseKeys.has(baseTwo))
      ) {
        phantomMembershipAuditSet("mirror_rejected", {
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
      const admissionMember = phantomShortKey(admission?.member);

      if (
        removedKey !== terminalKey ||
        added.length !== (admissionMember ? 1 : 0) ||
        (admissionMember && added[0] !== admissionMember)
      ) {
        phantomMembershipAuditSet("mirror_rejected", {
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
        phantomMembershipAuditSet("mirror_rejected", {
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
        phantomMembershipAuditSet("mirror_rejected", {
          peer: sender,
          id: pendingId,
          reason: "healthy_committed_mirror_replaced",
          baseTwo,
          nextTwo: pendingMirror,
        });
        return false;
      }
    }

    const local = phantomLocalCommittedAuthority();
    const localMatchesBase = !!(
      !genesis &&
      local &&
      Number(local.epoch || 0) === baseEpoch &&
      Number(local.rosterSeq || 0) === baseSeq &&
      phantomAuthorityRosterKey(local.roster) ===
        phantomAuthorityRosterKey(baseRoster)
    );
    const localIsGenesis = !!(
      genesis &&
      !Number(phantomSysAuthority.epoch || 0) &&
      !Number(phantomSysAuthority.rosterSeq || 0)
    );
    const freshMirrorAdmission = !!(
      !genesis &&
      !local &&
      !Number(phantomSysAuthority.epoch || 0) &&
      !Number(phantomSysAuthority.rosterSeq || 0) &&
      phantomShortKey(admission?.member) === self &&
      String(admission?.bootId || "") === String(phantomBootId || "") &&
      !baseKeys.has(self) &&
      nextKeys.has(self)
    );

    if (!localMatchesBase && !localIsGenesis && !freshMirrorAdmission) {
      phantomMembershipAuditSet("mirror_rejected", {
        peer: sender,
        id: pendingId,
        reason: "base_mismatch",
        baseEpoch,
        baseSeq,
        localEpoch: Number(phantomSysAuthority.epoch || 0),
        localSeq: Number(phantomSysAuthority.rosterSeq || 0),
      });
      emitSnapshot("sys_authority_pending_base_mismatch");
      return false;
    }

    const snapshotEpoch = Number(snapshot.epoch || 0);
    if (!snapshotEpoch || snapshotEpoch < baseEpoch) {
      phantomMembershipAuditSet("mirror_rejected", {
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
      pendingLeader !== phantomShortKey(baseAuthority?.syscon1) &&
      snapshotEpoch <= baseEpoch
    ) {
      phantomMembershipAuditSet("mirror_rejected", {
        peer: sender,
        id: pendingId,
        reason: "leader_change_without_epoch_advance",
        snapshotEpoch,
        baseEpoch,
      });
      return false;
    }

    phantomSysAuthority.pending = {
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

    phantomMembershipAuditSet("mirror_accepted", {
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
  function phantomPromotePendingAuthority(acceptedId, transportPeer) {
    if (!phantomAuthorizeSyscon1Duty('commit:'+String(acceptedId), () => phantomPromotePendingAuthority(acceptedId, transportPeer))) return false;
    const pending = phantomSysAuthority.pending;
    const sender = phantomShortKey(transportPeer);
    const accepted = String(acceptedId || "");

    if (!pending && !accepted) return false;
    if (!pending) {
      phantomMembershipAuditSet("promote_rejected", {
        peer: sender,
        id: accepted,
        reason: "no_local_pending",
      });
      return false;
    }

    if (!sender || !accepted || accepted !== String(pending.id || "")) {
      if (sender === phantomShortKey(pending.mirror)) {
        phantomMembershipAuditSet("promote_rejected", {
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

    if (sender !== phantomShortKey(pending.mirror)) {
      phantomMembershipAuditSet("promote_rejected", {
        peer: sender,
        id: pending.id,
        reason: "ack_not_from_exact_mirror",
      });
      return false;
    }

    if (phantomShortKey(pending.snapshot?.syscon1) !== phantomShortKey(selfHKey)) {
      phantomMembershipAuditSet("promote_rejected", {
        peer: sender,
        id: pending.id,
        reason: "local_not_pending_leader",
      });
      return false;
    }

    const promoted = phantomApplyCommittedAuthority(
      pending.snapshot,
      pending.via + "_mirror_accepted_commit",
      pending.baseRoster,
      pending,
    );

    if (!promoted) {
      phantomMembershipAuditSet("promote_rejected", {
        peer: sender,
        id: pending.id,
        reason: "committed_apply_failed",
      });
      return false;
    }

    const promotedId = pending.id;
    const promotedSeq = Number(pending.snapshot?.rosterSeq || 0);
    const promotedCount = phantomAuthorityRosterRows(
      pending.snapshot?.roster,
    ).length;

    phantomSysAuthority.pending = null;
    phantomPublishCommittedAuthority(
      "sys_authority_committed_after_mirror_accept",
    );
    phantomMembershipAuditSet("promoted", {
      peer: sender,
      id: promotedId,
      seq: promotedSeq,
      count: promotedCount,
    });
    emitSnapshot("sys_authority_pending_promoted_to_committed");
    queueMicrotask(sdkResumeAdmissions);
    phantomResumeTerminalAuthority("post_authority_commit");
    return true;
  }
  const sdkDeferredAdmissions=new Map();
  function sdkApplySysAdmission(key,inner) {
    if(String(concierge.localRoleCandidate || concierge.role || "peer")!=="syscon1")return;
    const fromKey=phantomShortKey(inner.from),selfKey=phantomShortKey(selfHKey);
    if (fromKey !== selfKey && !host.canAdmitPeer(fromKey)) {
      sdkDeferredAdmissions.delete(fromKey);
      emitSnapshot('peer_authorization_denied');
      return;
    }
            const committedRoster = phantomAuthorityRosterRows(
              phantomSysAuthority.roster,
            );
            const alreadyCommitted = committedRoster.some(
              (row) => row.hKey === fromKey,
            );
            const doorwayAdmitted = !!(
              fromKey && phantomDoorwayAdmittedPeers.has(fromKey)
            );
            const sourcePipeProven = !!(
              key &&
              phantomHasOpenSysPipe(key) &&
              (key === fromKey || phantomShortKey(inner?.forwardedBy) === key)
            );
            const directTransport = !!(key && key === fromKey);
            const explicitAdmission = inner?.admit === true;
            const blocked = phantomIsBlocked(fromKey);
            const blockedBy = phantomIsBlockedBy(fromKey);
            const goneLatched = __phantomGoneNotified.has(fromKey);

            if (!alreadyCommitted && fromKey && fromKey !== selfKey) {
              phantomMembershipAuditSet("admit_received", {
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
              inner?.bootId || phantomPresenceBook.get(fromKey)?.bootId || "",
            );
            const knownBootId = String(phantomRemoteBootIds.get(fromKey) || "");
            const exitedIncarnation = __phantomMembershipExitApplied.get(fromKey);
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

            if (freshDirectAdmission && phantomSysAuthority.pending) {
              sdkDeferredAdmissions.set(fromKey,{key,inner:structuredClone(inner),entry:phantomSysPeers.get(key)});
              emitSnapshot("sys_admission_deferred_pending_commit");
              return;
            }
            if (freshDirectAdmission) {
              // A staged admission is not committed yet. Keep its proven request
              // so replacing a pending transaction after mirror loss can replay it.
              sdkDeferredAdmissions.set(fromKey,{key,inner:structuredClone(inner),entry:phantomSysPeers.get(key)});
              const presence = phantomPresenceBook.get(fromKey) || inner || {};
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
                seenVia: "phantom_sys_authority",
                t: now(),
              });

              if (
                phantomCommitSyscon1Roster(
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
                phantomRefreshSysCandidates(
                  phantomLatchedSysRosterList(),
                  "syscon1_member_admission",
                );
                phantomPublishCommittedAuthority(
                  "syscon1_member_admission_published",
                );
                emitSnapshot(
                  "syscon1_member_admission_staged_from_proven_sys_tether",
                );
              }
            } else if (alreadyCommitted) {
              sdkDeferredAdmissions.delete(fromKey);
              // Presence decorates committed identity only. It never rewrites
              // the immutable authority value or advances rosterSeq.
              phantomEmitPresenceRoster("syscon1_presence_metadata");
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
                phantomMembershipAuditSet("admit_rejected", {
                  peer: fromKey,
                  reason: rejection,
                  doorway: doorwayAdmitted,
                  pipe: sourcePipeProven,
                  direct: directTransport,
                  admit: explicitAdmission,
                  gone: __phantomGoneNotified.has(fromKey),
                  blocked,
                  blockedBy,
                });
              }

              emitSnapshot("syscon1_presence_not_committed");
            }

            phantomResumeTerminalAuthority("syscon1_presence_rx");
  }
  function sdkResumeAdmissions() {
    resumeDeferredAdmissions(sdkDeferredAdmissions,{
      hasPending:()=>!!phantomSysAuthority.pending,
      currentEntry:key=>phantomSysPeers.get(key),
      pipeOpen:phantomHasOpenSysPipe,
      apply:sdkApplySysAdmission,
      onRetired:()=>emitSnapshot("sys_deferred_admission_transport_retired")
    });
  }
  function phantomCommitSyscon1Roster(
    nextRows,
    via = "syscon1_commit",
    transactionMeta = null,
  ) {
    return phantomStageSyscon1Authority(
      nextRows,
      via,
      null,
      null,
      transactionMeta,
    );
  }

  function phantomPublishCommittedAuthority(reason = "syscon1_publish") {
    if (!phantomAuthorizeSyscon1Duty('publish:'+reason, () => phantomPublishCommittedAuthority(reason))) return 0;
    const self = phantomShortKey(selfHKey);
    const pending = phantomSysAuthority.pending;
    const pendingLeader = phantomShortKey(pending?.snapshot?.syscon1);
    const committedLeader = phantomShortKey(phantomSysAuthority.syscon1);

    // A successor may publish a staged transaction whose base was imported
    // from the mirror without first projecting that historical base into live
    // authority. Therefore publication ownership may come from either the
    // current committed leader or the exact staged snapshot leader.
    if (!self || (committedLeader !== self && pendingLeader !== self)) return 0;

    const status = phantomSysStatusPayload("SYS_STATUS");
    const pendingId = String(status?.pendingAuthority?.id || "");
    const publishRows = pendingId
      ? phantomAuthorityRosterRows(status.pendingAuthority?.snapshot?.roster)
      : phantomAuthorityRosterRows(status.roster);

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

      const targetEnt = phantomSysPeers.get(target);
      const targetDc = targetEnt?.sysdc || null;

      if (!phantomHasOpenSysPipe(target)) {
        if (pendingId) {
          phantomMembershipAuditSet("publish_skipped_pipe", {
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
      const sent = phantomSendSysDC(target, status);
      const sendErr = String(concierge.sysLastErr || "none");

      if (pendingId) {
        phantomMembershipAuditSet(sent ? "publish_sent" : "publish_send_failed", {
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
      const publicationId = phantomAuthoritySnapshotId(status);
      if (ws.lastAuthorityPublication !== publicationId) {
        try { ws.publishAuthority(status); ws.lastAuthorityPublication = publicationId; }
        catch (error) { concierge.sysLastErr = "doorway_authority_publish_failed:" + String(error?.message || error); }
      }
    }
    emitSnapshot(String(reason || "syscon1_publish") + "_" + published);
    return published;
  }
  function phantomRememberSysAuthority(msg, via = "sys_status") {
    const applied = phantomApplyCommittedAuthority(msg, via);

    if (
      applied &&
      phantomSysAuthority.pending?.acceptedBy === phantomShortKey(selfHKey) &&
      phantomAuthoritySnapshotId(msg) ===
        phantomAuthoritySnapshotId(phantomSysAuthority.pending?.snapshot)
    ) {
      phantomSysAuthority.pending = null;
      emitSnapshot("sys_authority_mirror_pending_cleared_on_commit");
    }

    if (applied) {
      sdkBootstrapTarget = null;
      sdkBootstrapUnanswered.clear();
      phantomGossipRecovery = {
        source: null,
        targets: [],
        index: 0,
        epoch: 0,
        rosterSeq: 0,
        at: 0,
      };
      phantomResumeTerminalAuthority("committed_authority_received");
      emitSnapshot("authority_gossip_recovery_completed_by_committed_sys");
    }
    return applied;
  }
  function phantomAdoptMirrorAuthority(msg, transportPeer) {
    if (!msg || typeof msg !== "object") return false;

    const self = phantomShortKey(selfHKey);
    const sender = phantomShortKey(msg.from || transportPeer);
    const currentOne = phantomShortKey(concierge.syscon1);
    const currentTwo = phantomShortKey(concierge.syscon2);
    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const mirror = msg.mirrorAuthority;

    if (!self || role !== "syscon1" || currentOne !== self) return false;
    if (
      !sender ||
      sender !== phantomShortKey(transportPeer) ||
      sender !== currentTwo
    ) {
      return false;
    }
    if (!mirror || typeof mirror !== "object") return false;

    const historicalFrom = phantomShortKey(mirror.from || mirror.syscon1);
    const historicalOne = phantomShortKey(mirror.syscon1);
    const historicalTwo = phantomShortKey(mirror.syscon2);
    const historicalEpoch = Number(mirror.epoch || 0);
    const historicalSeq = Number(mirror.rosterSeq || 0);
    const inherited = phantomAuthorityRosterRows(mirror.roster);
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

    const local = phantomLocalCommittedAuthority();
    if (
      local &&
      (Number(local.epoch || 0) > historicalEpoch ||
        (Number(local.epoch || 0) === historicalEpoch &&
          Number(local.rosterSeq || 0) >= historicalSeq))
    ) {
      return false;
    }

    const recovered = phantomApplyCommittedAuthority(
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
      const member = phantomShortKey(command?.member);
      if (!member || !inheritedKeys.has(member)) continue;

      const incoming = {
        bootId: String(command?.bootId || "") || null,
        epoch: Number(command?.epoch || historicalEpoch),
        at: Number(command?.at || now()),
        via: "mirror_authority_recovery",
        reason: String(command?.via || "mirror_authority_recovery"),
      };
      const current = phantomPendingTerminalExits.get(member);
      if (!current || Number(incoming.at || 0) >= Number(current.at || 0)) {
        phantomPendingTerminalExits.set(member, incoming);
      }
    }

    // Provider revision counters cannot invalidate committed SYS authority.
    const acceptedEpoch = Number(concierge.sysconEpoch || 0);

    if (acceptedEpoch > historicalEpoch) {
      const staged = phantomStageSyscon1Authority(
        inherited,
        "syscon1_exact_mirror_epoch_recovery",
        phantomLocalCommittedAuthority(),
        { syscon1: self, syscon2: sender },
      );

      if (!staged) {
        emitSnapshot("syscon1_exact_mirror_epoch_recovery_waiting");
        return false;
      }

      phantomPublishCommittedAuthority(
        "syscon1_exact_mirror_epoch_recovery_published",
      );
    } else {
      phantomPublishCommittedAuthority("syscon1_exact_mirror_base_recovered");
    }

    phantomResumeTerminalAuthority("mirror_base_recovery");
    emitSnapshot("syscon1_exact_mirror_base_recovered");
    return true;
  }

  function phantomLocalCommittedAuthority() {
    const roster = phantomAuthorityRosterRows(phantomSysAuthority.roster);
    const from = phantomShortKey(phantomSysAuthority.from);
    const one = phantomShortKey(phantomSysAuthority.syscon1);
    const two = phantomShortKey(phantomSysAuthority.syscon2);
    const epoch = Number(phantomSysAuthority.epoch || 0);
    const rosterSeq = Number(phantomSysAuthority.rosterSeq || 0);
    const keys = new Set(roster.map((row) => row.hKey));

    if (
      !phantomSysAuthority.at ||
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
      at: Number(phantomSysAuthority.at || 0),
      via: String(phantomSysAuthority.via || "none"),
      from,
      syscon1: one,
      syscon2: two || null,
      epoch,
      rosterSeq,
      roster,
    };
  }

  function phantomFreshSysAuthority() {
    if (!phantomSysAuthority.at) return null;

    const authorityEpoch = Number(phantomSysAuthority.epoch || 0);
    // Provider revision counters cannot invalidate committed SYS authority.
    const acceptedEpoch = Number(concierge.sysconEpoch || 0);

    if (authorityEpoch && acceptedEpoch && authorityEpoch < acceptedEpoch) {
      return null;
    }

    const one = phantomShortKey(phantomSysAuthority.syscon1);
    const two = phantomShortKey(phantomSysAuthority.syscon2);

    if (!one && !two) return null;

    return {
      syscon1: one || null,
      syscon2: two || null,
      epoch: Number(phantomSysAuthority.epoch || 0),
      rosterSeq: Number(phantomSysAuthority.rosterSeq || 0),
      roster: Array.isArray(phantomSysAuthority.roster)
        ? phantomSysAuthority.roster
        : [],
      via: phantomSysAuthority.via || "unknown",
      at: Number(phantomSysAuthority.at || 0),
    };
  }

  function phantomApplyLocalRoleCandidate(reason = "candidate") {
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

  function phantomCompareSysconClaims(localClaim, remoteClaim) {
    const le = Number(localClaim?.epoch || 0);
    const re = Number(remoteClaim?.epoch || 0);

    if (re > le) return { winner: "remote", why: "epoch" };
    if (le > re) return { winner: "local", why: "epoch" };

    const lj = Number(localClaim?.joinedAt || 0) || Number.MAX_SAFE_INTEGER;
    const rj = Number(remoteClaim?.joinedAt || 0) || Number.MAX_SAFE_INTEGER;

    if (rj < lj) return { winner: "remote", why: "older" };
    if (lj < rj) return { winner: "local", why: "older" };

    const lh = phantomShortKey(localClaim?.hKey);
    const rh = phantomShortKey(remoteClaim?.hKey);

    if (rh && lh && rh < lh) return { winner: "remote", why: "tie_hkey" };
    return { winner: "local", why: "tie_hkey" };
  }

  function phantomReconcileRoleFromSysStatus(msg, via = "sys_status") {
    if (!msg || typeof msg !== "object") return false;

    const self = phantomShortKey(selfHKey);
    const remote = phantomShortKey(msg.from);
    if (!self || !remote || remote === self) return false;

    const localRole = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const remoteRole = String(msg.role || msg.candidateRole || "peer");

    if (!phantomIsSysconRole(localRole)) return false;
    if (!phantomIsSysconRole(remoteRole)) return false;
    if (localRole !== remoteRole) return false;

    const result = phantomCompareSysconClaims(
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
      const one = phantomShortKey(msg.syscon1 || remote);
      const two = phantomShortKey(msg.syscon2);

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

      phantomRememberSysAuthority(msg, `demotion_${via}`);

      concierge.candidateReason = `demoted_resynced_${concierge.roleDemotedBy}`;
      concierge.localRoleReason = `role_demoted_resynced_${concierge.roleDemotedBy}`;
      concierge.lastErr = null;

      phantomRefreshSysReadiness(`role_demoted_${via}`);
      phantomScheduleSysAdmission(`role_demoted_${via}`);
      emitSnapshot(`role_demoted_resynced_${localRole}_${result.why}`);
      return true;
    }

    concierge.role = localRole;
    concierge.localRoleReason = `role_kept_${localRole}_${result.why}_${via}`;
    emitSnapshot(`role_kept_${localRole}_${result.why}`);
    return false;
  }

  function phantomCanUseSys(hKey) {
    const key = phantomShortKey(hKey);
    if (!key) return false;

    if (phantomRelationshipRevoked.has(key)) return false;
    if (phantomIsBlocked(key) || phantomIsBlockedBy(key)) return false;

    const st = getState(key);
    return (
      st === VEIL.INVITE_SENT ||
      st === VEIL.INVITE_RECEIVED ||
      st === VEIL.ESTABLISHED ||
      st === VEIL.INSIDE
    );
  }

  function phantomCanUseChat(hKey) {
    const key = phantomShortKey(hKey);
    if (!key || !canUsePeer(key)) return false;
    if (phantomRelationshipRevoked.has(key)) return false;
    return getState(key) === VEIL.INSIDE;
  }

  function phantomIsSysMessage(sys) {
    return !!sys;
  }

  function phantomIsEstablishedLike(hKey) {
    const st = getState(hKey);
    return st === VEIL.ESTABLISHED || st === VEIL.INSIDE;
  }

  function phantomIsReconnectPrimary(remoteHKey) {
    const local = phantomShortKey(selfHKey);
    const remote = phantomShortKey(remoteHKey);
    return !!local && !!remote && local < remote;
  }

  function phantomPresentReconnect(
    remoteHKey,
    proof = "nonterminal_reconnect",
    observedBootId = "",
  ) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;

    const reconnect = reconnectingPeers.get(key);
    if (!reconnect || reconnect.presented === true) return false;

    const relationship = phantomRelationshipGet(key);
    if (
      phantomPendingTerminalExits.has(key) ||
      __phantomGoneNotified.has(key) ||
      !phantomRelationshipIsAccepted(relationship) ||
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

  function phantomEndFailedChat(remoteHKey, reason = "reconnect_failed") {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;

    const relationship = phantomRelationshipGet(key);
    if (
      phantomPendingTerminalExits.has(key) ||
      __phantomGoneNotified.has(key) ||
      !phantomRelationshipIsAccepted(relationship) ||
      !lastRoster.has(key)
    ) {
      emitSnapshot("chat_attempt_expiry_observed_terminal_relationship");
      return false;
    }

    const ent = phantomChatPeers.get(key);
    if (ent && ent.chatState !== "ready") {
      ent.chatFault = String(reason || "reconnect_failed");
    }

    phantomPresentReconnect(key, "nonterminal_deadline");

    concierge.lastErr =
      "chat_attempt_expired_observation:" +
      String(reason || "reconnect_failed");
    emitSnapshot("chat_attempt_expired_relationship_and_successor_preserved");
    return true;
  }

  function phantomCancelReconnect(remoteHKey, restored = false) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return;

    phantomCancelChatAttemptDeadline(key);

    const reconnect = reconnectingPeers.get(key) || null;
    const wasReconnecting = !!reconnect;
    const wasPresented = reconnect?.presented === true;
    reconnectingPeers.delete(key);

    if (restored) {
      const ent = phantomChatPeers.get(key);
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

  function phantomCanReconnect(remoteHKey) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;
    if (__phantomExitSent) return false;
    if (__phantomGoneNotified.has(key)) return false;
    return phantomIsEstablishedLike(key);
  }

  function phantomScheduleReconnect(remoteHKey, reason = "transport_lost") {
    const key = phantomShortKey(remoteHKey);
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

    if (!phantomCanReconnect(key)) {
      phantomCancelReconnect(key);
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

    const relationship = phantomRelationshipGet(key);
    const livenessProbe = phantomPrepareRelationshipControl(
      {
        type: "veil_status",
        from: selfHKey,
        to: key,
        videoId,
        state: getState(key),
        cycleId: relationship?.cycleId || null,
        reconnectProbe: true,
        bootId: phantomBootId,
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
        relationship?.remoteBootId || phantomRemoteBootIds.get(key) || "",
      ),
    };
    reconnectingPeers.set(key, reconnect);

    if (!phantomChatReconnectDeadlines.has(key)) {
      const deadline = setTimeout(() => {
        phantomChatReconnectDeadlines.delete(key);

        if (!reconnectingPeers.has(key)) return;
        if (!phantomCanReconnect(key)) return;

        phantomEndFailedChat(key, "reconnect_deadline_observed");
      }, PHANTOM_CHAT_RECONNECT_MS);

      phantomChatReconnectDeadlines.set(key, deadline);
    }

    // Transport repair starts immediately, but a shell-visible failure is not
    // projected yet. A directed relationship proof from the same incarnation
    // distinguishes a real chat-only interruption from a peer that is already
    // exiting and awaiting committed authority removal.
    reconnect.probeSent = phantomSendControl(
      livenessProbe,
      "veil_status_reconnect_probe",
    );
    reconnectingPeers.set(key, reconnect);
    emitSnapshot(
      reconnect.probeSent
        ? "chat_reconnect_waiting_for_peer_liveness_proof"
        : "chat_reconnect_waiting_for_authority_or_deadline",
    );

    if (!phantomIsReconnectPrimary(key)) {
      /*
       * Deterministic non-owner path:
       * teardown is already complete; wait for the pair owner’s offer.
       */
      emitSnapshot("carrier_reconnect_waiting_for_pair_owner_" + why);
      return true;
    }

    emitSnapshot("carrier_reconnect_owner_birth_" + why);

    void phantomRequestChatPipe(
      key,
      "carrier_reconnect_owner_" + String(why || "transport_lost"),
    );

    return true;
  }

  function phantomHandleTransportLoss(remoteHKey, reason = "transport_lost") {
    const key = phantomShortKey(remoteHKey);
    if (!key) return;

    const why = String(reason || "transport_lost").toLowerCase();
    const sysFault = /^(sysdc|sys_pipe|sys_transport|sys)_/.test(why);
    const chatFault = /^(dc|chat|chat_pipe|pc|ice)_/.test(why);

    if (sysFault) {
      // Only a confirmed failure on this exact transport may credit an earlier
      // disconnected interval. Teardown below removes that transport entry.
      const outageStartedAt = sysOutageClock.confirmedStart(phantomSysPeers.get(key)?.pc);
      if (phantomTemporarySysTransactions.has(key)) {
        phantomCloseTemporarySysTransaction(key, "transport_fault_" + why);
        emitSnapshot("temporary_sys_transport_fault_ignored_by_room_sys");
        return;
      }

      const gossipTarget = phantomShortKey(
        phantomGossipRecovery.targets[phantomGossipRecovery.index],
      );

      if (gossipTarget && gossipTarget === key) {
        phantomTeardownSysPeer(key, "authority_gossip_target_failed_" + why);

        if (phantomShortKey(concierge.sysReadyPeer) === key) {
          concierge.sysReadyPeer = null;
          concierge.sysReadyAt = 0;
          concierge.sysReadyProof = "none";
          concierge.sysReadyEpoch = 0;
        }

        if (phantomShortKey(concierge.sysPeer) === key) {
          concierge.sysPeer = null;
          concierge.sysPipeState = "faulted";
          concierge.sysLastErr = why;
        }

        phantomGossipRecovery.index += 1;
        if (phantomGossipRecovery.kind === 'committed_mirror') {
          const failures = Number(phantomGossipRecovery.retryCount || 0) + 1;
          phantomGossipRecovery.retryCount = failures;
          phantomGossipRecovery.nextRetryAt = now() + Math.min(15000, 1000 * 2 ** Math.min(failures - 1, 4));
        }
        phantomRefreshSysReadiness("authority_gossip_target_failed");

        const nextTarget = phantomShortKey(
          phantomGossipRecovery.targets[phantomGossipRecovery.index],
        );

        if (nextTarget) {
          phantomScheduleSysAdmission("authority_gossip_next_syscon");
          emitSnapshot("authority_gossip_next_syscon_requested");
        } else {
          emitSnapshot("authority_gossip_syscons_unreachable");
          phantomBeginGossipSuccessorRecovery(
            "authority_gossip_syscons_unreachable",
          );
        }
        return;
      }

      if (__phantomGoneNotified.has(key)) {
        phantomTeardownSysPeer(key, "sys_loss_peer_gone");
        emitSnapshot("sys_pipe_loss_ignored_peer_gone");
        return;
      }

      if (phantomSysReplacementTerminal(key)) {
        phantomTeardownSysPeer(key, "sys_loss_terminal_exit_pending");
        phantomSysDialing.delete(key);
        phantomSysPreflightUnstableRetries.delete(key);
        phantomPendingRtcIce.delete(key);
        emitSnapshot("sys_pipe_loss_held_terminal_exit_pending");
        return;
      }

      phantomTeardownSysPeer(key, "sys_transport_retired_" + why);

      if (phantomShortKey(concierge.sysReadyPeer) === key) {
        concierge.sysReadyPeer = null;
        concierge.sysReadyAt = 0;
        concierge.sysReadyProof = "none";
        concierge.sysReadyEpoch = 0;
      }

      if (phantomShortKey(concierge.sysPeer) === key) {
        concierge.sysPipeState = "faulted";
        concierge.sysLastErr = why;
      }

      phantomRefreshSysReadiness("sys_transport_fault_" + why);

      const role = String(
        concierge.localRoleCandidate || concierge.role || "peer",
      );

      if (role === "peer" || !phantomCommittedMember(selfHKey)) {
        phantomScheduleSysAdmission("sys_transport_fault_" + why);
      } else {
        phantomArmSysReplacement(key, why, outageStartedAt);
        phantomMaybeRelinkSysconPair("sys_transport_fault_" + why);
      }

      queueMicrotask(sdkResumeDeferredOffers);
      emitSnapshot("sys_transport_fault_" + why);
      return;
    }

    if (chatFault) {
      const st = getState(key);

      rtcTeardown(key, "chat_transport_retired_" + why);

      if (
        __phantomGoneNotified.has(key) ||
        st === VEIL.OUTSIDE ||
        st === VEIL.REMOVED
      ) {
        emitSnapshot("chat_transport_loss_ignored_not_established");
        return;
      }

      concierge.chatDropCount = Number(concierge.chatDropCount || 0) + 1;
      concierge.chatLastDropAt = now();
      concierge.chatLastDropPeer = key;

      phantomScheduleReconnect(key, why);
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
      bootId: phantomBootId,
    };

    if (pipe === "sys") {
      const base = phantomLocalCommittedAuthority();
      if (kind === "offer" && base && phantomShortKey(base.syscon2) === fromKey &&
          phantomPendingTerminalExits.has(phantomShortKey(base.syscon1))) {
        payload.successorProof = {room: videoId, from: fromKey, to: toKey, bootId: phantomBootId,
          baseOne: base.syscon1, baseTwo: base.syscon2, baseEpoch: base.epoch,
          baseSeq: base.rosterSeq, baseRoster: phantomAuthorityRosterRows(base.roster), terminalLeader: base.syscon1};
      }
      const ent = phantomSysPeers.get(toKey);
      const attemptId = String(ent?.attempt?.id || ent?.sysAttemptId || "");

      if (attemptId) payload.sysAttemptId = attemptId;

      const transaction = phantomTemporarySysTransactions.get(toKey);
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
      const relationship = phantomRelationshipGet(toKey);
      if (
        !phantomRelationshipIsAccepted(relationship) ||
        phantomPendingTerminalExits.has(toKey) ||
        __phantomGoneNotified.has(toKey)
      ) {
        emitSnapshot("chat_rtc_tx_blocked_relationship_cycle");
        return false;
      }
      msg.relationshipCycleId = relationship.cycleId;
      const ent = phantomChatPeers.get(toKey);
      const chatAttemptId = String(ent?.chatAttemptId || "");
      if (chatAttemptId) msg.chatAttemptId = chatAttemptId;
    }

    CORE_DEBUG && console.log("[RTC TX]", msg.type, "→", to);

    let sent,sequential=false;
    if(pipe==='sys'&&!phantomHasOpenSysPipe(toKey)){
      sequential=true;
      const entry=phantomSysPeers.get(toKey),attemptId=msg.sysAttemptId,room=videoId;
      sent=networkFirstSignaling.send({peer:toKey,attemptId,signalType:msg.type,
        current:()=>videoId===room&&phantomSysPeers.get(toKey)===entry&&
          String(entry?.attempt?.id||entry?.sysAttemptId||'')===String(attemptId||'')&&
          !phantomPendingTerminalExits.has(toKey)&&host.canAdmitPeer(toKey)===true,
        complete:()=>entry?.pc?.connectionState==='connected'||phantomHasOpenSysPipe(toKey),
        routes:[
          {name:'existing_peer_pipe',send:()=>{
            const peer=phantomChatPeers.get(toKey);
            if(!canCarrySysRecovery({packet:msg,room:videoId,self:fromKey,remote:toKey,
              member:!!phantomCommittedMember(toKey),terminal:phantomPendingTerminalExits.has(toKey),
              accepted:phantomRelationshipIsAccepted(phantomRelationshipGet(toKey)),ready:peer?.dc?.readyState==='open'&&peer?.chatState==='ready'}))return false;
            peer.dc.send(JSON.stringify({t:'SYS_RECOVERY_RTC',packet:msg}));emitSnapshot('sys_recovery_signal_existing_peer_pipe');
            if(kind==='ice')traceIce('route_submission',toKey,pipe,data,{...msg,submitted:true,route:'existing_peer_pipe'});return true;
          }},
          {name:'peer_bridge',send:()=>{const ok=signalingBridge.send(msg);if(kind==='ice'&&ok)traceIce('route_submission',toKey,pipe,data,{...msg,submitted:ok,route:'peer_bridge'});return ok;}},
          {name:'relay_fallback',send:()=>{const ok=phantomSendControl(msg,msg.type);if(kind==='ice')traceIce('route_submission',toKey,pipe,data,{...msg,submitted:ok,route:concierge.lastRouteMode??'relay_fallback'});return ok;}}
        ]
      });
    }else sent=phantomSendControl(msg,msg.type);
    if(kind==='ice'&&!sequential)traceIce('route_submission',toKey,pipe,data,{...msg,submitted:sent,route:concierge.lastRouteMode??'unknown'});

    if (pipe === "chat") {
      const ent = phantomChatPeers.get(toKey);
      phantomChatSignalAudit("tx", {
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

  function phantomIsArrayBuffer(x) {
    return x && Object.prototype.toString.call(x) === "[object ArrayBuffer]";
  }

  function rtcSendDC(remoteHKey, obj) {
    if(!canUsePeer(phantomShortKey(remoteHKey)))return false;
    const ent = phantomChatPeers.get(
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

  function rtcWaitDCDrain(dc,bytes=0) {
    return waitForDataChannelCapacity(dc,{bytes,timeoutMs:5000,setTimeout,clearTimeout}).then(()=>true,()=>false);
  }

  async function rtcSendRawDC(remoteHKey, buf) {
    const ent = phantomChatPeers.get(
      String(remoteHKey || "")
        .slice(0, 16)
        .toLowerCase(),
    );
    const dc = ent?.dc;
    if (!dc || dc.readyState !== "open") return false;

    const ok = await rtcWaitDCDrain(dc,buf.byteLength??0);
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
    CORE_DEBUG && console.log("[phantom][RTC] TX HELLO →", remoteHKey, payload);
    rtcSendDC(remoteHKey, payload);
  }

  function phantomMarkSysTx(type, remoteHKey = "") {
    const key = phantomShortKey(remoteHKey);

    if (phantomOwnsPrimarySysState(key)) {
      concierge.sysPipeState = concierge.sysPipeState || "missing";
      concierge.sysPeer = key;
    }

    concierge.sysLastTxAt = now();
    concierge.sysLastTxType = String(type || "unknown");
  }

  function phantomMarkSysRx(type, remoteHKey = "") {
    const key = phantomShortKey(remoteHKey);
    const t = String(type || "unknown");

    const ownsPrimarySysState = phantomOwnsPrimarySysState(key);

    if (ownsPrimarySysState) {
      concierge.sysPeer = key;
    }

    concierge.sysLastRxAt = now();
    concierge.sysLastRxType = t;

    const temporary = phantomTemporarySysTransactions.get(key);
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
      phantomSysLastGoodRx.set(key, now());

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

  function phantomSendSysDC(remoteHKey, obj) {
    const t = String(obj?.t || '');
    const inner = obj?.msg;
    // Setup and departure packets must flow even while leadership duties are blocked.
    const transportControl = t === 'SYS_CONTROL' &&
      ((inner?.pipe === 'sys' && ['rtc_offer','rtc_answer','rtc_ice'].includes(inner?.type)) ||
       ['authority_exit','authority_detach'].includes(inner?.type));
    if ((t === 'SYS_STATUS' || t === 'SYS_STATUS_ACK' || (t === 'SYS_CONTROL' && !transportControl)) &&
        !phantomAuthorizeSyscon1Duty(t+':'+String(remoteHKey)+':'+(++sdkAdminDutySequence), () => phantomSendSysDC(remoteHKey,obj))) return false;

    const key = phantomShortKey(remoteHKey);
    if (!key || !obj || typeof obj !== "object") return false;

    const ent = phantomSysPeers.get(key);
    const dc = ent?.sysdc;

    const type = String(obj.t || obj.type || "SYS_UNKNOWN");

    if (!phantomTemporarySysAllowsEnvelope(key, obj)) {
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

    phantomMarkSysTx(type, key);

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

  function phantomSysHelloPayload() {
    return {
      t: "SYS_HELLO",
      at: now(),
      room: videoId || null,
      from: phantomShortKey(selfHKey) || null,
      bootId: phantomBootId,
      role: concierge.role || "peer",
      candidateRole: concierge.localRoleCandidate || "peer",
      epoch: Number(concierge.sysconEpoch || 0),
      roomJoinedAt: Number(concierge.roomJoinedAt || 0),
    };
  }
  function phantomSysStatusPayload(type = "SYS_STATUS") {
    const self = phantomShortKey(selfHKey);
    const authority = {
      from: phantomShortKey(phantomSysAuthority.from),
      syscon1: phantomShortKey(phantomSysAuthority.syscon1),
      syscon2: phantomShortKey(phantomSysAuthority.syscon2),
      epoch: Number(phantomSysAuthority.epoch || 0),
      rosterSeq: Number(phantomSysAuthority.rosterSeq || 0),
      roster: phantomAuthorityRosterRows(phantomSysAuthority.roster),
    };

    const publishesAuthority = !!(
      self &&
      authority.from === self &&
      authority.syscon1 === self &&
      authority.epoch === Number(concierge.sysconEpoch || 0) &&
      authority.rosterSeq > 0 &&
      authority.roster.length > 0
    );

    const localPending = phantomSysAuthority.pending;
    const pendingAuthority =
      localPending && phantomShortKey(localPending?.snapshot?.syscon1) === self
        ? {
            id: String(localPending.id || ""),
            via: String(localPending.via || "syscon1_commit"),
            baseEpoch: Number(localPending.baseEpoch || 0),
            baseRosterSeq: Number(localPending.baseRosterSeq || 0),
            baseRoster: phantomAuthorityRosterRows(localPending.baseRoster),
            baseAuthority: localPending.baseAuthority
              ? {
                  ...localPending.baseAuthority,
                  roster: phantomAuthorityRosterRows(
                    localPending.baseAuthority?.roster,
                  ),
                }
              : null,
            terminalExits: phantomAuthorityTerminalExits(localPending),
            admission: localPending.admission
              ? { ...localPending.admission }
              : null,
            leadership: localPending.leadership
              ? { ...localPending.leadership }
              : null,
            snapshot: {
              ...localPending.snapshot,
              roster: phantomAuthorityRosterRows(localPending.snapshot?.roster),
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
            terminalExits: Array.from(phantomPendingTerminalExits.entries()).map(
              ([member, command]) => ({
                member: phantomShortKey(member),
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
      bootId: phantomBootId,
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
        : phantomShortKey(concierge.syscon1) || null,
      syscon2: publishesAuthority
        ? authority.syscon2
        : phantomShortKey(concierge.syscon2) || null,
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
  function phantomPresencePayload() {
    return {
      type: "presence",
      t: now(),
      videoId,
      from: selfHKey,
      handle: selfHandle,
      bootId: phantomBootId,
      epoch: Number(concierge.sysconEpoch || 0),
      roomJoinedAt: Number(concierge.roomJoinedAt || 0),
      hex: selfHex,
      avatar: selfAvatar || undefined,
    };
  }
  function phantomLocalPresenceRow() {
    const self = phantomShortKey(selfHKey);

    return {
      hKey: self,
      from: self,
      handle: selfHandle || null,
      bootId: phantomBootId,
      epoch: Number(concierge.sysconEpoch || 0),
      roomJoinedAt: Number(concierge.roomJoinedAt || 0),
      hex: selfHex || null,
      avatar: selfAvatar || undefined,
      videoId: videoId || null,
      seenVia: "self",
      t: now(),
    };
  }

  function phantomPresenceRowFromPeer(peer, via = "unknown") {
    if (!peer || typeof peer !== "object") return null;

    const hKey = phantomShortKey(peer.hKey || peer.from || peer.hash);
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

  function phantomMergePresenceRow(oldRow, nextRow, via = "unknown") {
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

  // Automatic messaging reuses the existing relationship and transport owners.
  function phantomReconcileMessaging() {
    const self=phantomShortKey(selfHKey);
    if(host.messaging!=='automatic'||!enabled||__phantomExitSent||!phantomCommittedMember(self))return;
    for(const key of lastRoster){
      if(key===self||!phantomCommittedMember(key)||!host.canAdmitPeer(key)||
          phantomPendingTerminalExits.has(key)||__phantomGoneNotified.has(key)||
          phantomIsBlocked(key)||phantomIsBlockedBy(key)||phantomRelationshipRevoked.has(key))continue;
      const relationship=phantomRelationshipGet(key);
      if(relationship.endedAt)continue;
      if(relationship.state===VEIL.INVITE_RECEIVED){
        window.dispatchEvent({type:'message',data:{phantom:1,type:'VEIL_ACCEPT',payload:{from:key}}});
      }else if(relationship.state===VEIL.OUTSIDE&&self<key){
        window.dispatchEvent({type:'message',data:{phantom:1,type:'VEIL_INVITE',payload:{hKey:key}}});
      }else if(phantomRelationshipIsAccepted(relationship)){
        void phantomRequestChatPipe(key,'automatic_messaging_roster');
      }
    }
  }

  function phantomPeerConnections() {
    const self=phantomShortKey(selfHKey);
    return [...lastRoster].filter(key=>key!==self&&phantomCommittedMember(key)).map(peerId=>{
      const ent=phantomChatPeers.get(peerId),relationship=phantomRelationships.get(peerId);
      const blocked=!host.canAdmitPeer(peerId)||phantomIsBlocked(peerId)||phantomIsBlockedBy(peerId)||phantomRelationshipRevoked.has(peerId)||phantomPendingTerminalExits.has(peerId)||__phantomGoneNotified.has(peerId);
      const ready=!blocked&&phantomRelationshipIsAccepted(relationship)&&ent?.dc?.readyState==='open'&&ent.chatState==='ready'&&ent.chatProof==='chat_hello';
      const state=blocked?'blocked':ready?'ready':ent?.chatAttemptState==='terminal'?'failed':reconnectingPeers.has(peerId)?'recovering':relationship?.endedAt?'closed':relationship?.state&&relationship.state!==VEIL.OUTSIDE?'connecting':'idle';
      return {peerId,state,reason:ready?'chat_hello_verified':blocked?'relationship_unavailable':ent?.chatFault&&ent.chatFault!=='none'?ent.chatFault:relationship?.endReason&&relationship.endReason!=='none'?relationship.endReason:state,
        attemptId:ent?.chatAttemptId||null};
    });
  }

  function phantomEmitPresenceRoster(reason = "presence_roster") {
    const self = phantomShortKey(selfHKey);
    if (!self) return false;

    const rows = new Map();

    rows.set(self, phantomLocalPresenceRow());

    for (const hKey of Array.from(lastRoster || [])) {
      const key = phantomShortKey(hKey);
      if (!key || key === self) continue;

      rows.set(
        key,
        phantomPresenceBook.get(key) || {
          hKey: key,
          from: key,
          videoId: videoId || null,
          handle: null,
          seenVia: "roster",
        },
      );
    }

    const list = Array.from(rows.values()).filter((p) =>
      phantomShortKey(p.hKey || p.from),
    );

    if (!list.length) return false;

    post("PRESENCE_ROSTER", {
      videoId: videoId || null,
      list,
      via: reason,
    });

    phantomRefreshSysReadiness(reason);
    phantomRefreshSysCandidates(list, reason);

    emitSnapshot(reason);
    if(host.messaging==='automatic')queueMicrotask(phantomReconcileMessaging);
    return true;
  }

  function phantomRememberPresencePeer(peer, via = "unknown", opts = {}) {
    const row = phantomPresenceRowFromPeer(peer, via);
    if (!row) return false;

    const hKey = phantomShortKey(row.hKey || row.from);
    if (!hKey) return false;

    // Presence is identity/metadata evidence only.
    // It cannot add room membership or clear a committed terminal removal.
    const oldRow = phantomPresenceBook.get(hKey) || null;
    const mergedRow = phantomMergePresenceRow(oldRow, row, via);
    phantomPresenceBook.set(hKey, mergedRow);

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
      phantomEmitPresenceRoster("presence_" + via);
    }

    return true;
  }

  function phantomOwnsDirectedSysRelay() {
    const self = phantomShortKey(selfHKey);
    if (!self) return false;

    const committedOne = phantomShortKey(
      phantomFreshSysAuthority()?.syscon1 || phantomSysAuthority.syscon1,
    );
    const effectiveOne = phantomShortKey(
      committedOne || concierge.syscon1 || concierge.cfConciergeSyscon1,
    );

    return !!(effectiveOne && self === effectiveOne);
  }

  function phantomQueueDirectedSysControl(
    destinationHKey,
    envelope,
    detail = "queued",
  ) {
    const destination = phantomShortKey(destinationHKey);
    const self = phantomShortKey(selfHKey);

    if (!destination || destination === self || !envelope) return false;
    if (__phantomGoneNotified.has(destination)) return false;

    let pending = phantomPendingSysRelay.get(destination);

    if (!pending) {
      pending = [];
      phantomPendingSysRelay.set(destination, pending);
    }

    pending.push({
      envelope: {
        ...envelope,
        relayedBy: self,
        relayedAt: now(),
      },
      at: now(),
    });

    if (pending.length > PHANTOM_SYS_RELAY_PENDING_MAX) {
      pending.splice(0, pending.length - PHANTOM_SYS_RELAY_PENDING_MAX);
    }

    phantomChatSignalAudit("relay_held", {
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

  function phantomHoldDirectedSysRelay(destinationHKey, envelope) {
    const destination = phantomShortKey(destinationHKey);
    const self = phantomShortKey(selfHKey);

    if (!destination || destination === self) return false;

    if (!phantomOwnsDirectedSysRelay()) return false;
    if (!lastRoster.has(destination)) return false;
    if (__phantomGoneNotified.has(destination)) return false;

    const held = phantomQueueDirectedSysControl(
      destination,
      envelope,
      "authoritative_syscon1_handoff",
    );

    if (held) {
      emitSnapshot("sys_control_relay_held_for_peer_owned_tether");
    }

    return held;
  }

  function phantomFlushDirectedSysRelay(destinationHKey) {
    const destination = phantomShortKey(destinationHKey);
    if (!destination) return false;
    if (!phantomHasOpenSysPipe(destination)) return false;

    const pending = phantomPendingSysRelay.get(destination);
    if (!pending?.length) return false;

    phantomPendingSysRelay.delete(destination);

    const held = [];

    for (const item of pending) {
      const envelope = item?.envelope;

      if (!envelope || !phantomSendSysDC(destination, envelope)) {
        held.push(item);
      }
    }

    if (held.length) {
      phantomPendingSysRelay.set(
        destination,
        held.slice(-PHANTOM_SYS_RELAY_PENDING_MAX),
      );

      concierge.lastErr = "sys_control_relay_flush_failed:" + destination;

      emitSnapshot("sys_control_relay_flush_failed");
      return false;
    }

    concierge.lastErr = null;

    phantomChatSignalAudit("relay_flushed", {
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

  function phantomSendSysControl(msg, type = "") {
    if (!msg || typeof msg !== "object") return false;

    const kind = String(msg.type || type || "unknown");
    const cls = phantomControlClassFor(kind);

    if (cls === "relationship_control" && !phantomShortKey(msg.to)) {
      concierge.lastErr = "sys_control_missing_to:" + kind;
      emitSnapshot("sys_control_missing_to_" + kind);
      return false;
    }

    const destination = phantomShortKey(msg.to || concierge.sysPeer);

    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );
    const ownsDirectedRelay = phantomOwnsDirectedSysRelay();

    const self = phantomShortKey(selfHKey);
    const boss = phantomShortKey(concierge.syscon1);
    const secondary = phantomShortKey(concierge.syscon2);
    const currentNeighbor = phantomShortKey(concierge.sysPeer);

    const directDestinationOpen = !!(
      destination &&
      destination !== self &&
      phantomHasOpenSysPipe(destination)
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
      return phantomHoldDirectedSysRelay(destination, envelope);
    }

    const key =
      cls === "presence"
        ? phantomShortKey(msg.to || concierge.sysPeer)
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
      phantomPendingSysRelay.get(destination)?.length
    ) {
      const flushed = phantomFlushDirectedSysRelay(destination);

      if (!flushed) {
        return phantomQueueDirectedSysControl(
          destination,
          envelope,
          "ordered_behind_unflushed_directed_control",
        );
      }
    }

    const sent = phantomSendSysDC(key, envelope);
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
      const held = phantomQueueDirectedSysControl(
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

  function phantomWireSysDC(remoteHKey, dc) {
    const key = phantomShortKey(remoteHKey);
    if (!key || !dc) return;

    try {
      dc.binaryType = "arraybuffer";
    } catch {}

    const wiredEntry=phantomSysPeers.get(key),wiredPc=wiredEntry?.pc;
    let openedOnce=false;
    const ownsPrimarySysState = phantomOwnsPrimarySysState(key);

    if (ownsPrimarySysState&&wiredEntry?.sysdc===dc) {
      concierge.sysPeer = key;
      concierge.sysPipeState = dc.readyState || "connecting";
    }

    emitSnapshot(
      ownsPrimarySysState ? "sysdc_wire_primary" : "sysdc_wire_peer_tether",
    );

    dc.onopen = () => {
      if(openedOnce)return;
      const current=phantomSysPeers.get(key);
      if(!current||current!==wiredEntry||current.pc!==wiredPc)return;
      if(current.sysdc!==dc){
        const prior=current.sysdc;
        const live=prior&&['open','connecting'].includes(prior.readyState);
        // Both ends observe the same SCTP stream IDs. Crossed DCEP opens
        // must select the same channel instead of each retaining the last arrival.
        if(live&&(!Number.isInteger(dc.id)||!Number.isInteger(prior.id)||dc.id>=prior.id)){
          emitSnapshot('sysdc_crossed_channel_ignored');
          if(Number.isInteger(dc.id)&&Number.isInteger(prior.id))try{dc.close();}catch{}
          return;
        }
        current.sysdc=dc;
        if(prior&&prior!==dc)try{prior.close();}catch{}
        emitSnapshot('sysdc_crossed_channel_selected');
      }
      openedOnce=true;
      const diagnosticEntry=phantomSysPeers.get(phantomShortKey(remoteHKey));
      if(diagnosticEntry?.sysdc===dc)rtcDiagnostics.capture(diagnosticEntry.pc,"datachannel_open");
      const ent = phantomSysPeers.get(key);
      if (!ent || ent.sysdc !== dc) return;
      if (ent) {
        ent.sysCarrier = false;
        const edgeAttempt = phantomSysEdgeAttempt(key, false);
        if (edgeAttempt) {
          edgeAttempt.state = "open";
          edgeAttempt.lastEvent = "datachannel_open";
          edgeAttempt.faultReason = "none";
        }
      }

      if (phantomSysPeers.get(key)?.sysdc !== dc) return;

      post('PHANTOM_ADMIN_LINK_READY', {from:key});
      const temporary = phantomTemporarySysTransactions.get(key) || null;
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
        phantomSysLastGoodRx.set(key, now());

        if (phantomCancelSysReplacement(key, "replacement_restored")) {
          emitSnapshot("sys_peer_replacement_restored");
        }

        phantomRefreshSysReadiness("sysdc_open");
        phantomFlushQueuedRtcIce(key, "rtc_ice_queue_flush_sysdc_open");
        phantomFlushDirectedSysRelay(key);
      }

      const relPending = phantomPendingRelationship.get(key);
      if (relPending?.length) {
        phantomPendingRelationship.delete(key);

        for (const item of relPending) {
          const relType = String(item?.type || item?.msg?.type || "unknown");
          const relMsg = item?.msg;

          if (
            !relMsg ||
            phantomControlClassFor(relType) !== "relationship_control"
          ) {
            continue;
          }

          if (
            temporary &&
            (relType !== "sys_unblock" ||
              String(relMsg.sysTxId || "") !== String(temporary.id || ""))
          ) {
            let held = phantomPendingRelationship.get(key);
            if (!held) {
              held = [];
              phantomPendingRelationship.set(key, held);
            }
            held.push(item);
            continue;
          }

          phantomMarkControlTx(relType, "phantom_sys");

          concierge.routeReason = "sys_route_relationship_control_pending_open";
          concierge.lastRouteAt = now();
          concierge.lastRouteType = relType;
          concierge.lastRouteReason = concierge.routeReason;
          concierge.lastRouteClass = "relationship_control";
          concierge.routeMode = "sys_primary";
          concierge.lastRouteMode = "sys_primary";
          concierge.wouldRoute = "phantom_sys";
          concierge.wouldRouteReason =
            "sys_ready_route_relationship_pending_open";

          concierge.lastRelationshipTxAt = now();
          concierge.lastRelationshipTxType = relType;
          concierge.lastRelationshipRoute = "phantom_sys";
          concierge.lastRelationshipReason =
            "sys_route_relationship_control_pending_open";

          const ok = phantomSendSysControl(relMsg, relType);

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

      phantomResumeTerminalAuthority("sysdc_open");
      if (phantomSysAuthority.pending) {
        phantomPublishCommittedAuthority("sysdc_open_pending_authority");
      }

      phantomSendSysDC(key, phantomSysHelloPayload());
      phantomSendSysDC(key, {
        t: "SYS_PING",
        at: now(),
        from: phantomShortKey(selfHKey),
        room: videoId || null,
      });
      phantomSendSysDC(key, phantomSysStatusPayload("SYS_STATUS"));

      const localAuthority = phantomLocalCommittedAuthority();
      const selfKey = phantomShortKey(selfHKey);
      const selfCommitted = phantomAuthorityRosterRows(
        localAuthority?.roster,
      ).some((row) => row.hKey === selfKey);
      const presence = phantomPresencePayload();

      if (!selfCommitted) {
        presence.to = key;
        presence.admit = true;
        presence.admitReason = "sysdc_open_uncommitted_incarnation";

        const ok = phantomSendSysControl(presence, "presence");

        phantomMembershipAuditSet(ok ? "admit_sent" : "admit_send_failed", {
          peer: key,
          reason: "sysdc_open_uncommitted_incarnation",
          pipe: String(dc.readyState || "missing"),
          role: concierge.localRoleCandidate || concierge.role || "peer",
        });

        if (ok) {
          phantomSysAdmissionLastSent.set(key, now());
          emitSnapshot("sys_admission_presence_sent_open");
        }
      } else {
        phantomSendControl(presence, "presence");
      }
    };

    dc.onclose = () => {
      const diagnosticEntry=phantomSysPeers.get(phantomShortKey(remoteHKey));
      if(diagnosticEntry?.sysdc===dc)rtcDiagnostics.capture(diagnosticEntry.pc,"datachannel_close");
      const ent = phantomSysPeers.get(key);

      // A retired SYS channel cannot mutate its successor attempt.
      if (!ent || ent.sysdc !== dc) {
        emitSnapshot("stale_sys_pipe_close_ignored");
        return;
      }

      ent.sysCarrier = false;
      const edgeAttempt = phantomSysEdgeAttempt(key, false);
      if (edgeAttempt) {
        edgeAttempt.state = "closed";
        edgeAttempt.lastEvent = "datachannel_closed";
        edgeAttempt.faultReason = "sysdc_closed";
      }

      ent.sysdc = null;

      if (ent.temporary === true || phantomTemporarySysTransactions.has(key)) {
        phantomCloseTemporarySysTransaction(key, "remote_closed");
        return;
      }

      if (__phantomGoneNotified.has(key)) {
        emitSnapshot("sysdc_close_ignored_peer_gone");
        return;
      }

      const relPending = phantomPendingRelationship.get(key);
      if (relPending?.length) {
        concierge.lastRelationshipRoute = "phantom_sys_pending";
        concierge.lastRelationshipReason = "sysdc_closed_pending_held";
        concierge.lastErr = "relationship_pending_held_sysdc_close";
        emitSnapshot("relationship_pending_held_sysdc_close");
      }

      phantomHandleTransportLoss(key, "sysdc_closed");
    };

    dc.onerror = () => {
      const ent = phantomSysPeers.get(key);

      // A retired SYS channel cannot mutate its successor attempt.
      if (!ent || ent.sysdc !== dc) {
        emitSnapshot("stale_sys_pipe_error_ignored");
        return;
      }

      ent.sysCarrier = false;
      const edgeAttempt = phantomSysEdgeAttempt(key, false);
      if (edgeAttempt) {
        edgeAttempt.state = "closed";
        edgeAttempt.lastEvent = "datachannel_closed";
        edgeAttempt.faultReason = "sysdc_closed";
      }

      ent.sysdc = null;

      if (ent.temporary === true || phantomTemporarySysTransactions.has(key)) {
        phantomCloseTemporarySysTransaction(key, "error");
        return;
      }

      phantomHandleTransportLoss(key, "sysdc_error");
    };

    dc.onmessage = (ev) => {
      if(phantomSysPeers.get(key)!==wiredEntry||wiredEntry?.pc!==wiredPc||wiredEntry?.sysdc!==dc||!canUsePeer(key))return;
      let msg = null;
      try {
        msg = JSON.parse(String(ev?.data || ""));
      } catch {
        msg = { t: "SYS_RAW", raw: String(ev?.data || "") };
      }

      if (msg?.t === 'PHANTOM_DIST') {
        if (phantomSysPeers.get(key)?.sysdc === dc && !phantomTemporarySysTransactions.has(key) && JSON.stringify(msg).length <= 32768)
          post('PHANTOM_DIST', {from:key, frame:msg.frame});
        return;
      }
      if (msg?.t === 'PHANTOM_ADMIN') {
        if (phantomSysPeers.get(key)?.sysdc === dc && !phantomTemporarySysTransactions.has(key) && JSON.stringify(msg).length <= 4096)
          post('PHANTOM_ADMIN', {from:key, record:msg.record});
        return;
      }
      const type = String(msg?.t || msg?.type || "SYS_UNKNOWN");
      const temporaryRx = phantomTemporarySysTransactions.get(key) || null;

      if (temporaryRx && type !== "SYS_CONTROL" && type !== "SYS_CONTROL_ACK") {
        emitSnapshot("temporary_sys_nontransaction_packet_rejected_" + type);
        return;
      }

      phantomMarkSysRx(type, key);

      if (
        type === "SYS_HELLO" ||
        type === "SYS_STATUS" ||
        type === "SYS_STATUS_ACK"
      ) {
        phantomRememberPresencePeer(
          {
            hKey: key,
            from: key,
            bootId: String(msg?.bootId || "") || null,
            epoch: Number(msg?.epoch || 0),
            roomJoinedAt: Number(msg?.roomJoinedAt || msg?.joinedAt || 0),
            videoId: msg?.room || videoId || null,
            t: Number(msg?.at || now()),
          },
          "phantom_sys_claim",
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
        phantomObserveRemoteIncarnation(key, {
          bootId: String(msg.bootId || ""),
          attemptId: String(phantomSysPeers.get(key)?.attempt?.id || ""),
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
        phantomMarkGoodSysRx(key, type);
        phantomRefreshSysReadiness("sys_rx_" + type);
        emitSnapshot("sys_rx_" + type);
      }

      if (type === "SYS_PING") {
        phantomSendSysDC(key, {
          t: "SYS_PONG",
          at: now(),
          from: phantomShortKey(selfHKey),
          room: videoId || null,
        });
      } else if (type === "SYS_STATUS") {
        const localRole = String(
          concierge.localRoleCandidate || concierge.role || "peer",
        );

        const remoteRole = String(msg?.role || msg?.candidateRole || "peer");

        const sender = phantomShortKey(msg?.from || key);
        const boss = phantomShortKey(concierge.syscon1);

        const acceptedPending = phantomSysAuthority.pending;
        const acceptedCommit = !!(acceptedPending &&
          acceptedPending.acceptedBy === phantomShortKey(selfHKey) &&
          sender === key && sender === phantomShortKey(acceptedPending.snapshot?.syscon1) &&
          phantomAuthoritySnapshotId(msg) === phantomAuthoritySnapshotId(acceptedPending.snapshot) &&
          String(acceptedPending.id || "") === phantomAuthorityTransactionId(acceptedPending));
        const recoveryCommit = localRole === 'peer' && acceptsSuccessorStatus({
          base:{...phantomLocalCommittedAuthority(),room:videoId},message:msg,sender:key,self:phantomShortKey(selfHKey),
          recoveryTarget:phantomGossipRecovery.targets[phantomGossipRecovery.index],
          leaderPipeOpen:phantomHasOpenSysPipe(boss)});
        const bootstrapCommit = !phantomLocalCommittedAuthority() &&
          localRole === 'peer' && sender === key && sender === sdkBootstrapTarget &&
          phantomDoorwayAdmittedPeers.has(sender) && msg.role === 'syscon1' && msg.room === videoId;
        const authorityStatusAllowed = acceptedCommit || recoveryCommit || bootstrapCommit || !!(
          sender &&
          sender === key &&
          sender === boss &&
          (localRole === "peer" || localRole === "syscon2")
        );

        const duplicateSysconClaim = !!(
          phantomIsSysconRole(localRole) &&
          phantomIsSysconRole(remoteRole) &&
          localRole === remoteRole
        );

        const pendingAuthorityAccepted = phantomAcceptPendingAuthority(msg, key);
        const mirrorHandoffAccepted = pendingAuthorityAccepted
          ? false
          : phantomAdoptMirrorAuthority(msg, key);

        if (pendingAuthorityAccepted) {
          emitSnapshot("sys_status_pending_authority_accepted");
        } else if (mirrorHandoffAccepted) {
          emitSnapshot("sys_status_mirror_handoff_accepted");
        } else if (duplicateSysconClaim) {
          phantomReconcileRoleFromSysStatus(msg, "SYS_STATUS");
        } else if (
          authorityStatusAllowed &&
          phantomRememberSysAuthority(msg, "SYS_STATUS")
        ) {
          phantomRefreshSysCandidates(phantomLatchedSysRosterList(), "SYS_STATUS");
        }

        phantomMarkGoodSysRx(key, "SYS_STATUS");
        phantomRefreshSysReadiness("sys_rx_SYS_STATUS");
        emitSnapshot("sys_rx_SYS_STATUS");

        phantomSendSysDC(key, {
          ...phantomSysStatusPayload("SYS_STATUS_ACK"),
          ackFor: msg?.from || null,
          ackAt: msg?.at || null,
        });
      } else if (type === "SYS_STATUS_ACK") {
        concierge.sysLastRxAt = now();
        concierge.sysLastRxType = "SYS_STATUS_ACK";

        const sender = phantomShortKey(msg?.from || key);
        const boss = phantomShortKey(concierge.syscon1);
        const stampedBoss = phantomShortKey(msg?.syscon1);
        const self = phantomShortKey(selfHKey);

        const incomingEpoch = Number(msg?.epoch || 0);
        const currentEpoch = Number(concierge.sysconEpoch || 0);

        const incomingSeq = Number(msg?.rosterSeq || 0);
        const currentSeq = Number(phantomSysAuthority.rosterSeq || 0);

        const roomOk =
          !msg?.room || !videoId || String(msg.room) === String(videoId);

        const pendingAuthorityPromoted = phantomPromotePendingAuthority(
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
          phantomShortKey(msg?.ackFor) === self &&
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
          phantomRememberSysAuthority(msg, "SYS_STATUS_ACK_ROSTER");
          emitSnapshot("sys_status_ack_authoritative_roster_rx");
        } else {
          emitSnapshot("sys_status_ack_pipe_proof_only");
        }

        if (pendingAuthorityPromoted || authoritativeRosterAck) {
          phantomRefreshSysCandidates(
            phantomLatchedSysRosterList(),
            "SYS_STATUS_ACK",
          );
        }

        phantomMarkGoodSysRx(key, "SYS_STATUS_ACK");
        phantomRefreshSysReadiness("sys_rx_SYS_STATUS_ACK");
        emitSnapshot("sys_rx_SYS_STATUS_ACK");
      } else if (type === "SYS_CONTROL") {
        const inner = msg?.msg || null;
        const kind = String(msg?.kind || inner?.type || "unknown");

        const selfKey = phantomShortKey(selfHKey);
        const destination = phantomShortKey(inner?.to);

        const role = String(
          concierge.localRoleCandidate || concierge.role || "peer",
        );

        const temporary = phantomTemporarySysTransactions.get(key) || null;
        if (temporary) {
          const allowed = !!(
            inner &&
            destination === selfKey &&
            phantomTemporarySysAllowsEnvelope(key, msg)
          );

          if (allowed) {
            phantomHandleInboundRelationshipControl(inner, "phantom_sys_temporary");
          } else {
            emitSnapshot("temporary_sys_control_rejected");
          }

          phantomSendSysDC(key, {
            t: "SYS_CONTROL_ACK",
            at: now(),
            from: phantomShortKey(selfHKey),
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
          const boss = phantomShortKey(concierge.syscon1);

          let forwarded = false;
          let held = false;

          if (phantomHasOpenSysPipe(destination)) {
            forwarded = phantomSendSysDC(destination, {
              ...msg,
              relayedBy: selfKey,
              relayedAt: now(),
            });
          } else if (role === "syscon1") {
            held = phantomHoldDirectedSysRelay(destination, msg);
          } else if (
            role === "syscon2" &&
            boss &&
            boss !== selfKey &&
            boss !== key &&
            phantomHasOpenSysPipe(boss)
          ) {
            forwarded = phantomSendSysDC(boss, {
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
          const commandFrom = phantomShortKey(inner.from);

          // Departure evidence is accepted only from the exact member's owned
          // SYS edge. It is never relayable or forgeable by a third peer.
          const envelopeFrom = phantomShortKey(msg?.from);
          const senderBound = !!(
            commandFrom &&
            envelopeFrom === commandFrom &&
            commandFrom === key
          );

          if (!senderBound) {
            emitSnapshot(kind + "_sender_mismatch_ignored");
          } else if (kind === "authority_exit") {
            phantomTerminalExitCommand(inner, "phantom_sys_authority_exit");
          } else {
            const incomingBootId = String(inner?.bootId || "");
            const knownBootId = String(
              phantomRemoteBootIds.get(commandFrom) || "",
            );

            if (
              incomingBootId &&
              knownBootId &&
              incomingBootId !== knownBootId
            ) {
              emitSnapshot("stale_authority_detach_boot_ignored");
            } else {
              const armed = phantomArmSysReplacement(
                commandFrom,
                "explicit_document_detach",
              );
              const accepted =
                armed || phantomSysReplacementWindows.has(commandFrom);
              emitSnapshot(
                accepted
                  ? "authority_detach_replacement_window_owned"
                  : "authority_detach_no_legal_owner",
              );
            }
          }
        } else if (kind === "presence" && inner?.from) {
          const fromKey = phantomShortKey(inner.from);
          const selfKey = phantomShortKey(selfHKey);
          const role = String(
            concierge.localRoleCandidate || concierge.role || "peer",
          );

          phantomRememberPresencePeer(inner, "phantom_sys", {
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
            phantomSendControl(
              {
                type: "veil_status",
                from: selfHKey,
                to: fromKey,
                videoId,
                state: relState,
                cycleId: phantomRelationshipGet(fromKey)?.cycleId || null,
              },
              "veil_status_presence_rx",
            );

            emitSnapshot("relationship_status_presence_rx");
          }

          if (role === "syscon2") {
            const boss = phantomShortKey(concierge.syscon1);

            if (boss && boss !== selfKey && fromKey && fromKey !== boss) {
              phantomSendSysControl(
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
          concierge.lastRtcRxVia = "phantom_sys";

          if (!isDirectMsgForMe(inner)) {
            // ignore safely, but still ACK wrapper below
          } else {
            if (inner?.pipe !== "sys" && inner?.sysCarrier !== true) {
              const ent = phantomChatPeers.get(phantomShortKey(inner.from));
              phantomChatSignalAudit("rx_offer", {
                peer: inner.from,
                kind: "rtc_offer",
                route: "phantom_sys",
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
          concierge.lastRtcRxVia = "phantom_sys";

          if (!isDirectMsgForMe(inner)) {
            // ignore safely, but still ACK wrapper below
          } else {
            if (inner?.pipe !== "sys" && inner?.sysCarrier !== true) {
              const ent = phantomChatPeers.get(phantomShortKey(inner.from));
              phantomChatSignalAudit("rx_answer", {
                peer: inner.from,
                kind: "rtc_answer",
                route: "phantom_sys",
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
          concierge.lastRtcRxVia = "phantom_sys";

          concierge.lastRtcIceRxAt = now();
          concierge.lastRtcIceRxVia = "phantom_sys";

          if (!isDirectMsgForMe(inner)) {
            // ignore safely, but still ACK wrapper below
          } else if (!phantomRememberRtcSignal(inner, "phantom_sys")) {
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
          phantomHandleInboundRelationshipControl(inner, "phantom_sys");
        }

        phantomSendSysDC(key, {
          t: "SYS_CONTROL_ACK",
          at: now(),
          from: phantomShortKey(selfHKey),
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

  function phantomGetOrCreateChatPeer(remoteHKey) {
    const k = phantomShortKey(remoteHKey);
    if (!k) return null;

    let ent = phantomChatPeers.get(k);
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
    phantomChatPeers.set(k, ent);
    rtcDiagnostics.track(pc,{peer:k,pipe:'chat',context:()=>({attemptId:ent.sysAttemptId??ent.chatAttemptId??ent.attempt?.id??null,channelState:ent.sysdc?.readyState??ent.dc?.readyState??null,pendingIce:ent.pendingRemoteIce?.length??0})});
    pc.onsignalingstatechange=()=>rtcDiagnostics.capture(pc,'signaling_state');
    pc.onicegatheringstatechange=()=>rtcDiagnostics.capture(pc,'ice_gathering_state');
    pc.onicecandidateerror=ev=>rtcDiagnostics.capture(pc,'ice_candidate_error',true,{errorCode:ev.errorCode,errorText:String(ev.errorText??'').slice(0,512),url:String(ev.url??'').slice(0,512),address:ev.address,port:ev.port});

    pc.onicecandidate = (ev) => {
      rtcDiagnostics.capture(pc,ev?.candidate?"local_candidate":"local_candidates_complete",!ev?.candidate);
      const c = ev?.candidate;
      traceIce(c?'generated':'gathering_complete',k,'chat',c,{},pc);

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
      if (!ch || String(ch.label || "") !== COMPATIBILITY_WIRE.chatChannel) return;

      ent.dc = ch;
      rtcWireDC(k, ent.dc);
    };

    pc.onsignalingstatechange = () => {
      const current = phantomChatPeers.get(k);
      if (!current || current.pc !== pc || rtcClosing.has(k)) {
        emitSnapshot("stale_chat_signaling_state_ignored");
        return;
      }

      phantomChatSignalAudit("signal_state", {
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
        phantomOwnsChatPipeOffer(k) &&
        (getState(k) === VEIL.INSIDE || getState(k) === VEIL.ESTABLISHED)
      ) {
        emitSnapshot("chat_carrier_stable_resuming_owner_offer");
        void phantomRequestChatPipe(k, "chat_carrier_signaling_stable");
      }
    };

    pc.onconnectionstatechange = () => {
      rtcDiagnostics.capture(pc,"connection_state");
      const current = phantomChatPeers.get(k);
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
        phantomRequestChatPipe(k, "chat_carrier_connected_after_accept");
      }

      if (st === "failed" || st === "closed") {
        ent.chatState = "failed";
        ent.chatReadyAt = 0;
        ent.chatProof = "none";
        ent.chatFault = "pc_" + st;
        phantomHandleTransportLoss(k, "pc_" + st);
      }
    };

    pc.oniceconnectionstatechange = () => {
      rtcDiagnostics.capture(pc,"ice_connection_state");
      const current = phantomChatPeers.get(k);
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
        phantomRequestChatPipe(k, "chat_carrier_ice_connected_after_accept");
      }

      if (st === "failed" || st === "closed") {
        ent.chatState = "failed";
        ent.chatReadyAt = 0;
        ent.chatProof = "none";
        ent.chatFault = "ice_" + st;
        phantomHandleTransportLoss(k, "ice_" + st);
      }
    };

    return ent;
  }

  function phantomGetOrCreateSysPeer(remoteHKey) {
    const k = phantomShortKey(remoteHKey);
    if (!k) return null;

    const transaction = phantomTemporarySysTransactions.get(k) || null;
    let ent = phantomSysPeers.get(k);
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
      ownerAttemptId: phantomOwnsPrimarySysState(k)
        ? String(concierge.sysAttempt?.id || "") || null
        : null,
      remoteBootId: String(phantomRemoteBootIds.get(k) || "") || null,
      sysCarrier: false,
      temporary: !!transaction,
      tempSysPurpose: String(transaction?.purpose || ""),
      tempSysTxId: String(transaction?.id || ""),
      rtcAnswerApplying: false,
      rtcInboundChain: Promise.resolve(),
      pendingRemoteIce: [],
      attempt: {
        id: phantomNewSysTransportAttemptId(k),
        role: "unknown",
        state: "missing",
        startedAt: 0,
        lastEvent: "created",
        faultReason: "none",
      },
    };
    phantomSysPeers.set(k, ent);
    rtcDiagnostics.track(pc,{peer:k,pipe:'sys',context:()=>({attemptId:ent.sysAttemptId??ent.chatAttemptId??ent.attempt?.id??null,channelState:ent.sysdc?.readyState??ent.dc?.readyState??null,pendingIce:ent.pendingRemoteIce?.length??0})});
    pc.onsignalingstatechange=()=>rtcDiagnostics.capture(pc,'signaling_state');
    pc.onicegatheringstatechange=()=>rtcDiagnostics.capture(pc,'ice_gathering_state');
    pc.onicecandidateerror=ev=>rtcDiagnostics.capture(pc,'ice_candidate_error',true,{errorCode:ev.errorCode,errorText:String(ev.errorText??'').slice(0,512),url:String(ev.url??'').slice(0,512),address:ev.address,port:ev.port});

    pc.onicecandidate = (ev) => {
      rtcDiagnostics.capture(pc,ev?.candidate?"local_candidate":"local_candidates_complete",!ev?.candidate);
      const c = ev?.candidate;
      traceIce(c?'generated':'gathering_complete',k,'sys',c,{},pc);

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
      if (!ch || String(ch.label || "") !== COMPATIBILITY_WIRE.systemChannel) return;

      if(phantomSysPeers.get(k)!==ent||ent.pc!==pc){try{ch.close();}catch{}return;}
      if(!ent.sysdc||['closed','closing'].includes(ent.sysdc.readyState))ent.sysdc=ch;
      if(ent.sysdc===ch){
      const attempt = phantomSysEdgeAttempt(k);
      if (attempt) {
        attempt.role = "acceptor";
        attempt.state = "connecting";
        attempt.startedAt ||= now();
        attempt.lastEvent = "datachannel_received";
        attempt.faultReason = "none";
        ent.sysAttemptId = attempt.id;
      }
      if (phantomOwnsPrimarySysState(k)) {
        ent.ownerAttemptId = String(concierge.sysAttempt?.id || "") || null;
      }
      ent.sysCarrier = true;
      }
      phantomWireSysDC(k,ch);
      if(ch.readyState==='open')ch.onopen?.();
    };

    pc.onconnectionstatechange = () => {
      rtcDiagnostics.capture(pc,"connection_state");
      const current = phantomSysPeers.get(k);
      if (!current || current.pc !== pc) {
        emitSnapshot("stale_sys_pc_state_ignored");
        return;
      }

      sysOutageClock.observe(pc);
      const st = pc.connectionState;
      if(st==="connected")networkFirstSignaling.cancel(k,"native_connection_ready");
      if (st === "failed" || st === "closed") {
        if (current.temporary === true || phantomTemporarySysTransactions.has(k)) {
          phantomCloseTemporarySysTransaction(k, "pc_" + st);
          return;
        }
        phantomHandleTransportLoss(k, "sys_transport_pc_" + st);
      }
    };

    pc.oniceconnectionstatechange = () => {
      rtcDiagnostics.capture(pc,"ice_connection_state");
      const current = phantomSysPeers.get(k);
      if (!current || current.pc !== pc) {
        emitSnapshot("stale_sys_ice_state_ignored");
        return;
      }

      sysOutageClock.observe(pc);
      const st = pc.iceConnectionState;
      if (st === "failed" || st === "closed") {
        if (current.temporary === true || phantomTemporarySysTransactions.has(k)) {
          phantomCloseTemporarySysTransaction(k, "ice_" + st);
          return;
        }
        phantomHandleTransportLoss(k, "sys_transport_ice_" + st);
      }
    };

    return ent;
  }

  function rtcTeardown(hKey, reason = "unspecified") {
    const key = phantomShortKey(hKey);
    if (!key || rtcClosing.has(key)) return;

    // Teardown is an edge lifecycle transition. Expected retirement reasons
    // must not overwrite current concierge/SYS readiness with stale history.
    rtcClosing.add(key);

    const ent = phantomChatPeers.get(key);

    loungeWaiters.delete(key);
    phantomCancelChatAttemptDeadline(key);
    phantomChatPeers.delete(key);
    rtcDialing.delete(key);

    try {
      ent?.dc?.close();
    } catch {}
    try {
      rtcDiagnostics.retire(ent?.pc,reason);
      ent?.pc?.close();
    } catch {}

    rtcClosing.delete(key);
  }

  function phantomTeardownSysPeer(hKey, reason = "sys_teardown") {
    const key = phantomShortKey(hKey);
    if (!key) return;

    networkFirstSignaling.cancel(key);
    const ent = phantomSysPeers.get(key);
    const temporary = phantomTemporarySysTransactions.get(key);
    if (ent?.signalResponseTimer) clearTimeout(ent.signalResponseTimer);
    try {
      if (temporary?.deadlineTimer) clearTimeout(temporary.deadlineTimer);
    } catch {}
    phantomSysPeers.delete(key);
    phantomTemporarySysTransactions.delete(key);
    phantomSysDialing.delete(key);
    phantomSysPreflightUnstableRetries.delete(key);

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
      rtcDiagnostics.retire(pc,reason);
      pc?.close();
    } catch {}

    emitSnapshot(String(reason || "sys_teardown"));
  }

  function rtcTeardownAll(reason = "session_teardown") {
    for (const key of Array.from(phantomChatPeers.keys())) {
      rtcTeardown(key, reason);
    }

    for (const key of Array.from(phantomSysPeers.keys())) {
      phantomTeardownSysPeer(key, "sys_" + String(reason || "session_teardown"));
    }

    loungeWaiters.clear();
    rtcDialing.clear();
    phantomSysDialing.clear();
    rtcClosing.clear();
  }

  function rtcWireDC(remoteHKey, dc) {
    if (!dc) return;
    try {
      dc.binaryType = "arraybuffer";
    } catch {}
    dc.onopen = () => {
      const diagnosticEntry=phantomChatPeers.get(phantomShortKey(remoteHKey));
      if(diagnosticEntry?.dc===dc)rtcDiagnostics.capture(diagnosticEntry.pc,"datachannel_open");
      const key = phantomShortKey(remoteHKey);
      const ent = key ? phantomChatPeers.get(key) : null;

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
      CORE_DEBUG && console.log("[phantom][RTC] TX PING →", remoteHKey);

      post("P2P_DIRECT_OPEN", {
        from: selfHKey,
        to: String(remoteHKey || ""),
        videoId: videoId || null,
      });
    };

    dc.onclose = () => {
      const diagnosticEntry=phantomChatPeers.get(phantomShortKey(remoteHKey));
      if(diagnosticEntry?.dc===dc)rtcDiagnostics.capture(diagnosticEntry.pc,"datachannel_close");
      const key = phantomShortKey(remoteHKey);
      if (!key) return;

      const ent = phantomChatPeers.get(key);

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

      if (__phantomGoneNotified.has(key)) {
        emitSnapshot("chat_pipe_close_ignored_peer_gone");
        return;
      }

      ent.chatDropPending = true;

      concierge.chatDropCount = Number(concierge.chatDropCount || 0) + 1;
      concierge.chatLastDropAt = now();
      concierge.chatLastDropPeer = key;

      phantomHandleTransportLoss(key, "chat_dc_closed");
      emitSnapshot("chat_pipe_closed");
    };

    dc.onerror = () => {
      const key = phantomShortKey(remoteHKey);
      const ent = key ? phantomChatPeers.get(key) : null;
      if(!ent||ent.dc!==dc)return;

      if (ent) {
        ent.chatState = "lost";
        ent.chatReadyAt = 0;
        ent.chatProof = "none";
        ent.chatFault = "dc_error";
      }

      phantomHandleTransportLoss(key, "chat_dc_error");
      emitSnapshot("chat_pipe_lost_dc_error");
    };

    dc.onmessage = async (ev) => {
      if(phantomChatPeers.get(phantomShortKey(remoteHKey))?.dc!==dc||!canUsePeer(phantomShortKey(remoteHKey)))return;
      let raw = ev?.data;

      if (raw instanceof Blob) {
        try {
          raw = await raw.arrayBuffer();
        } catch {
          return;
        }
      }

      if(phantomChatPeers.get(phantomShortKey(remoteHKey))?.dc!==dc||!canUsePeer(phantomShortKey(remoteHKey)))return;
      if (phantomIsArrayBuffer(raw) && !phantomCanUseChat(remoteHKey)) return;
      if (phantomIsArrayBuffer(raw)) {
        const ent = phantomChatPeers.get(
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
      CORE_DEBUG && console.log("[phantom][RTC] RX", remoteHKey, msg);

      if(msg?.t==='SYS_BRIDGE_RTC'){const key=phantomShortKey(remoteHKey);if(phantomChatPeers.get(key)?.dc===dc)signalingBridge.receive(msg,key);return;}
      if(msg?.t==='SYS_RECOVERY_RTC'){
        const key=phantomShortKey(remoteHKey),entry=phantomChatPeers.get(key),packet=msg.packet;
        if(!canCarrySysRecovery({packet,room:videoId,self:key,remote:phantomShortKey(selfHKey),
          member:!!phantomCommittedMember(key),terminal:phantomPendingTerminalExits.has(key),
          accepted:phantomRelationshipIsAccepted(phantomRelationshipGet(key)),ready:entry?.dc?.readyState==='open'&&entry?.chatState==='ready',current:entry?.dc===dc}))return;
        const meta={...packet,room:videoId,pipe:'sys',recoveryPeerPipe:true};
        if(packet.type==='rtc_offer')rtcOnOffer(key,packet.sdp,meta);
        else if(packet.type==='rtc_answer')rtcOnAnswer(key,packet.sdp,meta);
        else rtcOnIce(key,packet.cand,meta);
        emitSnapshot('sys_recovery_signal_received_existing_peer_pipe');return;
      }
      if (msg?.t === "FILE_BIN_META" && !phantomCanUseChat(remoteHKey)) return;
      if (msg?.t === "FILE_BIN_META" && msg.file?.id) {
        const ent = phantomChatPeers.get(
          String(remoteHKey || "")
            .slice(0, 16)
            .toLowerCase(),
        );
        if (ent) ent.pendingFileBinMeta = msg.file;
        return;
      }

      if (msg?.t === 'PHANTOM_ADMIN') {
        if (phantomChatPeers.get(phantomShortKey(remoteHKey))?.dc === dc && phantomCanUseChat(remoteHKey) && JSON.stringify(msg).length <= 4096)
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
          const blockedByKey = phantomShortKey(remoteHKey);
          if (blockedByKey) {
            phantomBlockedByPeers.add(blockedByKey);
            phantomBlockedPeers.delete(blockedByKey);
          }

          phantomCancelReconnect(remoteHKey);
          setState(remoteHKey, VEIL.OUTSIDE);
          rtcTeardown(remoteHKey, "blocked_by_peer");

          post("BLOCKED_BY", { from: remoteHKey, at: msg.at || Date.now() });
          return;
        }

        const isSys = phantomIsSysMessage(msg.sys);
        if (!isSys && !phantomCanUseChat(remoteHKey)) return;

        if (msg.sys === "ROOM_EXITED") {
          emitSnapshot("legacy_chat_room_exit_ignored");
          return;
        }

        if (msg.sys === "CHAT_ENDED") {
          const key = phantomShortKey(remoteHKey);
          const currentEnt = key ? phantomChatPeers.get(key) : null;

          if (!currentEnt || currentEnt.dc !== dc) {
            emitSnapshot("stale_chat_end_ignored");
            return;
          }

          phantomNotifyPeerGone(key, "CHAT_ENDED", msg.at || Date.now());
          return;
        }

        if (msg.sys === "FILE_OFFER" && msg.file?.id) {
          const key = phantomShortKey(remoteHKey);
          if (key) {
            let offers = phantomPendingIncomingFileOffers.get(key);
            if (!offers) {
              offers = new Map();
              phantomPendingIncomingFileOffers.set(key, offers);
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
          const key = phantomShortKey(remoteHKey);
          const offers = key ? phantomPendingIncomingFileOffers.get(key) : null;

          if (offers) {
            offers.delete(String(msg.file.id));
            if (!offers.size) phantomPendingIncomingFileOffers.delete(key);
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
          const directKey=phantomShortKey(remoteHKey);
          if(pf===directKey&&/^[0-9a-f]{64}$/.test(phex)&&phex.slice(0,16)===directKey){const direct=phantomChatPeers.get(directKey);if(direct?.dc===dc)direct.signalingPublicKey=phex;if(phantomRelationshipIsAccepted(phantomRelationshipGet(directKey)))bindSignalingPeerKey(directKey,phex);}
          // reinforce keystore (same maps you already use)
          try {
            if (phex && /^[0-9a-f]{64}$/.test(phex)) ksHexToHKey.set(phex, pf);
            if (ph && ph.startsWith("@")) ksNameToHKey.set(ph, pf);
            ksSave();
          } catch {}

          // Publish the direct peer identity and display name.
          post("P2P_IDENTITY", {
            from: pf,
            handle: ph,
            hex: phex && phex.length === 64 ? phex : null,
            videoId: videoId || null,
            avatar: msg.avatar || undefined,
            at: now(),
          });

          CORE_DEBUG && console.log("[phantom][RTC] HELLO RX", { remoteHKey });

          const key = phantomShortKey(remoteHKey);
          const ent = key ? phantomChatPeers.get(key) : null;

          if (ent && ent.chatProof !== "chat_hello") {
            ent.chatState = "open";
            ent.chatReadyAt = 0;
            ent.chatProof = "hello_identity_only";
            ent.chatFault = phantomCanUseChat(key)
              ? "none"
              : "relationship_not_inside";
          }

          if (!phantomIsEstablishedLike(key)) {
            emitSnapshot("rtc_hello_identity_only_chat_not_allowed");
            return;
          }

          phantomEnsureChatPipe(key, "rtc_hello_rx");
          return;
        }
      }
    };
  }

  function phantomHasOpenSysPipe(remoteHKey = "") {
    const key = phantomShortKey(remoteHKey || concierge.sysPeer);
    if (!key) return concierge.sysPipeState === "open";

    const ent = phantomSysPeers.get(key);
    return !!(ent?.sysdc && ent.sysdc.readyState === "open");
  }

  function phantomSysTargetFromCandidates() {
    const self = phantomShortKey(selfHKey);
    if (!self) return "";

    const one = phantomShortKey(concierge.syscon1);
    const two = phantomShortKey(concierge.syscon2);
    const role = String(concierge.localRoleCandidate || "peer");
    const epoch = Number(concierge.sysconEpoch || 0);

    if (role === "syscon1") {
      return two && two !== self ? two : "";
    }

    if (role === "syscon2") {
      return one && one !== self ? one : "";
    }

    if (role === "peer") {
      // Discovery coordinates can lag committed leadership after a handoff.
      // Trying another live hint never commits membership or leadership.
      if (!phantomLocalCommittedAuthority() && sdkBootstrapTarget &&
          phantomDoorwayAdmittedPeers.has(sdkBootstrapTarget)) return sdkBootstrapTarget;
      if (phantomPeerSysCarrier.epoch !== epoch) {
        phantomPeerSysCarrier.epoch = epoch;
        phantomPeerSysCarrier.target = null;
        phantomPeerSysCarrier.primaryRecoveryUsed = false;
        phantomPeerSysCarrier.authorityRefreshPending = false;
      }

      if (phantomPeerSysCarrier.authorityRefreshPending) {
        return "";
      }

      const recoveryTarget = phantomShortKey(
        phantomGossipRecovery.targets[phantomGossipRecovery.index],
      );

      if (recoveryTarget && recoveryTarget !== self) {
        return recoveryTarget;
      }

      if (phantomGossipRecovery.targets.length) {
        return "";
      }

      const latched = phantomShortKey(phantomPeerSysCarrier.target);

      if (latched && latched !== self && (latched === one || latched === two)) {
        return latched;
      }

      if (one && one !== self) return one;
      if (two && two !== self) return two;
    }

    return "";
  }

  function phantomMarkGoodSysRx(remoteHKey, type = "SYS_RX") {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;

    const t = String(type || "SYS_RX");
    const isProof = t === "SYS_STATUS" || t === "SYS_STATUS_ACK";
    const epoch = Number(concierge.sysconEpoch || 0);

    phantomSysLastGoodRx.set(key, now());
    const proofEnt = phantomSysPeers.get(key);
    if (proofEnt) {
      proofEnt.lastProofEpoch = Number(concierge.sysconEpoch || 0);
      proofEnt.lastProofAt = now();
    }

    const shouldOwnSysPeer = phantomOwnsPrimarySysState(key);

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

    phantomSysPreflightUnstableRetries.delete(key);

    phantomRefreshSysReadiness("sys_good_rx_" + String(type || "SYS_RX"));
    emitSnapshot("sys_good_rx_" + String(type || "SYS_RX"));

    const rxKind = String(type || "SYS_RX");

    const proofOrHandshake =
      rxKind === "SYS_HELLO" ||
      rxKind === "SYS_PING" ||
      rxKind === "SYS_PONG" ||
      rxKind === "SYS_STATUS" ||
      rxKind === "SYS_STATUS_ACK";

    const selfCommitted = phantomCommittedMember(selfHKey);
    const role = String(
      concierge.localRoleCandidate || concierge.role || "peer",
    );

    if (
      proofOrHandshake &&
      !concierge.sysReady &&
      (role === "syscon1" || role === "syscon2")
    ) {
      phantomScheduleSysPreflight("sys_good_rx_" + rxKind);
      phantomMaybeRelinkSysconPair("sys_good_rx_" + rxKind);
    }

    if (
      proofOrHandshake &&
      (!selfCommitted || (!concierge.sysReady && role === "peer"))
    ) {
      phantomScheduleSysAdmission("sys_good_rx_" + rxKind);
    }

    return true;
  }

  function phantomSysAdmissionTargets() {
    const self = phantomShortKey(selfHKey);
    if (!self) return [];

    const target = phantomShortKey(phantomSysTargetFromCandidates());

    if (!target || target === self) return [];

    return [target];
  }

  function phantomSendSysAdmissionPresence(
    reason = "sys_admission",
    targetOnly = "",
  ) {
    const targets = targetOnly
      ? [phantomShortKey(targetOnly)].filter(Boolean)
      : phantomSysAdmissionTargets();

    if (!targets.length) {
      concierge.sysLastErr = "sys_admission_no_target";
      emitSnapshot("sys_admission_no_target");
      return false;
    }

    let sent = false;

    for (const target of targets) {
      if (phantomSendSysAdmissionPresenceTo(target, reason)) {
        sent = true;
      }
    }

    return sent;
  }

  function phantomSendSysAdmissionPresenceTo(target, reason = "sys_admission") {
    const self = phantomShortKey(selfHKey);
    target = phantomShortKey(target);

    if (!self || !target || target === self) return false;

    const nowMs = now();
    const last = Number(phantomSysAdmissionLastSent.get(target) || 0);

    if (nowMs - last < 2500) {
      emitSnapshot("sys_admission_throttled");
      return false;
    }

    if (!phantomHasOpenSysPipe(target)) {
      concierge.sysPeer = target;
      concierge.sysPipeState = "missing";
      concierge.sysLastErr = "sys_admission_waiting_for_pipe_owner";

      phantomRefreshSysReadiness("sys_admission_waiting_for_pipe_owner");
      emitSnapshot("sys_admission_waiting_for_pipe_owner");

      phantomEnsureSysPipe(
        target,
        "peer_sys_admission_" + String(reason || "sys_admission"),
      );
      return false;
    }

    const msg = phantomPresencePayload();
    msg.to = target;

    // The same SYS tether owner carries both committed-member refresh and
    // fresh-incarnation admission. Membership intent is explicit only while
    // self is absent from committed authority; role labels never decide it.
    const selfCommitted = phantomCommittedMember(self);
    if (!selfCommitted) {
      msg.admit = true;
      msg.admitReason = String(reason || "sys_admission");
    }

    phantomSysAdmissionLastSent.set(target, nowMs);

    const ok = phantomSendSysControl(msg, "presence");
    const auditKind = selfCommitted ? "presence" : "admit";

    phantomMembershipAuditSet(
      ok ? auditKind + "_sent" : auditKind + "_send_failed",
      {
        peer: target,
        reason: String(reason || "sys_admission"),
        pipe: String(phantomSysPeers.get(target)?.sysdc?.readyState || "missing"),
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
      concierge.route = "phantom_sys";
      concierge.wouldRoute = "phantom_sys";
      concierge.wouldRouteReason = "sys_admission_presence";
      emitSnapshot("sys_admission_presence_sent");
      return true;
    }

    concierge.sysLastErr = "sys_admission_presence_failed";
    emitSnapshot("sys_admission_presence_failed");
    return false;
  }

  function phantomScheduleSysAdmission(reason = "schedule") {
    const targets = phantomSysAdmissionTargets();
    if (!targets.length) return false;

    let touched = false;

    for (const target of targets) {
      const key = phantomShortKey(target);
      if (!key) continue;

      if (phantomHasOpenSysPipe(key)) {
        phantomSendSysAdmissionPresenceTo(
          key,
          "sys_admission_open_" + String(reason || "schedule"),
        );
        touched = true;
        continue;
      }

      concierge.sysPeer = key;
      concierge.sysLastErr = null;

      void phantomEnsureSysPipe(
        key,
        "peer_sys_admission_pipe_" + String(reason || "schedule"),
      );

      touched = true;
      emitSnapshot("sys_admission_birth_requested");
    }

    return touched;
  }

  function phantomScheduleSysPreflightUnstableRetry(
    remoteHKey,
    reason = "pc_not_stable",
  ) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;

    const ent = phantomSysPeers.get(key);
    const pc = ent?.pc || null;
    if (!pc) return false;

    const cleanupWaiter = () => {
      try {
        pc.removeEventListener("signalingstatechange", resume);
        pc.removeEventListener("connectionstatechange", resume);
        pc.removeEventListener("iceconnectionstatechange", resume);
      } catch {}

      try {
        pc.__phantomSysStableWaiter = false;
      } catch {}
    };

    const resume = () => {
      const currentSysPeer = phantomShortKey(concierge.sysPeer);
      const freshTarget = phantomShortKey(phantomSysTargetFromCandidates());

      const stillOwner = key && (currentSysPeer === key || freshTarget === key);

      if (!stillOwner) {
        cleanupWaiter();
        phantomSysPreflightUnstableRetries.delete(key);

        concierge.sysLastErr = null;
        phantomRefreshSysReadiness("sys_preflight_wait_target_changed");
        emitSnapshot("sys_preflight_wait_target_changed");
        return;
      }

      if (phantomHasOpenSysPipe(key)) {
        cleanupWaiter();
        phantomSysPreflightUnstableRetries.delete(key);

        concierge.sysPeer = key;
        concierge.sysPipeState = "open";
        concierge.sysLastErr = null;

        phantomRefreshSysReadiness("sys_preflight_wait_existing_open");

        if (
          String(concierge.localRoleCandidate || "peer") === "peer" ||
          !phantomCommittedMember(selfHKey)
        ) {
          phantomSendSysAdmissionPresence("sys_preflight_wait_existing_open", key);
        } else {
          phantomRefreshOpenSysPeer("sys_preflight_wait_existing_open");
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
        phantomSysPreflightUnstableRetries.delete(key);

        concierge.sysPeer = key;
        concierge.sysPipeState = "closed";
        concierge.sysLastErr = "sys_preflight_wait_pc_closed:" + key;

        phantomRefreshSysReadiness("sys_preflight_wait_pc_closed");
        emitSnapshot("sys_preflight_wait_pc_closed");

        if (
          String(concierge.localRoleCandidate || "peer") === "peer" ||
          !phantomCommittedMember(selfHKey)
        ) {
          phantomScheduleSysAdmission("sys_preflight_wait_pc_closed");
        } else {
          phantomScheduleSysPreflight("sys_preflight_wait_pc_closed");
        }

        return;
      }

      if (pc.signalingState !== "stable") return;

      cleanupWaiter();
      phantomSysPreflightUnstableRetries.delete(key);
      concierge.sysLastErr = null;

      const recentGoodSysRx = Number(phantomSysLastGoodRx.get(key) || 0);

      if (recentGoodSysRx && now() - recentGoodSysRx < 5000) {
        const ent = phantomSysPeers.get(key);
        const sysState = String(ent?.sysdc?.readyState || "");

        if (sysState === "open") {
          concierge.sysPeer = key;
          concierge.sysPipeState =
            concierge.sysReadyPeer === key && concierge.sysReadyProof !== "none"
              ? "ready"
              : "open";
          concierge.sysLastErr = null;

          phantomRefreshSysReadiness("sys_preflight_wait_recent_good_rx_open");
          emitSnapshot("sys_preflight_wait_recent_good_rx_open");
          return;
        }

        phantomSysLastGoodRx.delete(key);

        if (phantomShortKey(concierge.sysReadyPeer) === key) {
          concierge.sysReadyPeer = null;
          concierge.sysReadyAt = 0;
          concierge.sysReadyProof = "none";
          concierge.sysReadyEpoch = 0;
        }

        concierge.sysPeer = key;
        concierge.sysPipeState =
          sysState === "connecting" ? "connecting" : "missing";
        concierge.sysLastErr = "wait_recent_good_rx_without_open_pipe";

        phantomRefreshSysReadiness("wait_recent_good_rx_without_open_pipe");
        emitSnapshot("wait_recent_good_rx_without_open_pipe");

        // no return; fall through to phantomEnsureSysPipe below
      }

      phantomEnsureSysPipe(
        key,
        "sys_preflight_pc_stable:" + String(reason || "sys_preflight"),
      );
    };

    concierge.sysPeer = key;
    concierge.sysPipeState = "missing";
    concierge.sysLastErr =
      "sys_preflight_waiting_for_pc_stable:" +
      String(pc.signalingState || "unknown");

    phantomRefreshSysReadiness("sys_preflight_waiting_for_pc_stable");
    emitSnapshot("sys_preflight_waiting_for_pc_stable");

    resume();

    if (
      pc.signalingState === "stable" ||
      pc.signalingState === "closed" ||
      pc.connectionState === "failed" ||
      pc.connectionState === "closed" ||
      pc.iceConnectionState === "failed" ||
      pc.iceConnectionState === "closed" ||
      phantomHasOpenSysPipe(key)
    ) {
      return true;
    }

    if (!pc.__phantomSysStableWaiter) {
      try {
        pc.__phantomSysStableWaiter = true;
        pc.addEventListener("signalingstatechange", resume);
        pc.addEventListener("connectionstatechange", resume);
        pc.addEventListener("iceconnectionstatechange", resume);
      } catch {
        try {
          pc.__phantomSysStableWaiter = false;
        } catch {}
        return false;
      }
    }

    const old = phantomSysPreflightUnstableRetries.get(key) || {};
    const attempts = Number(old.attempts || 0) + 1;

    phantomSysPreflightUnstableRetries.set(key, {
      attempts,
      at: Date.now(),
      reason: String(reason || "pc_not_stable"),
    });

    if (attempts >= PHANTOM_SYS_PREFLIGHT_UNSTABLE_MAX) {
      cleanupWaiter();
      phantomSysPreflightUnstableRetries.delete(key);

      concierge.sysPeer = key;
      concierge.sysPipeState = "closed";
      concierge.sysLastErr =
        "sys_preflight_unstable_dead:" + String(pc.signalingState || "unknown");

      phantomRefreshSysReadiness("sys_preflight_unstable_dead");
      emitSnapshot("sys_preflight_unstable_dead");

      return false;
    }

    return true;
  }

  const sdkSysBirthFlights=new Map();
  function phantomEnsureSysPipe(remoteHKey, reason = "ensure") {
    const key=phantomShortKey(remoteHKey);
    if(!key)return Promise.resolve(false);
    return runSysBirth(sdkSysBirthFlights,key,()=>phantomPerformSysBirth(key,reason));
  }
  async function phantomPerformSysBirth(remoteHKey, reason = "ensure") {
    const key = phantomShortKey(remoteHKey);
    if (!key) return false;
    if (!selfHKey || !videoId) return false;

    if (phantomSysReplacementTerminal(key)) {
      phantomSysDialing.delete(key);
      phantomSysPreflightUnstableRetries.delete(key);
      emitSnapshot("sys_birth_blocked_terminal_exit_pending");
      return false;
    }

    const ownsPrimarySysState = phantomOwnsPrimarySysState(key);

    const recentGoodSysRx = Number(phantomSysLastGoodRx.get(key) || 0);

    if (recentGoodSysRx && now() - recentGoodSysRx < 5000) {
      const ent = phantomSysPeers.get(key);
      const sysState = String(ent?.sysdc?.readyState || "");

      if (sysState === "open") {
        if (ownsPrimarySysState) {
          concierge.sysPeer = key;
          concierge.sysPipeState =
            concierge.sysReadyPeer === key && concierge.sysReadyProof !== "none"
              ? "ready"
              : "open";
          concierge.sysLastErr = null;

          phantomRefreshSysReadiness("sys_preflight_recent_good_rx_open");
        }

        emitSnapshot("sys_preflight_recent_good_rx_open");
        return true;
      }

      phantomSysLastGoodRx.delete(key);

      if (phantomShortKey(concierge.sysReadyPeer) === key) {
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

        phantomRefreshSysReadiness("recent_good_rx_without_open_pipe");
      }

      emitSnapshot("recent_good_rx_without_open_pipe");

      // Do not return. Fall through to the real owner/acceptor logic below.
    }

    const birthRole = phantomSysBirthRole(key);
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
      const ent = phantomSysPeers.get(key);
      const dc = ent?.sysdc || null;

      if (ownsPrimarySysState) {
        concierge.sysPeer = key;
      }

      if (dc && (dc.readyState === "open" || dc.readyState === "connecting")) {
        if (ownsPrimarySysState) {
          concierge.sysPipeState = dc.readyState;
          concierge.sysLastErr = null;
          phantomRefreshSysReadiness(
            "sys_preflight_acceptor_observing_" + dc.readyState,
          );
        }

        emitSnapshot("sys_preflight_acceptor_observing_" + dc.readyState);
        return dc.readyState === "open";
      }

      if (ownsPrimarySysState) {
        concierge.sysPipeState = "missing";
        concierge.sysLastErr = "sys_preflight_acceptor_waiting_for_owner";
        phantomRefreshSysReadiness("sys_preflight_acceptor_waiting_for_owner");
      }

      emitSnapshot("sys_preflight_acceptor_waiting_for_owner");
      return false;
    }

    let birthEntry=null;
    const failOwnedSysBirth = (faultReason, detail = "") => {
      const fault = String(faultReason || "sys_birth_failed");
      const current = phantomSysPeers.get(key);
      if(birthEntry && current!==birthEntry){emitSnapshot("sys_birth_stale_completion_ignored");return false;}

      // Retire this failed birth before reporting it. Any resulting callbacks
      // are stale and cannot mutate the replacement attempt.
      if (current) {
        clearTimeout(current.signalResponseTimer);
        const failedDc = current.sysdc || null;
        const failedPc = current.pc || null;

        current.sysdc = null;
        current.sysAttemptId = null;
        current.ownerAttemptId = null;
        current.sysCarrier = false;

        phantomSysPeers.delete(key);

        try {
          failedDc?.close();
        } catch {}

        try {
          rtcDiagnostics.retire(failedPc,"sys_birth_failure:"+fault);
          failedPc?.close();
        } catch {}
      }

      phantomSysDialing.delete(key);
      phantomSysPreflightUnstableRetries.delete(key);

      if (detail) {
        concierge.sysLastErr = `${fault}:${String(detail)}`;
        emitSnapshot(fault + "_detail");
      }

      phantomHandleTransportLoss(key, fault);
      return false;
    };

    if (phantomHasOpenSysPipe(key)) {
      phantomSysPreflightUnstableRetries.delete(key);

      if (ownsPrimarySysState) {
        concierge.sysPeer = key;
        concierge.sysPipeState = "open";
        concierge.sysLastErr = null;

        phantomRefreshSysReadiness("sys_preflight_already_open");
      }

      if (
        String(concierge.localRoleCandidate || "peer") === "peer" ||
        !phantomCommittedMember(selfHKey)
      ) {
        phantomSendSysAdmissionPresence("sys_preflight_already_open", key);
      } else {
        phantomRefreshOpenSysPeer("sys_preflight_already_open");
      }

      emitSnapshot("sys_preflight_already_open");
      return true;
    }

    if (phantomSysDialing.has(key)) {
      emitSnapshot("sys_pipe_owner_already_dialing");
      return false;
    }

    phantomSysDialing.add(key);

    try {
      if (ownsPrimarySysState) {
        concierge.sysPeer = key;
      }

      const ent = phantomGetOrCreateSysPeer(key);
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
        phantomSysPreflightUnstableRetries.delete(key);

        if (ownsPrimarySysState) {
          concierge.sysPeer = key;
          concierge.sysPipeState = "open";
          concierge.sysLastErr = null;

          phantomRefreshSysReadiness("sys_preflight_existing_dc_open");
        }

        if (
          String(concierge.localRoleCandidate || "peer") === "peer" ||
          !phantomCommittedMember(selfHKey)
        ) {
          phantomSendSysAdmissionPresence("sys_preflight_existing_dc_open", key);
        } else {
          phantomRefreshOpenSysPeer("sys_preflight_existing_dc_open");
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
            phantomShortKey(concierge.sysPeer) === key &&
            phantomShortKey(concierge.sysReadyPeer) === key &&
            phantomHasOpenSysPipe(key)
          );

          if (ownsSysDial && transportDead && !legalReadyPipe) {
            return failOwnedSysBirth("sys_transport_birth_failed");
          }

          if (ownsSysDial && transportDead && legalReadyPipe) {
            concierge.sysLastErr =
              "transport_dead_ignored_legal_ready_sys_pipe";
            phantomRefreshSysReadiness(
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

            phantomRefreshSysReadiness(
              "sys_preflight_existing_dc_connecting_waiting",
            );
          }

          emitSnapshot("sys_preflight_existing_dc_connecting_waiting");

          phantomScheduleSysPreflightUnstableRetry(
            key,
            "sys_preflight_existing_dc_connecting",
          );

          return false;
        }

        if (sysState === "open") {
          phantomSysPreflightUnstableRetries.delete(key);

          if (ownsPrimarySysState) {
            concierge.sysPeer = key;
            concierge.sysPipeState = "open";
            concierge.sysLastErr = null;

            phantomRefreshSysReadiness("sys_preflight_existing_dc_open");
          }

          if (
            String(concierge.localRoleCandidate || "peer") === "peer" ||
            !phantomCommittedMember(selfHKey)
          ) {
            phantomSendSysAdmissionPresence("sys_preflight_existing_dc_open", key);
          } else {
            phantomRefreshOpenSysPeer("sys_preflight_existing_dc_open");
          }

          emitSnapshot("sys_preflight_existing_dc_open");
          return true;
        }

        if (ownsPrimarySysState) {
          concierge.sysPeer = key;
          concierge.sysPipeState = "missing";
          concierge.sysLastErr =
            "sys_preflight_waiting_for_pc_stable:" + pcState;

          phantomRefreshSysReadiness("sys_preflight_waiting_for_pc_stable");
        }

        emitSnapshot("sys_preflight_waiting_for_pc_stable");

        phantomScheduleSysPreflightUnstableRetry(key, String(reason || "ensure"));
        return false;
      }

      if (!ent.sysdc || ent.sysdc.readyState === "closed") {
        ent.sysdc = pc.createDataChannel(COMPATIBILITY_WIRE.systemChannel, { ordered: true });

        const edgeAttempt = phantomSysEdgeAttempt(key);
        if (edgeAttempt) {
          edgeAttempt.role = "opener";
          edgeAttempt.state = "creating";
          edgeAttempt.startedAt ||= now();
          edgeAttempt.lastEvent = "datachannel_created";
          edgeAttempt.faultReason = "none";
          ent.sysAttemptId = edgeAttempt.id;
        }

        phantomWireSysDC(key, ent.sysdc);
        createdSysDc = true;
      }

      if (!createdSysDc && ent.sysdc?.readyState === "connecting") {
        const disposition=sysBirthDisposition(pc,ent.sysdc);
        if(disposition==='failed')return failOwnedSysBirth("sys_transport_birth_failed");
        if(disposition==='waiting') {
          if(ownsPrimarySysState){concierge.sysPipeState="connecting";concierge.sysLastErr=null;}
          phantomRefreshSysReadiness("sys_birth_waiting_for_channel_open");
          emitSnapshot("sys_birth_waiting_for_channel_open");
          return false;
        }
      }

      const offer = await pc.createOffer();
      if(!isCurrentSysBirth(phantomSysPeers,key,ent))return false;
      await pc.setLocalDescription(offer);
      if(!isCurrentSysBirth(phantomSysPeers,key,ent))return false;

      const edgeAttempt = phantomSysEdgeAttempt(key);
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

      if(!isCurrentSysBirth(phantomSysPeers,key,ent))return false;
      if (!sent) {
        return failOwnedSysBirth("sys_birth_offer_send_failed");
      }

      if (ownsPrimarySysState) {
        concierge.sysPeer = key;
        concierge.sysPipeState = "connecting";
        concierge.sysLastErr = null;

        phantomRefreshSysReadiness("sys_preflight_offer_sent");
      }

      // A sent offer can be rejected by an owner that is still recovering.
      // Retire only an unanswered signaling attempt whose native ICE has not
      // begun. An answered/checking transport remains owned by native RTC.
      const offeredAttempt = ent.sysAttemptId;
      clearTimeout(ent.signalResponseTimer);
      ent.signalResponseTimer = setTimeout(() => {
        ent.signalResponseTimer = null;
        if (phantomSysPeers.get(key) !== ent || ent.sysAttemptId !== offeredAttempt ||
            pc.remoteDescription || pc.signalingState !== "have-local-offer" ||
            pc.connectionState !== "new" || pc.iceConnectionState !== "new" ||
            ent.sysdc?.readyState === "open") return;
        post("SYS_SIGNAL_TIMEOUT", {peer: key, attemptId: offeredAttempt,
          phase: "awaiting_answer", timeoutMs: 15000});
        if (!phantomLocalCommittedAuthority() &&
            String(concierge.localRoleCandidate || concierge.role || "peer") === "peer") {
          sdkBootstrapUnanswered.add(key);
          const candidates = [...phantomDoorwayAdmittedPeers].filter(peer => peer !== phantomShortKey(selfHKey)).sort();
          let next = candidates.find(peer => !sdkBootstrapUnanswered.has(peer));
          if (!next) {sdkBootstrapUnanswered.clear();next = candidates.find(peer => peer !== key);}
          sdkBootstrapTarget = next || null;
          if (next) post("SYS_BOOTSTRAP_ROUTE", {from: key, to: next, reason: "unanswered_discovery_hint"});
        }
        phantomHandleTransportLoss(key, "sys_transport_signal_response_timeout");
      }, 15000);
      phantomSysPreflightUnstableRetries.delete(key);
      emitSnapshot("sys_preflight_offer_sent");

      return true;
    } catch (e) {
      return failOwnedSysBirth("sys_birth_exception", String(e?.message || e));
    } finally {
      if(!birthEntry || !phantomSysPeers.has(key) || phantomSysPeers.get(key)===birthEntry)phantomSysDialing.delete(key);
    }
  }

  function phantomScheduleSysPreflight(reason = "schedule") {
    const role = String(concierge.localRoleCandidate || "peer");

    if (role !== "syscon1" && role !== "syscon2") {
      return false;
    }

    const target = phantomShortKey(phantomSysTargetFromCandidates());
    if (!target) return false;

    if (phantomSysReplacementTerminal(target)) {
      phantomSysDialing.delete(target);
      phantomSysPreflightUnstableRetries.delete(target);
      emitSnapshot("sys_preflight_blocked_terminal_exit_pending");
      return false;
    }

    const ent = phantomSysPeers.get(target);
    const dcState = String(ent?.sysdc?.readyState || "");

    if (dcState === "open" || dcState === "connecting") {
      concierge.sysPeer = target;
      concierge.sysPipeState = dcState;

      phantomRefreshSysReadiness(
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

      phantomRefreshSysReadiness(
        "sys_preflight_acceptor_wait_" + String(reason || "schedule"),
      );
      emitSnapshot("sys_preflight_acceptor_wait");
      return false;
    }

    /*
     * Syscon1 is the deterministic pair opener.
     * Birth immediately from the authority/target event that called us.
     */
    return phantomEnsureSysPipe(
      target,
      "syscon1_primary_" + String(reason || "schedule"),
    );
  }

  async function rtcDial(remoteHKey, opts = {}) {
    const key = phantomShortKey(remoteHKey);
    if (!key) return;

    const self = phantomShortKey(selfHKey);
    if (!self || key === self) return;

    const requestedPipe = String(opts?.pipe || "chat");
    const dialReason = String(opts?.reason || requestedPipe || "rtc_dial");

    if (requestedPipe !== "chat") {
      return phantomEnsureSysPipe(key, dialReason);
    }

    if (!phantomOwnsChatPipeOffer(key)) {
      concierge.lastErr = "chat_pipe_not_offer_owner:" + dialReason;
      emitSnapshot("chat_pipe_not_offer_owner");
      return;
    }

    if (rtcDialing.has(key)) return;

    const live = phantomChatPeers.get(key);

    if (live?.dc?.readyState === "closed") {
      phantomChatPeers.delete(key);
    }

    if (live?.dc?.readyState === "open") {
      return;
    }

    if (live?.dc?.readyState === "connecting") {
      if (live.chatAttemptId && !phantomChatReconnectDeadlines.has(key)) {
        phantomArmChatAttemptDeadline(
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

      const ent = phantomGetOrCreateChatPeer(key);
      if (!ent?.pc) return;
      const pc = ent.pc;

      if (ent.dc?.readyState === "open") {
        return;
      }

      if (ent.dc?.readyState === "connecting") {
        if (ent.chatAttemptId && !phantomChatReconnectDeadlines.has(key)) {
          phantomArmChatAttemptDeadline(
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

      ent.chatAttemptId = phantomNewChatAttemptId(key);
      ent.chatAttemptState = "creating";
      ent.chatAttemptStartedAt = now();
      ent.chatState = "creating";
      ent.chatFault = "none";
      ent.chatProof = "owner_created_dc";

      ent.dc = pc.createDataChannel(COMPATIBILITY_WIRE.chatChannel, { ordered: true });
      rtcWireDC(key, ent.dc);
      emitSnapshot("chat_pipe_owner_created_dc");

      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);

      const offerSent = await rtcSend(key, "offer", pc.localDescription, {
        pipe: "chat",
      });

      ent.chatAttemptState = offerSent ? "awaiting_answer" : "signal_held";
      phantomArmChatAttemptDeadline(key, ent.chatAttemptId, dialReason);
      emitSnapshot("chat_pipe_offer_sent");
    } catch (e) {
      const ent = phantomChatPeers.get(key);
      if (ent?.chatAttemptId) {
        ent.chatAttemptState = "signal_failed";
        ent.chatFault = "offer_failed";
        phantomArmChatAttemptDeadline(key, ent.chatAttemptId, "offer_failed");
      }
      concierge.lastErr = "chat_pipe_offer_failed:" + String(e?.message || e);
      emitSnapshot("chat_pipe_offer_failed");
    } finally {
      rtcDialing.delete(key);
    }
  }

  function phantomRtcPipeFromMeta(meta = {}) {
    return meta?.pipe === "sys" || meta?.sysCarrier === true ? "sys" : "chat";
  }

  function phantomRtcEdgeForInbound(fromHKey, meta = {}) {
    const key = phantomShortKey(fromHKey);
    if (!key) return null;

    const pipe = phantomRtcPipeFromMeta(meta);
    const ent =
      pipe === "sys"
        ? phantomGetOrCreateSysPeer(key)
        : phantomGetOrCreateChatPeer(key);

    if (!ent) return null;
    if (!ent.rtcInboundChain) ent.rtcInboundChain = Promise.resolve();
    if (!Array.isArray(ent.pendingRemoteIce)) ent.pendingRemoteIce = [];

    return { key, pipe, ent };
  }

  function phantomQueueInboundRtc(fromHKey, meta, kind, apply, candidate) {
    const edge = phantomRtcEdgeForInbound(fromHKey, meta);
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
          pipe === "sys" ? phantomSysPeers.get(key) : phantomChatPeers.get(key);

        if (!current || current !== ent || !ent.pc) {
          if(label==='ice')traceIce('discarded_retired_queue',key,pipe,candidate,meta,ent.pc);
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

  async function phantomFlushInboundRtcIce(
    fromHKey,
    pipe,
    ent,
    reason = "remote_description_applied",
  ) {
    const key = phantomShortKey(fromHKey);
    if (!key || !ent?.pc || !ent.pc.remoteDescription) return false;

    const pending = Array.isArray(ent.pendingRemoteIce)
      ? ent.pendingRemoteIce.splice(0)
      : [];

    if (!pending.length) return true;

    for (const cand of pending) {
      try {
        await ent.pc.addIceCandidate(cand);
        traceIce('applied_from_buffer',key,pipe,cand,{},ent.pc);
        rtcDiagnostics.capture(ent.pc,'remote_candidate_applied');
      } catch (e) {
        traceIce('apply_rejected',key,pipe,cand,{error:{name:e?.name,message:String(e?.message??e)}},ent.pc);
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

  function sdkSendBootstrapRouteHint(key,meta) {
    const self=phantomShortKey(selfHKey),base=phantomLocalCommittedAuthority(),leader=phantomShortKey(base?.syscon1);
    const leaderEntry=phantomSysPeers.get(leader);
    if(!base||!leader||leader===self||leader===key||meta.sysTemporary===true||meta.room!==videoId||
        typeof meta.bootId!=='string'||!meta.bootId||meta.bootId.length>256||
        typeof meta.sysAttemptId!=='string'||!meta.sysAttemptId||meta.sysAttemptId.length>256||phantomCommittedMember(key)||!phantomDoorwayAdmittedPeers.has(key)||
        !concierge.sysReady||!phantomHasOpenSysPipe(leader)||leaderEntry?.pc?.connectionState!=='connected'||
        !['connected','completed'].includes(leaderEntry?.pc?.iceConnectionState))return false;
    const packet={type:'rtc_route_hint',pipe:'sys',videoId,from:self,to:key,bootId:phantomBootId,
      requestBootId:meta.bootId,sysAttemptId:meta.sysAttemptId,leader};
    const sent=phantomSendControl(packet,'bootstrap_route_hint');
    post('SYS_BOOTSTRAP_ROUTE_HINT',{action:'sent',to:key,leader,attemptId:meta.sysAttemptId,sent});return sent;
  }
  function sdkReceiveBootstrapRouteHint(packet) {
    const key=phantomShortKey(packet?.from),entry=phantomSysPeers.get(key);
    const accepted=host.canAdmitPeer(key)&&acceptsBootstrapRouteHint({packet,room:videoId,self:phantomShortKey(selfHKey),
      bootId:phantomBootId,role:String(concierge.localRoleCandidate||concierge.role||'peer'),
      committed:!!phantomLocalCommittedAuthority(),expectedPeer:phantomShortKey(phantomSysTargetFromCandidates()),
      attemptId:entry?.sysAttemptId,awaitingAnswer:!!(entry?.pc?.signalingState==='have-local-offer'&&!entry.pc.remoteDescription&&!entry.rtcAnswerApplying&&entry.sysdc?.readyState!=='open'),
      livePeers:phantomDoorwayAdmittedPeers,visited:sdkBootstrapUnanswered,knownResponderBootId:phantomRemoteBootIds.get(key)});
    if(!accepted){post('SYS_BOOTSTRAP_ROUTE_HINT',{action:'ignored',from:key,reason:'uncorrelated_or_ineligible'});return;}
    sdkBootstrapUnanswered.add(key);sdkBootstrapTarget=packet.leader;
    post('SYS_BOOTSTRAP_ROUTE',{from:key,to:packet.leader,reason:'peer_route_hint'});
    phantomHandleTransportLoss(key,'sys_transport_bootstrap_route_hint');
  }
  function sdkHandoffPeerEligible(key,meta) {
    const self=phantomShortKey(selfHKey),knownBoot=phantomRemoteBootIds.get(key);
    return key && key!==self && host.canAdmitPeer(key) &&
      meta.room===videoId && (!knownBoot||knownBoot===meta.bootId) &&
      !phantomPendingTerminalExits.has(key) && !__phantomGoneNotified.has(key) &&
      (phantomCommittedMember(key)||phantomDoorwayAdmittedPeers.has(key));
  }
  function sdkShouldDeferHandoffOffer(key,meta) {
    if(phantomRtcPipeFromMeta(meta)!=='sys'||meta.sysTemporary===true||phantomHasOpenSysPipe(key))return false;
    const base=phantomLocalCommittedAuthority(),self=phantomShortKey(selfHKey);
    if(!base||phantomShortKey(base.syscon2)!==self||key===phantomShortKey(base.syscon1))return false;
    const leader=phantomSysPeers.get(phantomShortKey(base.syscon1));
    const healthy=phantomHasOpenSysPipe(base.syscon1)&&leader?.pc?.connectionState==='connected'&&
      ['connected','completed'].includes(leader.pc.iceConnectionState);
    return !healthy&&sdkHandoffPeerEligible(key,meta);
  }
  function sdkValidSuccessorOffer(key,meta) {
    if(phantomRtcPipeFromMeta(meta)!=='sys'||meta.sysTemporary===true||!sdkHandoffPeerEligible(key,meta))return false;
    return acceptsSuccessorHandshake({proof:meta.successorProof,base:phantomLocalCommittedAuthority(),
      sender:key,self:phantomShortKey(selfHKey),room:videoId,bootId:String(meta.bootId||''),leaderPipeOpen:false});
  }
  function sdkResumeDeferredOffers() {
    const isLeader=()=>phantomShortKey(phantomLocalCommittedAuthority()?.syscon1)===phantomShortKey(selfHKey)&&
      String(concierge.localRoleCandidate||concierge.role)==='syscon1';
    sdkDeferredOffers.resume({isLeader:()=>true,
      eligible:row=>!phantomHasOpenSysPipe(row.peer)&&(row.kind==='successor'
        ?sdkValidSuccessorOffer(row.peer,row.meta)
        :sdkHandoffPeerEligible(row.peer,row.meta)&&(!isLeader()||phantomSysBirthRole(row.peer)==='acceptor')),
      waiting:row=>row.kind==='successor'
        ?phantomHasOpenSysPipe(phantomLocalCommittedAuthority()?.syscon1):!isLeader(),
      accept:row=>{rtcOnOffer(row.peer,row.offer,row.meta);for(const ice of row.ice)rtcOnIce(row.peer,ice.candidate,ice.meta);}
    });
  }
  function rtcOnOffer(fromHKey, offer, meta = {}) {
    if (!host.canAdmitPeer(phantomShortKey(fromHKey))) return;
    const key=phantomShortKey(fromHKey);
    if(phantomSysBirthRole(key)!=='acceptor'&&sdkValidSuccessorOffer(key,meta)&&phantomHasOpenSysPipe(phantomLocalCommittedAuthority()?.syscon1)) {
      if(!sdkDeferredOffers.hold(key,offer,meta,'successor'))post('SYS_SUCCESSOR_OFFER',{action:'rejected',peer:key,reason:'invalid_or_capacity'});
      return;
    }
    if(sdkShouldDeferHandoffOffer(key,meta)) {
      if(!sdkDeferredOffers.hold(key,offer,meta,'handoff'))post('SYS_HANDOFF_OFFER',{action:'rejected',peer:key,reason:'invalid_or_capacity'});
      return;
    }
    // Replace the old SYS incarnation before any work captures its queue.
    // Following ICE then binds to the same connection as the incoming offer.
    if (!phantomPrepareSysOffer(fromHKey, meta)) return;
    if(phantomRtcPipeFromMeta(meta)==='sys'){sdkDeferredOffers.drop(key,'accepted_new_offer');}
    return phantomQueueInboundRtc(fromHKey, meta, "offer", () =>
      phantomApplyRtcOffer(fromHKey, offer, meta),
    );
  }

  function rtcOnAnswer(fromHKey, answer, meta = {}) {
    if (!host.canAdmitPeer(phantomShortKey(fromHKey))) return;
    if (phantomRtcPipeFromMeta(meta) === "sys" && !phantomSysPeers.has(phantomShortKey(fromHKey))) {
      emitSnapshot("sys_rtc_answer_ignored_missing_attempt");
      return;
    }
    return phantomQueueInboundRtc(fromHKey, meta, "answer", () =>
      phantomApplyRtcAnswer(fromHKey, answer, meta),
    );
  }

  function rtcOnIce(fromHKey, cand, meta = {}) {
    traceIce('received',fromHKey,phantomRtcPipeFromMeta(meta),cand,meta);
    if (!host.canAdmitPeer(phantomShortKey(fromHKey))) {traceIce('rejected_admission',fromHKey,phantomRtcPipeFromMeta(meta),cand,meta);return;}
    const key=phantomShortKey(fromHKey);
    if(phantomRtcPipeFromMeta(meta)==='sys'&&sdkDeferredOffers.has(key)) {
      const result=sdkDeferredOffers.ice(key,cand,meta);
      traceIce(result==='held'||result==='duplicate'?'buffered_deferred':'discarded_deferred_'+result,key,'sys',cand,meta);
      return;
    }
    return phantomQueueInboundRtc(fromHKey, meta, "ice", () =>
      phantomApplyRtcIce(fromHKey, cand, meta),
      cand,
    );
  }

  function phantomPrepareSysOffer(fromHKey, meta = {}, replace = true) {
    const fromKey = phantomShortKey(fromHKey);
    const selfKey = phantomShortKey(selfHKey);
    if (!fromKey || fromKey === selfKey) return false;

    const pipe = phantomRtcPipeFromMeta(meta);

    let sysBirthRole = "";

    if (pipe === "sys") {
      if (phantomSysReplacementTerminal(fromKey)) {
        concierge.lastErr =
          "sys_offer_rejected_terminal_exit_pending:" + fromKey;
        emitSnapshot("sys_offer_rejected_terminal_exit_pending");
        return false;
      }

      if (meta?.sysTemporary === true) {
        if (!phantomBindInboundTemporarySysTransaction(fromKey, meta)) {
          concierge.lastErr =
            "sys_offer_rejected_invalid_temporary_transaction:" + fromKey;
          return false;
        }
      }

      sysBirthRole = phantomSysBirthRole(fromKey);
      const base = phantomLocalCommittedAuthority();
      if (sysBirthRole !== "acceptor" && meta.sysTemporary !== true &&
          acceptsSuccessorHandshake({proof: meta.successorProof, base, sender: fromKey,
            self: selfKey, room: videoId, bootId: String(meta.bootId || ""),
            leaderPipeOpen: base ? phantomHasOpenSysPipe(base.syscon1) : true})) {
        sysBirthRole = "acceptor";
        emitSnapshot("historical_mirror_recovery_pipe_accepted");
      }

      if (sysBirthRole !== "acceptor") {
        if(replace&&meta.sysTemporary!==true)sdkSendBootstrapRouteHint(fromKey,meta);
        concierge.lastErr =
          "sys_offer_rejected_non_owner:" + sysBirthRole + ":" + fromKey;
        emitSnapshot("sys_offer_rejected_non_owner");
        return false;
      }
    }

    if(pipe==='sys' && replace) {
      // Validate the offer owner before an incoming attempt can retire a pipe.
      phantomObserveRemoteIncarnation(fromKey,{bootId:String(meta.bootId||''),attemptId:String(meta.sysAttemptId||''),signalType:'rtc_offer',via:'rtc_offer',bindCurrent:meta.recoveryPeerPipe===true});
      const incoming=String(meta.sysAttemptId||''),current=phantomSysPeers.get(fromKey);
      if(incoming && current?.sysAttemptId && incoming!==current.sysAttemptId && !phantomHasOpenSysPipe(fromKey)) {
        phantomTeardownSysPeer(fromKey,"sys_attempt_superseded_by_remote_offer");
        emitSnapshot("sys_attempt_superseded_by_remote_offer");
      }
    }

    return true;
  }

  async function phantomApplyRtcOffer(fromHKey, offer, meta = {}) {
    // Roles can change while a queued offer waits. Recheck without replacing
    // the connection that now owns this offer and its queued ICE candidates.
    if (!phantomPrepareSysOffer(fromHKey, meta, false)) return;
    const fromKey = phantomShortKey(fromHKey);
    const selfKey = phantomShortKey(selfHKey);
    const pipe = phantomRtcPipeFromMeta(meta);

    if (pipe === "chat") {
      const incomingAttemptId = String(meta?.chatAttemptId || "");
      const current = phantomChatPeers.get(fromKey);
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
        ? phantomGetOrCreateSysPeer(fromKey)
        : phantomGetOrCreateChatPeer(fromKey);
    if (!ent?.pc) return;

    if (pipe === "chat") {
      const incomingAttemptId = String(meta?.chatAttemptId || "");
      if (incomingAttemptId) ent.chatAttemptId = incomingAttemptId;
      ent.chatAttemptState = "offer_received";
      ent.chatAttemptStartedAt ||= now();
    }

    if (pipe === "sys") {
      ent.sysCarrier = true;
      const edgeAttempt = phantomSysEdgeAttempt(fromKey);
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
        String(meta?.bootId || phantomRemoteBootIds.get(fromKey) || "") || null;
      if (phantomOwnsPrimarySysState(fromKey)) {
        ent.ownerAttemptId = String(concierge.sysAttempt?.id || "") || null;
      }
    }

    const pc = ent.pc;

    if (pc.signalingState !== "stable") {
      if (pipe === "sys") {
        concierge.lastErr =
          "sys_offer_rejected_acceptor_not_stable:" + pc.signalingState;
        const edgeAttempt = phantomSysEdgeAttempt(fromKey, false);
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
    await phantomFlushInboundRtcIce(
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
      const edgeAttempt = phantomSysEdgeAttempt(fromKey, false);
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
      phantomRequestChatPipe(fromKey, "chat_carrier_stable_after_answer_sent");
    }
  }

  async function phantomApplyRtcAnswer(fromHKey, answer, meta = {}) {
    const key = phantomShortKey(fromHKey);
    if (!key) return;

    const pipe = phantomRtcPipeFromMeta(meta);
    const ent = pipe === "sys" ? phantomSysPeers.get(key) : phantomChatPeers.get(key);
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
        const transaction = phantomTemporarySysTransactions.get(key);
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

      phantomObserveRemoteIncarnation(key, {
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
      if (pipe === "sys") {clearTimeout(ent.signalResponseTimer);ent.signalResponseTimer = null;}
      await phantomFlushInboundRtcIce(key, pipe, ent, "answer_remote_description");
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
        phantomRequestChatPipe(key, "chat_carrier_stable_after_accept");
      }
      return;
    }

    const sysState = String(ent.sysdc?.readyState || "");
    const edgeAttempt = phantomSysEdgeAttempt(key, false);
    if (edgeAttempt) {
      edgeAttempt.state = sysState === "open" ? "open" : "connecting";
      edgeAttempt.lastEvent = "answer_applied";
      edgeAttempt.faultReason = "none";
    }

    if (!phantomOwnsPrimarySysState(key)) {
      emitSnapshot("sys_peer_tether_answer_applied");
      return;
    }

    if (!sysState || sysState === "closed") {
      concierge.sysLastErr = "sys_answer_applied_pipe_missing";
      phantomRefreshSysReadiness("sys_answer_applied_pipe_missing");
    } else if (sysState === "connecting") {
      concierge.sysPeer = key;
      concierge.sysPipeState = "connecting";
      phantomRefreshSysReadiness("sys_answer_applied_connecting");
    } else if (sysState === "open") {
      concierge.sysPeer = key;
      concierge.sysPipeState = "open";
      phantomRefreshOpenSysPeer("sys_answer_applied_open");
    }
  }

  async function phantomApplyRtcIce(fromHKey, cand, meta = {}) {
    const key = phantomShortKey(fromHKey);
    if (!key) return;

    const pipe = phantomRtcPipeFromMeta(meta);
    const ent = pipe === "sys" ? phantomSysPeers.get(key) : phantomChatPeers.get(key);
    if (!ent?.pc) {traceIce('discarded_missing_connection',key,pipe,cand,meta);return;}

    if (pipe === "chat") {
      const incomingAttemptId = String(meta?.chatAttemptId || "");
      const currentAttemptId = String(ent?.chatAttemptId || "");
      if (
        incomingAttemptId &&
        currentAttemptId &&
        incomingAttemptId !== currentAttemptId
      ) {
        traceIce('discarded_attempt_mismatch',key,pipe,cand,meta,ent.pc);
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
        traceIce('discarded_attempt_mismatch',key,pipe,cand,meta,ent.pc);
        emitSnapshot("sys_rtc_ice_ignored_stale_transport_attempt");
        return false;
      }

      if (ent.temporary === true) {
        const transaction = phantomTemporarySysTransactions.get(key);
        if (
          meta?.sysTemporary !== true ||
          !transaction ||
          String(meta?.sysPurpose || "") !==
            String(transaction.purpose || "") ||
          String(meta?.sysTxId || "") !== String(transaction.id || "")
        ) {
          traceIce('discarded_attempt_mismatch',key,pipe,cand,meta,ent.pc);
        emitSnapshot("temporary_sys_ice_ignored_transaction_mismatch");
          return false;
        }
      }
    }

    if (!cand) return false;

    if (!ent.pc.remoteDescription) {
      traceIce('buffered_before_description',key,pipe,cand,meta,ent.pc);
      ent.pendingRemoteIce.push(cand);

      if (ent.pendingRemoteIce.length > PHANTOM_RTC_ICE_QUEUE_MAX) {
        ent.pendingRemoteIce.splice(
          0,
          ent.pendingRemoteIce.length - PHANTOM_RTC_ICE_QUEUE_MAX,
        );
      }

      emitSnapshot(pipe + "_rtc_ice_latched_waiting_remote_description");
      return true;
    }

    try {
      await ent.pc.addIceCandidate(cand);
      traceIce('applied',key,pipe,cand,meta,ent.pc);
      rtcDiagnostics.capture(ent.pc,'remote_candidate_applied');
      emitSnapshot(pipe + "_rtc_ice_applied");
      return true;
    } catch (e) {
      traceIce('apply_rejected',key,pipe,cand,{...meta,error:{name:e?.name,message:String(e?.message??e)}},ent.pc);
      concierge.lastErr =
        pipe + "_rtc_ice_apply_failed:" + String(e?.message || e);
      emitSnapshot(pipe + "_rtc_ice_apply_failed");
      return false;
    }
  }

  function post(type, payload) {
    try { phantomCountEvent("post:" + String(type || "unknown")); } catch {}
    host.emit(type, payload);
  }
  function postLocal(type, payload) { host.emit(type, payload); }
  post("CORE_ALIVE", { loaded: true, early: true, ts: now() });

  function snapshot(reason) {
    return {
      t: now(),
      reason: reason || null,
      eventCounts: phantomEventCountRows(20),
      rtcDiagnostics: rtcDiagnostics.snapshot(),
      iceTrace: iceTrace.snapshot(),

      peerConnections: phantomPeerConnections(),
      chatPairs: Array.from(phantomChatPeers.entries())
        .map(([hKey, ent]) => ({
          hKey: phantomShortKey(hKey),
          handle: String(
            phantomPresenceBook.get(phantomShortKey(hKey))?.handle || "",
          ),
          relationship: getState(phantomShortKey(hKey)),
          offerOwner: phantomOwnsChatPipeOffer(phantomShortKey(hKey)),

          pcState: String(ent?.pc?.connectionState || "missing"),
          iceState: String(ent?.pc?.iceConnectionState || "missing"),
          signalingState: String(ent?.pc?.signalingState || "missing"),

          dcState: String(ent?.dc?.readyState || "missing"),
          dcLabel: String(ent?.dc?.label || "none"),

          chatState: String(ent?.chatState || "missing"),
          chatProof: String(ent?.chatProof || "none"),
          chatReadyAt: Number(ent?.chatReadyAt || 0),
          chatFault: String(ent?.chatFault || "none"),

          reconnecting: reconnectingPeers.has(phantomShortKey(hKey)),
          pendingDm: Number(phantomPendingDm.get(phantomShortKey(hKey))?.length || 0),
        }))
        .filter((row) => row.hKey),

      enabled: !!enabled,
      self: { handle: selfHandle, hex: selfHex, hKey: selfHKey },
      ctx: { roomId: ctx?.roomId || null },
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
        source: phantomRosterAudit.source || "none",
        count: Number(phantomRosterAudit.count || 0),
        added: phantomRosterAudit.added || "none",
        removed: phantomRosterAudit.removed || "none",
        pruneReason: phantomRosterAudit.pruneReason || "none",
        at: Number(phantomRosterAudit.at || 0),
      },

      temporarySys: Array.from(phantomTemporarySysTransactions.values()).map(
        (rec) => ({
          id: String(rec?.id || ""),
          purpose: String(rec?.purpose || "none"),
          peer: phantomShortKey(rec?.peer),
          initiator: phantomShortKey(rec?.initiator),
          state: String(rec?.state || "unknown"),
          inbound: rec?.inbound === true,
          createdAt: Number(rec?.createdAt || 0),
          openedAt: Number(rec?.openedAt || 0),
          ackAt: Number(rec?.ackAt || 0),
          deadlineAt: Number(rec?.deadlineAt || 0),
        }),
      ),

      handoffOffers: sdkDeferredOffers.snapshot().filter(row=>row.kind==='handoff'),
      successorOffers: sdkDeferredOffers.snapshot().filter(row=>row.kind==='successor'),
      gossipRecovery: {
        source: phantomShortKey(phantomGossipRecovery.source) || null,
        targets: Array.isArray(phantomGossipRecovery.targets)
          ? phantomGossipRecovery.targets.map(phantomShortKey).filter(Boolean)
          : [],
        index: Number(phantomGossipRecovery.index || 0),
        epoch: Number(phantomGossipRecovery.epoch || 0),
        rosterSeq: Number(phantomGossipRecovery.rosterSeq || 0),
        at: Number(phantomGossipRecovery.at || 0),
        state: String(phantomGossipRecovery.state || "idle"),
        successor: phantomShortKey(phantomGossipRecovery.successor) || null,
        exhaustedAt: Number(phantomGossipRecovery.exhaustedAt || 0),
        kind: String(phantomGossipRecovery.kind || 'gossip'),
        retryCount: Number(phantomGossipRecovery.retryCount || 0),
        nextRetryAt: Number(phantomGossipRecovery.nextRetryAt || 0),
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
          [concierge.sysLastErr, phantomMembershipAuditText()]
            .filter(Boolean)
            .join(" | ") || null,
        membershipAudit: {
          at: Number(phantomMembershipAudit.at || 0),
          stage: phantomMembershipAudit.stage || "idle",
          peer: phantomMembershipAudit.peer || null,
          id: phantomMembershipAudit.id || "none",
          detail: phantomMembershipAudit.detail || "none",
          topology: phantomMembershipAuditTopology(),
          trace: phantomMembershipAuditTrace.slice(-10).map((row) => ({
            at: Number(row.at || 0),
            stage: String(row.stage || "unknown"),
            peer: row.peer || null,
            id: String(row.id || "none"),
            detail: String(row.detail || "none"),
          })),
          chatSignalTrace: phantomChatSignalTrace
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

  const phantomEventCounts = new Map();
  const PHANTOM_EVENT_COUNTS_MAX = 80;

  function phantomCountEvent(name) {
    const key = String(name || "unknown").slice(0, 80);
    if (!key) return;

    const old = phantomEventCounts.get(key) || { count: 0, at: 0 };

    phantomEventCounts.set(key, {
      count: Number(old.count || 0) + 1,
      at: now(),
    });

    if (phantomEventCounts.size > PHANTOM_EVENT_COUNTS_MAX) {
      const rows = Array.from(phantomEventCounts.entries()).sort(
        (a, b) => Number(a[1]?.at || 0) - Number(b[1]?.at || 0),
      );

      while (
        phantomEventCounts.size > Math.floor(PHANTOM_EVENT_COUNTS_MAX * 0.75) &&
        rows.length
      ) {
        phantomEventCounts.delete(rows.shift()[0]);
      }
    }
  }

  function phantomEventCountRows(limit = 20) {
    return Array.from(phantomEventCounts.entries())
      .sort((a, b) => Number(b[1]?.count || 0) - Number(a[1]?.count || 0))
      .slice(0, limit)
      .map(([name, rec]) => ({
        name,
        count: Number(rec?.count || 0),
        at: Number(rec?.at || 0),
      }));
  }

  let phantomLastSnapshotSig = "";
  let phantomLastSnapshotReason = "none";

  function phantomSnapshotSignature() {
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
      String(phantomMembershipAudit.stage || "idle"),
      String(phantomMembershipAudit.peer || "none"),
      String(phantomMembershipAudit.id || "none"),
      String(phantomMembershipAudit.detail || "none"),
    ].join("|");
  }

  function emitSnapshot(reason) {
    try {
      const why = String(reason || "snapshot");
      phantomCountEvent("snapshot:" + why);

      const sig = phantomSnapshotSignature();

      if (sig === phantomLastSnapshotSig && why === phantomLastSnapshotReason) {
        return;
      }

      if (
        sig === phantomLastSnapshotSig &&
        (why.includes("rtc_ice") ||
          why.includes("ice_candidate") ||
          why.includes("sysdc_rx_SYS_CONTROL_ACK") ||
          why.includes("sysdc_rx_SYS_STATUS_ACK"))
      ) {
        phantomLastSnapshotReason = why;
        return;
      }

      phantomLastSnapshotSig = sig;
      phantomLastSnapshotReason = why;

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

      // Normalize a pinned full public identity to its short transport ID.
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
      !phantomSessionKilling && enabled && !!selfHKey && !!videoId;

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
          String(failure.localCloseReason || "phantom_doorway_failure"),
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
      ? "phantom_connect_deadline"
      : "phantom_ready_deadline";

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
      p === "ready" ? PHANTOM_WS_READY_DEADLINE_MS : PHANTOM_WS_CONNECT_DEADLINE_MS;

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
    }, PHANTOM_WS_WATCH_TICK_MS);
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
    if (phantomSessionKilling) return;

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
        bootId: phantomBootId,
        t: now(),
      };

      bootstrap.lastSendType = "concierge_connect";
      bootstrap.lastSendAt = now();
      bootstrap.lastSendOk = false;

      const ok = phantomSendControl(connectMsg, "concierge_connect");
      bootstrap.lastSendOk = !!ok;

      if (!ok) {
        bootstrap.lastErr = "concierge_connect_send_failed";
        emitSnapshot("concierge_connect_send_failed");
        return;
      }

      post("NETWORK_READY", {
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
        phantomMarkControlRx(msg.type);

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

      if (msg && phantomIsRtcSignalType(msg.type)) {
        concierge.lastRtcRxAt = now();
        concierge.lastRtcRxType = String(msg.type || "unknown");
        concierge.lastRtcRxVia = "bootstrap_ws";
      }

      if (msg && msg.type === "status_request") {
        phantomSendControl(
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

        const acceptedDoorwayRoster = phantomApplyConciergeRoute(msg, msg.type);

        const provedDoorwayRoster = !!(
          Array.isArray(msg.list) &&
          msg.list.some((row) => {
            const key = phantomShortKey(typeof row === "string" ? row : row?.hKey);

            return key && key === phantomShortKey(selfHKey);
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
            .map((k) => phantomShortKey(k)),
        );

        const sysAuthority = phantomLocalCommittedAuthority();
        const sysOwnsRoster = !!(concierge.sysReady || sysAuthority);

        if (!sysOwnsRoster) {
          for (const oldKey of Array.from(lastRoster || [])) {
            const gone = phantomShortKey(oldKey);
            if (!gone || gone === phantomShortKey(selfHKey)) continue;
            if (nextRosterKeys.has(gone)) continue;

            emitSnapshot("bootstrap_roster_peer_temporarily_absent");
          }

          lastRoster = phantomMarkRosterAudit(
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

          phantomRememberPresencePeer(row, "bootstrap_roster", {
            peerEvent: false,
            rosterEvent: false,
          });
        }

        dialReady = true;

        if (sysOwnsRoster) {
          phantomEmitPresenceRoster("bootstrap_roster_observed_sys_owns_roster");
        } else {
          post("PRESENCE_ROSTER", {
            videoId: msg.videoId || videoId || null,
            list: msg.list,
          });
        }

        phantomRefreshSysReadiness("roster_rx");
        phantomRefreshSysCandidates(msg.list, "roster_rx");
        phantomReconcileRelationshipStatus("relationship_status_roster_rx");

        if (!concierge.sysReady) {
          if (
            concierge.localRoleCandidate === "syscon1" ||
            concierge.localRoleCandidate === "syscon2"
          ) {
            phantomScheduleSysPreflight("roster_rx");
          }

          if (
            concierge.localRoleCandidate === "peer" ||
            !phantomCommittedMember(selfHKey)
          ) {
            phantomScheduleSysAdmission("roster_rx");
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

      if(msg&&msg.type==='rtc_route_hint') {
        if(isDirectMsgForMe(msg))sdkReceiveBootstrapRouteHint(msg);
        return;
      }
      if (msg && msg.type === "rtc_undeliverable") {
        const target = phantomShortKey(msg.target);
        const attemptTarget = phantomShortKey(concierge.sysAttempt?.target);
        const role = String(concierge.localRoleCandidate || "peer");

        if (target && phantomSysReplacementTerminal(target)) {
          phantomTeardownSysPeer(
            target,
            "rtc_undeliverable_terminal_exit_pending",
          );
          phantomSysDialing.delete(target);
          phantomSysPreflightUnstableRetries.delete(target);
          phantomPendingRtcIce.delete(target);
          emitSnapshot("rtc_undeliverable_held_terminal_exit_pending");
          return;
        }

        if (
          target &&
          msg.signalType === "rtc_offer" &&
          msg.sysCarrier === true &&
          phantomTemporarySysTransactions.has(target)
        ) {
          phantomCloseTemporarySysTransaction(target, "rtc_undeliverable");
          emitSnapshot("temporary_sys_rtc_undeliverable");
          return;
        }

        const ownsFailedBirth = !!(
          msg.signalType === "rtc_offer" &&
          msg.sysCarrier === true &&
          role === "syscon1" &&
          target &&
          target === phantomShortKey(concierge.syscon2) &&
          target === attemptTarget &&
          Number(concierge.sysAttempt?.recoveryCount || 0) === 1
        );

        if (!ownsFailedBirth) {
          emitSnapshot("rtc_undeliverable_ignored_stale_attempt");
          return;
        }

        const ent = phantomSysPeers.get(target);

        if (ent) {
          const failedDc = ent.sysdc || null;
          const failedPc = ent.pc || null;

          ent.sysdc = null;
          ent.sysAttemptId = null;
          ent.ownerAttemptId = null;
          ent.sysCarrier = false;

          phantomSysPeers.delete(target);

          try {
            failedDc?.close();
          } catch {}

          try {
            rtcDiagnostics.retire(failedPc,"rtc_undeliverable");
            failedPc?.close();
          } catch {}
        }

        phantomSysDialing.delete(target);
        phantomSysPreflightUnstableRetries.delete(target);

        if (!phantomCompleteFailedSysReplacement(target, "rtc_undeliverable")) {
          phantomHandleTransportLoss(target, "sys_transport_undeliverable");
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
          const from = phantomShortKey(msg.from);
          const relationship = phantomRelationshipGet(from);
          const incomingCycle = String(msg.relationshipCycleId || "");
          if (
            !from ||
            phantomPendingTerminalExits.has(from) ||
            __phantomGoneNotified.has(from) ||
            !phantomRelationshipIsAccepted(relationship) ||
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

        const from = phantomShortKey(msg.from);
        const ent = from ? phantomSysPeers.get(from) : null;
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

        if (!phantomRememberRtcSignal(msg, iceVia)) {
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

      // Use the shared relationship fallback path.
      // Handles veil_invite / veil_accept / veil_deny / sys_block / sys_unblock / sys_unblock_ack.
      if (phantomHandleInboundRelationshipControl(msg, "bootstrap_ws")) {
        return;
      }

      if (msg && msg.type === "presence" && msg.from) {
        phantomRememberPresencePeer(msg, "bootstrap_ws");
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
        closeDetail: "phantom_ws_error",
        closeClean: false,
        closeClass: "local_ws_error_close",
        lastErr: "ws_error",
        snapshot: "ws_error",
        localCloseCode: 4000,
        localCloseReason: "phantom_ws_error",
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
      } else if (closeCode === 4000 && closeDetail === "phantom_ws_error") {
        closeClass = "local_ws_error_close";
      } else if (
        closeCode === 4001 &&
        closeDetail === "phantom_connect_deadline"
      ) {
        closeClass = "connect_deadline";
      } else if (closeCode === 4002 && closeDetail === "phantom_ready_deadline") {
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

  window.__phantom_core_listener_ready = true;
  post("CORE_LISTENER_READY", { ts: now() });

  let __phantomExitSent = false;
  const __phantomGoneNotified = new Set();
  // Membership exit has its own idempotence latch. The committed authority
  // consequence is the only owner allowed to project room-exit teardown.
  const __phantomMembershipExitApplied = new Map(); // hKey -> { bootId, epoch, at, reason }

  function phantomNotifyPeerGone(hKey, sys = "ROOM_EXITED", at = Date.now()) {
    const key = String(hKey || "")
      .slice(0, 16)
      .toLowerCase();

    if (!key) return false;

    const membershipExit = String(sys || "ROOM_EXITED") !== "CHAT_ENDED";

    if (membershipExit) {
      if (__phantomMembershipExitApplied.has(key)) return false;
      const terminal = phantomPendingTerminalExits.get(key) || null;
      __phantomMembershipExitApplied.set(key, {
        bootId:
          String(terminal?.bootId || phantomRemoteBootIds.get(key) || "") || null,
        epoch: Number(
          terminal?.epoch ||
            phantomSysAuthority.epoch ||
            concierge.sysconEpoch ||
            0,
        ),
        at: Number(at || Date.now()),
        reason: String(sys || "ROOM_EXITED"),
      });
      __phantomGoneNotified.add(key);
    } else {
      if (__phantomGoneNotified.has(key)) return false;
      __phantomGoneNotified.add(key);
    }

    // Relationship/chat teardown may already have run. Committed membership
    // removal still owns one final, idempotent consequence projection.
    phantomRelationshipEnd(key, String(sys || "peer_gone"), at, true);
    phantomCancelReconnect(key);
    const reconnectDeadline = phantomChatReconnectDeadlines.get(key);
    if (reconnectDeadline) clearTimeout(reconnectDeadline);
    phantomChatReconnectDeadlines.delete(key);

    const ent = phantomChatPeers.get(key);

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
    phantomPendingRelationship.delete(key);
    phantomPendingSysRelay.delete(key);
    phantomPendingDm.delete(key);
    phantomPendingIncomingFileOffers.delete(key);
    reconnectingPeers.delete(key);

    phantomRelationshipRevoked.add(key);
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
        phantomTeardownSysPeer(key, "sys_room_exited");
      }
    } catch {}

    if (membershipExit) {
      try {
        phantomPresenceBook.delete(key);
        phantomPendingRelationship.delete(key);
        phantomPendingRtcIce.delete(key);
        phantomSysAdmissionLastSent.delete(key);
        phantomSysPreflightUnstableRetries.delete(key);
        phantomSysDialing.delete(key);
      } catch {}

      if (
        phantomShortKey(concierge.sysPeer) === key ||
        phantomShortKey(concierge.sysReadyPeer) === key ||
        phantomShortKey(concierge.sysAttempt?.target) === key
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

      if (phantomShortKey(concierge.syscon1) === key) concierge.syscon1 = null;
      if (phantomShortKey(concierge.syscon2) === key) concierge.syscon2 = null;

      phantomRefreshSysCandidates(
        phantomLatchedSysRosterList(),
        "peer_gone_committed_projection",
      );
      phantomRefreshSysReadiness("peer_gone_dead_session");
      phantomEmitPresenceRoster("peer_gone_dead_session");
    }

    return true;
  }

  function phantomCancelPendingVeilInvites(reason = "leaving") {
    try {
      for (const [hKey, relationship] of Array.from(
        phantomRelationships.entries(),
      )) {
        const st = String(relationship?.state || VEIL.OUTSIDE);
        if (st !== VEIL.INVITE_SENT && st !== VEIL.INVITE_RECEIVED) continue;

        phantomSendControl(
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

  function phantomDeclinePendingIncomingFileOffers(
    reason = "session_kill",
    at = Date.now(),
  ) {
    try {
      for (const [to, offers] of Array.from(
        phantomPendingIncomingFileOffers.entries(),
      )) {
        const key = phantomShortKey(to);
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

        phantomPendingIncomingFileOffers.delete(key);
      }
    } catch {}
  }

  function phantomSurrenderRoomSession(txId = null) {
    if (__phantomExitSent) {
      post("PHANTOM_SESSION_SUSPENDED", {
        txId,
        room: videoId || null,
        alreadySuspended: true,
        at: Date.now(),
      });
      return;
    }

    __phantomExitSent = true;
    phantomSessionKilling = true;
    enabled = false;

    const retiredRoom = videoId || null;
    const at = Date.now();

    phantomDeclinePendingIncomingFileOffers("session_suspend", at);
    phantomSendTerminalExitCommand("session_suspend");

    phantomRelationships.clear();
    phantomRelationshipRevoked.clear();
    phantomPendingRelationship.clear();
    phantomPendingSysRelay.clear();
    phantomPendingDm.clear();
    phantomSeenControlCids.clear();

    rtcTeardownAll("session_suspend");
    wsClose("session_suspend");

    phantomRetireRoomAuthority("session_suspend");

    bootOnce = false;

    post("PHANTOM_SESSION_SUSPENDED", {
      txId,
      room: retiredRoom,
      at,
    });
  }

  function phantomSendRoomExitedAndTeardown(finalDeparture = true) {
    if (__phantomExitSent) return;
    __phantomExitSent = true;

    phantomSessionKilling = true;
    enabled = false;

    const at = Date.now();

    const exitReason = finalDeparture ? "session_kill" : "document_detach";

    phantomCancelPendingVeilInvites(exitReason);
    phantomDeclinePendingIncomingFileOffers(exitReason, at);
    if (finalDeparture) {
      phantomSendTerminalExitCommand(exitReason);
    } else {
      phantomSendDocumentDetachCommand(exitReason);
    }

    phantomRelationships.clear();
    phantomRelationshipRevoked.clear();
    phantomPendingRelationship.clear();
    phantomPendingSysRelay.clear();
    phantomPendingDm.clear();
    phantomPendingRtcIce.clear();
    phantomSeenControlCids.clear();
    phantomSeenRtcSignals.clear();

    rtcTeardownAll(exitReason);
    wsClose(exitReason);

    phantomRetireRoomAuthority(exitReason);

    bootOnce = false;

    wsDoorwayRecovering = false;
    wsDoorwayBirthFailures = 0;
    wsDoorwayRecoveryExhausted = false;

    emitSnapshot(finalDeparture ? "session_killed" : "session_detached");
  }

  function phantomRetireRoomAuthority(reason = "room_retired") {
    lastRoster = new Set();
    phantomPresenceBook.clear();
    phantomRemoteBootIds.clear();
    phantomRemoteEndpointIds.clear();
    phantomDoorwayAdmittedPeers.clear();
    sdkBootstrapTarget = null;
    sdkBootstrapUnanswered.clear();
    sdkDeferredOffers.clear('room_closed');
    sdkDeferredAdmissions.clear();
    networkFirstSignaling.clear();signalingBridge.clear();signalingPeerKeys.clear();
    phantomPendingTerminalExits.clear();
    for (const rec of phantomTemporarySysTransactions.values()) {
      try {
        if (rec?.deadlineTimer) clearTimeout(rec.deadlineTimer);
      } catch {}
    }
    phantomTemporarySysTransactions.clear();
    phantomAuthorityGossipHints.clear();
    phantomGossipRecovery = {
      source: null,
      targets: [],
      index: 0,
      epoch: 0,
      rosterSeq: 0,
      at: 0,
    };
    for (const rec of phantomSysReplacementWindows.values()) {
      try {
        clearTimeout(rec?.timer);
      } catch {}
    }
    phantomSysReplacementWindows.clear();
    reconnectingPeers.clear();
    __phantomGoneNotified.clear();
    __phantomMembershipExitApplied.clear();

    concierge = JSON.parse(JSON.stringify(PHANTOM_EMPTY_CONCIERGE));

    Object.assign(phantomSysAuthority, {
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

    Object.assign(phantomPeerSysCarrier, {
      epoch: 0,
      target: null,
      primaryRecoveryUsed: false,
      authorityRefreshPending: false,
    });

    phantomRosterAudit = {
      source: String(reason || "room_retired"),
      count: 0,
      added: "none",
      removed: "none",
      pruneReason: "room_retired",
      at: now(),
    };

    videoId = null;
  }

  function phantomLifecycleCommandMatchesCurrentRoom(payload = {}) {
    const commandRoom = String(payload?.room || payload?.videoId || "").trim();

    const currentRoom = String(videoId || "").trim();

    // Preserve compatibility for non-room-tagged commands.
    if (!commandRoom || !currentRoom) return true;

    return commandRoom === currentRoom;
  }

  // ---- Inbound CORE_* ----
  window.addEventListener("message", (ev) => {
    const d = ev?.data;
    if (!d || d.phantom !== 1) return;

    wsObserveDoorwayDeadline("core_message");

    // Debug ping
    if (d.type === "PING") {
      post("PONG", { t: now() });
      return;
    }

    if (d.type === "PHANTOM_SESSION_SUSPEND") {
      if (!phantomLifecycleCommandMatchesCurrentRoom(d.payload)) {
        emitSnapshot("stale_room_session_suspend_ignored");
        return;
      }

      phantomSurrenderRoomSession(d.payload?.txId || null);
      return;
    }

    if (d.type === "PHANTOM_SESSION_RESET") {
      if (!phantomLifecycleCommandMatchesCurrentRoom(d.payload)) {
        emitSnapshot("stale_room_session_reset_ignored");
        return;
      }

      enabled = false;

      wsClose("session_reset");
      rtcTeardownAll("session_reset");

      phantomRelationships.clear();
      phantomRelationshipRevoked.clear();
      phantomPendingRelationship.clear();
      phantomPendingSysRelay.clear();
      phantomPendingDm.clear();

      phantomRetireRoomAuthority("session_reset");

      bootOnce = false;
      __phantomExitSent = false;
      phantomSessionKilling = false;

      wsDoorwayRecovering = false;
      wsDoorwayBirthFailures = 0;
      wsDoorwayRecoveryExhausted = false;

      emitSnapshot("session_reset");
      return;
    }

    if (d.type === "PHANTOM_SESSION_DETACH") {
      if (!phantomLifecycleCommandMatchesCurrentRoom(d.payload)) {
        emitSnapshot("stale_room_session_detach_ignored");
        return;
      }

      phantomSendRoomExitedAndTeardown(false);
      return;
    }

    if (d.type === "PHANTOM_SESSION_KILL") {
      if (!phantomLifecycleCommandMatchesCurrentRoom(d.payload)) {
        emitSnapshot("stale_room_session_kill_ignored");
        return;
      }

      phantomSendRoomExitedAndTeardown(true);
      return;
    }

    if(d.type==='PHANTOM_ADMISSION_REFRESH') {
      const self=phantomShortKey(selfHKey);
      const peers=new Set([...lastRoster,...phantomChatPeers.keys(),...phantomSysPeers.keys()]);
      for(const key of peers)if(key!==self&&!host.canAdmitPeer(key)) {
        // Trusted local checkpoint change, not an unverified remote removal request.
        phantomTerminalExitCommand({member:key,room:videoId,reason:'membership_revoked'},'membership_revoked');
        phantomRetireRemoteIncarnationTransport(key,'membership_revoked');
        post('PHANTOM_MEMBERSHIP_REVOKED',{peerId:key});
      }
      emitSnapshot('membership_admission_refreshed');
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

      for (const [hKey, ent] of phantomChatPeers.entries()) {
        const key = phantomShortKey(hKey);
        if (!key || !ent) continue;

        const chatReady =
          ent.dc?.readyState === "open" &&
          ent.chatState === "ready" &&
          ent.chatProof === "chat_hello" &&
          ent.chatReadyAt > 0 &&
          phantomCanUseChat(key);

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
        !phantomCommittedMember(to) ||
        phantomPendingTerminalExits.has(to) ||
        __phantomMembershipExitApplied.has(to) ||
        phantomIsBlocked(to) ||
        phantomIsBlockedBy(to)
      ) {
        emitSnapshot("local_invite_blocked_nonmember_or_terminal");
        return;
      }

      const state = getState(to);
      if (state !== VEIL.OUTSIDE && state !== VEIL.ESTABLISHED) return;

      if (state === VEIL.ESTABLISHED) {
        return;
      }

      phantomRelationshipRevoked.delete(to);
      __phantomGoneNotified.delete(to);

      const invite = phantomPrepareRelationshipControl(
        {
          type: "veil_invite",
          from: selfHKey,
          to,
          videoId,
        },
        "veil_invite",
      );

      const relationship = phantomRelationshipBegin(to, VEIL.INVITE_SENT, {
        cycleId: invite.cid,
        inviteCid: invite.cid,
        origin: "local_invite",
      });
      invite.cycleId = relationship.cycleId;
      setState(to, VEIL.INVITE_SENT, true);
      phantomSendControl(invite, "veil_invite");

      return;
    }

    if (d.type === "VEIL_ACCEPT") {
      const from = String(d.payload?.from || "").slice(0, 16);
      if (!from) return;
      if (phantomIsBlocked(from) || phantomIsBlockedBy(from)) return;

      const relationship = phantomRelationshipGet(from);
      if (
        relationship.state !== VEIL.INVITE_RECEIVED ||
        relationship.endedAt ||
        !relationship.cycleId
      )
        return;

      phantomRelationshipRevoked.delete(from);
      relationship.state = VEIL.INSIDE;
      relationship.updatedAt = now();
      relationship.endedAt = 0;
      relationship.endReason = "none";
      phantomRelationships.set(from, relationship);
      setState(from, VEIL.INSIDE);

      phantomSendControl(
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

      phantomRequestChatPipe(from, "relationship_accept");
      return;
    }

    if (d.type === "VEIL_DENY") {
      const h = String(d.payload?.hKey || "").slice(0, 16);
      if (!h) return;

      const relationship = phantomRelationshipGet(h);
      if (getState(h) !== VEIL.INSIDE) {
        phantomRelationshipEnd(h, "local_deny", now(), false);
        setState(h, VEIL.OUTSIDE, true);
        try {
          rtcTeardown(h, "relationship_denied_local");
        } catch {}
      }

      phantomSendControl(
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

      const relationship = phantomRelationshipGet(h);
      if (getState(h) !== VEIL.INSIDE) {
        phantomRelationshipEnd(h, "local_deny", now(), false);
        setState(h, VEIL.OUTSIDE, true);
        try {
          rtcTeardown(h, "relationship_denied_local");
        } catch {}
      }

      phantomSendControl(
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
      phantomRequestChatPipe(hKey, "user_open_dm");
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
        phantomSendControl(
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

      phantomBlockedPeers.add(to);
      phantomBlockedByPeers.delete(to);
      phantomRelationshipEnd(to, "blocked_local", now(), true);

      phantomCancelReconnect(to);
      loungeWaiters.delete(to);
      phantomPendingRelationship.delete(to);
      phantomPendingDm.delete(to);

      phantomPendingRtcIce.delete(to);

      setState(to, VEIL.OUTSIDE, true);
      rtcTeardown(to, "blocked_local");
      post("BLOCKED_LOCAL", { to, at: Date.now() });
      return;
    }

    if (d.type === "UNBLOCK_PEER") {
      const to = String(d.payload?.to || "").slice(0, 16);
      if (!to) return;

      phantomBlockedPeers.delete(to);
      phantomBlockedByPeers.delete(to);
      phantomRelationshipRevoked.delete(to);
      if (
        phantomCommittedMember(to) &&
        !phantomPendingTerminalExits.has(to) &&
        !__phantomMembershipExitApplied.has(to)
      ) {
        __phantomGoneNotified.delete(to);
      }
      phantomRelationshipEnd(to, "unblocked_local", now(), false);
      setState(to, VEIL.OUTSIDE, true);

      phantomSendControl(
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
        phantomSendControl(
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

        if (!phantomCanUseChat(to)) {
          postLocal("FILE_BIN_CHUNK_SENT", {
            ok: false,
            to,
            file,
            error: "Chat not allowed",
          });
          return;
        }

        if (!to || !file?.id || !phantomIsArrayBuffer(bytes)) {
          postLocal("FILE_BIN_CHUNK_SENT", {
            ok: false,
            to,
            file,
            error: "Invalid binary chunk",
          });
          return;
        }

        const channel = phantomChatPeers.get(phantomShortKey(to))?.dc;
        const bufferWaitStartedAt=performance.now();
        const drained = await rtcWaitDCDrain(channel,bytes.byteLength);
        const bufferWaitMs=performance.now()-bufferWaitStartedAt;
        if (!drained || channel !== phantomChatPeers.get(phantomShortKey(to))?.dc || !phantomCanUseChat(to) || host.canSendFile?.(file) === false) {
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
          bufferWaitMs,
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
      const to = phantomShortKey(d.payload?.to);
      const active = d.payload?.active === true;

      if (!to) return;

      const relationship = phantomRelationshipGet(to);

      if (
        phantomIsBlocked(to) ||
        phantomIsBlockedBy(to) ||
        !phantomCommittedMember(to) ||
        !phantomRelationshipIsAccepted(relationship) ||
        !relationship.cycleId
      ) {
        return;
      }

      phantomSendControl(
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
      const base = phantomLocalCommittedAuthority();
      const self = phantomShortKey(selfHKey);
      const one = phantomShortKey(base?.syscon1);
      const two = phantomShortKey(base?.syscon2);
      // Use only the historical committed mirror, never an untrusted doorway
      // roster or distribution table. Existing SYS proof/admission rules apply.
      if (String(concierge.localRoleCandidate || 'peer') !== 'peer' || !base ||
          !one || !two || two === self || one === self || phantomHasOpenSysPipe(one) ||
          !phantomCommittedMember(self) || phantomPendingTerminalExits.has(two)) return;
      let retryCount = 0;
      if(phantomGossipRecovery.targets.length){
        // Leave active attempts and other gossip/election recovery untouched.
        // Retry only the same exhausted mirror route from this committed base.
        const r=phantomGossipRecovery;
        if(r.kind!=='committed_mirror'||r.targets.length!==1||r.targets[0]!==two||
            r.index<r.targets.length||r.epoch!==Number(base.epoch)||
            r.rosterSeq!==Number(base.rosterSeq)||now()<Number(r.nextRetryAt||0))return;
        retryCount=Number(r.retryCount||0);
      }
      phantomGossipRecovery = {source:self, targets:[two], index:0,kind:'committed_mirror',retryCount,nextRetryAt:0,
        epoch:Number(base.epoch), rosterSeq:Number(base.rosterSeq), at:now()};
      phantomScheduleSysAdmission('distribution_restore_committed_mirror');
      emitSnapshot('distribution_restore_committed_mirror');
      return;
    }

    // Distribution owns its protocol and transports outside the roster engine.
    // This command only carries bounded control frames over existing SYS links.
    if (d.type === 'PHANTOM_DIST_SEND') {
      const packet = {t:'PHANTOM_DIST', frame:d.payload?.frame};
      if (JSON.stringify(packet).length > 32768) return;
      const requested = d.payload?.to ? phantomShortKey(d.payload.to) : null;
      for (const [key, ent] of phantomSysPeers) {
        if ((requested && key !== requested) || !canUsePeer(key) || ent.sysdc?.readyState !== 'open' || phantomTemporarySysTransactions.has(key)) continue;
        try { ent.sysdc.send(JSON.stringify(packet)); } catch {}
      }
      return;
    }

    if (d.type === 'PHANTOM_ADMIN_SEND') {
      const packet = {t:'PHANTOM_ADMIN', record:d.payload?.record};
      if (JSON.stringify(packet).length > 4096) return;
      const requested = d.payload?.to ? phantomShortKey(d.payload.to) : null;
      const sent = new Set();
      for (const [key, ent] of phantomSysPeers) {
        if ((requested && key !== requested) || !canUsePeer(key) || ent.sysdc?.readyState !== 'open' || phantomTemporarySysTransactions.has(key)) continue;
        try { ent.sysdc.send(JSON.stringify(packet)); sent.add(key); } catch {}
      }
      for (const [key, ent] of phantomChatPeers) {
        if ((requested && key !== requested) || sent.has(key) || !phantomCanUseChat(key) || ent.dc?.readyState !== 'open') continue;
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
        const key = phantomShortKey(to);
        const offers = key ? phantomPendingIncomingFileOffers.get(key) : null;

        if (offers) {
          offers.delete(String(d.payload.file.id));
          if (!offers.size) phantomPendingIncomingFileOffers.delete(key);
        }
      }

      if (!phantomIsSysMessage(sys) && !phantomCanUseChat(to)) {
        CORE_DEBUG &&
          console.log("[phantom][CORE] SEND_DM blocked; chat not allowed →", to);

        concierge.lastErr = "dm_send_blocked_chat_not_allowed";
        emitSnapshot("dm_send_blocked_chat_not_allowed");
        return;
      }

      const chatEnt = phantomChatPeers.get(to);
      const chatDcState = String(chatEnt?.dc?.readyState || "missing");

      let ok = false;

      if (
        chatDcState === "open" &&
        chatEnt?.chatState === "ready" &&
        chatEnt?.chatProof === "chat_hello" &&
        chatEnt?.chatReadyAt > 0 &&
        phantomCanUseChat(to)
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
          let q = phantomPendingDm.get(to);
          if (!q) {
            q = [];
            phantomPendingDm.set(to, q);
          }

          q.push({
            text,
            sys,
            file: d.payload?.file || null,
            at: Date.now(),
          });

          if (q.length > PHANTOM_DM_PENDING_MAX) {
            q.splice(0, q.length - PHANTOM_DM_PENDING_MAX);
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

      CORE_DEBUG && console.log("[phantom][CORE] SEND_DM →", to, "ok=", ok);
      if (ok) {
        post("messageEcho", {
          to,
          text,
          sys,
          file: d.payload?.file || null,
          at: Date.now(),
        });

        if (sys === "CHAT_ENDED") {
          __phantomGoneNotified.add(to);
          phantomCancelReconnect(to);
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
        phantomSendRoomExitedAndTeardown(true);
      }

      videoId = nextRoom || null;

      try {
        const saved = JSON.parse(
          sessionStorage.getItem("phantom_refresh_relationships_v1") || "null",
        );

        const sameRoom =
          !!videoId && !!saved?.room && String(saved.room) === String(videoId);

        if (sameRoom && Array.isArray(saved?.peers)) {
          phantomPendingRefreshRelationships.clear();
          phantomPendingRefreshRoom = String(videoId || "");

          const savedRecords = new Map(
            (Array.isArray(saved.relationships) ? saved.relationships : [])
              .map((row) => [phantomShortKey(row?.hKey), row])
              .filter(([key]) => !!key),
          );

          for (const peer of saved.peers) {
            const key = phantomShortKey(peer);
            if (!key) continue;

            const row = savedRecords.get(key) || {};
            const cycleId = String(row.cycleId || "");
            if (!cycleId) continue;

            const relationship = phantomRelationshipGet(key);
            relationship.state = VEIL.OUTSIDE;
            relationship.cycleId = cycleId;
            relationship.inviteCid = String(row.inviteCid || "") || null;
            relationship.localBootId = phantomBootId;
            relationship.remoteBootId =
              String(row.remoteBootId || phantomRemoteBootIds.get(key) || "") ||
              null;
            relationship.membershipEpoch = Number(row.membershipEpoch || 0);
            relationship.createdAt = now();
            relationship.updatedAt = now();
            relationship.endedAt = 0;
            relationship.endReason = "continuity_pending";
            phantomRelationships.set(key, relationship);
            phantomPendingRefreshRelationships.add(key);
          }

          emitSnapshot("relationship_continuity_staged_for_authority");
        } else {
          phantomPendingRefreshRelationships.clear();
          phantomPendingRefreshRoom = "";
          sessionStorage.removeItem("phantom_refresh_relationships_v1");
        }
      } catch {
        try {
          sessionStorage.removeItem("phantom_refresh_relationships_v1");
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
            phantomBootId = crypto.randomUUID();
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
        // Publish identity and network context to SDK observers.
        post("IDENTITY_UPDATE", {
          handle: selfHandle,
          hex: selfHex,
          hKey: selfHKey,

          // Pin identity to the current origin and network context.
          videoId,
          origin,

          keystore: ksSerialize(),
        });

        origin = computeOrigin();
        if (!videoId) videoId = resolveVideoId();
        applyTOFU();
        emitSnapshot("CORE_SET_SELF");

        // Identity initialization does not complete network startup.
        // CORE_BOOTSTRAP owns the boot latch and NETWORK_READY event.
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
      __phantomExitSent = false;
      phantomSessionKilling = false;

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
        console.log("[phantom][CORE] CORE_BOOTSTRAP HIT", {
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
          console.debug("[phantom][CORE] CORE_BOOTSTRAP waiting for identity");
        return;
      }

      phantomBootId = crypto.randomUUID();
      bootOnce = true;

      const url =
        d.payload?.url ||
        `${DEFAULT_BOOTSTRAP_WS}discover?v=${encodeURIComponent(videoId)}`;

      emitSnapshot("CORE_BOOTSTRAP");

      wsOpenBootstrap(url);
      return;
    }

    // ======================================================
    // Peer WebRTC signaling
    // ======================================================

    // RTC_DIAL disabled; TRY_DIAL remains the only public dial request path

    // Apply incoming signaling to the peer transport.
    // d.type: "RTC_SIGNAL" payload: { from, kind, data, room, to }
    if (d.type === "RTC_SIGNAL") {
      (async () => {
        try {
          const p = d.payload || {};
          const from = String(p.from || "");
          const kind = String(p.kind || "");
          const data = p.data;

          if (!from || !kind) return;
          if (phantomSessionKilling) return;

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

  ksLoad();
  CORE_DEBUG && console.log(TAG, "boot", location.href);
  emitSnapshot("core_boot");

  return {
    command(type, payload = {}) {
      window.dispatchEvent({ type: "message", data: { phantom: 1, type, payload } });
    },
    snapshot() { return snapshot("sdk_read"); },
    peerConnections() {return phantomPeerConnections();},
    dispose() {
      networkFirstSignaling.clear();signalingBridge.clear();signalingPeerKeys.clear();
      phantomSendRoomExitedAndTeardown(true);
      wsClose("sdk_dispose");
      rtcTeardownAll("sdk_dispose");
    }
  };
}
