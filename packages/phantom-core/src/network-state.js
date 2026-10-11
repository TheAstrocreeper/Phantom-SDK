/** Read-only projection of existing SIX7 state; never drives elections or retries. */
export function projectNetworkState(snapshot={}) {
 const c=snapshot.concierge??{},b=snapshot.bootstrap??{},t=c.membershipAudit?.topology??{};
 const admitted=!!snapshot.self?.hKey && String(t.committedKeys??'').split(',').includes(snapshot.self.hKey.slice(0,8));
 let state,reason,nextAction;
 if(admitted&&c.sysReady){state='admitted';reason='committed_membership_and_system_ready';nextAction='none';}
 else if(c.sysAttempt?.faultReason==='sys_birth_response_deadline_exhausted'||c.sysLastErr==='sys_birth_response_deadline_exhausted') {state='failed';reason='sys_birth_response_deadline_exhausted';nextAction='inspect_diagnostics';}
 else if(c.sysAttempt?.state==='faulted'||b.doorwayState==='failed') {state='recovering';reason=c.sysAttempt?.faultReason??b.doorwayFault??'transport_fault';nextAction='observe_core_recovery';}
 else if(admitted){state='recovering';reason=c.sysUnavailableReason??'system_unavailable';nextAction='wait_for_system_proof';}
 else if(['connecting','creating','offer_sent','open'].includes(c.sysAttempt?.state)||c.sysPipeState==='connecting'){state='connecting';reason=c.sysUnavailableReason??'waiting_for_system_proof';nextAction='wait_for_peer_response';}
 else if(b.doorwayState==='ready'){state='discovering';reason='waiting_for_admission';nextAction='wait_for_system_connection';}
 else {state='discovering';reason=b.doorwayReason??'waiting_for_doorway';nextAction='wait_for_doorway';}
 return {state,reason,nextAction,peerId:snapshot.self?.hKey??null,role:c.role??null,leader:c.syscon1??null,mirror:c.syscon2??null,
  doorway:{state:b.doorwayState??'unknown',reason:b.doorwayReason??null,fault:b.doorwayFault??null},
  attempt:{id:c.sysAttempt?.id??null,state:c.sysAttempt?.state??null,fault:c.sysAttempt?.faultReason??null},admitted};
}
