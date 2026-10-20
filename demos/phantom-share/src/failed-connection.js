/** Complete one failed admission's teardown before releasing its local owner.
 * The existing membership-release RPC gets a cancellation signal, so it cannot
 * fall back to another transport after its cleanup budget is exhausted.
 */
export async function disposeFailedAttempt({nativeSession, control, network, code, onEvent}) {
  const report = event => { try { onEvent?.(event); } catch {} };
  const close = async owner => {
    try { await owner?.close(); }
    catch (error) { report({type:'CONNECTION_CLEANUP_ERROR',payload:{message:error.message}}); }
  };
  await close(nativeSession);
  // A grant may exist even though RTC admission never completed.
  if (code && control?.releaseMembership) {
    let timer;
    const release = new AbortController();
    try {
      await Promise.race([control.releaseMembership({signal:release.signal}), new Promise((_, reject) => {
        timer = setTimeout(() => { release.abort(); reject(Error('Membership release timed out')); }, 2000);
      })]);
      report({type:'MEMBERSHIP_JOIN_RELEASE',payload:{status:'success'}});
    } catch {
      report({type:'MEMBERSHIP_JOIN_RELEASE',payload:{status:'failure'}});
    } finally { clearTimeout(timer); release.abort(); }
  }
  await close(network);
  await close(control);
}
