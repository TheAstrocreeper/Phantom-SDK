// The join promise may reject. Capture its snapshots before a network handle
// exists, without retaining raw events or waiting for admission.
export function createStartupRecovery(diagnostics) {
  let state={};
  return {
    state(value){state=value;},
    snapshot(value){diagnostics.recovery(value,state);},
    reset(){state={};},
  };
}
