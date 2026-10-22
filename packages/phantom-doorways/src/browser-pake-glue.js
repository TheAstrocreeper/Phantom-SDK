/** Adapt the pinned wasm-bindgen loader for native ESM and a CSP without JS eval. */
export function browserPakeGlue(original) {
  const marker="import * as wasm from './index_bg.wasm';";
  const lookup='var ret = new Function(getStringFromWasm0(arg0, arg1));';
  if(!original.startsWith(marker)||original.split(lookup).length!==2)throw Error('Unexpected SPAKE2 glue format; review the loader before updating.');
  return original.replace(marker,'let wasm; export function initialize(exports){wasm=exports;}').replace(lookup,
    `// The pinned WASM requests only Function("return this") to find the global object.
    // Keep the call/heap ABI while refusing every other dynamic function body.
    if (getStringFromWasm0(arg0, arg1) !== 'return this') throw Error('Unexpected SPAKE2 global lookup');
    var ret = function () { return globalThis; };`);
}
