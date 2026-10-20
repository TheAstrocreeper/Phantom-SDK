// Lazy local assets; no remote loader or lookup service.
let loading;
export function loadBrowserPake() {
  return (loading ??= (async () => {
    try {
      const glue = await import(
        new URL("./spake2-glue.js", import.meta.url).href
      );
      const response = await fetch(new URL("./spake2.wasm", import.meta.url));
      if (!response.ok) throw Error("Pairing module could not load");
      const { instance } = await WebAssembly.instantiate(
        await response.arrayBuffer(),
        { "./index_bg.js": glue },
      );
      glue.initialize(instance.exports);
      return glue;
    } catch (error) {
      loading = null;
      throw error;
    }
  })());
}
