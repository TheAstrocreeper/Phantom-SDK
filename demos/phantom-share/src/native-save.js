/** Request/acknowledgement streaming avoids passing whole files through the bridge. */
export function createNativeSaver(bridge, { timeoutMs = 120000 } = {}) {
  const pending = new Map();
  let busy = false, generation = 0, sessionToken = null;
  const cancelled = () => Object.assign(Error("Save cancelled"), {code:"SAVE_CANCELLED"});
  bridge.onmessage = ({ data }) => {
    let message;
    try {
      message = JSON.parse(data);
    } catch {
      return;
    }
    const job = pending.get(message.id);
    if (!job) return;
    pending.delete(message.id);
    clearTimeout(job.timer);
    message.ok
      ? job.resolve(message.result)
      : job.reject(Error(message.error || "Save failed"));
  };
  function request(action, data = {}) {
    return new Promise((resolve, reject) => {
      const id = crypto.randomUUID(),
        timer = setTimeout(() => {
          pending.delete(id);
          reject(Error("Save timed out"));
        }, timeoutMs);
      pending.set(id, { resolve, reject, timer });
      try {
        bridge.postMessage(JSON.stringify({ id, action, ...data }));
      } catch (error) {
        clearTimeout(timer);
        pending.delete(id);
        reject(error);
      }
    });
  }
  function cancel() {
    generation++;
    for (const job of pending.values()) {
      clearTimeout(job.timer);
      job.reject(
        cancelled(),
      );
    }
    pending.clear();
    try {
      bridge.postMessage(
        JSON.stringify({ id: crypto.randomUUID(), action: "cancel" }),
      );
    } catch {}
  }
  return {
    cancel,
    ownsSession: token => typeof token === "string" && token === sessionToken,
    prepareConnection: () => request("network"),
    async startSession() {
      const token = crypto.randomUUID();
      sessionToken = token;
      let closed = false, active = false, closing;
      const update = () => request("transfer", {token, active}).catch(() => {});
      const close = () => {
        if (closed) return closing;
        closed = true;
        if (sessionToken === token) sessionToken = null;
        // This token can only end its own session, even after a delayed reply.
        closing = request("endSession", {token}).catch(() => {});
        return closing;
      };
      try { await request("startSession", {token}); }
      catch (error) { close(); throw error; }
      return {
        close,
        setTransferActive(value) {
          if (closed || active === !!value) return;
          active = !!value; update();
        },
      };
    },
    async save(blob, name) {
      if (busy) throw Error("Finish the current save first");
      busy = true;
      const current = generation;
      const check = () => {if(current!==generation)throw cancelled();};
      try {
        const result = await request("begin", {
          name,
          type: blob.type || "application/octet-stream",
          size: blob.size,
        });
        check();
        if (!result) return false;
        let index = 0;
        for (let offset = 0; offset < blob.size; offset += 65536) {
          const chunk = new Uint8Array(
            await blob.slice(offset, offset + 65536).arrayBuffer(),
          );
          check();
          // A 64 KiB chunk exceeds some engines' argument-count limits when spread.
          let binary = "";
          for (let i = 0; i < chunk.length; i += 8192)
            binary += String.fromCharCode(...chunk.subarray(i, i + 8192));
          await request("chunk", {
            token: result.token,
            index: index++,
            data: btoa(binary),
          });
        }
        check();
        await request("finish", { token: result.token });
        return true;
      } catch (error) {
        cancel();
        throw error;
      } finally {
        busy = false;
      }
    },
  };
}
let nativeSaver;
export function cancelNativeSave() {
  nativeSaver?.cancel();
}
export async function saveBlob(blob, name) {
  if (globalThis.PhantomNative?.postMessage) {
    nativeSaver ??= createNativeSaver(globalThis.PhantomNative);
    return nativeSaver.save(blob, name);
  }
  const url = URL.createObjectURL(blob),
    link = document.createElement("a");
  link.href = url;
  link.download = name;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 60000);
  return true;
}

export async function prepareNativeConnection(){
 if(!globalThis.PhantomNative?.postMessage)return;
 nativeSaver??=createNativeSaver(globalThis.PhantomNative);
 await nativeSaver.prepareConnection();
}

export async function startNativeSession() {
  if (!globalThis.PhantomNative?.postMessage) return null;
  nativeSaver ??= createNativeSaver(globalThis.PhantomNative);
  return nativeSaver.startSession();
}

export function ownsNativeSession(token) { return nativeSaver?.ownsSession(token) === true; }
