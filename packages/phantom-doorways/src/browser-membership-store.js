/** IndexedDB optimistic transactions; async signing occurs outside the IDB transaction.
 * A revision check atomically commits only the state that was actually read.
 */
export function createBrowserMembershipStore({
  indexedDB = globalThis.indexedDB,
  name = "phantom-membership-v1",
} = {}) {
  if (!indexedDB)
    throw new Error(
      "Persistent browser storage is required for membership administration",
    );
  const opened = new Promise((resolve, reject) => {
    const request = indexedDB.open(name, 1);
    request.onupgradeneeded = () =>
      request.result.createObjectStore("authorities");
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
    request.onblocked = () =>
      reject(
        new Error("Close other Phantom tabs to upgrade membership storage"),
      );
  });
  async function read(db, scope) {
    return new Promise((resolve, reject) => {
      const tx = db.transaction("authorities", "readonly"),
        r = tx.objectStore("authorities").get(scope);
      r.onsuccess = () => resolve(r.result);
      r.onerror = () => reject(r.error);
    });
  }
  async function commit(db, scope, revision, state) {
    return new Promise((resolve, reject) => {
      const tx = db.transaction("authorities", "readwrite"),
        object = tx.objectStore("authorities"),
        r = object.get(scope);
      let accepted = false;
      r.onsuccess = () => {
        if ((r.result?.revision ?? 0) !== revision) return;
        object.put(
          { revision: revision + 1, state: structuredClone(state) },
          scope,
        );
        accepted = true;
      };
      tx.oncomplete = () => resolve(accepted);
      tx.onerror = () => reject(tx.error);
      tx.onabort = () =>
        reject(tx.error ?? new Error("Membership persistence aborted"));
    });
  }
  return {
    async transaction(scope, update) {
      const db = await opened;
      for (let attempt = 0; attempt < 32; attempt++) {
        const previous = await read(db, scope),
          { state, result } = await update(structuredClone(previous?.state));
        if (await commit(db, scope, previous?.revision ?? 0, state))
          return structuredClone(result);
      }
      throw new Error("Membership state changed too often; retry");
    },
  };
}
