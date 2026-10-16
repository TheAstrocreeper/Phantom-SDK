// Only diagnostics are persisted. Invitations, RTC settings and identity seeds are not.
const LIMIT=350000;
export function createJournal(storage, key) {
  let previous=null, error=null;
  try { previous=JSON.parse(storage.getItem(key)||'null'); } catch(e) { error=String(e.message); }
  return {
    previous,
    get error(){return error;},
    save(value){try {const text=JSON.stringify(value);if(text.length>LIMIT)throw Error('Diagnostic journal exceeded 350,000 characters');storage.setItem(key,text);error=null;return true;}catch(e){error=String(e.message);return false;}},
    clear(){storage.removeItem(key);previous=null;},
  };
}
