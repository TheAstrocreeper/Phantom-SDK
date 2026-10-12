/** Stable transport labels and migration support for existing installations. */
export const COMPATIBILITY_WIRE = Object.freeze({
  chatChannel: 'six7',
  systemChannel: 'six7-sys',
});

export function isConciergeProtocol(protocol) {
  return protocol === 'phantom-concierge-v1' || protocol === 'six7-concierge-v1';
}

const STORAGE_MIGRATIONS = Object.freeze([
  ['six7_tofu_origin', 'phantom_tofu_origin'],
  ['six7_tofu_hkey', 'phantom_tofu_hkey'],
  ['six7_tofu_firstSeenAt', 'phantom_tofu_firstSeenAt'],
  ['six7_keystore_v1', 'phantom_keystore_v1'],
  ['six7_refresh_relationships_v1', 'phantom_refresh_relationships_v1'],
]);

/** Copy existing records without overwriting new state or deleting old state. */
export function migrateStorage(storage) {
  const marker = 'phantom_storage_migration_v1';
  try { if (storage.getItem(marker) === 'complete') return; } catch { return; }
  let complete = true;
  for (const [previous, current] of STORAGE_MIGRATIONS) {
    try {
      if (storage.getItem(current) !== null) continue;
      const value = storage.getItem(previous);
      if (value !== null) storage.setItem(current, value);
    } catch { complete = false; }
  }
  if (complete) try { storage.setItem(marker, 'complete'); } catch {}
}

const COMMAND_ALIASES = Object.freeze({
  SIX7_SESSION_SUSPEND: 'PHANTOM_SESSION_SUSPEND',
  SIX7_SESSION_RESET: 'PHANTOM_SESSION_RESET',
  SIX7_SESSION_DETACH: 'PHANTOM_SESSION_DETACH',
  SIX7_SESSION_KILL: 'PHANTOM_SESSION_KILL',
});

export function normalizeCommand(type) {
  return Object.hasOwn(COMMAND_ALIASES, type) ? COMMAND_ALIASES[type] : type;
}
