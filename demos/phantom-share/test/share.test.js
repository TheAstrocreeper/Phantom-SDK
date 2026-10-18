import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { indexedDB } from "fake-indexeddb";
import { createBrowserMembershipStore } from "../../../packages/phantom-doorways/src/browser-membership-store.js";
import { selectRecipients } from "../src/session.js";
import { codeFromInput } from "../src/membership.js";

test("Direct restricts recipients, while Exclude omits them from the default set", () => {
  const peers = [{ id: "a" }, { id: "b" }, { id: "c" }];
  assert.deepEqual(selectRecipients(peers, new Map([["b", "exclude"]])), [
    "a",
    "c",
  ]);
  assert.deepEqual(
    selectRecipients(
      peers,
      new Map([
        ["b", "direct"],
        ["c", "exclude"],
      ]),
    ),
    ["b"],
  );
  assert.deepEqual(
    selectRecipients(peers, new Map(peers.map((p) => [p.id, "exclude"]))),
    [],
  );
});
test("code and fragment invitation route into the same pairing path", () => {
  const code = "P1-ABCD-EFGH-JKMN-PQRS-TVWX-YZ01-2345";
  assert.equal(codeFromInput(code), code);
  assert.equal(
    codeFromInput(
      "https://share.example/#" + new URLSearchParams({ pair: code }),
    ),
    code,
  );
  assert.throws(
    () => codeFromInput("phantom://v3/old"),
    /current Phantom Share/,
  );
});
test("IndexedDB store serializes competing authorities and survives adapter recreation", async () => {
  const name = "membership-test-" + crypto.randomUUID(),
    a = createBrowserMembershipStore({ indexedDB, name }),
    b = createBrowserMembershipStore({ indexedDB, name });
  const increment = (store) =>
    store.transaction("room", async (value) => {
      await new Promise((r) => setTimeout(r, 1));
      const state = { count: (value?.count ?? 0) + 1 };
      return { state, result: state.count };
    });
  await Promise.all(
    Array.from({ length: 12 }, (_, i) => increment(i % 2 ? a : b)),
  );
  const c = createBrowserMembershipStore({ indexedDB, name });
  assert.equal(
    await c.transaction("room", (state) => ({ state, result: state.count })),
    12,
  );
  await assert.rejects(
    a.transaction("room", (state) => {
      state.count = 0;
      throw Error("signing failed");
    }),
    /signing failed/,
  );
  assert.equal(
    await b.transaction("room", (state) => ({ state, result: state.count })),
    12,
  );
});
test("every static application DOM reference exists in the approved interface", async () => {
  const [source, html] = await Promise.all([
    readFile(new URL("../src/app.js", import.meta.url), "utf8"),
    readFile(new URL("../src/index.html", import.meta.url), "utf8"),
  ]);
  const ids = new Set([...html.matchAll(/\bid="([^"]+)"/g)].map((m) => m[1]));
  for (const [, id] of source.matchAll(/byId\("([^"]+)"\)/g))
    assert.ok(ids.has(id), id);
  assert.ok(ids.has("invite-code"));
  assert.ok(ids.has("invite-qr"));
  assert.ok(ids.has("invite-link"));
});
