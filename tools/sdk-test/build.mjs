import { build } from "../swarm/node_modules/esbuild/lib/main.js";
import { readFileSync, readdirSync, mkdirSync, copyFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
const root = new URL("../../", import.meta.url), out = new URL("./dist/", import.meta.url), hashes = {};
function scan(dir) {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = new URL(e.name + (e.isDirectory() ? "/" : ""), dir);
    if (e.isDirectory() && !["dist","reports"].includes(e.name)) scan(p);
    else if (/\.(js|mjs|json|html)$/.test(e.name)) hashes[p.pathname.slice(root.pathname.length)] = createHash("sha256").update(readFileSync(p)).digest("hex");
  }
}
scan(new URL("packages/", root));
scan(new URL("tools/sdk-test/", root));
const metadata = { revision: execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(), dirty: !!execFileSync("git", ["status", "--porcelain"], { cwd: root, encoding: "utf8" }).trim(), sourceHashes: hashes };
mkdirSync(out, { recursive: true });
await build({ entryPoints: [new URL("./app.js", import.meta.url).pathname], bundle: true, format: "esm", target: "es2022", outfile: new URL("app.js", out).pathname, define: { __SDK_TEST_BUILD__: JSON.stringify(metadata) } });
copyFileSync(new URL("./index.html", import.meta.url), new URL("index.html", out));
writeFileSync(new URL("build.json", out), JSON.stringify(metadata, null, 2));

copyFileSync(new URL("THIRD_PARTY_NOTICES.md",root),new URL("THIRD_PARTY_NOTICES.md",out));
copyFileSync(new URL("LICENSE",root),new URL("LICENSE",out));
