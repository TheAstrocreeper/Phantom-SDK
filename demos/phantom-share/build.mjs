import { build } from "esbuild";
import { mkdir, rm, copyFile, readFile, writeFile } from "node:fs/promises";
await rm(new URL("dist/", import.meta.url), {recursive:true,force:true});
await mkdir(new URL("dist/", import.meta.url), { recursive: true });
await build({
  entryPoints: [new URL("src/app.js", import.meta.url).pathname],
  outfile: new URL("dist/app.js", import.meta.url).pathname,
  bundle: true,
  format: "esm",
  target: "es2022",
  minify: true,
});
for (const name of ["index.html", "style.css", "layout.css", "theme.css", "phantom-mark.png", "jakarta-400.ttf", "jakarta-600.ttf", "jakarta-700.ttf", "JAKARTA-OFL.txt"])
  await copyFile(
    new URL("src/" + name, import.meta.url),
    new URL("dist/" + name, import.meta.url),
  );

await copyFile(
  new URL("README.md", import.meta.url),
  new URL("dist/README.md", import.meta.url),
);
for (const name of ["LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md"])
  await copyFile(
    new URL("../../" + name, import.meta.url),
    new URL("dist/" + name, import.meta.url),
  );

await copyFile(
  new URL("node_modules/jsqr/LICENSE", import.meta.url),
  new URL("dist/jsQR-LICENSE.txt", import.meta.url),
);

// wasm-bindgen's bundler-target glue needs explicit initialization in native ESM.
const pakeRoot = new URL(
  "../../node_modules/spake2-wasm/mjs/",
  import.meta.url,
);
const original = await readFile(new URL("index_bg.js", pakeRoot), "utf8");
const marker = "import * as wasm from './index_bg.wasm';";
if (!original.startsWith(marker)) throw Error("Unexpected SPAKE2 glue format");
await writeFile(
  new URL("dist/spake2-glue.js", import.meta.url),
  original.replace(
    marker,
    "let wasm; export function initialize(exports){wasm=exports;}",
  ),
);
await copyFile(
  new URL("index_bg.wasm", pakeRoot),
  new URL("dist/spake2.wasm", import.meta.url),
);
await copyFile(
  new URL("../LICENSE", pakeRoot),
  new URL("dist/SPAKE2-LICENSE.txt", import.meta.url),
);
