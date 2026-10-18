import { build } from "esbuild";
import { mkdir, copyFile } from "node:fs/promises";
await mkdir(new URL("dist/", import.meta.url), { recursive: true });
await build({
  entryPoints: [new URL("src/app.js", import.meta.url).pathname],
  outfile: new URL("dist/app.js", import.meta.url).pathname,
  bundle: true,
  format: "esm",
  target: "es2022",
  minify: true,
});
for (const name of ["index.html", "style.css"])
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
