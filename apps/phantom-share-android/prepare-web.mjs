import { execFileSync } from "node:child_process";
import {
  mkdir,
  rm,
  readdir,
  copyFile,
  readFile,
  writeFile,
} from "node:fs/promises";
const project = new URL("./", import.meta.url),
  demo = new URL("../../demos/phantom-share/", project);
execFileSync("npm", ["run", "build"], { cwd: demo, stdio: "inherit" });
const assets = new URL("app/src/main/assets/share/", project);
await rm(assets, { recursive: true, force: true });
await mkdir(assets, { recursive: true });
for (const file of await readdir(new URL("dist/", demo)))
  await copyFile(new URL("dist/" + file, demo), new URL(file, assets));
// Public invitations must not contain the app's private asset origin.
const publicUrl = new URL(
  process.env.PHANTOM_PUBLIC_URL ?? "https://phantom-share.pages.dev/",
);
if (publicUrl.protocol !== "https:" || publicUrl.username || publicUrl.password)
  throw Error("PHANTOM_PUBLIC_URL must be a public HTTPS URL");
publicUrl.hash = "";
publicUrl.search = "";
let html = await readFile(new URL("index.html", assets), "utf8");
html = html.replace(
  /<link[^>]+(?:fonts\.googleapis\.com|fonts\.gstatic\.com)[^>]*>/g,
  "",
);
html = html.replace(
  "<head>",
  '<head>\n<meta name="phantom-public-url" content="' +
    publicUrl.href.replaceAll("&", "&amp;").replaceAll('"', "&quot;") +
    '">',
);
await writeFile(new URL("index.html", assets), html);
console.log("Bundled Share assets. Public invitation base: " + publicUrl.href);
