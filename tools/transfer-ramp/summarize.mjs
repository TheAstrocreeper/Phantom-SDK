import { readFile } from "node:fs/promises";
const files = process.argv.slice(2);
if (!files.length) {
  console.error(
    "Usage: node tools/transfer-ramp/summarize.mjs report.json [report2.json ...]",
  );
  process.exitCode = 1;
}
for (const file of files) {
  try {
    const report = JSON.parse(await readFile(file, "utf8"));
    if (report.tool !== "phantom-ramp-v1" || !Array.isArray(report.directions))
      throw Error("Not a ramp report");
    console.log(
      JSON.stringify({
        file,
        startedAt: report.startedAt,
        status: report.status || "complete",
        conditions: report.testConditions || "",
        path: report.path,
        remotePath: report.remotePath,
        directions: report.directions.map((d) => ({
          direction: d.direction,
          baselineRttMs: d.baselineRttMs,
          recommendation: d.recommendation,
          stages: d.rows.map((r) => ({
            windowMiB: r.windowBytes / 1048576,
            receivedMBps: r.receiverBytesPerSecond / 1e6,
            rttMs: r.rttMedianMs,
            peakQueuedMiB: r.peakBufferedBytes / 1048576,
            peakUnackedMiB: r.peakOutstandingBytes / 1048576,
            drainMs: Math.max(0, r.completionMs - r.activeMs),
            byteCapped: r.limitedByByteBudget,
          })),
        })),
      }),
    );
  } catch (error) {
    console.error(file + ": " + error.message);
    process.exitCode = 1;
  }
}
