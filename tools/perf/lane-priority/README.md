# Lane priority scheduling model

This is an isolated event-driven model, not an SDK transfer implementation or wire protocol. It assigns one distinct range per available lane, gives queued control work to the first idle available lane, returns unfinished ranges to the queue when a lane closes, and fences stale completions by assignment generation. Queue bounds are explicit. It has no progress timer and does not infer a stall from a time delay; recovering a stalled-but-open lane needs a separately defined, reliable signal.

The ordering test drives the existing striped adapter and a scheduling model with an intentionally withheld first message. The adapter holds already-arrived later messages until that message arrives; the model can complete other assigned ranges. It demonstrates a scheduling difference, not bandwidth improvement. The model does not implement networking, hashing, authorization, retransmission deduplication, durable resume, compatibility, or handling loss of already-sent control messages. It cannot preempt bytes already handed to WebRTC. Do not ship this model as a transport.

Run: `node --test tools/perf/lane-priority/*.test.js tools/perf/delivery-native/striped-rtc.test.mjs`.
