// Experiment: an ephemeral, single-use browser download fed by a MessagePort.
const sessions = new Map();
self.addEventListener('install', event => event.waitUntil(self.skipWaiting()));
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));
self.addEventListener('message', event => {
  if (event.data?.type !== 'register' || !event.ports[0]) return;
  const {id, size} = event.data, port = event.ports[0];
  let controller, waiting = [], ended = false, failure;
  const pump = () => {
    if (!controller) return;
    port.postMessage({type:'debug',stage:'pump',queued:waiting.length,want:controller.desiredSize});
    if (failure) { controller.error(failure); controller = null; return; }
    while (waiting.length && controller.desiredSize > 0) {
      const {index, bytes} = waiting.shift();
      controller.enqueue(new Uint8Array(bytes));
      port.postMessage({type:'consumed', index});
    }
    if (ended && !waiting.length) { controller.close(); controller = null; port.postMessage({type:'closed'}); }
  };
  port.onmessage = e => {
    if (e.data?.type === 'chunk') waiting.push(e.data);
    else if (e.data?.type === 'end') ended = true;
    else if (e.data?.type === 'abort') failure = Error(e.data.error || 'Transfer aborted');
    pump();
  };
  sessions.set(id, {size, port, create() { return new ReadableStream({start(c){controller=c; pump();},pull(){pump();},cancel(reason){port.postMessage({type:'cancel',error:String(reason)});sessions.delete(id);}}); }});
  port.postMessage({type:'registered'});
});
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);
  const prefix = '/tools/perf/continuous-prototype/download/';
  if (!url.pathname.startsWith(prefix)) return;
  const id = url.pathname.slice(prefix.length), session = sessions.get(id);
  if (!session) { event.respondWith(new Response('Unknown download',{status:404})); return; }
  session.port.postMessage({type:'debug',stage:'fetch'});
  sessions.delete(id);
  event.respondWith(new Response(session.create(), {headers:{'Content-Type':'application/octet-stream','Content-Disposition':'attachment; filename="phantom-stream-test.bin"','Content-Length':String(session.size),'Cache-Control':'no-store'}}));
});
