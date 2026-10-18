/** Nonblocking, abortable approval. Canceling an offer removes the prompt and its handlers. */
export function requestRampApproval({
  root,
  accept,
  decline,
  label,
  message,
  signal,
}) {
  if (signal?.aborted) return Promise.resolve(false);
  root.hidden = false;
  label.textContent = message;
  return new Promise((resolve) => {
    const finish = (value) => {
      root.hidden = true;
      accept.removeEventListener("click", yes);
      decline.removeEventListener("click", no);
      signal?.removeEventListener("abort", no);
      resolve(value);
    };
    const yes = () => finish(true),
      no = () => finish(false);
    accept.addEventListener("click", yes);
    decline.addEventListener("click", no);
    signal?.addEventListener("abort", no, { once: true });
    if (signal?.aborted) no();
    else accept.focus();
  });
}
