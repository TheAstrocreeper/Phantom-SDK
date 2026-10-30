# Native dependency notices

This experimental component's dependencies retain their own licenses.

- libjuice, Paul-Louis Ageneau and contributors, MPL-2.0. Unmodified source at
  https://github.com/paullouisageneau/libjuice/tree/cb523763255f241442da1fbdd72c498f33a54b35
  is built from the caller's pinned checkout, not vendored into this package.
  Preserve its notices and applicable source-access obligations when distributing
  a linked binary. Its full license is alongside this file as LICENSE.libjuice.
- Quinn, rustls, rcgen, Tokio, serde, futures-util, hex, cmake and the transitive
  Rust dependencies are fetched from crates.io and pinned in Cargo.lock. Their
  source packages include license terms; retain the applicable notices when
  packaging binaries. The exact resolved dependency graph is available with
  `cargo tree --locked`; no native binaries are distributed by this commit.

These licenses are not replaced by Phantom's license.
