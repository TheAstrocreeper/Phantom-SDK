fn main() {
    println!("cargo:rerun-if-env-changed=PHANTOM_LIBJUICE_SOURCE");
    println!("cargo:rerun-if-changed=ice-shim");
    let source = std::env::var("PHANTOM_LIBJUICE_SOURCE")
        .expect("Set PHANTOM_LIBJUICE_SOURCE to the pinned libjuice checkout (see native README)");
    let source = std::fs::canonicalize(source).expect("libjuice source directory unavailable");
    let revision = std::process::Command::new("git")
        .arg("-C")
        .arg(&source)
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("git is required to verify libjuice");
    assert!(
        revision.status.success()
            && String::from_utf8_lossy(&revision.stdout).trim()
                == "cb523763255f241442da1fbdd72c498f33a54b35",
        "libjuice revision mismatch; use the pinned revision in native README"
    );
    assert!(
        std::process::Command::new("git")
            .arg("-C")
            .arg(&source)
            .args(["diff", "--quiet", "HEAD", "--"])
            .status()
            .expect("git diff failed")
            .success(),
        "libjuice checkout must be unmodified"
    );
    let root = cmake::Config::new("ice-shim")
        .define("LIBJUICE_SOURCE", source)
        .define("CMAKE_INSTALL_LIBDIR", "lib")
        .build();
    println!("cargo:rustc-link-search=native={}/lib", root.display());
    println!("cargo:rustc-link-lib=static=phantom_ice");
    println!("cargo:rustc-link-lib=static=juice-static");
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap() == "windows" {
        println!("cargo:rustc-link-lib=ws2_32");
        println!("cargo:rustc-link-lib=bcrypt");
    }
}
