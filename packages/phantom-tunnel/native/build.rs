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
    let mut config = cmake::Config::new("ice-shim");
    config
        .define("LIBJUICE_SOURCE", source)
        .define("CMAKE_INSTALL_LIBDIR", "lib");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("android") {
        println!("cargo:rerun-if-env-changed=ANDROID_NDK_HOME");
        println!("cargo:rerun-if-env-changed=PHANTOM_ANDROID_API");
        let target = std::env::var("TARGET").unwrap();
        let abi = match target.as_str() {
            "aarch64-linux-android" => "arm64-v8a",
            "armv7-linux-androideabi" => "armeabi-v7a",
            "x86_64-linux-android" => "x86_64",
            "i686-linux-android" => "x86",
            _ => panic!("Unsupported Android target"),
        };
        let ndk =
            std::env::var("ANDROID_NDK_HOME").expect("Set ANDROID_NDK_HOME for Android builds");
        let api = std::env::var("PHANTOM_ANDROID_API")
            .unwrap_or_else(|_| "33".into())
            .parse::<u32>()
            .expect("Invalid Android API");
        assert!(api >= 24, "Android API 24+ is required by this ICE backend");
        config
            .define(
                "CMAKE_TOOLCHAIN_FILE",
                std::path::Path::new(&ndk).join("build/cmake/android.toolchain.cmake"),
            )
            .define("ANDROID_ABI", abi)
            .define("ANDROID_PLATFORM", format!("android-{api}"));
    }
    let root = config.build();
    println!("cargo:rustc-link-search=native={}/lib", root.display());
    println!("cargo:rustc-link-lib=static=phantom_ice");
    println!("cargo:rustc-link-lib=static=juice-static");
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap() == "windows" {
        println!("cargo:rustc-link-lib=ws2_32");
        println!("cargo:rustc-link-lib=bcrypt");
    }
}
