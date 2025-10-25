// native/readstat-sys/build.rs

use std::{
    env, fs,
    path::{Path, PathBuf},
};

// --- diagnostics -------------------------------------------------------------

fn debug_on() -> bool {
    matches!(
        env::var("READSTAT_BUILD_DEBUG").as_deref(),
        Ok("1") | Ok("true") | Ok("yes") | Ok("on")
    )
}

macro_rules! diag {
    ($($t:tt)*) => {{
        if debug_on() {
            println!("cargo:warning={}", format!($($t)*));
        }
    }};
}

fn dump_env(keys: &[&str]) {
    for k in keys {
        match env::var(k) {
            Ok(v) => diag!("env {k}={v}"),
            Err(_) => diag!("env {k}=(unset)"),
        }
    }
}

// --- bindgen ----------------------------------------------------------------

fn bindgen_with_includes(include_dir: &Path) {
    let builder = bindgen::Builder::default()
        .header("wrapper.h")
        .allowlist_function("readstat_.*")
        .allowlist_type("readstat_.*")
        .allowlist_var("READSTAT_.*")
        .layout_tests(false)
        .clang_arg(format!("-I{}", include_dir.display()));

    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    builder
        .generate()
        .expect("bindgen failed for readstat")
        .write_to_file(out.join("bindings.rs"))
        .expect("Couldn't write bindings!");
    println!("cargo:rerun-if-changed=wrapper.h");
}

// --- locating ReadStat sources ----------------------------------------------

fn find_readstat_dir() -> Option<PathBuf> {
    // 1) explicit override
    if let Some(p) = env::var_os("READSTAT_SRC") {
        let p = PathBuf::from(p);
        if p.join("src/readstat.h").exists() {
            return Some(p);
        }
    }

    // 2) walk up from the crate dir to catch both repo and sdist layouts
    let mut cur = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    for _ in 0..6 {
        // native/readstat-sys/third_party/readstat (if vendored subtree exists)
        let third_party = cur.join("native/readstat-sys/third_party/readstat");
        if third_party.join("src/readstat.h").exists() {
            return Some(third_party);
        }
        // ./ReadStat (git submodule at top-level)
        let readstat_top = cur.join("ReadStat");
        if readstat_top.join("src/readstat.h").exists() {
            return Some(readstat_top);
        }
        if !cur.pop() {
            break;
        }
    }

    None
}

// --- zlib detection / configuration -----------------------------------------

/// Configure zlib for the C build if available.
///
/// Returns `true` if zlib should be used (headers are expected to be found),
/// and `false` if the build must proceed without zlib (.zsav unsupported).
fn configure_zlib(build: &mut cc::Build) -> bool {
    // Manual override (useful in CI or exotic platforms)
    if let Ok(v) = env::var("READSTAT_WITH_ZLIB") {
        let on = v != "0";
        diag!(
            "READSTAT_WITH_ZLIB override -> {}",
            if on { "ON" } else { "OFF" }
        );
        if on {
            println!("cargo:rustc-link-lib=z");
        }
        return on;
    }

    // Windows: default OFF unless explicitly overridden
    #[cfg(target_os = "windows")]
    {
        diag!("Defaulting zlib OFF on Windows (no bundled zlib headers). Set READSTAT_WITH_ZLIB=1 if you provide them.");
        return false;
    }

    // Try pkg-config first
    match pkg_config::Config::new().probe("zlib") {
        Ok(lib) => {
            diag!("Found zlib via pkg-config");
            for p in &lib.include_paths {
                diag!("  zlib include: {}", p.display());
                build.include(p);
            }
            for p in &lib.link_paths {
                diag!("  zlib link   : {}", p.display());
                println!("cargo:rustc-link-search=native={}", p.display());
            }
            println!("cargo:rustc-link-lib=z");
            return true;
        }
        Err(e) => {
            diag!("pkg-config zlib not found: {e}");
        }
    }

    // Heuristic header probe
    let candidates = &[
        "/usr/include/zlib.h",
        "/usr/local/include/zlib.h",
        // Homebrew
        "/opt/homebrew/opt/zlib/include/zlib.h",
        "/usr/local/opt/zlib/include/zlib.h",
    ];
    if let Some(hit) = candidates.iter().map(Path::new).find(|p| p.exists()) {
        if let Some(dir) = hit.parent() {
            diag!("Found zlib.h at {}", hit.display());
            build.include(dir);
            println!("cargo:rustc-link-lib=z");
            return true;
        }
    }

    diag!("zlib headers not found; building WITHOUT .zsav support");
    false
}

// --- build vendored ReadStat -------------------------------------------------

fn build_vendored(rs_dir: &Path) {
    let src_dir = rs_dir.join("src");
    let inc_dir = rs_dir.join("src");
    assert!(
        src_dir.exists(),
        "ReadStat sources not found at {}",
        src_dir.display()
    );

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let mut build = cc::Build::new();

    // Summarize environment (only if debug enabled)
    let target = env::var("TARGET").unwrap_or_default();
    let host = env::var("HOST").unwrap_or_default();
    dump_env(&[
        "READSTAT_BUILD_DEBUG",
        "TARGET",
        "HOST",
        "CFLAGS",
        "LIBCLANG_PATH",
        "PKG_CONFIG_PATH",
        "PKG_CONFIG_SYSROOT_DIR",
        "LIBRARY_PATH",
        "CPATH",
        "READSTAT_SRC",
        "READSTAT_WITH_ZLIB",
    ]);
    diag!("Using ReadStat sources at {}", rs_dir.display());
    diag!("Compiling for TARGET={target} (host={host})");

    // Put OUT_DIR first so our stub headers win on Windows
    build.include(&out_dir);
    build.include(&inc_dir);
    build.include(&rs_dir);

    // Minimal config.h stand-ins
    build.define("HAVE_STDDEF_H", Some("1"));
    build.define("HAVE_STDINT_H", Some("1"));
    build.define("HAVE_INTTYPES_H", Some("1"));
    build.define("HAVE_STDLIB_H", Some("1"));
    build.define("HAVE_STRING_H", Some("1"));
    build.define("HAVE_STRINGS_H", Some("1"));

    // zlib detection
    let has_zlib = configure_zlib(&mut build);
    if has_zlib {
        build.define("READSTAT_HAVE_ZLIB", Some("1"));
        build.define("HAVE_ZLIB", Some("1"));
        diag!("Building WITH zlib support");
    } else {
        build.define("READSTAT_HAVE_ZLIB", Some("0"));
        build.define("HAVE_ZLIB", Some("0"));
        diag!("Building WITHOUT zlib support (.zsav disabled)");
    }

    // iconv availability + stub implementations for Windows
    if cfg!(target_os = "windows") {
        build.define("HAVE_ICONV", Some("0"));

        // Write a minimal iconv.h so readstat_iconv.h can #include it
        let stub_h = out_dir.join("iconv.h");
        fs::write(
            &stub_h,
            r#"
#ifndef ICONV_STUB_H
#define ICONV_STUB_H
#include <stddef.h>
typedef void* iconv_t;
#define ICONV_CONST const
#endif
"#,
        )
        .expect("write iconv.h stub");

        // Create comprehensive stub implementations for Windows
        let stub_c = out_dir.join("posix_stubs.c");
        fs::write(
            &stub_c,
            r#"
#include <stddef.h>
#include <errno.h>

typedef void* iconv_t;

iconv_t iconv_open(const char* tocode, const char* fromcode) {
    (void)tocode;
    (void)fromcode;
    return (iconv_t)-1;
}

int iconv_close(iconv_t cd) {
    (void)cd;
    return 0;
}

size_t iconv(iconv_t cd, const char** inbuf, size_t* inbytesleft,
             char** outbuf, size_t* outbytesleft) {
    (void)cd;
    (void)inbuf;
    (void)inbytesleft;
    (void)outbuf;
    (void)outbytesleft;
    errno = EINVAL;
    return (size_t)-1;
}

// Fallback for Windows I/O init (real symbol lives in readstat_io_win.c)
typedef struct readstat_io_s readstat_io_t;
readstat_io_t* unistd_io_init(void) { return NULL; }
"#,
        )
        .expect("write posix_stubs.c");

        // Add stub to build
        build.file(&stub_c);
        println!("cargo:rerun-if-changed={}", stub_c.display());
        diag!("Created POSIX stubs for Windows");
    } else {
        build.define("HAVE_ICONV", Some("1"));
        #[cfg(target_os = "macos")]
        {
            println!("cargo:rustc-link-lib=iconv");
            diag!("Linking with -liconv (macOS)");
        }
    }

    // Force-include readstat.h for all translation units
    if cfg!(target_env = "msvc") {
        build.flag("/FIreadstat.h");
    } else {
        build.flag("-include").flag("readstat.h");
    }

    // Collect C files
    let mut files: Vec<PathBuf> = Vec::new();
    let mut skipped_files: Vec<String> = Vec::new();

    for entry in walkdir::WalkDir::new(&src_dir) {
        let entry = entry.unwrap();
        let p = entry.path();
        if !entry.file_type().is_file() {
            continue;
        }
        if p.extension().and_then(|s| s.to_str()) != Some("c") {
            continue;
        }

        let rel = p.strip_prefix(&src_dir).unwrap();

        // Skip test/bin/fuzz directories
        let skip_dir = rel.components().any(|c| {
            matches!(
                c.as_os_str().to_str(),
                Some("bin" | "fuzz" | "test" | "tests" | "txt")
            )
        });
        if skip_dir {
            continue;
        }

        let name = rel.file_name().and_then(|s| s.to_str()).unwrap_or("");

        // Platform IO backend
        if cfg!(target_os = "windows") {
            if name == "readstat_io_unistd.c" {
                skipped_files.push(format!("{} (Unix I/O)", name));
                continue;
            }
            // ensure win I/O is included
            if name == "readstat_io_win.c" {
                diag!("Including Windows I/O: {}", p.display());
            }
        } else if name == "readstat_io_win.c" {
            skipped_files.push(format!("{} (Windows I/O)", name));
            continue;
        }

        // Skip zlib-dependent sources if zlib is unavailable
        if !has_zlib {
            if name == "readstat_zsav_compress.c"
                || name == "readstat_zsav_read.c"
                || name == "readstat_zsav_write.c"
            {
                skipped_files.push(format!("{} (requires zlib)", name));
                continue;
            }
        }

        files.push(p.to_path_buf());
    }

    if !skipped_files.is_empty() {
        diag!("Skipping {} files:", skipped_files.len());
        for f in &skipped_files {
            diag!("  - {}", f);
        }
    }
    diag!("Compiling {} C source files", files.len());

    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
        build.file(f);
    }

    build.define("READSTAT_VERSION", Some("\"vendored\""));
    build.warnings(false).compile("readstat");

    // Generate Rust bindings
    bindgen_with_includes(&inc_dir);
}

// --- non-vendored link paths -------------------------------------------------

fn link_from_prefix(prefix: &str) {
    println!("cargo:rustc-link-search=native={prefix}/lib");
    println!("cargo:rustc-link-lib=readstat");
    println!("cargo:rustc-link-lib=z");
    println!("cargo:include={prefix}/include");
    bindgen_with_includes(&PathBuf::from(format!("{prefix}/include")));
}

fn link_from_pkg_config() -> bool {
    match pkg_config::Config::new().probe("readstat") {
        Ok(lib) => {
            if let Some(inc) = lib.include_paths.get(0) {
                bindgen_with_includes(inc);
            } else {
                bindgen_with_includes(Path::new("."));
            }
            true
        }
        Err(_) => false,
    }
}

// --- main -------------------------------------------------------------------

fn main() {
    // Re-run on these env changes for easier debugging
    println!("cargo:rerun-if-env-changed=READSTAT_BUILD_DEBUG");
    println!("cargo:rerun-if-env-changed=READSTAT_SRC");
    println!("cargo:rerun-if-env-changed=READSTAT_WITH_ZLIB");
    println!("cargo:rerun-if-env-changed=READSTAT_PREFIX");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");
    println!("cargo:rerun-if-env-changed=LIBCLANG_PATH");

    // Prefer vendored build when the feature is enabled
    if cfg!(feature = "vendored") {
        if let Some(dir) = find_readstat_dir() {
            build_vendored(&dir);
            return;
        }
        panic!(
            "`vendored` enabled but could not find ReadStat sources.\n\
             - Set READSTAT_SRC to the ReadStat directory, or\n\
             - Add a submodule at native/readstat-sys/third_party/readstat, or\n\
             - Add ./ReadStat at the repository root."
        );
    }

    // Use system ReadStat via pkg-config if available
    if link_from_pkg_config() {
        return;
    }
    // Or from a provided prefix
    if let Ok(prefix) = env::var("READSTAT_PREFIX") {
        link_from_prefix(&prefix);
        return;
    }
    // Or ~/.local as a last resort
    if let Ok(home) = env::var("HOME") {
        link_from_prefix(&format!("{home}/.local"));
        return;
    }

    panic!(
        "Unable to locate ReadStat.\n\
         Options:\n\
         - Enable feature `vendored` (and include the ReadStat sources), or\n\
         - Install `libreadstat` and expose it via pkg-config, or\n\
         - Set READSTAT_PREFIX to a prefix containing include/ and lib/."
    );
}
