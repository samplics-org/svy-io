use std::{
    env, fs,
    path::{Path, PathBuf},
};

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

fn find_readstat_dir() -> Option<PathBuf> {
    use std::env;
    use std::path::PathBuf;

    // Explicit override wins
    if let Some(p) = env::var_os("READSTAT_SRC") {
        let p = PathBuf::from(p);
        if p.join("src/readstat.h").exists() {
            return Some(p);
        }
    }

    // Start at the crate dir and walk up ~5 levels to handle both
    // repo checkout and maturin sdist ("src/..." wrapper) layouts.
    let mut cur = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    for _ in 0..5 {
        // Prefer a third_party vendored path if present at this level
        let third_party = cur.join("native/readstat-sys/third_party/readstat");
        if third_party.join("src/readstat.h").exists() {
            return Some(third_party);
        }
        // Otherwise, look for a top-level ReadStat/ tree
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

    // zlib: off on Windows (we also skip zsav sources below)
    let has_zlib = !cfg!(target_os = "windows");
    if has_zlib {
        build.define("READSTAT_HAVE_ZLIB", Some("1"));
        build.define("HAVE_ZLIB", Some("1"));
        println!("cargo:rustc-link-lib=z");
        eprintln!("Building WITH zlib support");
    } else {
        build.define("READSTAT_HAVE_ZLIB", Some("0"));
        build.define("HAVE_ZLIB", Some("0"));
        eprintln!("WARNING: Building WITHOUT zlib support");
        eprintln!("WARNING: .zsav (compressed SPSS) files will NOT be supported on Windows");
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

// iconv stubs (character encoding conversion)
iconv_t iconv_open(const char* tocode, const char* fromcode) {
    (void)tocode;
    (void)fromcode;
    return (iconv_t)-1;  // Return error
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
    return (size_t)-1;  // Return error
}

// Forward declaration of the Windows I/O structure
typedef struct readstat_io_s readstat_io_t;

// Windows I/O initialization stub
// This should be defined in readstat_io_win.c, but we provide a fallback
readstat_io_t* unistd_io_init(void) {
    return NULL;  // Return NULL to indicate no I/O support
}
"#,
        )
        .expect("write posix_stubs.c");

        // Add stub to build
        build.file(&stub_c);
        println!("cargo:rerun-if-changed={}", stub_c.display());
        eprintln!("Created POSIX stubs for Windows");
    } else {
        build.define("HAVE_ICONV", Some("1"));
        #[cfg(target_os = "macos")]
        println!("cargo:rustc-link-lib=iconv");
    }

    // Force-include readstat.h for all TUs
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

        // Skip test/bin directories
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

        // Platform IO backend - IMPORTANT: Include readstat_io_win.c on Windows!
        if cfg!(target_os = "windows") {
            if name == "readstat_io_unistd.c" {
                skipped_files.push(format!("{} (Unix I/O)", name));
                continue;
            }
            // Make sure we include readstat_io_win.c
            if name == "readstat_io_win.c" {
                eprintln!("Including Windows I/O: {}", p.display());
            }
        } else {
            if name == "readstat_io_win.c" {
                skipped_files.push(format!("{} (Windows I/O)", name));
                continue;
            }
        }

        // CRITICAL: Skip zlib-dependent files when zlib is not available
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

    // Print what we're skipping for debugging
    if !skipped_files.is_empty() {
        eprintln!("Skipping {} files:", skipped_files.len());
        for f in &skipped_files {
            eprintln!("  - {}", f);
        }
    }

    eprintln!("Compiling {} C source files", files.len());

    // Add files to build
    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
        build.file(f);
    }

    build.define("READSTAT_VERSION", Some("\"vendored\""));
    build.warnings(false).compile("readstat");

    // Bindings
    bindgen_with_includes(&inc_dir);
}

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

fn main() {
    if cfg!(feature = "vendored") {
        if let Some(dir) = find_readstat_dir() {
            build_vendored(&dir);
            return;
        }
        panic!("`vendored` enabled but could not find ReadStat sources. Set READSTAT_SRC or add a submodule at native/readstat-sys/third_party/readstat or ./ReadStat");
    }

    if link_from_pkg_config() {
        return;
    }
    if let Ok(prefix) = env::var("READSTAT_PREFIX") {
        link_from_prefix(&prefix);
        return;
    }
    if let Ok(home) = env::var("HOME") {
        link_from_prefix(&format!("{home}/.local"));
        return;
    }

    panic!("Unable to locate ReadStat: enable feature `vendored`, install via pkg-config, or set READSTAT_PREFIX.");
}
