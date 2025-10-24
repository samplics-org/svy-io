use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn bindgen_with_includes(include_dir: &Path) {
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed={}", include_dir.display());

    let builder = bindgen::Builder::default()
        .header("wrapper.h")
        .allowlist_function("readstat_.*")
        .allowlist_type("readstat_.*")
        .allowlist_var("READSTAT_.*")
        .layout_tests(false)
        .clang_arg(format!("-I{}", include_dir.display()))
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    builder
        .generate()
        .expect("bindgen failed for readstat")
        .write_to_file(out.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

fn find_readstat_dir() -> Option<PathBuf> {
    if let Some(p) = env::var_os("READSTAT_SRC") {
        let p = PathBuf::from(p);
        if p.join("src/readstat.h").exists() {
            return Some(p);
        }
    }
    let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let vendored = crate_dir.join("third_party/readstat");
    if vendored.join("src/readstat.h").exists() {
        return Some(vendored);
    }
    if let Some(root) = crate_dir.parent().and_then(|p| p.parent()) {
        let root_readstat = root.join("ReadStat");
        if root_readstat.join("src/readstat.h").exists() {
            return Some(root_readstat);
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

    let mut build = cc::Build::new();

    // Put OUT_DIR includes first so any stubs override project headers if needed
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    build.include(&out_dir);
    build.include(&inc_dir);
    build.include(&rs_dir);

    // Minimal config
    for m in [
        "HAVE_STDDEF_H",
        "HAVE_STDINT_H",
        "HAVE_INTTYPES_H",
        "HAVE_STDLIB_H",
        "HAVE_STRING_H",
        "HAVE_STRINGS_H",
    ] {
        build.define(m, Some("1"));
    }

    // zlib
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        build.define("READSTAT_HAVE_ZLIB", Some("1"));
        build.define("HAVE_ZLIB", Some("1"));
        println!("cargo:rustc-link-lib=z");
    }
    #[cfg(target_os = "windows")]
    {
        build.define("READSTAT_HAVE_ZLIB", Some("0"));
        build.define("HAVE_ZLIB", Some("0"));
    }

    // iconv
    #[cfg(target_os = "macos")]
    {
        build.define("HAVE_ICONV", Some("1"));
        println!("cargo:rustc-link-lib=iconv");
    }
    #[cfg(target_os = "linux")]
    {
        build.define("HAVE_ICONV", Some("1"));
    }
    #[cfg(target_os = "windows")]
    {
        // Explicitly disable iconv and provide a stub header to avoid #include <iconv.h> failures
        build.define("HAVE_ICONV", Some("0"));
        let stub = out_dir.join("iconv.h");
        // minimal typedef is enough because code guarded by HAVE_ICONV won't be compiled
        fs::write(&stub, "#pragma once\ntypedef void* iconv_t;\n").expect("write stub iconv.h");
    }

    // Force-include readstat.h in all TUs (MSVC uses /FI)
    let tool = build.get_compiler();
    if tool.is_like_msvc() {
        build.flag("/FIreadstat.h");
    } else {
        build.flag("-include").flag("readstat.h");
    }

    // Pick only library .c files and the right IO backend
    let mut files: Vec<PathBuf> = Vec::new();
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
        if cfg!(target_os = "windows") {
            if name == "readstat_io_unistd.c" {
                continue;
            }
        } else if name == "readstat_io_win.c" {
            continue;
        }

        files.push(p.to_path_buf());
    }
    for f in &files {
        build.file(f);
        println!("cargo:rerun-if-changed={}", f.display());
    }

    build.define("READSTAT_VERSION", Some("\"vendored\""));
    build.warnings(false).compile("readstat");

    bindgen_with_includes(&inc_dir);
}

fn link_from_prefix(prefix: &str) {
    println!("cargo:rustc-link-search=native={prefix}/lib");
    println!("cargo:rustc-link-lib=readstat");
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    println!("cargo:rustc-link-lib=z");
    #[cfg(target_os = "macos")]
    println!("cargo:rustc-link-lib=iconv");
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
