use std::{
    env,
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
    build.include(&inc_dir);
    build.include(&rs_dir);

    // Minimal config.h stand-ins so headers are self-sufficient
    build.define("HAVE_STDDEF_H", Some("1"));
    build.define("HAVE_STDINT_H", Some("1"));
    build.define("HAVE_INTTYPES_H", Some("1"));
    build.define("HAVE_STDLIB_H", Some("1"));
    build.define("HAVE_STRING_H", Some("1"));
    build.define("HAVE_STRINGS_H", Some("1"));

    // zlib: enable on Unix, disable on Windows unless you provide a zlib SDK
    let has_zlib = !cfg!(target_os = "windows");
    if has_zlib {
        build.define("READSTAT_HAVE_ZLIB", Some("1"));
        build.define("HAVE_ZLIB", Some("1"));
        println!("cargo:rustc-link-lib=z");
    } else {
        build.define("READSTAT_HAVE_ZLIB", Some("0"));
        build.define("HAVE_ZLIB", Some("0"));
    }

    // iconv availability
    if cfg!(target_os = "macos") || cfg!(target_os = "linux") {
        build.define("HAVE_ICONV", Some("1"));
        #[cfg(target_os = "macos")]
        println!("cargo:rustc-link-lib=iconv");
    } else if cfg!(target_os = "windows") {
        build.define("HAVE_ICONV", Some("0"));
    }

    // Make sure every TU sees the public API types before private headers
    if cfg!(target_env = "msvc") {
        build.flag("/FIreadstat.h"); // MSVC
    } else {
        build.flag("-include").flag("readstat.h"); // GCC/Clang
    }

    // Collect ONLY library .c files; exclude bins/tests/fuzz/txt and platform-incompatible IO
    let mut files: Vec<PathBuf> = Vec::new();
    for entry in walkdir::WalkDir::new(&src_dir) {
        let entry = entry.unwrap();
        let p = entry.path();
        if !entry.file_type().is_file() || p.extension().and_then(|s| s.to_str()) != Some("c") {
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

        // Platform IO backend
        if cfg!(target_os = "windows") {
            if name == "readstat_io_unistd.c" {
                continue;
            }
        } else if name == "readstat_io_win.c" {
            continue;
        }

        // If zlib is disabled, skip files that include zlib.h unconditionally
        if !has_zlib {
            let rel_str = rel.to_string_lossy();
            if rel_str.ends_with("spss/readstat_zsav_compress.c")
                || rel_str.ends_with("spss/readstat_zsav_read.c")
                || rel_str.ends_with("spss/readstat_zsav_write.c")
                || rel_str.ends_with("spss/readstat_sav_compress.c")
            {
                continue;
            }
        }

        files.push(p.to_path_buf());
    }

    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
        build.file(f);
    }

    build.define("READSTAT_VERSION", Some("\"vendored\""));
    build.warnings(false).compile("readstat");

    // Generate bindings
    bindgen_with_includes(&inc_dir);

    // Point rustc at the produced static lib
    println!("cargo:rustc-link-lib=static=readstat");
    println!(
        "cargo:rustc-link-search=native={}",
        PathBuf::from(env::var("OUT_DIR").unwrap()).display()
    );
}

fn link_from_prefix(prefix: &str) {
    println!("cargo:rustc-link-search=native={prefix}/lib");
    println!("cargo:rustc-link-lib=readstat");
    println!("cargo:rustc-link-lib=z");
    #[cfg(target_os = "macos")]
    println!("cargo:rustc-link-lib=iconv");
    println!("cargo:include={prefix}/include");
    bindgen_with_includes(&PathBuf::from(format!("{prefix}/include")));
}

fn link_from_pkg_config() -> bool {
    if pkg_config::Config::new().probe("readstat").is_ok() {
        bindgen_with_includes(Path::new("."));
        true
    } else {
        false
    }
}

fn main() {
    if cfg!(feature = "vendored") {
        if let Some(dir) = find_readstat_dir() {
            build_vendored(&dir);
            return;
        }
        panic!(
            "`vendored` enabled but could not find ReadStat sources. \
             Set READSTAT_SRC, or add a submodule at \
             native/readstat-sys/third_party/readstat or ./ReadStat"
        );
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
