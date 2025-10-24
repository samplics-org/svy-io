use std::{
    env, fs,
    path::{Path, PathBuf},
};

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

    // Our stubs first
    build.include(&out_dir);
    build.include(&inc_dir);
    build.include(&rs_dir);

    // Minimal config.h-like defines
    build.define("HAVE_STDDEF_H", Some("1"));
    build.define("HAVE_STDINT_H", Some("1"));
    build.define("HAVE_INTTYPES_H", Some("1"));
    build.define("HAVE_STDLIB_H", Some("1"));
    build.define("HAVE_STRING_H", Some("1"));
    build.define("HAVE_STRINGS_H", Some("1"));

    // zlib toggling: disable on Windows to avoid external deps in CI
    let has_zlib = !cfg!(target_os = "windows");
    if has_zlib {
        build.define("READSTAT_HAVE_ZLIB", Some("1"));
        build.define("HAVE_ZLIB", Some("1"));
        println!("cargo:rustc-link-lib=z");
    } else {
        build.define("READSTAT_HAVE_ZLIB", Some("0"));
        build.define("HAVE_ZLIB", Some("0"));
    }

    // iconv: provide stub header on Windows
    if cfg!(target_os = "windows") {
        build.define("HAVE_ICONV", Some("0"));
        let stub = out_dir.join("iconv.h");
        fs::write(
            &stub,
            r#"
#ifndef ICONV_STUB_H
#define ICONV_STUB_H
#include <stddef.h>
typedef void* iconv_t; /* pointer type to match usage */
#endif
"#,
        )
        .expect("write iconv stub");
    } else {
        build.define("HAVE_ICONV", Some("1"));
        #[cfg(target_os = "macos")]
        println!("cargo:rustc-link-lib=iconv");
    }

    // Force-include public header in every TU
    if cfg!(target_env = "msvc") {
        build.flag("/FIreadstat.h");
    } else {
        build.flag("-include").flag("readstat.h");
    }

    // Gather C sources
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

        // Skip bin/, fuzz/, test(s)/, txt/ anywhere in the path
        let skip_dir = rel.components().any(|c| {
            matches!(
                c.as_os_str().to_str(),
                Some("bin" | "fuzz" | "test" | "tests" | "txt")
            )
        });
        if skip_dir {
            continue;
        }

        // Platform I/O backend
        let name = rel.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if cfg!(target_os = "windows") {
            if name == "readstat_io_unistd.c" {
                continue;
            }
        } else if name == "readstat_io_win.c" {
            continue;
        }

        // Drop zlib users when zlib is off (Windows)
        if !has_zlib {
            if name == "readstat_zsav_compress.c"
                || name == "readstat_zsav_read.c"
                || name == "readstat_zsav_write.c"
                || name == "readstat_sav_compress.c"
            {
                // println!("Skipping zlib user: {}", rel.display());
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

    // Bindgen + link
    bindgen_with_includes(&inc_dir);
    println!("cargo:rustc-link-lib=static=readstat");
    println!("cargo:rustc-link-search=native={}", out_dir.display());
}
