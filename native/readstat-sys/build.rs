// readstat-sys/build.rs
use std::{env, path::PathBuf};

fn main() {
    // 1) Try pkg-config first (works on Linux distros, conda, etc.)
    let have_pkg = pkg_config::Config::new().probe("readstat").is_ok();

    // 2) If no .pc file, fall back to your local prefix (default ~/.local)
    if !have_pkg {
        let prefix = env::var("READSTAT_PREFIX")
            .ok()
            .unwrap_or_else(|| format!("{}/.local", env::var("HOME").unwrap()));

        // Tell rustc where to find libreadstat.dylib (or .so / .dll)
        println!("cargo:rustc-link-search=native={}/lib", prefix);
        println!("cargo:rustc-link-lib=readstat");
        // zlib is needed for .zsav
        println!("cargo:rustc-link-lib=z");

        // Help bindgen find headers
        println!("cargo:include={}/include", prefix);
        // Pass include dir to clang (bindgen)
        let inc = format!("-I{}/include", prefix);

        let bindings = bindgen::Builder::default()
            .header("wrapper.h")
            .clang_arg(inc)
            .allowlist_function("readstat_.*")
            .allowlist_type("readstat_.*")
            .allowlist_var("READSTAT_.*")
            .layout_tests(false)
            .generate()
            .expect("bindgen failed for readstat");
        let out = PathBuf::from(env::var("OUT_DIR").unwrap());
        bindings
            .write_to_file(out.join("bindings.rs"))
            .expect("Couldn't write bindings!");
        println!("cargo:rerun-if-changed=wrapper.h");

        // On macOS: add rpath so the loader finds ~/.local/lib at runtime
        #[cfg(target_os = "macos")]
        {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}/lib", prefix);
        }
        return;
    }

    // pkg-config path (if found)
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .allowlist_function("readstat_.*")
        .allowlist_type("readstat_.*")
        .allowlist_var("READSTAT_.*")
        .layout_tests(false)
        .generate()
        .expect("bindgen failed for readstat");
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out.join("bindings.rs"))
        .expect("Couldn't write bindings!");
    println!("cargo:rerun-if-changed=wrapper.h");
}
