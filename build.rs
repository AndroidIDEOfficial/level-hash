/*
 *  This file is part of AndroidIDE.
 *
 *  AndroidIDE is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  AndroidIDE is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *   along with AndroidIDE.  If not, see <https://www.gnu.org/licenses/>.
 */

use cmake;
use std::{env, path::PathBuf};

fn main() {
    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let triple = env::var("TARGET").unwrap();
    let cpufeat = PathBuf::from("cpu_features").canonicalize().unwrap();
    let cpufeat_src = cpufeat.join("src");
    let cpufeat_include = cpufeat.join("include");
    let cpufeat_build = cpufeat.join("build").join(triple);
    let cpufeat_out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("cpu_features.rs");

    println!("cargo::rustc-link-lib=cpu_features");
    println!("cargo::rerun-if-changed={}", cpufeat_src.display());
    println!("cargo::rerun-if-changed={}", cpufeat_include.display());
    println!("cargo::rerun-if-changed=build.rs");

    let mut header = format!("cpuinfo_{}.h", arch);
    if arch == "x86_64" {
        header = String::from("cpuinfo_x86.h");
    }

    let mut config = cmake::Config::new(cpufeat);
    config.out_dir(cpufeat_build).define("BUILD_TESTING", "OFF");
    if os == "android" {
        let ndk_home = PathBuf::from(
            env::var("ANDROID_NDK_HOME").expect("ANDROID_NDK_HOME variable is not set."),
        );
        config
            .define(
                "CMAKE_TOOLCHAIN_FILE",
                format!(
                    "{}",
                    ndk_home
                        .join("build/cmake/android.toolchain.cmake")
                        .display()
                ),
            )
            .define(
                "ANDROID_ABI",
                match &arch[..] {
                    "aarch64" => "arm64-v8a",
                    "arm" => "armeabi-v7a",
                    "x86" => "x86",
                    "x86_64" => "x86_64",
                    _ => panic!("Unknown Android arch: {}", arch),
                },
            )
            .define("ANDROID_PLATFORM", "android-26");
    }

    println!("cargo::rustc-link-search={}/lib", config.build().display());

    let _ = bindgen::Builder::default()
        .clang_arg(format!("-I{}", cpufeat_include.to_str().unwrap()))
        .header(format!("{}/{}", cpufeat_include.display(), header))
        .generate()
        .expect("Failed to generate Rust bindings")
        .write_to_file(cpufeat_out)
        .expect("Failed to write bindings");
}
