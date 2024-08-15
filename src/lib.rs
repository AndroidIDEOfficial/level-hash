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

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#![cfg_attr(target_arch = "arm", feature(stdarch_arm_neon_intrinsics))] 
 
// Include the generated Rust bindings for libcpu_features
// Needed only on armv7a as Neon support is optional on such machines
// aarch64 is guaranteed to have Neon support
#[cfg(target_arch = "arm")]
include!(concat!(env!("OUT_DIR"), "/cpu_features.rs"));

pub use level_hash::*;
pub use util::generate_seeds;

pub(crate) mod fs;
pub(crate) mod io;
pub(crate) mod level_io;
pub(crate) mod log;
pub(crate) mod meta;
pub(crate) mod size;
pub(crate) mod types;
pub mod util;

mod level_hash;

#[cfg(target_os = "android")]
pub(crate) mod io_android;

#[cfg(target_os = "linux")]
pub(crate) mod io_linux;

#[cfg(target_arch = "arm")]
pub(crate) mod io_arm;
#[cfg(target_arch = "arm")]
pub(crate) use io_arm::__memneq;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub(crate) mod io_aarch64;

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
pub(crate) use io_aarch64::__memneq;

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_feature = "sse2"
))]
pub(crate) mod io_x86;

#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_feature = "sse2"
))]
pub(crate) use io_x86::__memneq;
