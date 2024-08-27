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
#![feature(assert_matches)]
#![cfg_attr(target_arch = "arm", feature(stdarch_arm_neon_intrinsics))]

#[cfg(not(any(target_os = "linux", target_os = "android")))]
compile_err!("This library only works on Linux/Android!");

// Include the generated Rust bindings for libcpu_features
// Needed only on armv7a as Neon support is optional on such machines
// aarch64 is guaranteed to have Neon support
#[cfg(target_arch = "arm")]
include!(concat!(env!("OUT_DIR"), "/cpu_features.rs"));

pub use level_hash::*;

pub(crate) mod fs;
pub(crate) mod io;
pub(crate) mod level_io;
pub(crate) mod meta;
pub(crate) mod reprs;
pub(crate) mod size;
pub(crate) mod types;

pub mod log;
pub mod result;
pub mod util;

mod level_hash;
