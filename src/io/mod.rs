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

#[cfg(target_os = "android")]
#[path = "mmap_android.rs"]
pub mod mmap;

#[cfg(target_os = "linux")]
#[path = "mmap_linux.rs"]
pub mod mmap;

#[cfg(target_arch = "aarch64")]
#[path = "memops_aarch64.rs"]
pub mod memops;

#[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
#[path = "memops_x86.rs"]
pub mod memops;

#[cfg(not(any(
    all(target_arch = "x86_64", target_feature = "sse2"),
    target_arch = "aarch64"
)))]
#[path = "memops_fallback.rs"]
pub mod memops;

use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::fd::OwnedFd;
use std::path::Path;

use byteorder::ByteOrder;
use memmap2::MmapMut;
use memmap2::MmapOptions;

use crate::fs::fallocate_safe_punch;
use crate::result::IntoLevelIOErr;
use crate::result::IntoLevelMapErr;
use crate::result::LevelMapError;
use crate::result::LevelResult;
use crate::size::SIZE_U64;
use crate::types::OffT;

pub type IOEndianness = byteorder::NativeEndian;

/// A memory-mapped file.
#[derive(Debug)]
pub struct MappedFile {
    pub map: MmapMut,
    pub fd: OwnedFd,

    #[cfg_attr(target_os = "linux", allow(dead_code))]
    pub off: OffT,
    pub size: OffT,
}

impl MappedFile {
    /// Create a new [MappedFile] from the given file path. The region of the file from
    /// offset `off` to `off + size` will be mapped.
    pub fn from_path(path: &Path, off: OffT, size: OffT) -> LevelResult<Self, LevelMapError> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(false)
            .open(path)
            .into_lvl_io_e_msg(format!("failed to open file: {}", path.display()))?;

        Self::new(file.into(), off, size)
    }

    /// Create a new [MappedFile] from the given file. The region of the file from offset
    /// `off` to `off + size` will be mapped.
    pub fn new(fd: OwnedFd, off: OffT, size: OffT) -> LevelResult<Self, LevelMapError> {
        let map = Self::do_map(&fd, off, size)?;
        Ok(Self { map, fd, off, size })
    }

    pub fn do_map(fd: &OwnedFd, off: OffT, size: OffT) -> LevelResult<MmapMut, LevelMapError> {
        unsafe {
            MmapOptions::new()
                .offset(off)
                .len(size as usize)
                .map_mut(fd.as_raw_fd())
        }
        .into_lvl_io_e_msg("failed to memory map file".to_string())
        .into_lvl_mmap_err()
    }

    pub fn memeq(&self, offset: OffT, arr: &[u8]) -> bool {
        let len = arr.len();
        if len == 0 || offset + len as u64 > self.size {
            return false;
        }

        unsafe {
            let lhs = self.map.as_ptr().add(offset as usize);
            let rhs = arr.as_ptr();
            self::memops::__memeq(lhs, rhs, len)
        }
    }

    #[inline]
    pub fn deallocate(&mut self, offset: OffT, len: OffT) {
        fallocate_safe_punch(self.fd.as_raw_fd(), offset, len)
    }

    pub fn read_at(&self, off: OffT, dst: &mut [u8]) {
        let pos = off as usize;
        let size = self.size as usize;
        let len = dst.len();
        assert!(pos + len <= size);
        unsafe { self::memops::__memcpy(dst.as_mut_ptr(), self.map[pos..pos + len].as_ptr(), len) }
    }

    pub fn write_at(&mut self, off: OffT, src: &[u8]) {
        let pos = off as usize;
        let size = self.size as usize;
        let len = src.len();
        assert!(pos + len <= size);
        unsafe { self::memops::__memcpy(self.map[pos..pos + len].as_mut_ptr(), src.as_ptr(), len) }
    }

    pub fn r_u64(&self, off: OffT) -> u64 {
        assert!(off + SIZE_U64 <= self.size);
        let pos = off as usize;
        IOEndianness::read_u64(&self.map[pos..pos + SIZE_U64 as usize])
    }

    pub fn w_u64(&mut self, off: OffT, value: u64) {
        assert!(off + SIZE_U64 <= self.size);
        let pos = off as usize;
        IOEndianness::write_u64(&mut self.map[pos..pos + SIZE_U64 as usize], value);
    }
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        self.map.flush().expect("failed to flush memory map");
    }
}
