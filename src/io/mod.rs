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
pub(crate) mod io_android;

#[cfg(target_os = "linux")]
pub(crate) mod io_linux;

#[cfg(all(target_arch = "aarch64"))]
pub(crate) mod io_aarch64;
#[cfg(all(target_arch = "aarch64"))]
pub(crate) use io_aarch64::__memneq;

#[cfg(target_arch = "x86_64")]
pub(crate) mod io_x86;
#[cfg(target_arch = "x86_64")]
pub(crate) use io_x86::__memneq;

use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::fd::OwnedFd;
use std::path::Path;

use byteorder::ByteOrder;
use memmap2::MmapMut;
use memmap2::MmapOptions;

use crate::fs::fallocate_safe_punch;
use crate::io;
use crate::result::IntoLevelIOErr;
use crate::result::IntoLevelMapErr;
use crate::result::LevelMapError;
use crate::result::LevelResult;
use crate::size::SIZE_U32;
use crate::size::SIZE_U64;
use crate::types::OffT;

pub type IOEndianness = byteorder::NativeEndian;

/// A memory-mapped file.
#[derive(Debug)]
pub(crate) struct MappedFile {
    pub(crate) map: MmapMut,
    pub(crate) fd: OwnedFd,

    #[cfg_attr(target_os = "linux", allow(dead_code))]
    pub(crate) off: OffT,
    pub(crate) size: OffT,
}

impl MappedFile {
    /// Create a new [MappedFile] from the given file path. The region of the file from
    /// offset `off` to `off + size` will be mapped.
    pub(crate) fn from_path(
        path: &Path,
        off: OffT,
        size: OffT,
    ) -> LevelResult<Self, LevelMapError> {
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
    pub(crate) fn new(fd: OwnedFd, off: OffT, size: OffT) -> LevelResult<Self, LevelMapError> {
        let map = Self::do_map(&fd, off)?;
        Ok(Self { map, fd, off, size })
    }

    pub(crate) fn do_map(
        fd: &OwnedFd,
        off: OffT,
    ) -> LevelResult<MmapMut, LevelMapError> {
        unsafe {
            MmapOptions::new()
                .offset(off)
                .map_mut(fd.as_raw_fd())
        }
        .into_lvl_io_e_msg("failed to memory map file".to_string())
        .into_lvl_mmap_err()
    }

    pub(crate) fn memeq(&self, offset: OffT, arr: &[u8]) -> bool {
        let len = arr.len();
        if len == 0 || offset + len as u64 > self.size {
            return false;
        }

        unsafe {
            let lhs = self.map.as_ptr().add(offset as usize);
            let rhs = arr.as_ptr();

            __memeq(lhs, rhs, len)
        }
    }

    #[inline]
    pub(crate) fn deallocate(&mut self, offset: OffT, len: OffT) {
        fallocate_safe_punch(self.fd.as_raw_fd(), offset, len)
    }

    pub(crate) fn read_at(&self, off: OffT, dst: &mut [u8]) {
        let pos = off as usize;
        let size = self.size as usize;
        let len = dst.len();
        assert!(pos + len <= size);
        unsafe { io::__memcpy(dst.as_mut_ptr(), self.map[pos..pos + len].as_ptr(), len) }
    }

    /// Copy the bytes for the given byte array into the memory mapped file.
    pub(crate) fn write_at(&mut self, off: OffT, src: &[u8]) {
        let pos = off as usize;
        let size = self.size as usize;
        let len = src.len();
        assert!(pos + len <= size);
        unsafe { io::__memcpy(self.map[pos..pos + len].as_mut_ptr(), src.as_ptr(), len) }
    }

    pub(crate) fn r_u32(&self, off: OffT) -> u32 {
        assert!(off + SIZE_U32 <= self.size);
        let pos = off as usize;
        IOEndianness::read_u32(&self.map[pos..pos + SIZE_U32 as usize])
    }

    pub(crate) fn r_u64(&self, off: OffT) -> u64 {
        assert!(off + SIZE_U64 <= self.size);
        let pos = off as usize;
        IOEndianness::read_u64(&self.map[pos..pos + SIZE_U64 as usize])
    }

    pub(crate) fn w_u64(&mut self, off: OffT, value: u64) {
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

/// Compare `len` bytes of data from `lhs` with `len` bytes of data from `rhs`.
pub(crate) unsafe fn __memeq(lhs: *const u8, rhs: *const u8, len: usize) -> bool {
    if len < 16 {
        // don't bother
        return libc::memcmp(lhs as *const libc::c_void, rhs as *const libc::c_void, len) == 0;
    }

    // Use SIMD instructions for bulk comparison
    let mut i = 0;

    if __memneq(lhs, rhs, &mut i, len) {
        // not equal
        return false;
    }

    // Compare the remaining bytes
    if i < len {
        let remaining = len - i;
        let lhs_ptr = lhs.add(i);
        let rhs_ptr = rhs.add(i);
        return libc::memcmp(
            lhs_ptr as *const libc::c_void,
            rhs_ptr as *const libc::c_void,
            remaining,
        ) == 0;
    }

    true
}

/// Copy `len` bytes of data into `dst` from `src`.
pub(crate) unsafe fn __memcpy(dst: *mut u8, src: *const u8, len: usize) {
    unsafe {
        libc::memcpy(dst as *mut libc::c_void, src as *const libc::c_void, len);
    }
}
