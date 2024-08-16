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
use std::cmp::min;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::fd::OwnedFd;
use std::path::Path;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use memmap2::MmapMut;
use memmap2::MmapOptions;

use crate::__memneq;
use crate::fs::fallocate_safe_punch;
use crate::result::IntoLevelIOErr;
use crate::result::IntoLevelMapErr;
use crate::result::LevelMapError;
use crate::result::LevelResult;
use crate::types::OffT;

/// The endian-ness of the file I/O operations in level hash.
pub(crate) type IOEndianness = LittleEndian;

/// A memory-mapped file.
pub(crate) struct MappedFile {
    pub(crate) map: MmapMut,
    pub(crate) fd: OwnedFd,

    #[allow(dead_code)] // used in io_android.rs
    pub(crate) off: OffT,
    pub(crate) pos: OffT,
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
        let map = Self::do_map(&fd, off, size)?;
        Ok(Self {
            map,
            fd,
            off,
            pos: 0,
            size,
        })
    }

    pub(crate) fn do_map(
        fd: &OwnedFd,
        off: OffT,
        size: OffT,
    ) -> LevelResult<MmapMut, LevelMapError> {
        unsafe {
            MmapOptions::new()
                .offset(off)
                .len(size as usize)
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
            let mem_ptr = self.map.as_ptr().add(offset as usize);
            let arr_ptr = arr.as_ptr();

            if len < 16 {
                // don't bother
                return libc::memcmp(
                    mem_ptr as *const libc::c_void,
                    arr_ptr as *const libc::c_void,
                    len,
                ) == 0;
            }

            // Use SIMD instructions for bulk comparison
            let mut i = 0;

            if __memneq(mem_ptr, arr_ptr, &mut i, len) {
                // not equal
                return false;
            }

            // Compare the remaining bytes
            if i < len {
                let remaining = len - i;
                let mem_rem_ptr = mem_ptr.add(i);
                let arr_rem_ptr = arr_ptr.add(i);
                return libc::memcmp(
                    mem_rem_ptr as *const libc::c_void,
                    arr_rem_ptr as *const libc::c_void,
                    remaining,
                ) == 0;
            }
        }

        return true;
    }
}

impl MappedFile {
    #[inline]
    pub(crate) fn deallocate(&mut self, offset: OffT, len: OffT) {
        fallocate_safe_punch(self.fd.as_raw_fd(), offset, len)
    }
}

impl MappedFile {
    #[inline]
    pub(crate) fn r_u8(&mut self) -> u8 {
        return self.read_u8().unwrap();
    }

    #[inline]
    pub(crate) fn r_u32(&mut self) -> u32 {
        return self.read_u32::<IOEndianness>().unwrap();
    }

    #[inline]
    pub(crate) fn r_u64(&mut self) -> u64 {
        return self.read_u64::<IOEndianness>().unwrap();
    }

    #[inline]
    pub(crate) fn w_u8(&mut self, v: u8) {
        self.write_u8(v).unwrap()
    }

    #[inline]
    pub(crate) fn w_u32(&mut self, v: u32) {
        self.write_u32::<IOEndianness>(v).unwrap()
    }

    #[inline]
    pub(crate) fn w_u64(&mut self, v: u64) {
        self.write_u64::<IOEndianness>(v).unwrap()
    }
}

impl Seek for MappedFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let (final_off, fail) = match pos {
            SeekFrom::Start(off) => (off, false),
            SeekFrom::End(_) => (0, true),
            SeekFrom::Current(off) => self.pos.overflowing_add_signed(off),
        };

        if fail || final_off > self.size {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput));
        }

        self.pos = final_off;
        Ok(self.pos)
    }
}

impl Read for MappedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let pos = self.pos as usize;
        let size = self.size as usize;
        let to_read = min(buf.len(), size - pos);
        if to_read == 0 {
            return Ok(0);
        }

        buf.copy_from_slice(&self.map[pos..pos + to_read]);
        self.pos += to_read as u64;
        Ok(to_read)
    }
}

impl Write for MappedFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let pos = self.pos as usize;
        let size = self.size as usize;
        let to_write = min(buf.len(), size - pos);
        if to_write == 0 {
            return Ok(0);
        }

        self.map[pos..pos + to_write].copy_from_slice(&buf[0..to_write]);
        self.pos += to_write as u64;
        Ok(to_write)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.map.flush()
    }
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        self.map.flush().expect("failed to flush memory map");
    }
}
