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
use std::path::Path;

use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use memmap2::MmapOptions;
use memmap2::{MmapMut, RemapOptions};

use crate::fs::fallocate_safe_punch_file;
use crate::types::OffT;
use crate::util::file_open_or_panic;

/// A memory-mapped file.
pub(crate) struct MappedFile {
    pub(crate) map: MmapMut,
    pub(crate) file: File,
    pub(crate) pos: OffT,
    pub(crate) size: OffT,
}

impl MappedFile {
    /// Create a new [MappedFile] from the given file path. The region of the file from
    /// offset `off` to `off + size` will be mapped.
    pub(crate) fn from_path(path: &Path, off: OffT, size: OffT) -> Self {
        let file = file_open_or_panic(path, true, true, false);
        Self::new(file, off, size)
    }

    /// Create a new [MappedFile] from the given file. The region of the file from offset
    /// `off` to `off + size` will be mapped.
    pub(crate) fn new(file: File, off: OffT, size: OffT) -> Self {
        let map = unsafe {
            MmapOptions::new()
                .offset(off)
                .len(size as usize)
                .map_mut(&file)
        };
        let map = match map {
            Ok(map) => map,
            Err(why) => panic!("couldn't map file: {}", why),
        };

        Self {
            map,
            file,
            pos: 0,
            size,
        }
    }
}

impl MappedFile {
    #[inline]
    pub(crate) fn deallocate(&mut self, offset: OffT, len: OffT) {
        fallocate_safe_punch_file(&self.file, offset, len)
    }

    pub(crate) fn remap(&mut self, size: OffT) {
        unsafe {
            self.map
                .remap(size as usize, RemapOptions::new().may_move(true))
        }
        .expect("remap failed");

        self.size = size
    }
}

impl MappedFile {
    #[inline]
    pub(crate) fn r_u8(&mut self) -> u8 {
        return self.read_u8().unwrap();
    }

    #[inline]
    pub(crate) fn r_u32(&mut self) -> u32 {
        return self.read_u32::<byteorder::BigEndian>().unwrap();
    }

    #[inline]
    pub(crate) fn r_u64(&mut self) -> u64 {
        return self.read_u64::<byteorder::BigEndian>().unwrap();
    }

    #[inline]
    pub(crate) fn w_u8(&mut self, v: u8) {
        self.write_u8(v).unwrap()
    }

    #[inline]
    pub(crate) fn w_u32(&mut self, v: u32) {
        self.write_u32::<byteorder::BigEndian>(v).unwrap()
    }

    #[inline]
    pub(crate) fn w_u64(&mut self, v: u64) {
        self.write_u64::<byteorder::BigEndian>(v).unwrap()
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
