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
use std::fs::create_dir_all;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::os::fd::AsRawFd;
use std::path::Path;

use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use crate::log::loge;
use crate::size::SIZE_U64;
use crate::types::OffT;
use crate::util::file_open_or_panic;

pub(crate) fn init_sparse_file(path: &Path, magic_number: Option<u64>) {
    if !path.exists() {
        write_magic_path(path, magic_number);
        return;
    }

    assert!(!path.is_dir(), "path is a directory: {}", path.display());

    let mut file = file_open_or_panic(path, true, true, false);

    if let Some(magic) = magic_number {
        if file.metadata().expect("couldn't get metadata").len() >= SIZE_U64 {
            match file.read_u64::<byteorder::BigEndian>() {
                Ok(magic_f) => {
                    if magic_f != magic {
                        loge(&format!("magic number mismatch: {} != {}", magic_f, magic));
                        loge(&format!("removing {}", path.display()));
                        file.set_len(0).expect("couldn't truncate file");
                        write_magic_file(&mut file, Some(magic));
                    } else {
                        return;
                    }
                }
                Err(why) => panic!("couldn't read magic number: {}", why),
            }
        }
    }

    write_magic_file(&mut file, magic_number);
}

fn write_magic_path(path: &Path, magic_number: Option<u64>) {
    let file = if !path.exists() {
        path.parent().map(|p| create_dir_all(p));
        File::create_new(path)
    } else {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
    };

    let mut file = match file {
        Ok(file) => file,
        Err(why) => panic!("couldn't open {}: {}", path.display(), why),
    };

    write_magic_file(&mut file, magic_number);
}

fn write_magic_file(file: &mut File, magic_number: Option<u64>) {
    if let Some(magic) = magic_number {
        file.seek(SeekFrom::Start(0)).expect("couldn't seek file");
        file.write_u64::<byteorder::BigEndian>(magic)
            .expect("couldn't write magic number");
    }
}

#[inline]
pub(crate) fn ftruncate_safe(fd: libc::c_int, len: OffT) {
    unsafe {
        libc::ftruncate(fd, len as libc::off_t);
    }
}

#[inline]
pub(crate) fn ftruncate_safe_file(file: &File, len: OffT) {
    ftruncate_safe(file.as_raw_fd(), len);
}

#[inline]
pub(crate) fn ftruncate_safe_path(file: &Path, len: OffT) {
    let file = file_open_or_panic(file, true, true, false);
    ftruncate_safe_file(&file, len);
}

#[inline]
pub(crate) fn fallocate_safe(fd: libc::c_int, mode: libc::c_int, offset: OffT, len: OffT) {
    unsafe {
        libc::fallocate(fd, mode, offset as libc::off_t, len as libc::off_t);
    }
}

#[inline]
pub(crate) fn fallocate_safe_punch(fd: libc::c_int, offset: OffT, len: OffT) {
    fallocate_safe(
        fd,
        libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
        offset,
        len,
    );
}
