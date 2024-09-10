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

use crate::io::IOEndianness;
use crate::log_macros::log_error;
use crate::result::IntoLevelIOErr;
use crate::result::IntoLevelInitErr;
use crate::result::LevelInitError;
use crate::result::LevelResult;
use crate::result::StdIOError;
use crate::size::SIZE_U64;
use crate::types::OffT;
use crate::util::file_open_or_panic;

#[derive(Debug)]
pub(crate) struct LockFile {
    _file: File,
}

impl LockFile {
    /// Create a new lock file.
    pub fn new(path: &Path) -> LevelResult<Self, LevelInitError> {
        // we do not request blocking if the lock is already acquired
        // in that case, this `open` call will fail
        let file = File::options()
            .read(true)
            .write(true)
            .create_new(!path.exists())
            .open(path)
            .into_lvl_io_e_msg(format!("failed to open lock file: {}", path.display()))
            .into_lvl_init_err()?;

        let result = __flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB);
        if result != 0 {
            // any other error returned by flock
            return Err(LevelInitError::IOError(StdIOError::new(
                Some(format!(
                    "failed to acquire lock on lock file: {}",
                    path.display()
                )),
                std::io::Error::last_os_error(),
            )));
        };

        Ok(Self { _file: file })
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        __flock(self._file.as_raw_fd(), libc::LOCK_UN | libc::LOCK_NB);
    }
}

pub(crate) fn init_sparse_file(
    path: &Path,
    magic_number: Option<u64>,
) -> LevelResult<(), LevelInitError> {
    if !path.exists() {
        return write_magic_path(path, magic_number);
    }

    if path.is_dir() {
        return Err(LevelInitError::InvalidArg(format!(
            "Expected file, but found directory: {}",
            path.display()
        )));
    }

    let mut file = File::options()
        .read(true)
        .write(true)
        .create(false)
        .open(path)
        .into_lvl_io_e_msg(format!("failed to open file: {}", path.display()))
        .into_lvl_init_err()?;

    if let Some(magic) = magic_number {
        if file.metadata().map(|m| m.len()).unwrap_or(0) >= SIZE_U64 {
            match file.read_u64::<IOEndianness>() {
                Ok(magic_f) => {
                    if magic_f != magic {
                        log_error!("magic number mismatch: {} != {}", magic_f, magic);
                        log_error!("removing {}", path.display());
                        file.set_len(0)
                            .into_lvl_io_e_msg("couldn't truncate file".to_string())?;
                        write_magic_file(&mut file, Some(magic))?;
                    } else {
                        return Ok(());
                    }
                }
                Err(why) => {
                    return Err(LevelInitError::IOError(StdIOError::with_message(
                        format!("failed to read magic number in file: {}", path.display()),
                        why,
                    )))
                }
            }
        }
    }

    write_magic_file(&mut file, magic_number)
}

fn write_magic_path(path: &Path, magic_number: Option<u64>) -> LevelResult<(), LevelInitError> {
    let mut file = if !path.exists() {
        path.parent().map(|p| create_dir_all(p));
        File::create_new(path)
    } else {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
    }
    .into_lvl_io_e_msg(format!("failed to open file: {}", path.display()))
    .into_lvl_init_err()?;

    return write_magic_file(&mut file, magic_number);
}

fn write_magic_file(file: &mut File, magic_number: Option<u64>) -> LevelResult<(), LevelInitError> {
    if let Some(magic) = magic_number {
        file.seek(SeekFrom::Start(0))
            .into_lvl_io_e_msg("failed to seek to start".to_string())
            .into_lvl_init_err()?;
        file.write_u64::<IOEndianness>(magic)
            .into_lvl_io_e_msg("failed to write to file".to_string())
            .into_lvl_init_err()?;
    }

    Ok(())
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

#[inline]
fn __flock(fd: i32, operation: i32) -> i32 {
    unsafe { libc::flock(fd, operation) }
}
