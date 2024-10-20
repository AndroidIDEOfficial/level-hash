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
use std::path::Path;

use crate::fs::init_sparse_file;
use crate::io::MappedFile;
use crate::level_io::LevelHashIO;
use crate::level_io::LEVEL_KEYMAP_VERSION;
use crate::level_io::LEVEL_VALUES_VERSION;
use crate::reprs::LevelMeta;
use crate::result::IntoLevelIOErr;
use crate::result::IntoLevelInitErr;
use crate::result::LevelInitError;
use crate::result::LevelResult;
use crate::types::BucketSizeT;
use crate::types::LevelSizeT;
use crate::types::OffT;

#[derive(Debug)]
struct LevelMetaPtr(*mut LevelMeta);
impl LevelMetaPtr {
    fn new(ptr: *mut LevelMeta) -> Self {
        LevelMetaPtr(ptr)
    }

    fn ptr(&self) -> *const LevelMeta {
        self.0
    }

    fn ptr_mut(&mut self) -> *mut LevelMeta {
        self.0
    }

    fn get(&self) -> &LevelMeta {
        unsafe { &*self.ptr() }
    }

    fn get_mut(&mut self) -> &mut LevelMeta {
        unsafe { &mut *self.ptr_mut() }
    }
}

#[derive(Debug)]
pub(crate) struct MetaIO {
    _file: MappedFile,
    meta: LevelMetaPtr,
}

impl MetaIO {
    #[allow(non_upper_case_globals)]
    pub const META__SIZE_BYTES: OffT = size_of::<LevelMeta>() as OffT;

    pub fn new(
        path: &Path,
        level_size: LevelSizeT,
        bucket_size: BucketSizeT,
    ) -> LevelResult<MetaIO, LevelInitError> {
        init_sparse_file(path, None)?;

        let file = File::options()
            .read(true)
            .write(true)
            .create(false)
            .open(path)
            .into_lvl_io_e_msg(format!("failed to open file: {}", path.display()))
            .into_lvl_init_err()?;

        file.set_len(Self::META__SIZE_BYTES)
            .into_lvl_io_e_msg(format!(
                "failed to set length [{}] of file: {}",
                Self::META__SIZE_BYTES,
                path.display()
            ))
            .into_lvl_init_err()?;

        let mut mmap =
            MappedFile::new(file.into(), 0, Self::META__SIZE_BYTES).into_lvl_init_err()?;
        let meta = LevelMetaPtr::new(mmap.map.as_mut_ptr() as *mut LevelMeta);
        let mut meta_io = MetaIO { _file: mmap, meta };
        let meta = meta_io.write();
        if meta.val_version == 0 {
            meta.val_version = LEVEL_VALUES_VERSION;
        }

        if meta.km_version == 0 {
            meta.km_version = LEVEL_KEYMAP_VERSION;
        }

        if meta.val_next_addr == 0 {
            meta.val_next_addr = 1;
        }

        if meta.km_level_size == 0 {
            meta.km_level_size = level_size;
        }

        if meta.km_bucket_size == 0 {
            meta.km_bucket_size = bucket_size;
        }

        // default value of l0Addr is 0
        // only the value of l1Addr should be updated
        if meta.km_l1_addr == 0 {
            let bsize = meta.km_bucket_size as u64;
            let addr = (1u64 << level_size) * bsize * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES;
            meta.km_l1_addr = addr;
        }

        Ok(meta_io)
    }

    pub fn km_start_addr(&mut self) -> OffT {
        let meta = self.read();
        min(meta.km_l0_addr, meta.km_l1_addr)
    }

    pub fn km_size(&mut self) -> OffT {
        let meta = self.read();
        let l0_bytes = (1u64 << meta.km_level_size)
            * meta.km_bucket_size as u64
            * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES;
        let mut size = self.km_start_addr();
        size += l0_bytes;
        size += l0_bytes >> 1;
        return size;
    }

    #[inline]
    pub fn read(&self) -> &LevelMeta {
        self.meta.get()
    }

    #[inline]
    pub fn write(&mut self) -> &mut LevelMeta {
        self.meta.get_mut()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::{BUCKET_SIZE_DEFAULT, LEVEL_SIZE_DEFAULT};

    fn create_meta_io(name: &str, create_new: bool) -> MetaIO {
        let meta_dir = Path::new("target/tests/level-hash").join(format!("meta-{}", name));
        if meta_dir.exists() && create_new {
            fs::remove_dir_all(&meta_dir).expect("Failed to delete existing directory");
        }
        fs::create_dir_all(&meta_dir).expect("Failed to create directories");

        let meta_file = meta_dir.join(format!("{}.storage._meta", name));
        MetaIO::new(meta_file.as_path(), LEVEL_SIZE_DEFAULT, BUCKET_SIZE_DEFAULT)
            .expect("failed to create meta file")
    }

    #[test]
    fn test_meta_init_with_default_values() {
        let io = create_meta_io("init-with-default", true);
        let meta = io.read();
        assert_eq!(meta.val_version, LEVEL_VALUES_VERSION);
        assert_eq!(meta.km_version, LEVEL_KEYMAP_VERSION);
        assert_eq!(meta.val_tail_addr, 0);
        assert_eq!(meta.val_next_addr, 1);
        assert_eq!(meta.val_file_size, 0); // this is set by LevelHashIO
        assert_eq!(meta.km_level_size, LEVEL_SIZE_DEFAULT);
        assert_eq!(meta.km_bucket_size, BUCKET_SIZE_DEFAULT);
        assert_eq!(meta.km_l0_addr, 0);
        assert_eq!(
            meta.km_l1_addr,
            (1u64 << LEVEL_SIZE_DEFAULT)
                * BUCKET_SIZE_DEFAULT as u64
                * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES
        );
    }

    #[test]
    fn test_meta_init_with_existing_file() {
        {
            let mut io = create_meta_io("init-with-existing", true);
            let meta = io.write();
            meta.val_version = 2;
            meta.km_version = 3;
            meta.val_next_addr = 200;
            meta.val_tail_addr = 300;
            meta.val_file_size = 1024;
            meta.km_level_size = 10;
            meta.km_bucket_size = 20;
        }

        {
            let io = create_meta_io("init-with-existing", false);
            let meta = io.read();
            assert_eq!(meta.val_version, 2);
            assert_eq!(meta.km_version, 3);
            assert_eq!(meta.val_next_addr, 200);
            assert_eq!(meta.val_tail_addr, 300);
            assert_eq!(meta.val_file_size, 1024);
            assert_eq!(meta.km_level_size, 10);
            assert_eq!(meta.km_bucket_size, 20);

            // The levels are NOT moved when the fields in the meta file are updated (as expected)
            // therefore, these fields must still have the default values
            assert_eq!(meta.km_l0_addr, 0);
            assert_eq!(
                meta.km_l1_addr,
                (1u64 << LEVEL_SIZE_DEFAULT)
                    * BUCKET_SIZE_DEFAULT as u64
                    * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES
            );
        }
    }
}
