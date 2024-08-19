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
use crate::level_io::VALUES_SEGMENT_SIZE_DEFAULT;
use crate::reprs::LevelMeta;
use crate::result::IntoLevelIOErr;
use crate::result::IntoLevelInitErr;
use crate::result::LevelInitError;
use crate::result::LevelResult;
use crate::types::BucketSizeT;
use crate::types::LevelSizeT;
use crate::types::OffT;

pub(crate) struct MetaIO {
    pub _file: MappedFile,
    pub meta: *mut LevelMeta,
}

impl MetaIO {
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
        let meta = mmap.map.as_mut_ptr() as *mut LevelMeta;
        let mut meta = MetaIO { _file: mmap, meta };
        if meta.val_version() == 0 {
            meta.set_val_version(LEVEL_VALUES_VERSION);
        }

        if meta.km_version() == 0 {
            meta.set_km_version(LEVEL_KEYMAP_VERSION);
        }

        if meta.val_file_size() == 0 {
            meta.set_val_file_size(VALUES_SEGMENT_SIZE_DEFAULT);
        }

        if meta.km_level_size() == 0 {
            meta.set_km_level_size(level_size);
        }

        if meta.km_bucket_size() == 0 {
            meta.set_km_bucket_size(bucket_size);
        }

        // default value of l0Addr is 0
        // only the value of l1Addr should be updated
        if meta.km_l1_addr() == 0 {
            let bsize = meta.km_bucket_size() as u64;
            let addr = (1u64 << level_size) * bsize * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES;
            meta.set_km_l1_addr(addr);
        }

        Ok(meta)
    }
}

impl MetaIO {
    pub const META__SIZE_BYTES: OffT = size_of::<LevelMeta>() as OffT;

    pub fn km_start_addr(&mut self) -> OffT {
        min(self.km_l0_addr(), self.km_l1_addr())
    }

    pub fn km_size(&mut self) -> OffT {
        let l0_bytes = (1u64 << self.km_level_size())
            * self.km_bucket_size() as u64
            * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES;
        let mut size = self.km_start_addr();
        size += l0_bytes;
        size += l0_bytes >> 1;
        return size;
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{BUCKET_SIZE_DEFAULT, LEVEL_SIZE_DEFAULT};

    use super::*;

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
        assert_eq!(io.val_version(), LEVEL_VALUES_VERSION);
        assert_eq!(io.km_version(), LEVEL_KEYMAP_VERSION);
        assert_eq!(io.val_head_addr(), 0);
        assert_eq!(io.val_tail_addr(), 0);
        assert_eq!(io.val_file_size(), VALUES_SEGMENT_SIZE_DEFAULT);
        assert_eq!(io.km_level_size(), LEVEL_SIZE_DEFAULT);
        assert_eq!(io.km_bucket_size(), BUCKET_SIZE_DEFAULT);
        assert_eq!(io.km_l0_addr(), 0);
        assert_eq!(
            io.km_l1_addr(),
            (1u64 << LEVEL_SIZE_DEFAULT)
                * BUCKET_SIZE_DEFAULT as u64
                * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES
        );
    }

    #[test]
    fn test_meta_init_with_existing_file() {
        {
            let mut io = create_meta_io("init-with-existing", true);
            io.set_val_version(2);
            io.set_km_version(3);
            io.set_val_head_addr(200);
            io.set_val_tail_addr(300);
            io.set_val_file_size(1024);
            io.set_km_level_size(10);
            io.set_km_bucket_size(20);
        }

        {
            let io = create_meta_io("init-with-existing", false);
            assert_eq!(io.val_version(), 2);
            assert_eq!(io.km_version(), 3);
            assert_eq!(io.val_head_addr(), 200);
            assert_eq!(io.val_tail_addr(), 300);
            assert_eq!(io.val_file_size(), 1024);
            assert_eq!(io.km_level_size(), 10);
            assert_eq!(io.km_bucket_size(), 20);

            // The levels are NOT moved when the fields in the meta file are updated (as expected)
            // therefore, these fields must still have the default values
            assert_eq!(io.km_l0_addr(), 0);
            assert_eq!(
                io.km_l1_addr(),
                (1u64 << LEVEL_SIZE_DEFAULT)
                    * BUCKET_SIZE_DEFAULT as u64
                    * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES
            );
        }
    }
}
