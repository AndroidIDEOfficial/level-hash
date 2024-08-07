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
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

use crate::fs::init_sparse_file;
use crate::io::MappedFile;
use crate::level_io::LEVEL_KEYMAP_VERSION;
use crate::level_io::LEVEL_VALUES_VERSION;
use crate::level_io::LevelHashIO;
use crate::level_io::VALUES_SEGMENT_SIZE_DEFAULT;
use crate::size::SIZE_U32;
use crate::size::SIZE_U8;
use crate::size::SIZE_U64;
use crate::types::OffT;
use crate::util::file_open_or_panic;

pub struct MetaIO {
    pub(crate) file: MappedFile,
}

impl MetaIO {
    pub fn new(
        path: &Path,
        level_size: u8,
        bucket_size: u8,
    ) -> MetaIO {
        init_sparse_file(path, None);

        let file = file_open_or_panic(path, true, true, false);
        file.set_len(Self::META__SIZE_BYTES).expect("Failed to set length of file");

        let mmap = MappedFile::new(file, Self::VAL__VERSION__OFFSET, Self::META__SIZE_BYTES);
        let mut meta = MetaIO { file: mmap };

        if meta.val_version() == 0 {
            meta.set_val_version(LEVEL_VALUES_VERSION);
        }

        if meta.km_version()== 0 {
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

        return meta;
    }
}

impl MetaIO {

    #[inline]
    fn seek(&mut self, offset: OffT) {
        self.file.seek(SeekFrom::Start(offset as u64)).expect(&format!("Failed to seek to offset {}", offset));
    }

    fn write(&mut self, offset: OffT, value: u8) {
        self.seek(offset);
        return self.file.w_u8(value);
    }

    fn read(&mut self, offset: OffT) -> u8 {
        self.seek(offset);
        return self.file.r_u8();
    }

    fn write_u32(&mut self, offset: OffT, value: u32) {
        self.seek(offset);
        return self.file.w_u32(value);
    }

    fn read_u32(&mut self, offset: OffT) -> u32 {
        self.seek(offset);
        return self.file.r_u32();
    }

    fn write_u64(&mut self, offset: OffT, value: u64) {
        self.seek(offset);
        return self.file.w_u64(value);
    }

    fn read_u64(&mut self, offset: OffT) -> u64 {
        self.seek(offset);
        return self.file.r_u64();
    }

    pub fn val_version(&mut self) -> u32 {
        return self.read_u32(Self::VAL__VERSION__OFFSET);
    }

    pub fn set_val_version(&mut self, value: u32) {
        self.write_u32(Self::VAL__VERSION__OFFSET, value);
    }

    pub fn km_version(&mut self) -> u32 {
        return self.read_u32(Self::KM__VERSION__OFFSET);
    }

    pub fn set_km_version(&mut self, value: u32) {
        self.write_u32(Self::KM__VERSION__OFFSET, value);
    }

    pub fn val_head_addr(&mut self) -> OffT {
        return self.read_u64(Self::VAL__HEAD_ADDR__OFFSET);
    }

    pub fn set_val_head_addr(&mut self, value: OffT) {
        self.write_u64(Self::VAL__HEAD_ADDR__OFFSET, value);
    }

    pub fn val_tail_addr(&mut self) -> OffT {
        return self.read_u64(Self::VAL__TAIL_ADDR__OFFSET);
    }

    pub fn set_val_tail_addr(&mut self, value: OffT) {
        self.write_u64(Self::VAL__TAIL_ADDR__OFFSET, value);
    }

    pub fn val_file_size(&mut self) -> OffT {
        return self.read_u64(Self::VAL__VAL_SIZE__OFFSET);
    }

    pub fn set_val_file_size(&mut self, value: OffT) {
        self.write_u64(Self::VAL__VAL_SIZE__OFFSET, value);
    }

    pub fn km_level_size(&mut self) -> u8 {
        return self.read(Self::KM__LEVEL_SIZE__OFFSET);
    }

    pub fn set_km_level_size(&mut self, value: u8) {
        self.write(Self::KM__LEVEL_SIZE__OFFSET, value);
    }

    pub fn km_bucket_size(&mut self) -> u8 {
        return self.read(Self::KM__BUCKET_SIZE__OFFSET);
    }

    pub fn set_km_bucket_size(&mut self, value: u8) {
        self.write(Self::KM__BUCKET_SIZE__OFFSET, value);
    }

    pub fn km_l0_addr(&mut self) -> OffT {
        return self.read_u64(Self::KM__L0_ADDR__OFFSET);
    }

    pub fn set_km_l0_addr(&mut self, value: OffT) {
        self.write_u64(Self::KM__L0_ADDR__OFFSET, value);
    }

    pub fn km_l1_addr(&mut self) -> OffT {
        return self.read_u64(Self::KM__L1_ADDR__OFFSET);
    }

    pub fn set_km_l1_addr(&mut self, value: OffT) {
        self.write_u64(Self::KM__L1_ADDR__OFFSET, value);
    }
}

impl MetaIO {
    pub fn km_start_addr(&mut self) -> OffT {
        min(self.km_l0_addr(), self.km_l1_addr())
    }

    pub fn km_size(&mut self) -> OffT {
        let l0_bytes = (1u64 << self.km_level_size()) * self.km_bucket_size() as u64 * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES;
        let mut size = self.km_start_addr();
        size += l0_bytes;
        size += l0_bytes >> 1;
        return size;
    }
}

impl MetaIO {
    pub const VAL__VERSION__OFFSET: OffT = 0;
    pub const VAL__VERSION__SIZE_BYTES: OffT = SIZE_U32;

    pub const KM__VERSION__OFFSET: OffT = Self::VAL__VERSION__OFFSET + Self::VAL__VERSION__SIZE_BYTES;
    pub const KM__VERSION__SIZE_BYTES: OffT = SIZE_U32;

    pub const VAL__HEAD_ADDR__OFFSET: OffT = Self::KM__VERSION__OFFSET + Self::KM__VERSION__SIZE_BYTES;
    pub const VAL__HEAD_ADDR__SIZE_BYTES: OffT = SIZE_U64;

    pub const VAL__TAIL_ADDR__OFFSET: OffT = Self::VAL__HEAD_ADDR__OFFSET + Self::VAL__HEAD_ADDR__SIZE_BYTES;
    pub const VAL__TAIL_ADDR__SIZE_BYTES: OffT = SIZE_U64;

    pub const VAL__VAL_SIZE__OFFSET: OffT = Self::VAL__TAIL_ADDR__OFFSET + Self::VAL__TAIL_ADDR__SIZE_BYTES;
    pub const VAL__VAL_SIZE__SIZE_BYTES: OffT = SIZE_U64;

    pub const KM__LEVEL_SIZE__OFFSET: OffT = Self::VAL__VAL_SIZE__OFFSET + Self::VAL__VAL_SIZE__SIZE_BYTES;
    pub const KM__LEVEL_SIZE__SIZE_BYTES: OffT = SIZE_U8;

    pub const KM__BUCKET_SIZE__OFFSET: OffT = Self::KM__LEVEL_SIZE__OFFSET + Self::KM__LEVEL_SIZE__SIZE_BYTES;
    pub const KM__BUCKET_SIZE__SIZE_BYTES: OffT = SIZE_U8;

    pub const KM__L0_ADDR__OFFSET: OffT = Self::KM__BUCKET_SIZE__OFFSET + Self::KM__BUCKET_SIZE__SIZE_BYTES;
    pub const KM__L0_ADDR__SIZE_BYTES: OffT = SIZE_U64;

    pub const KM__L1_ADDR__OFFSET: OffT = Self::KM__L0_ADDR__OFFSET + Self::KM__L0_ADDR__SIZE_BYTES;
    pub const KM__L1_ADDR__SIZE_BYTES: OffT = SIZE_U64;

    // TODO: The properties below must be updated when a new field is added to the meta file

    /**
     * The number of fields in the meta file.
     */
    const META__FIELD_COUNT: OffT = 9;

    /**
     * The size of the meta file in bytes.
     */
    // Offset of the last field + its size
    const META__SIZE_BYTES: OffT = Self::KM__L1_ADDR__OFFSET + Self::KM__L1_ADDR__SIZE_BYTES ;
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
    }

    #[test]
    fn test_meta_init_with_default_values() {
        let mut io = create_meta_io("init-with-default", true);
        assert_eq!(io.val_version(), LEVEL_VALUES_VERSION);
        assert_eq!(io.km_version(), LEVEL_KEYMAP_VERSION);
        assert_eq!(io.val_head_addr(), 0);
        assert_eq!(io.val_tail_addr(), 0);
        assert_eq!(io.val_file_size(), VALUES_SEGMENT_SIZE_DEFAULT);
        assert_eq!(io.km_level_size(), LEVEL_SIZE_DEFAULT);
        assert_eq!(io.km_bucket_size(), BUCKET_SIZE_DEFAULT);
        assert_eq!(io.km_l0_addr(), 0);
        assert_eq!(io.km_l1_addr(), (1u64 << LEVEL_SIZE_DEFAULT) * BUCKET_SIZE_DEFAULT as u64 * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES);
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
            let mut io = create_meta_io("init-with-existing", false);
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
            assert_eq!(io.km_l1_addr(), (1u64 << LEVEL_SIZE_DEFAULT) * BUCKET_SIZE_DEFAULT as u64 * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES);
        }
    }
}