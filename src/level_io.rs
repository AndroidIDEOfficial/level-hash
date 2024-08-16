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
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom::*;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::Path;

use crate::fs::ftruncate_safe;
use crate::fs::ftruncate_safe_path;
use crate::fs::init_sparse_file;
use crate::io::MappedFile;
use crate::meta::MetaIO;
use crate::result::IntoLevelIOErr;
use crate::result::IntoLevelInitErr;
use crate::result::IntoLevelInsertionErr;
use crate::result::IntoLevelUpdateErr;
use crate::result::LevelClearResult;
use crate::result::LevelInitError;
use crate::result::LevelInsertionError;
use crate::result::LevelMapError;
use crate::result::LevelRemapResult;
use crate::result::LevelResult;
use crate::result::LevelUpdateError;
use crate::result::LevelUpdateResult;
use crate::size::SIZE_U32;
use crate::size::SIZE_U64;
use crate::types::BucketSizeT;
use crate::types::LevelKeyT;
use crate::types::LevelSizeT;
use crate::types::LevelValueT;
use crate::types::OffT;
use crate::types::_BucketIdxT;
use crate::types::_LevelIdxT;
use crate::types::_SlotIdxT;

pub const LEVEL_VALUES_VERSION: u32 = 1;
pub const LEVEL_KEYMAP_VERSION: u32 = 1;
pub const VALUES_SEGMENT_SIZE_DEFAULT: OffT = 512 * 1024;

/// Helper for handling I/O for level hash.
///
/// # Properties
///
/// * `values`: The memory-mapped file containing the value entries.
/// * `keymap`: The memory-mapped file containing the keymap.
/// * `meta`: The memory-mapped file containing the level hash metadata.
/// * `interim_lvm_addr`: The addreess of the interim level in the keymap file that is used during
/// expansion.
pub struct LevelHashIO {
    pub values: MappedFile,
    pub keymap: MappedFile,
    pub meta: MetaIO,
    pub interim_lvl_addr: Option<OffT>,
}

/// An entry in the values file.
#[repr(C)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ValuesEntry {
    pub(crate) addr: OffT,
}

impl ValuesEntry {
    const OFF_ENTRY_SIZE: OffT = 0;
    const OFF_PREV_ENTRY: OffT = Self::OFF_ENTRY_SIZE + SIZE_U32;
    const OFF_NEXT_ENTRY: OffT = Self::OFF_PREV_ENTRY + SIZE_U64;
    const OFF_KEY_SIZE: OffT = Self::OFF_NEXT_ENTRY + SIZE_U64;
    const OFF_KEY: OffT = Self::OFF_KEY_SIZE + SIZE_U32;

    const BYTES_U32_0: [u8; 4] = [0u8; 4];

    /// Create [ValuesEntry] representing the entry at `addr` in the values file.
    pub fn at(addr: OffT) -> Self {
        ValuesEntry { addr }
    }
}

impl ValuesEntry {
    /// Seek to the given offset from the start of this entry.
    #[inline]
    pub fn goto(&self, file: &mut MappedFile, off: OffT) {
        if file.pos == self.addr + off {
            return;
        }
        file.seek(Start(self.addr + off)).unwrap();
    }

    /// Compare the `entry_size` field of this entry with the given size.
    pub fn esizeeq(&self, file: &mut MappedFile, size: u32) -> bool {
        let mut bytes = Self::BYTES_U32_0;
        if size > 0 {
            bytes = size.to_le_bytes();
        }
        file.memeq(self.addr + Self::OFF_ENTRY_SIZE, &bytes)
    }

    /// Check whether this values entry has `entry_size` 0.
    #[inline]
    pub fn is_empty(&self, file: &mut MappedFile) -> bool {
        return self.esizeeq(file, 0);
    }

    /// Get the `prev_entry` field of this entry.
    pub fn prev_entry(&self, file: &mut MappedFile) -> OffT {
        self.goto(file, Self::OFF_PREV_ENTRY);
        file.r_u64()
    }

    /// Get the `next_entry` field of this entry.
    pub fn next_entry(&self, file: &mut MappedFile) -> OffT {
        self.goto(file, Self::OFF_NEXT_ENTRY);
        file.r_u64()
    }

    /// Seek over the key region of this entry, optionally reading the key bytes.
    ///
    /// ## Returns
    ///
    /// The key bytes if `read_key` is true and the size of key is > 0, `None` otherwise.
    pub(super) fn seek_over_key(&self, file: &mut MappedFile, read_key: bool) -> (u32, Vec<u8>) {
        let size = self.key_size(file);
        let result: Vec<u8>;
        if size > 0 && read_key {
            let mut key = vec![0u8; size as usize];
            file.read_exact(&mut key).unwrap();
            result = key;
        } else {
            file.seek(Current(size as i64)).unwrap();
            result = vec![];
        }

        return (size, result);
    }

    /// Get the `key_size` field of this entry.
    pub fn key_size(&self, file: &mut MappedFile) -> u32 {
        self.goto(file, Self::OFF_KEY_SIZE);
        file.r_u32()
    }

    /// Compare the `key_size` field of this entry with the given value.
    pub fn ksizecmp(&self, file: &mut MappedFile, size: u32) -> bool {
        let mut bytes = Self::BYTES_U32_0;
        if size > 0 {
            bytes = size.to_le_bytes();
        }
        return file.memeq(self.addr + Self::OFF_KEY_SIZE, &bytes);
    }

    /// Get the key bytes of this entry.
    pub fn key(&self, file: &mut MappedFile) -> Vec<u8> {
        return self.seek_over_key(file, true).1;
    }

    /// Compare the key region in the memory mapped file with the given key.
    pub fn keyeq(&self, file: &mut MappedFile, other: &LevelKeyT) -> bool {
        return self.ksizecmp(file, other.len() as u32)
            && file.memeq(self.addr + Self::OFF_KEY, other);
    }

    /// Get the value size of this entry.
    pub fn value_size(&self, file: &mut MappedFile) -> u32 {
        self.seek_over_key(file, false);
        file.r_u32()
    }

    /// Get the value bytes of this entry, and the size of the value.
    pub fn val_with_size(&self, file: &mut MappedFile) -> (u32, Vec<u8>) {
        self.seek_over_key(file, false);
        let size = file.r_u32();
        if size == 0 {
            return (size, vec![]);
        }

        let mut value = vec![0u8; size as usize];
        file.read_exact(&mut value).unwrap();
        (size, value)
    }

    /// Get the value bytes of this entry, only if the value size is > 0.
    pub fn value(&self, file: &mut MappedFile) -> Vec<u8> {
        let size = self.value_size(file);
        if size > 0 {
            let mut value = vec![0u8; size as usize];
            file.read_exact(value.as_mut_slice()).unwrap();
            value
        } else {
            vec![]
        }
    }
}

impl LevelHashIO {
    /// Create a new [LevelHashIO] from the given index directory and index name.
    ///
    /// If a level hash with the given name already exists, in the given directory, then the level hash
    /// is initialized from the existing level hash. In this case, the values of `level_size` and
    /// `bucket_size` are ignored.
    ///
    /// ## Params
    ///
    /// * `index_dir`: The directory of the index.
    /// * `index_name`: The name of the index.
    /// * `level_size`: The level size of the level hash. This is used to calculate the final capacity
    /// of the level hash.
    /// * `bucket_size`: The bucket size of the level hash. This is the number of slots that make up
    /// a single bucket.
    pub fn new(
        index_dir: &Path,
        index_name: &str,
        level_size: LevelSizeT,
        bucket_size: BucketSizeT,
    ) -> LevelResult<LevelHashIO, LevelInitError> {
        create_dir_all(index_dir)
            .into_lvl_io_e_msg(format!(
                "failed to create directory: {}",
                index_dir.display()
            ))
            .into_lvl_init_err()?;

        let file_name = format!("{}{}", index_name, Self::LEVEL_INDEX_EXT);
        let index_file = index_dir.join(&file_name);
        let meta_file = index_dir.join(format!("{}{}", &file_name, Self::LEVEL_META_EXT));
        let keymap_file = index_dir.join(format!("{}{}", &file_name, Self::LEVEL_KEYMAP_EXT));

        init_sparse_file(&index_file, Some(Self::VALUES_MAGIC_NUMBER))?;
        init_sparse_file(&keymap_file, Some(Self::KEYMAP_MAGIC_NUMBER))?;

        let mut meta = MetaIO::new(&meta_file, level_size, bucket_size)?;

        let val_size = meta.val_file_size();
        let km_size = meta.km_size();
        let val_file_size = Self::val_real_offset(val_size);
        let km_file_size = Self::km_real_offset(km_size);

        ftruncate_safe_path(&index_file, val_file_size);
        ftruncate_safe_path(&keymap_file, km_file_size);

        let values = MappedFile::from_path(&index_file, Self::VALUES_HEADER_SIZE_BYTES, val_size)
            .into_lvl_init_err()?;
        let keymap = MappedFile::from_path(&keymap_file, Self::KEYMAP_HEADER_SIZE_BYTES, km_size)
            .into_lvl_init_err()?;

        Ok(LevelHashIO {
            values,
            keymap,
            meta,
            interim_lvl_addr: None,
        })
    }
}

impl LevelHashIO {
    const POS_INVALID: OffT = 0;

    const LEVEL_META_EXT: &'static str = "._meta";
    const LEVEL_KEYMAP_EXT: &'static str = "._keymap";
    const LEVEL_INDEX_EXT: &'static str = ".index";

    /// 1 Kilobyte.
    const KB_1: u64 = 1024;

    /// The number of bytes it takes to store the magic number of the keymap/values file.
    pub const MAGIC_NUMBER_SIZE_BYTES: u64 = SIZE_U64;

    /// Magic number that is used as the file signature to identify the values file.
    pub const VALUES_MAGIC_NUMBER: u64 = 0x4149445856;

    /// The size of the header (bytes) in the values file.
    pub const VALUES_HEADER_SIZE_BYTES: u64 = Self::MAGIC_NUMBER_SIZE_BYTES;

    /// The size of one segment region in the values file.
    pub const VALUES_SEGMENT_SIZE_BYTES: u64 = 512 * 1024;

    /// The number of bytes used to store the header of the keymap file.
    pub const KEYMAP_HEADER_SIZE_BYTES: u64 = Self::MAGIC_NUMBER_SIZE_BYTES;

    /// The number of bytes that are used to store an entry in a key map file.
    pub const KEYMAP_ENTRY_SIZE_BYTES: u64 = SIZE_U64;

    /// Magic number that is used as the file signature to identify the keymap file.
    pub const KEYMAP_MAGIC_NUMBER: u64 = 0;
}

impl LevelHashIO {
    #[inline]
    pub(crate) fn val_real_offset(off: OffT) -> OffT {
        Self::VALUES_HEADER_SIZE_BYTES + off
    }

    #[inline]
    pub(crate) fn km_real_offset(off: OffT) -> OffT {
        Self::KEYMAP_HEADER_SIZE_BYTES + off
    }

    #[inline]
    pub(crate) fn val_deallocate(&mut self, off: OffT, len: OffT) {
        self.values.deallocate(Self::val_real_offset(off), len)
    }

    #[inline]
    pub(crate) fn km_deallocate(&mut self, off: OffT, len: OffT) {
        self.keymap.deallocate(Self::km_real_offset(off), len)
    }

    fn val_resize(&mut self, new_size: OffT) -> LevelRemapResult {
        if self.meta.val_file_size() == new_size {
            return Ok(());
        }

        self.meta.set_val_file_size(new_size);
        ftruncate_safe(self.values.fd.as_raw_fd(), new_size);
        self.values.remap(new_size)?;

        Ok(())
    }

    fn km_resize(&mut self, new_size: OffT) -> LevelRemapResult {
        if self.meta.km_size() == new_size {
            return Ok(());
        }

        ftruncate_safe(self.keymap.fd.as_raw_fd(), new_size);
        self.keymap.remap(new_size)?;

        Ok(())
    }

    /// Get the address of the slot entry in the keymap file for the given level, bucket and slot.
    pub(crate) fn slot_addr(
        &mut self,
        level: _LevelIdxT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
    ) -> OffT {
        let lvl_addr = match level {
            0 => self.meta.km_l0_addr(),
            1 => self.meta.km_l1_addr(),
            _ => panic!("invalid level index"),
        };

        return self.slot_addr_for_lvl_addr(lvl_addr, bucket, slot);
    }

    /// Get the address of the slot entry in the keymap file for the given level offset, bucket and slot.
    fn slot_addr_for_lvl_addr(
        &mut self,
        lvl_addr: OffT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
    ) -> OffT {
        lvl_addr + // start position of level
            (Self::KEYMAP_ENTRY_SIZE_BYTES * self.meta.km_bucket_size() as OffT * bucket as OffT) + // bucket position
            (Self::KEYMAP_ENTRY_SIZE_BYTES * slot as OffT)
    }

    /// Seek to the slot entry offset in the keymap file for the given level, bucket and slot.
    fn move_to_slot(&mut self, level: _LevelIdxT, bucket: _BucketIdxT, slot: _SlotIdxT) -> bool {
        let slot = self.slot_addr(level, bucket, slot);
        self.keymap.seek(Start(slot)).is_ok()
    }

    pub(crate) fn slot_and_val_addr_at(
        &mut self,
        level: _LevelIdxT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
    ) -> (OffT, Option<OffT>) {
        let slot = self.slot_addr(level, bucket, slot);
        let val = self
            .keymap
            .seek(Start(slot))
            .ok()
            .map(|_| self.keymap.r_u64())
            .take_if(|addr| *addr > Self::POS_INVALID);

        (slot, val)
    }

    /// Get the address of the value entry in the values file for the given level, bucket and slot.
    fn val_addr_at(
        &mut self,
        level: _LevelIdxT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
    ) -> Option<OffT> {
        self.move_to_slot(level, bucket, slot)
            .then(|| self.keymap.r_u64())
            .take_if(|addr| *addr > Self::POS_INVALID)
    }

    /// Get the [ValuesEntry] for the given level, bucket and slot.
    pub fn val_entry_for_slot(
        &mut self,
        level: _LevelIdxT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
    ) -> Option<ValuesEntry> {
        self.val_addr_at(level, bucket, slot)
            .map(|addr| ValuesEntry::at(addr - 1))
    }
}

impl LevelHashIO {
    /// Check if the slot is occupied.
    //noinspection RsSelfConvention
    pub(crate) fn is_occupied(
        &mut self,
        level: _LevelIdxT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
    ) -> bool {
        self.val_entry_for_slot(level, bucket, slot)
            .take_if(|entry| !entry.is_empty(&mut self.values))
            .is_some()
    }

    /// Get the value for the given level, bucket and slot.
    pub fn value(&mut self, level: _LevelIdxT, bucket: _BucketIdxT, slot: _SlotIdxT) -> Vec<u8> {
        self.val_entry_for_slot(level, bucket, slot)
            .take_if(|entry| !entry.is_empty(&mut self.values))
            .map(|entry| entry.value(&mut self.values))
            .unwrap_or(vec![])
    }
}

impl LevelHashIO {
    /// Update the value of the entry at the given level, bucket and slot. This will try to update
    /// the value of the entry in-place, if possible. If the value cannot be updated in-place, a new
    /// entry will be created in the values file and current entry will be removed. The keymap file
    /// willl be updated to point to the new entry.
    pub fn update_entry_value(
        &mut self,
        level: _LevelIdxT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
        new_value: &LevelValueT,
    ) -> LevelUpdateResult {
        let slot_addr = self.slot_addr(level, bucket, slot);
        self.keymap.seek(Start(slot_addr)).unwrap();

        let val_addr = self.keymap.r_u64();
        if val_addr == Self::POS_INVALID {
            return Err(LevelUpdateError::SlotEmpty);
        }

        let entry = ValuesEntry::at(val_addr - 1);
        if entry.is_empty(&mut self.values) {
            return Err(LevelUpdateError::EntryNotOccupied);
        }

        let key = entry.key(&mut self.values);
        let val_size_pos = self.values.pos;
        let (val_size, val) = entry.val_with_size(&mut self.values);
        let new_val_size = new_value.len() as u32;

        if val_size < new_val_size {
            // the existing entry cannot be used as it is too small,
            // so we need to re-allocate a new entry with appropriate size
            self.append_entry_at_slot(slot_addr, &key, new_value)
                .into_lvl_upd_err()?;
            return Ok(val);
        }

        self.values.seek(Start(val_size_pos)).unwrap();
        self.values.w_u32(new_val_size);

        if new_val_size > 0 {
            self.values.write(new_value).unwrap();
        }

        if new_val_size < val_size {
            // the new value is smaller than the old value,
            // so we need to deallocate the extra space
            let offset = val_size_pos + SIZE_U32 + new_val_size as OffT;
            self.val_deallocate(offset, (val_size - new_val_size) as OffT);
        }

        Ok(val)
    }

    /// Create a new entry or update the existing entry at the given slot position. If the given
    /// `key` is empty and the there exists an existing entry, then the entry will be removed and
    /// the keymap will be updated with a null pointer (0). Otherwise, a new entry will be appended
    /// to the values file, the existing entry will be removed and the keymap will be updated to
    /// point to the new entry.
    pub fn create_or_update_entry(
        &mut self,
        level: _LevelIdxT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
        key: &LevelKeyT,
        value: &LevelValueT,
    ) -> LevelResult<(), LevelInsertionError> {
        let slot_addr = self.slot_addr(level, bucket, slot);

        if key.len() == 0 {
            self.delete_at_slot(slot_addr, key, false);
            return Ok(());
        }

        self.keymap.seek(Start(slot_addr)).unwrap();

        let existing_val_addr = self.keymap.r_u64();
        let is_update = existing_val_addr > Self::POS_INVALID;

        self.append_entry_at_slot(slot_addr, key, value)?;

        if is_update {
            self.delete_at(existing_val_addr, None, false);
        }

        Ok(())
    }

    /// Append a new entry to the values file at the given slot position. The slot entry at the given
    /// slot address in the keymap file will be updated to point to the new entry.
    pub fn append_entry_at_slot(
        &mut self,
        slot_addr: OffT,
        key: &LevelKeyT,
        value: &LevelValueT,
    ) -> LevelResult<(), LevelInsertionError> {
        let tail_addr = self.meta.val_tail_addr();

        // this may be the first entry in the values file
        let tail = (tail_addr > Self::POS_INVALID).then(|| ValuesEntry::at(tail_addr - 1));
        let this_val_addr = tail
            .as_ref()
            .map(|e| e.next_entry(&mut self.values))
            .unwrap_or(1);
        if this_val_addr + 4 * Self::KB_1 > self.meta.val_file_size() {
            self.val_resize(this_val_addr - 1 + Self::VALUES_SEGMENT_SIZE_BYTES)
                .into_lvl_ins_err()?;
        }

        // valuesResize call above will reset the position to 0,
        // so we need to call seek(...) here
        self.values.seek(Start(this_val_addr - 1)).unwrap();

        if self.values.r_u32() > 0 {
            // entry_size > 0
            // entry is occupied
            return Err(LevelInsertionError::DuplicateKey);
        }

        let entry_start = self.values.pos;

        // seek over prev_entry and next_entry
        self.values
            .seek(Current((SIZE_U64 + SIZE_U64) as i64))
            .unwrap();

        self.write_obj(key);
        self.write_obj(value);

        let entry_end = self.values.pos;
        let entry_size = entry_end - entry_start;
        assert!(entry_size <= u32::MAX as OffT);

        if let Some(t) = tail.as_ref() {
            // make curret_tail.next -> this_entry
            self.values
                .seek(Start(t.addr + ValuesEntry::OFF_NEXT_ENTRY))
                .unwrap();
            self.values.w_u64(this_val_addr);
        }

        // this this_entry.prev -> current_tail
        self.values.seek(Start(this_val_addr - 1)).unwrap();
        self.values.w_u32(entry_size as u32);
        self.values.w_u64(tail_addr);
        self.values.w_u64(entry_end + 1);

        // finally, current_tail = this_entry
        self.meta.set_val_tail_addr(this_val_addr);

        if self.meta.val_head_addr() == Self::POS_INVALID {
            self.meta.set_val_head_addr(this_val_addr);
        }

        self.keymap.seek(Start(slot_addr)).unwrap();
        self.keymap.w_u64(this_val_addr);

        Ok(())
    }

    /// Write an object to the values file. This directly writes the object to the values file,
    /// without seeking the cursor. As a result, a caller must ensure that the values file cursor
    /// is at the right position.
    ///
    /// The size of the object will be written first as u32, followed by the object itself. The values
    /// file cursor will be advanced to the end of the written object.
    fn write_obj(&mut self, obj: &[u8]) {
        let len = obj.len();
        assert!(len <= u32::MAX as usize);
        let len = len as u32;

        let size_addr = self.values.pos;
        self.values.w_u32(len);

        if len == 0 {
            return;
        }

        let val_addr = self.values.pos;
        self.values.write(obj).unwrap();

        let final_addr = self.values.pos;
        self.values.seek(Start(size_addr)).unwrap();

        self.values.w_u32((final_addr - val_addr) as u32);
        self.values.seek(Start(final_addr)).unwrap();
    }

    /// Delete the entry at the given slot position, optionally reading the existing value if `read_value`
    /// is true. The slot entry at the given slot address in the keymap file will be updated to a
    /// null pointer (0). The entry will be deleted only if the keys match.
    fn delete_at_slot(
        &mut self,
        slot_addr: OffT,
        key: &LevelKeyT,
        read_value: bool,
    ) -> Option<Vec<u8>> {
        self.keymap.seek(Start(slot_addr)).unwrap();
        let val_addr = self.keymap.r_u64();
        self.km_deallocate(slot_addr, Self::KEYMAP_ENTRY_SIZE_BYTES);
        return self.delete_at(val_addr, Some(key), read_value);
    }

    /// Delete the entry at the given value address, optionally returning the existing value if `read_value`
    /// is `true`. The slot entry at the given slot address in the keymap file will be updated to a
    /// null pointer (0). If the `key` is [Some], then the entry will be deleted only if the keys match.
    ///
    /// ## Parameters
    ///
    /// * `val_addr`: The 1-based address of the value entry to delete.
    pub(crate) fn delete_at(
        &mut self,
        val_addr: OffT,
        key: Option<&[u8]>,
        read_value: bool,
    ) -> Option<Vec<u8>> {
        if val_addr <= Self::POS_INVALID {
            return None;
        }

        let entry = ValuesEntry::at(val_addr - 1);
        let prev = entry.prev_entry(&mut self.values); // 1-based
        let next = entry.next_entry(&mut self.values); // 1-based

        if let Some(k) = key {
            // if we have been provided with a key, then check if the key matches
            // if not, then do not delete
            if !entry.keyeq(&mut self.values, k) {
                return read_value.then(|| entry.value(&mut self.values));
            }
        }

        // if this entry has a previous entry, then,
        // update the 'next' of previous entry to point to the 'next' of this entry
        if prev > 0 {
            let mut addr = next;
            if next > Self::POS_INVALID {
                self.values.seek(Start(next - 1)).unwrap();
                if self.values.r_u32() <= 0 {
                    // 'nextEntry' points to an address which is not occupied
                    // in this case, the 'next' of previous entry should point to this
                    // (current) entry so the next time an entry will be written, it
                    // overwrites this region
                    addr = val_addr
                }
            }

            self.values
                .seek(Start(prev - 1 + ValuesEntry::OFF_NEXT_ENTRY))
                .unwrap();
            self.values.w_u64(addr); // 1-based
        }

        // if this entry has a next entry, then,
        // update the 'prev' of next entry to point to the 'prev' of this entry
        if next > 0 {
            self.values
                .seek(Start(next - 1 + ValuesEntry::OFF_PREV_ENTRY))
                .unwrap();
            self.values.w_u64(prev); // 1-based
        }

        if self.meta.val_head_addr() == val_addr {
            self.meta.set_val_head_addr(if next > Self::POS_INVALID {
                next
            } else {
                Self::POS_INVALID
            });
        }

        if self.meta.val_tail_addr() == val_addr {
            self.meta.set_val_tail_addr(if prev > Self::POS_INVALID {
                prev
            } else {
                Self::POS_INVALID
            });
        }

        self.values.seek(Start(entry.addr)).unwrap();

        let mut entry_size = SIZE_U32 + SIZE_U64 + SIZE_U64;
        self.values.seek(Current(entry_size as i64)).unwrap();

        let key_size = self.values.r_u32();
        entry_size += SIZE_U32;
        entry_size += key_size as OffT;

        if key_size > 0 {
            self.values.seek(Current(key_size as i64)).unwrap();
        }

        let val_size = self.values.r_u32();
        entry_size += SIZE_U32;
        entry_size += val_size as OffT;

        let mut result = None;

        if read_value && val_size > 0 {
            let mut value = vec![0u8; val_size as usize];
            self.values.read_exact(value.as_mut_slice()).unwrap();
            result = Some(value)
        }

        self.val_deallocate(entry.addr, entry_size);

        return result;
    }

    /// Clear all entries in the keymap and values files.
    pub(crate) fn clear(&mut self) -> LevelClearResult {
        self.meta.set_val_head_addr(0);
        self.meta.set_val_tail_addr(0);
        self.meta.set_km_l0_addr(0);

        let level_size = self.meta.km_level_size();
        let bucket_size = self.meta.km_bucket_size();
        let l1_addr = (1 << level_size) * bucket_size as OffT * Self::KEYMAP_ENTRY_SIZE_BYTES;
        self.meta.set_km_l1_addr(l1_addr);

        let km_size = l1_addr + (l1_addr >> 1);

        self.km_resize(Self::km_real_offset(km_size))?;
        self.km_deallocate(0, km_size);

        self.val_resize(Self::val_real_offset(Self::VALUES_SEGMENT_SIZE_BYTES))?;
        self.val_deallocate(0, Self::VALUES_SEGMENT_SIZE_BYTES);

        Ok(())
    }

    /// Prepare the interim level for the given number of buckets.
    pub(crate) fn prepare_interim(&mut self, bucket_count: u32) -> LevelResult<(), LevelMapError> {
        assert!(self.interim_lvl_addr.is_none());

        let interim_size: OffT = bucket_count as OffT
            * self.meta.km_bucket_size() as OffT
            * Self::KEYMAP_ENTRY_SIZE_BYTES;

        // ensure the keymap can accomodate the interim level
        let len = self.keymap.size;
        self.km_resize(Self::km_real_offset(len) + interim_size)?;
        self.interim_lvl_addr = Some(len);

        Ok(())
    }

    /// Move the given slot to the interim level, returning `true` if the move was successful.
    pub(crate) fn move_to_interim(
        &mut self,
        level: _LevelIdxT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
        interim_bucket: _BucketIdxT,
        interim_slot: _SlotIdxT,
    ) -> bool {
        assert!(self.interim_lvl_addr.is_some());

        let interim_lvl = self.interim_lvl_addr.unwrap();

        // current (source) slot
        let s_slot_addr = self.slot_addr(level, bucket, slot);

        // destination slot
        let d_slot_addr = self.slot_addr_for_lvl_addr(interim_lvl, interim_bucket, interim_slot);

        self.keymap.seek(Start(d_slot_addr)).unwrap();
        let d_val_addr = self.keymap.r_u64();
        if d_val_addr > Self::POS_INVALID {
            // this slot is occupied
            return false;
        }

        // interim slot is occupied
        // as the values in the keymap are just pointers to the actual value entries
        // in the values file, we just need to update the destination slot to point
        // where the source slot points to

        // 1. read the address where the source slot points
        self.keymap.seek(Start(s_slot_addr)).unwrap();
        let e_val_addr = self.keymap.r_u64();

        // 2. move the destination slot and write the address of the source slot's value
        self.keymap.seek(Start(d_slot_addr)).unwrap();
        self.keymap.w_u64(e_val_addr);

        // 3. deallocate the space occupied by the source slot
        self.km_deallocate(s_slot_addr, Self::KEYMAP_ENTRY_SIZE_BYTES);

        return true;
    }

    /// Finalize the expansion of the level hash. This updates the level metadata with the updated
    /// values of the level addresses in the keymap file.
    pub(crate) fn commit_interim(&mut self, new_level_size: u8) {
        assert!(self.interim_lvl_addr.is_some());

        // update the level size
        self.meta.set_km_level_size(new_level_size);

        // current top level becomes the new bottom level
        // and interim level becomes the new top level
        let l0_addr = self.meta.km_l0_addr();
        self.meta.set_km_l1_addr(l0_addr);
        self.meta.set_km_l0_addr(self.interim_lvl_addr.unwrap());
        self.interim_lvl_addr = None;
    }
}
