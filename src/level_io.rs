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
use std::os::fd::AsRawFd;
use std::path::Path;

use crate::fs::ftruncate_safe;
use crate::fs::ftruncate_safe_path;
use crate::fs::init_sparse_file;
use crate::fs::LockFile;
use crate::io::MappedFile;
use crate::meta::MetaIO;
use crate::reprs::ValuesData;
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
use crate::util::align_8;

pub const LEVEL_VALUES_VERSION: u32 = 1;
pub const LEVEL_KEYMAP_VERSION: u32 = 1;

/// Helper for handling I/O for level hash.
///
/// # Properties
///
/// * `values`: The memory-mapped file containing the value entries.
/// * `keymap`: The memory-mapped file containing the keymap.
/// * `meta`: The memory-mapped file containing the level hash metadata.
/// * `interim_lvm_addr`: The addreess of the interim level in the keymap file that is used during
/// expansion.
#[derive(Debug)]
pub struct LevelHashIO {
    pub values: MappedFile,
    pub keymap: MappedFile,
    pub meta: MetaIO,
    pub interim_lvl_addr: Option<OffT>,

    _lock_file: LockFile,
}

/// An entry in the values file.
///
/// # Properties
///
/// * - `addr` - The address of the entry in the values file.
/// * - `data` - A pointer to the data representation in memory.
#[repr(C)]
pub struct ValuesEntry<'inst> {
    pub addr: OffT,
    pub data: &'inst ValuesData,
}

#[repr(C)]
pub struct ValuesEntryMut<'inst> {
    pub addr: OffT,
    pub data: &'inst mut ValuesData,
}

pub trait ValEntryReadExt {
    fn esize(&self) -> u64 {
        let data = self.data();
        SIZE_U32 as u64 + SIZE_U32 as u64 + data.key_size as u64 + data.value_size as u64
    }

    fn is_empty(&self) -> bool {
        let data = self.data();
        data.key_size + data.value_size == 0
    }

    fn key_size(&self) -> u32 {
        self.data().key_size
    }

    fn ksizeeq(&self, size: u32) -> bool {
        self.key_size() == size
    }

    fn value_size(&self) -> u32 {
        self.data().value_size
    }

    fn data(&self) -> &ValuesData;

    fn key(&self, file: &MappedFile) -> Vec<u8>;
    fn keyeq(&self, file: &MappedFile, other: &LevelKeyT) -> bool;

    fn val_with_size(&self, file: &MappedFile) -> (u32, Vec<u8>);
    fn value(&self, file: &MappedFile) -> Vec<u8>;
}

pub trait ValEntryWriteExt {
    fn data_mut(&mut self) -> &mut ValuesData;
}

macro_rules! val_entry_read_impl {
    ($($typ:ident < $lt:lifetime > $(,)?)+) => {
        $(
            impl ValEntryReadExt for $typ <$lt> {

                fn data(&self) -> &ValuesData {
                    &self.data
                }

                fn key(&self, file: &MappedFile) -> Vec<u8> {
                    let size = self.key_size() as usize;
                    if size == 0 {
                        return vec![];
                    }

                    let mut key = vec![0u8; size];
                    file.read_at(self.addr + ValuesEntry::OFF_KEY, key.as_mut_slice());
                    key
                }

                fn keyeq(&self, file: &MappedFile, other: &LevelKeyT) -> bool {
                    return self.ksizeeq(other.len() as u32)
                        && file.memeq(self.addr + ValuesEntry::OFF_KEY, other);
                }

                fn val_with_size(&self, file: &MappedFile) -> (u32, Vec<u8>) {
                    let size = self.value_size();
                    if size == 0 {
                        return (size, vec![]);
                    }

                    let size = size as usize;
                    let key_size = self.key_size() as OffT;
                    let mut value = vec![0u8; size];
                    file.read_at(self.addr + ValuesEntry::OFF_KEY + key_size, value.as_mut_slice());
                    (size as u32, value)
                }

                fn value(&self, file: &MappedFile) -> Vec<u8> {
                    self.val_with_size(file).1
                }
            }
        )+
    };
}

impl ValuesEntry<'_> {
    pub const OFF_KEY_SIZE: OffT = 0;
    pub const OFF_VAL_SIZE: OffT = Self::OFF_KEY_SIZE + ValuesData::SIZE_key_size as OffT;
    pub const OFF_KEY: OffT = Self::OFF_VAL_SIZE + ValuesData::SIZE_value_size as OffT;

    pub const ENTRY_SIZE_MIN: OffT = Self::OFF_KEY - Self::OFF_KEY_SIZE;

    /// Create [ValuesEntry] representing the entry at `addr` in the values file.
    pub fn at(addr: OffT, file: &MappedFile) -> Self {
        let data = unsafe { &*(file.map.as_ptr().add(addr as usize) as *const ValuesData) };
        Self { addr, data }
    }
}

impl ValuesEntryMut<'_> {
    fn at(addr: OffT, file: &mut MappedFile) -> Self {
        let data = unsafe { &mut *(file.map.as_mut_ptr().add(addr as usize) as *mut ValuesData) };
        Self { addr, data }
    }
}

impl ValEntryWriteExt for ValuesEntryMut<'_> {
    fn data_mut(&mut self) -> &mut ValuesData {
        self.data
    }
}

val_entry_read_impl!(ValuesEntry<'_>, ValuesEntryMut<'_>);

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
        let lock_file = index_dir.join(format!("{}.lock", &file_name));
        let meta_file = index_dir.join(format!("{}{}", &file_name, Self::LEVEL_META_EXT));
        let keymap_file = index_dir.join(format!("{}{}", &file_name, Self::LEVEL_KEYMAP_EXT));

        let lock_file = LockFile::new(&lock_file)?;

        init_sparse_file(&index_file, Some(Self::VALUES_MAGIC_NUMBER))?;
        init_sparse_file(&keymap_file, Some(Self::KEYMAP_MAGIC_NUMBER))?;

        let mut meta = MetaIO::new(&meta_file, level_size, bucket_size)?;

        let val_size = meta.read().val_file_size;
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
            _lock_file: lock_file,
        })
    }
}

impl LevelHashIO {
    const POS_INVALID: OffT = 0;

    pub const LEVEL_META_EXT: &'static str = "._meta";
    pub const LEVEL_KEYMAP_EXT: &'static str = "._keymap";
    pub const LEVEL_INDEX_EXT: &'static str = ".index";

    /// The number of bytes it takes to store the magic number of the keymap/values file.
    pub const MAGIC_NUMBER_SIZE_BYTES: u64 = SIZE_U64;

    /// Magic number that is used as the file signature to identify the values file.
    pub const VALUES_MAGIC_NUMBER: u64 = 0x4149445856;

    /// The size of the header (bytes) in the values file.
    pub const VALUES_HEADER_SIZE_BYTES: u64 = Self::MAGIC_NUMBER_SIZE_BYTES;

    /// The size of one segment region in the values file.
    pub const VALUES_BLOCK_SIZE_BYTES: u64 = 512 * 1024;

    /// The number of bytes used to store the header of the keymap file.
    pub const KEYMAP_HEADER_SIZE_BYTES: u64 = Self::MAGIC_NUMBER_SIZE_BYTES;

    /// The number of bytes that are used to store an entry in a key map file.
    pub const KEYMAP_ENTRY_SIZE_BYTES: u64 = SIZE_U64;

    /// Magic number that is used as the file signature to identify the keymap file.
    pub const KEYMAP_MAGIC_NUMBER: u64 = 0;
}

impl LevelHashIO {
    #[inline]
    pub fn val_real_offset(off: OffT) -> OffT {
        Self::VALUES_HEADER_SIZE_BYTES + off
    }

    #[inline]
    pub fn km_real_offset(off: OffT) -> OffT {
        Self::KEYMAP_HEADER_SIZE_BYTES + off
    }

    #[inline]
    pub fn val_deallocate(&mut self, off: OffT, len: OffT) {
        self.values.deallocate(Self::val_real_offset(off), len)
    }

    #[inline]
    pub fn km_deallocate(&mut self, off: OffT, len: OffT) {
        self.keymap.deallocate(Self::km_real_offset(off), len)
    }

    #[inline]
    pub fn km_read_addr(&self, slot_addr: OffT) -> OffT {
        self.keymap.r_u64(slot_addr)
    }

    #[inline]
    pub fn km_write_addr(&mut self, slot_addr: OffT, addr: OffT) {
        self.keymap.w_u64(slot_addr, addr)
    }

    fn val_resize(&mut self, new_size: OffT) -> LevelRemapResult {
        let meta = self.meta.write();
        if meta.val_file_size == new_size {
            return Ok(());
        }

        ftruncate_safe(self.values.fd.as_raw_fd(), new_size);
        self.values.remap(new_size)?;
        meta.val_file_size = new_size;

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
    pub fn slot_addr(&self, level: _LevelIdxT, bucket: _BucketIdxT, slot: _SlotIdxT) -> OffT {
        let meta = self.meta.read();
        let lvl_addr = match level {
            0 => meta.km_l0_addr,
            1 => meta.km_l1_addr,
            _ => panic!("invalid level index"),
        };

        return self.slot_addr_for_lvl_addr(lvl_addr, bucket, slot);
    }

    /// Get the address of the slot entry in the keymap file for the given level offset, bucket and slot.
    fn slot_addr_for_lvl_addr(&self, lvl_addr: OffT, bucket: _BucketIdxT, slot: _SlotIdxT) -> OffT {
        lvl_addr + // start position of level
            (Self::KEYMAP_ENTRY_SIZE_BYTES * self.meta.read().km_bucket_size as OffT * bucket as OffT) + // bucket position
            (Self::KEYMAP_ENTRY_SIZE_BYTES * slot as OffT)
    }

    pub fn slot_and_val_addr_at(
        &self,
        level: _LevelIdxT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
    ) -> (OffT, Option<OffT>) {
        let slot_addr = self.slot_addr(level, bucket, slot);
        let addr = self.km_read_addr(slot_addr);

        if addr <= Self::POS_INVALID {
            return (slot_addr, None);
        }

        (slot_addr, Some(addr))
    }

    /// Get the address of the value entry in the values file for the given level, bucket and slot.
    fn val_addr_at(&self, level: _LevelIdxT, bucket: _BucketIdxT, slot: _SlotIdxT) -> Option<OffT> {
        self.slot_and_val_addr_at(level, bucket, slot).1
    }

    /// Get the [ValuesEntry] for the given level, bucket and slot.
    pub fn val_entry_for_slot(
        &self,
        level: _LevelIdxT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
    ) -> Option<ValuesEntry> {
        self.val_addr_at(level, bucket, slot)
            .map(|addr| ValuesEntry::at(addr - 1, &self.values))
    }
}

impl LevelHashIO {
    /// Check if the slot is occupied.
    //noinspection RsSelfConvention
    pub fn is_occupied(&self, level: _LevelIdxT, bucket: _BucketIdxT, slot: _SlotIdxT) -> bool {
        self.val_entry_for_slot(level, bucket, slot)
            .take_if(|entry| !entry.is_empty())
            .is_some()
    }

    /// Get the value for the given level, bucket and slot.
    pub fn value(&self, level: _LevelIdxT, bucket: _BucketIdxT, slot: _SlotIdxT) -> Vec<u8> {
        self.val_entry_for_slot(level, bucket, slot)
            .take_if(|entry| !entry.is_empty())
            .map(|entry| entry.value(&self.values))
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
        // IMP: Update slot_addr only after writing the new value entry

        let (slot_addr, val_addr) = self.slot_and_val_addr_at(level, bucket, slot);
        if val_addr.is_none() {
            return Err(LevelUpdateError::SlotEmpty);
        }

        let val_addr = val_addr.unwrap();
        let this_entry = ValuesEntry::at(val_addr - 1, &mut self.values);
        if this_entry.is_empty() {
            return Err(LevelUpdateError::EntryNotOccupied);
        }

        let key = this_entry.key(&self.values);
        let value = this_entry.value(&self.values);
        let esize = this_entry.esize();

        self.append_entry_at_slot(slot_addr, &key, new_value)
            .into_lvl_upd_err()?;

        self.val_deallocate(this_entry.addr, esize);

        Ok(value)
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

        let existing_val_addr = self.km_read_addr(slot_addr);
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
        let this_val_addr: OffT;
        let val_file_size: OffT;
        {
            let meta = self.meta.read();
            this_val_addr = meta.val_next_addr;
            val_file_size = meta.val_file_size;
        }

        let key_len = key.len() as u32;
        let val_len = value.len() as u32;

        let entry_size = ValuesEntry::ENTRY_SIZE_MIN + key_len as OffT + val_len as OffT;
        assert!(entry_size <= u64::MAX as OffT);

        {
            let min_file_size = this_val_addr - 1 + entry_size;
            let mut new_val_file_size = val_file_size;

            while new_val_file_size <= min_file_size {
                new_val_file_size += Self::VALUES_BLOCK_SIZE_BYTES;
            }

            self.val_resize(Self::val_real_offset(new_val_file_size))
                .into_lvl_ins_err()?;
        }

        let mut this_entry = ValuesEntryMut::at(this_val_addr - 1, &mut self.values);
        let this_entry_addr = this_entry.addr;

        assert!(
            this_entry.is_empty(),
            "addr pointed by meta.next_entry is already occupied"
        );

        let this_data = this_entry.data_mut();

        let key_off = this_entry_addr + ValuesEntry::OFF_KEY;
        self.values.write_at(key_off, key);
        this_data.key_size = key_len;

        self.values.write_at(key_off + key_len as OffT, value);
        this_data.value_size = val_len;

        // finally, current_tail = this_entry
        let meta = self.meta.write();
        meta.val_tail_addr = this_entry.addr + 1;
        meta.val_next_addr = meta.val_tail_addr + align_8(entry_size);

        self.km_write_addr(slot_addr, this_val_addr);

        Ok(())
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
        let val_addr = self.keymap.r_u64(slot_addr);
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
    pub fn delete_at(
        &mut self,
        val_addr: OffT,
        key: Option<&[u8]>,
        read_value: bool,
    ) -> Option<Vec<u8>> {
        if val_addr <= Self::POS_INVALID {
            return None;
        }

        let entry = ValuesEntry::at(val_addr - 1, &self.values);

        if let Some(k) = key {
            // if we have been provided with a key, then check if the key matches
            // if not, then do not delete
            if !entry.keyeq(&mut self.values, k) {
                return read_value.then(|| entry.value(&mut self.values));
            }
        }

        let meta = self.meta.write();

        if meta.val_tail_addr == val_addr {
            // let the next entry be written at this tail address
            meta.val_next_addr = val_addr;
        }

        let entry_size = entry.esize() as OffT;
        let mut result: Option<Vec<u8>> = None;

        if read_value {
            result = Some(entry.value(&mut self.values));
        }

        self.val_deallocate(entry.addr, entry_size);

        return result;
    }

    /// Clear all entries in the keymap and values files.
    pub fn clear(&mut self) -> LevelClearResult {
        let meta = self.meta.write();
        meta.val_tail_addr = Self::POS_INVALID;
        meta.val_next_addr = 1;
        meta.km_l0_addr = 0;

        let level_size = meta.km_level_size;
        let bucket_size = meta.km_bucket_size;
        let l1_addr = (1 << level_size) * bucket_size as OffT * Self::KEYMAP_ENTRY_SIZE_BYTES;
        meta.km_l1_addr = l1_addr;

        let km_size = l1_addr + (l1_addr >> 1);

        self.km_resize(Self::km_real_offset(km_size))?;
        self.km_deallocate(0, km_size);

        self.val_resize(Self::val_real_offset(Self::VALUES_BLOCK_SIZE_BYTES))?;
        self.val_deallocate(0, Self::VALUES_BLOCK_SIZE_BYTES);

        Ok(())
    }

    /// Prepare the interim level for the given number of buckets.
    pub fn prepare_interim(&mut self, bucket_count: u32) -> LevelResult<(), LevelMapError> {
        assert!(self.interim_lvl_addr.is_none());

        let interim_size: OffT = bucket_count as OffT
            * self.meta.read().km_bucket_size as OffT
            * Self::KEYMAP_ENTRY_SIZE_BYTES;

        // ensure the keymap can accomodate the interim level
        let len = self.keymap.size;
        self.km_resize(Self::km_real_offset(len) + interim_size)?;
        self.interim_lvl_addr = Some(len);

        Ok(())
    }

    /// Move the given slot to the interim level, returning `true` if the move was successful.
    pub fn move_to_interim(
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

        let d_val_addr = self.keymap.r_u64(d_slot_addr);
        if d_val_addr > Self::POS_INVALID {
            // this slot is occupied
            return false;
        }

        // interim slot is occupied
        // as the values in the keymap are just pointers to the actual value entries
        // in the values file, we just need to update the destination slot to point
        // where the source slot points to

        // 1. read the address where the source slot points
        let e_val_addr = self.km_read_addr(s_slot_addr);

        // 2. move the destination slot and write the address of the source slot's value
        self.km_write_addr(d_slot_addr, e_val_addr);

        // 3. deallocate the space occupied by the source slot
        // self.km_deallocate(s_slot_addr, Self::KEYMAP_ENTRY_SIZE_BYTES);

        return true;
    }

    /// Finalize the expansion of the level hash. This updates the level metadata with the updated
    /// values of the level addresses in the keymap file.
    pub fn commit_interim(&mut self, new_level_size: u8) {
        assert!(self.interim_lvl_addr.is_some());

        let meta = self.meta.write();
        let level_size = meta.km_level_size;
        let l0_addr = meta.km_l0_addr;
        let l1_addr = meta.km_l1_addr;

        // update the level size
        meta.km_level_size = new_level_size;

        // current top level becomes the new bottom level
        // and interim level becomes the new top level
        meta.km_l1_addr = l0_addr;
        meta.km_l0_addr = self.interim_lvl_addr.unwrap();
        self.interim_lvl_addr = None;

        self.km_deallocate(l1_addr, 1 << (level_size - 1))
    }
}
