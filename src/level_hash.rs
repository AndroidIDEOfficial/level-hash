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

use std::path::Path;
use std::path::PathBuf;

use crate::generate_seeds;
use crate::level_hash::ResizeState::NotResizing;
use crate::level_io::LevelHashIO;
use crate::level_io::ValuesEntry;
use crate::result::IntoLevelExpErr;
use crate::result::LevelClearResult;
use crate::result::LevelExpansionError;
use crate::result::LevelExpansionResult;
use crate::result::LevelInitError;
use crate::result::LevelInitResult;
use crate::result::LevelInsertionError;
use crate::result::LevelInsertionResult;
use crate::result::LevelUpdateError;
use crate::result::LevelUpdateResult;
use crate::types::BucketSizeT;
use crate::types::LevelKeyT;
use crate::types::LevelSizeT;
use crate::types::LevelValueT;
use crate::types::_BucketIdxT;
use crate::types::_LevelIdxT;
use crate::types::_SlotIdxT;
use crate::util::IsTrue;
use crate::Level::L0;
use crate::Level::L1;
use crate::ResizeState::Expanding;

pub const LEVEL_SIZE_DEFAULT: u8 = 8;
pub const LEVEL_SIZE_MAX: u8 = 31;
pub const BUCKET_SIZE_DEFAULT: u8 = 4;
pub const BUCKET_SIZE_MAX: u8 = 31;
pub const LEVEL_AUTO_EXPAND_THRESHOLD_DEFAULT: f32 = 0.9;

static LEVELS: [Level; 2] = [L0, L1];
static LEVELS_REV: [Level; 2] = [L1, L0];

#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Copy, Clone)]
pub enum Level {
    L0 = 0u8,
    L1 = 1u8,
}

#[repr(u8)]
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Copy, Clone)]
pub enum ResizeState {
    NotResizing,
    Expanding,
}

/// A function which accepts a seed and data and computes the 64-bit hash of the data.
pub type HashFn = fn(u64, &[u8]) -> u64;

/// Level hash is a write-optimized and high-performance hashing index scheme with cost-efficient
/// resizing and low-overhead consistency guarantee for persistent memory.
///
/// **Level size and bucket size**
///
/// The level size is used to determine the total capacity of the level hash,
/// while the bucket size is the number of slots in each bucket. These two
/// properties define the overall performance (load factor) of the level hash
/// (in terms of hash collisions and insertion failures). As a result, the values
/// for these properties must be chosen carefully.
///
/// ```text
/// Load factor = occupied slots / total slots
/// ```
///
/// When the load factor of the level hash increases, the hash collisions increase
/// as well. This results in decreased access performance and insertion failures.
pub struct LevelHash {
    unique_keys: bool,
    auto_expand: bool,
    load_factor_threshold: f32,
    seed_1: u64,
    seed_2: u64,
    hashfn_1: HashFn,
    hashfn_2: HashFn,
    item_counts: [u32; 2],
    expand_count: u32,
    resize_state: ResizeState,
    io: LevelHashIO,
}

/// Options for building a [LevelHash] instance.
pub struct LevelHashOptions {
    level_size: LevelSizeT,
    bucket_size: BucketSizeT,
    unique_keys: bool,
    auto_expand: bool,
    load_factor_threshold: f32,
    seeds: Option<(u64, u64)>,
    hashfn_1: Option<HashFn>,
    hashfn_2: Option<HashFn>,
    index_dir: Option<PathBuf>,
    index_name: Option<String>,
}

impl LevelHashOptions {
    /// Create new [LevelHashOptions].
    pub fn new() -> Self {
        LevelHashOptions {
            level_size: LEVEL_SIZE_DEFAULT,
            bucket_size: BUCKET_SIZE_DEFAULT,
            unique_keys: true,
            auto_expand: true,
            load_factor_threshold: LEVEL_AUTO_EXPAND_THRESHOLD_DEFAULT,
            seeds: None,
            hashfn_1: None,
            hashfn_2: None,
            index_dir: None,
            index_name: None,
        }
    }

    /// Set the level size of the level hash.
    ///
    /// ## Parameters
    ///
    /// * size: The level size.
    pub fn level_size(&mut self, size: LevelSizeT) -> &mut Self {
        assert!(
            size <= LEVEL_SIZE_MAX,
            "Level size must be <= {}",
            LEVEL_SIZE_MAX
        );
        self.level_size = size;
        self
    }

    /// Set the bucket size of the level hash.
    ///
    /// ## Parameters
    ///
    /// * size: The bucket size.
    pub fn bucket_size(&mut self, size: BucketSizeT) -> &mut Self {
        assert!(
            size <= BUCKET_SIZE_MAX,
            "Bucket size must be <= {}",
            BUCKET_SIZE_MAX
        );
        self.bucket_size = size;
        self
    }

    /// Set whether the level hash must have unique keys.
    pub fn unique_keys(&mut self, unique_keys: bool) -> &mut Self {
        self.unique_keys = unique_keys;
        self
    }

    /// Set whether the level hash should expand automatically when [Self::load_factor_threshold]
    /// is reached.
    pub fn auto_expand(&mut self, auto_expand: bool) -> &mut Self {
        self.auto_expand = auto_expand;
        self
    }

    /// Set the load factor threshold for automatically expanding the level hash.
    pub fn load_factor_threshold(&mut self, threshold: f32) -> &mut Self {
        assert!(
            threshold >= 0.5 && threshold <= 1.0,
            "threshold value must be between 0.5 and 1.0"
        );
        self.load_factor_threshold = threshold;
        self
    }

    /// Set the path of the directory where the index files will be stored. The directory,
    /// including the parent directories will be created if they do not exist.
    pub fn index_dir(&mut self, index_dir: &Path) -> &mut Self {
        self.index_dir = Some(index_dir.to_path_buf());
        self
    }

    /// Set the name of the index.
    pub fn index_name(&mut self, index_name: &str) -> &mut Self {
        self.index_name = Some(index_name.to_string());
        self
    }

    /// Set the two random seeds that will be used to calculate the slot positions in
    /// the level hash. While loading an existing level hash from the disk, the same
    /// seeds that were used to create the level hash must be used or the slot positions
    /// may not be calculated properly.
    ///
    /// The default value for these seeds is calculate using the
    /// [crate::util::generate_seeds] function.
    pub fn seeds(&mut self, seed_1: u64, seed_2: u64) -> &mut Self {
        assert!(seed_1 > 0, "Seed 1 must be greater than 0");
        assert!(seed_2 > 0, "Seed 2 must be greater than 0");
        self.seeds = Some((seed_1, seed_2));
        self
    }

    /// Set the two hash functions which will be used to compute the slot position for keys.
    ///
    /// ## Parameters
    ///
    /// * - `fn1` - The first hash function.
    /// * - `fn2` - The second hash function.
    pub fn hash_fns(&mut self, fn1: HashFn, fn2: HashFn) -> &mut Self {
        self.hashfn_1 = Some(fn1);
        self.hashfn_2 = Some(fn2);
        self
    }

    /// Build the level hash instance
    pub fn build(&mut self) -> LevelInitResult {
        let index_dir = self.index_dir.take().ok_or_else(|| {
            LevelInitError::InvalidArg("Index directory must be specified".to_string())
        })?;
        let index_name = self.index_name.take().ok_or_else(|| {
            LevelInitError::InvalidArg("Index name must be specified".to_string())
        })?;

        let seeds = self.seeds.take().unwrap_or_else(|| generate_seeds());
        let fn1 = self.hashfn_1.take().expect("HashFn 1 is not set");
        let fn2 = self.hashfn_2.take().expect("HashFn 2 is not set");
        LevelHash::new(
            &index_dir,
            &index_name,
            self.level_size,
            self.bucket_size,
            self.unique_keys,
            self.auto_expand,
            self.load_factor_threshold,
            seeds.0,
            seeds.1,
            fn1,
            fn2,
        )
    }
}

impl LevelHash {
    /// Create new [LevelHashOptions] used to build a [LevelHash].
    pub fn options() -> LevelHashOptions {
        LevelHashOptions::new()
    }

    fn new(
        index_dir: &Path,
        index_name: &str,
        level_size: LevelSizeT,
        bucket_size: BucketSizeT,
        unique_keys: bool,
        auto_expand: bool,
        load_factor_threshold: f32,
        seed_1: u64,
        seed_2: u64,
        hashfn_1: HashFn,
        hashfn_2: HashFn,
    ) -> LevelInitResult {
        let io = LevelHashIO::new(index_dir, index_name, level_size, bucket_size)?;
        Ok(Self {
            unique_keys,
            auto_expand,
            load_factor_threshold,
            seed_1,
            seed_2,
            hashfn_1,
            hashfn_2,
            item_counts: [0u32, 0],
            expand_count: 0,
            resize_state: NotResizing,
            io,
        })
    }

    /// Get the number of buckets in the top level.
    #[inline]
    pub fn top_level_bucket_count(&mut self) -> u32 {
        1 << self.io.meta.km_level_size()
    }

    /// Get the total number of buckets in the level hash.
    pub fn total_bucket_count(&mut self) -> u32 {
        let top_level = self.top_level_bucket_count();
        return top_level + (top_level >> 1);
    }

    /// Get the total number of slots in the level hash.
    #[inline]
    pub fn total_slots(&mut self) -> u32 {
        return self.total_bucket_count() * self.io.meta.km_bucket_size() as u32;
    }

    /// Get the load factor of the level hash.
    pub fn load_factor(&mut self) -> f32 {
        let sum = (self.item_counts[0] + self.item_counts[1]) as f32;
        return sum / self.total_slots() as f32;
    }
}

impl LevelHash {
    #[inline]
    fn fhash(&self, key: &LevelKeyT) -> u64 {
        return (self.hashfn_1)(self.seed_1, key);
    }

    #[inline]
    fn shash(&self, key: &LevelKeyT) -> u64 {
        return (self.hashfn_2)(self.seed_2, key);
    }

    fn buck_idx_lvl(&mut self, key_hash: u64, level: Level) -> u32 {
        let mut capacity = self.top_level_bucket_count() as u64;
        if level == L1 {
            capacity = capacity >> 1;
        }

        return Self::buck_idx_cap(key_hash, capacity);
    }

    fn buck_idx_cap(key_hash: u64, capacity: u64) -> u32 {
        // since capacity is a power of two and key hash is unsigned
        // keyHash % capacity can be simplified with simple bit shift operation
        return (key_hash & (capacity - 1)) as u32;
    }

    fn entry_at(
        &mut self,
        level: Level,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
    ) -> Option<ValuesEntry> {
        return self
            .io
            .val_entry_for_slot(level as _LevelIdxT, bucket, slot);
    }

    fn cmp_key_and_get_entry(
        &mut self,
        level: Level,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
        key: &LevelKeyT,
    ) -> Option<ValuesEntry> {
        return self.entry_at(level, bucket, slot).take_if(|e| {
            (!e.is_empty())
                .then(|| e.keyeq(&mut self.io.values, key))
                .is_true()
        });
    }

    fn find_slot(
        &mut self,
        key: &LevelKeyT,
    ) -> Option<(ValuesEntry, _LevelIdxT, _BucketIdxT, _SlotIdxT)> {
        let fhash = self.fhash(key);
        let shash = self.shash(key);

        let levels = if self.item_counts[0] < self.item_counts[1] {
            // if there are more occupied slots in the bottom level
            // than in the top level, then scan the bottom level first
            LEVELS_REV
        } else {
            LEVELS
        };

        let bucket_size = self.io.meta.km_bucket_size() as _SlotIdxT;

        for level in levels {
            let fidx = self.buck_idx_lvl(fhash, level);
            let sidx = self.buck_idx_lvl(shash, level);

            for j in 0..bucket_size {
                if let Some((e, buck)) = self
                    .cmp_key_and_get_entry(level, fidx, j, key)
                    .map(|e| (e, fidx))
                    .or_else(|| {
                        self.cmp_key_and_get_entry(level, sidx, j, key)
                            .map(|e| (e, sidx))
                    })
                {
                    return Some((e, level as _LevelIdxT, buck, j));
                }
            }
        }

        None
    }

    fn insert_entry_at_slot(
        &mut self,
        level: _LevelIdxT,
        bucket: _BucketIdxT,
        slot: _SlotIdxT,
        key: &LevelKeyT,
        value: &LevelValueT,
        fail_on_dup: bool,
    ) -> LevelInsertionResult {
        let (slot_addr, val_addr) = self.io.slot_and_val_addr_at(level, bucket, slot);
        if val_addr.is_none() {
            // slot is empty
            // append the value entry and return
            return self.io.append_entry_at_slot(slot_addr, key, value);
        }

        let val_addr = val_addr.unwrap();
        let entry = ValuesEntry::at(val_addr - 1, &mut self.io.values);

        if entry.is_empty() {
            // slot is occupied, but the entry is empty
            return self.io.append_entry_at_slot(slot_addr, key, value);
        }

        // check for duplicate key
        if fail_on_dup && entry.keyeq(&mut self.io.values, key) {
            return Err(LevelInsertionError::DuplicateKey);
        }

        return Err(LevelInsertionError::InsertionFailure);
    }

    fn try_movement(
        &mut self,
        level: Level,
        bucket: _BucketIdxT,
        bucket_size: _SlotIdxT,
        key: &LevelKeyT,
        value: &LevelValueT,
    ) -> LevelInsertionResult {
        for i in 0..bucket_size {
            let this_entry = self
                .io
                .val_entry_for_slot(level as _LevelIdxT, bucket, i)
                .unwrap();
            let this_key = this_entry.key(&mut self.io.values);
            let this_value = this_entry.value(&mut self.io.values);

            let fhash = self.fhash(&this_key);
            let shash = self.shash(&this_key);

            let fidx = self.buck_idx_lvl(fhash, level);
            let sidx = self.buck_idx_lvl(shash, level);
            let jidx = if fidx == bucket { sidx } else { fidx };

            for j in 0..bucket_size {
                if self
                    .insert_entry_at_slot(
                        level as _LevelIdxT,
                        jidx,
                        j,
                        &this_key,
                        &this_value,
                        false,
                    )
                    .is_ok()
                {
                    self.io
                        .create_or_update_entry(level as _LevelIdxT, bucket, i, key, value)?;
                    self.item_counts[level as usize] += 1;
                    return Ok(());
                }
            }
        }

        return Err(LevelInsertionError::MovementFailure);
    }

    fn b2t_movement(&mut self, bucket: _BucketIdxT, bucket_size: _SlotIdxT) -> Option<_SlotIdxT> {
        for i in 0..bucket_size {
            let bottom_entry = self
                .io
                .val_entry_for_slot(L1 as _LevelIdxT, bucket, i)
                .unwrap();
            let bottom_entry_key = bottom_entry.key(&mut self.io.values);
            let bottom_entry_value = bottom_entry.key(&mut self.io.values);
            let fhash = self.fhash(&bottom_entry_key);
            let shash = self.shash(&bottom_entry_key);
            let fidx = self.buck_idx_lvl(fhash, L0);
            let sidx = self.buck_idx_lvl(shash, L0);

            for j in 0..bucket_size {
                if self
                    .insert_entry_at_slot(
                        L0 as _LevelIdxT,
                        fidx,
                        j,
                        &bottom_entry_key,
                        &bottom_entry_value,
                        false,
                    )
                    .is_ok()
                    || self
                        .insert_entry_at_slot(
                            L0 as _LevelIdxT,
                            sidx,
                            j,
                            &bottom_entry_key,
                            &bottom_entry_value,
                            false,
                        )
                        .is_ok()
                {
                    // we could delete the bottom_entry here, but we don't,
                    // this is because we can simply provide the bottom entry to the caller
                    // and let it decide where the bottom_entry can be reused

                    self.item_counts[L0 as usize] += 1;
                    self.item_counts[L1 as usize] -= 1;
                    return Some(i);
                }
            }
        }

        return None;
    }

    fn do_expand(&mut self, level_size: LevelSizeT) -> LevelExpansionResult {
        let new_top_level_capacity: u64 = 1u64 << level_size;
        let mut new_level_item_count = 0u32;

        self.io
            .prepare_interim(new_top_level_capacity as u32)
            .into_lvl_exp_err()?;

        let bucket_size = self.io.meta.km_bucket_size() as _SlotIdxT;

        for old_buck_idx in 0..(self.top_level_bucket_count() >> 1) {
            for old_slot_idx in 0..bucket_size {
                if !self
                    .io
                    .is_occupied(L1 as _LevelIdxT, old_buck_idx, old_slot_idx)
                {
                    continue;
                }

                let entry = self
                    .io
                    .val_entry_for_slot(L1 as _LevelIdxT, old_buck_idx, old_slot_idx)
                    .unwrap();
                let key = entry.key(&mut self.io.values);
                let fhash = self.fhash(&key);
                let shash = self.shash(&key);

                let fidx = Self::buck_idx_cap(fhash, new_top_level_capacity);
                let sidx = Self::buck_idx_cap(shash, new_top_level_capacity);

                let mut insert_success = false;
                for new_slot_idx in 0..bucket_size {
                    if self.io.move_to_interim(
                        L1 as _LevelIdxT,
                        old_buck_idx,
                        old_slot_idx,
                        fidx,
                        new_slot_idx,
                    ) || self.io.move_to_interim(
                        L1 as _LevelIdxT,
                        old_buck_idx,
                        old_slot_idx,
                        sidx,
                        new_slot_idx,
                    ) {
                        insert_success = true;
                        new_level_item_count += 1;
                        break;
                    }
                }

                if !insert_success {
                    return Err(LevelExpansionError::from(
                        LevelInsertionError::InsertionFailure,
                    ));
                }
            }
        }

        self.io.commit_interim(level_size);
        self.item_counts = [new_level_item_count, self.item_counts[L0 as usize]];
        self.expand_count += 1;

        Ok(())
    }
}

impl LevelHash {
    /// Get the value associated with the given key.
    ///
    /// ## Parameters
    ///
    /// * `key` - The key to get the value for.
    ///
    /// ## Returns
    ///
    /// The raw bytes of the value if an entry is found, an empty [Vec] otherwise.
    pub fn get_value(&mut self, key: &LevelKeyT) -> Vec<u8> {
        return self
            .find_slot(key)
            .map(|e| e.0.value(&mut self.io.values))
            .unwrap_or(vec![]);
    }

    /// Get the value at the given slot position.
    ///
    /// ## Parameters
    ///
    /// * `level` - The level index of the slot.
    /// * `bucket` The bucket index of the slot.
    /// * `slot` - The slot index of the slot.
    ///
    /// # Returns
    ///
    /// The raw bytes of the value if an entry is found and is occupied, an empty [Vec] otherwise.
    pub fn get_value_at(&mut self, level: Level, bucket: _BucketIdxT, slot: _SlotIdxT) -> Vec<u8> {
        return self.io.value(level as _LevelIdxT, bucket, slot);
    }

    /// Insert the given key-value pair in the level hash.
    ///
    /// ## Parameters
    ///
    /// * `key` - The key for the entry. This must be a non-empty slice of bytes.
    /// * `value` - The value for the entry. This may be an empty slice in which case, [Self::get_value]
    ///     for the given key will return `None`.
    ///
    /// ## Returns
    ///
    /// `true` if the value was inserted successfully, `false` otherwise.
    pub fn insert(&mut self, key: &LevelKeyT, value: &LevelValueT) -> LevelInsertionResult {
        if self.load_factor() >= self.load_factor_threshold
            && self.auto_expand
            && self.expand().is_err()
        {
            return Err(LevelInsertionError::ExpansionFailure);
        }

        if self.load_factor() >= 1f32 {
            return Err(LevelInsertionError::LevelOverflow);
        }

        let fhash = self.fhash(key);
        let shash = self.shash(key);
        let bucket_size = self.io.meta.km_bucket_size() as _SlotIdxT;

        // Check if there are any empty slots availale in any of the levels
        // If there are, insert the key-value pair and return true
        for level in LEVELS {
            let fidx = self.buck_idx_lvl(fhash, level);
            let sidx = self.buck_idx_lvl(shash, level);
            for j in 0..bucket_size {
                if self
                    .insert_entry_at_slot(
                        level as _LevelIdxT,
                        fidx,
                        j,
                        key,
                        value,
                        self.unique_keys,
                    )
                    .is_ok()
                    || self
                        .insert_entry_at_slot(
                            level as _LevelIdxT,
                            sidx,
                            j,
                            key,
                            value,
                            self.unique_keys,
                        )
                        .is_ok()
                {
                    self.item_counts[level as usize] += 1;
                    return Ok(());
                }
            }
        }

        for level in LEVELS {
            let fidx = self.buck_idx_lvl(fhash, level);
            let sidx = self.buck_idx_lvl(shash, level);

            if self
                .try_movement(level, fidx, bucket_size, key, value)
                .is_ok()
                || self
                    .try_movement(level, sidx, bucket_size, key, value)
                    .is_ok()
            {
                return Ok(());
            }
        }

        if self.expand_count > 0 {
            let fidx = self.buck_idx_lvl(fhash, L0);
            let sidx = self.buck_idx_lvl(shash, L0);
            let from_pos = self
                .b2t_movement(fidx, bucket_size)
                .map(|slot| (fidx, slot))
                .or_else(|| {
                    self.b2t_movement(sidx, bucket_size)
                        .map(|slot| (sidx, slot))
                });

            if let Some((bucket, slot)) = from_pos {
                self.io
                    .create_or_update_entry(L0 as _LevelIdxT, bucket, slot, key, value)?;
                self.item_counts[L0 as usize] += 1;
                return Ok(());
            }
        }

        Err(LevelInsertionError::InsertionFailure)
    }

    /// Remove the entry associated with the given key.
    ///
    /// ## Parameters
    ///
    /// * `key` - The key for the entry to remove.
    ///
    /// ## Returns
    ///
    /// `Some` containing the raw bytes of the value of the deleted entry (if found and is occupied), `None` otherwise.
    pub fn remove(&mut self, key: &LevelKeyT) -> Option<Vec<u8>> {
        return self
            .find_slot(key)
            .and_then(|e| self.io.delete_at(e.0.addr + 1, Some(key), true));
    }

    /// Update the entry associated with the given key with the new value.
    ///
    /// ## Parameters
    ///
    /// * `key` - The key to update the value for.
    /// * `new_value` - The new value for the entry.
    ///
    /// ## Returns
    ///
    /// `Some` containing the raw bytes of the previous value of the entry (if found and is occupied), `None` otherwise.
    pub fn update(&mut self, key: &LevelKeyT, new_value: &LevelValueT) -> LevelUpdateResult {
        let slot = self.find_slot(key);

        if slot.is_none() {
            return Err(LevelUpdateError::SlotNotFound);
        }

        let slot = slot.unwrap();
        self.io
            .update_entry_value(slot.1, slot.2, slot.3, new_value)
    }

    /// Expand the level hash by one level size, doubling its capacity. This is an expensive operation
    /// and must be used carefully. Consider enabling [LevelHashOptions::auto_expand] to automatically expand
    /// the level hash when appropriate. A level hash can have a maximum of [LEVEL_SIZE_MAX] level size.
    ///
    /// # Returns
    ///
    /// The result of the expansion.
    pub fn expand(&mut self) -> LevelExpansionResult {
        assert_eq!(
            self.resize_state, NotResizing,
            "cannot expand while resizing"
        );
        let level_size = self.io.meta.km_level_size();
        if level_size == LEVEL_SIZE_MAX {
            return Err(crate::result::LevelExpansionError::MaxLevelSizeReached);
        }
        self.resize_state = Expanding;
        let result = self.do_expand(level_size + 1);
        self.resize_state = NotResizing;
        return result;
    }

    pub fn clear(&mut self) -> LevelClearResult {
        self.io.clear()?;
        self.expand_count = 0;
        self.item_counts = [0, 0];
        self.resize_state = NotResizing;
        Ok(())
    }
}

//noinspection DuplicatedCode
#[cfg(test)]
mod test {
    use std::assert_matches::assert_matches;
    use std::fs;
    use std::path::Path;

    use crate::io::IOEndianness;
    use crate::level_io::LevelHashIO;
    use crate::level_io::ValuesEntry;
    use crate::meta::MetaIO;
    use crate::reprs::ValuesData;
    use crate::result::LevelInsertionError;
    use crate::result::LevelUpdateError;
    use crate::size::SIZE_U64;
    use crate::util::align_8;
    use crate::util::generate_seeds;
    use crate::LevelHash;
    use crate::LevelHashOptions;

    use byteorder::ByteOrder;
    use gxhash::GxHasher;
    use std::hash::Hasher;

    fn gxhash(seed: u64, data: &[u8]) -> u64 {
        let mut hasher = GxHasher::with_seed(seed as i64);
        hasher.write(data);
        hasher.finish()
    }

    fn create_level_hash(
        name: &str,
        create_new: bool,
        conf: impl Fn(&mut LevelHashOptions),
    ) -> LevelHash {
        let dir_path = &format!("target/tests/level-hash/index-{}", name);
        let index_dir = Path::new(dir_path);
        if create_new && index_dir.exists() {
            fs::remove_dir_all(&index_dir).expect("Failed to delete existing directory");
        } else {
            fs::create_dir_all(&index_dir).expect("Failed to create directories");
        }

        let (s1, s2) = generate_seeds();
        let mut options = LevelHash::options();
        options
            .index_dir(index_dir)
            .index_name(name)
            .seeds(s1, s2)
            .hash_fns(self::gxhash, self::gxhash);

        conf(&mut options);

        options.build().expect("failed to create level hash")
    }

    fn default_level_hash(name: &str) -> LevelHash {
        create_level_hash(name, true, |options| {
            options.level_size(2).bucket_size(4).auto_expand(false);
        })
    }

    #[test]
    fn simple_insertion() {
        let mut hash = default_level_hash("insert");

        assert!(hash.insert(b"key1", b"value1").is_ok());
        assert_eq!(hash.get_value(b"key1"), b"value1".to_vec());
    }

    #[test]
    fn overflow() {
        let mut hash = default_level_hash("overflow");

        for i in 0..hash.total_slots() {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            assert!(hash.insert(&key, &value).is_ok());
        }

        assert_matches!(
            hash.insert(b"kkk", b"vvv").err(),
            Some(LevelInsertionError::LevelOverflow)
        );
    }

    #[test]
    fn removal() {
        let mut hash = default_level_hash("remove");
        assert!(hash.insert(b"key1", b"value1").is_ok());
        assert_eq!(hash.get_value(b"key1"), b"value1".to_vec());
        assert_eq!(hash.remove(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(hash.get_value(b"key1"), vec![]);
    }

    #[test]
    fn hash_clear() {
        let mut hash = default_level_hash("clear");
        assert!(hash.insert(b"key1", b"value1").is_ok());
        assert!(hash.insert(b"key2", b"value2").is_ok());

        hash.clear().expect("failed to clear level hash");

        assert_eq!(hash.get_value(b"key1"), vec![]);
        assert_eq!(hash.get_value(b"key2"), vec![]);
    }

    #[test]
    fn value_update_for_non_existent_entry() {
        let mut hash = default_level_hash("value_update_for_non_existent_entry");
        assert!(hash.insert(b"k", b"v").is_ok());

        let entry = hash.find_slot(b"k");
        assert!(entry.is_some());

        let entry = entry.unwrap();
        assert_eq!(entry.0.key(&mut hash.io.values), b"k".to_vec());
        assert_eq!(entry.0.value(&mut hash.io.values), b"v".to_vec());

        assert_matches!(
            hash.update(b"kk", b"vv"),
            Err(LevelUpdateError::SlotNotFound)
        );
    }

    #[test]
    fn value_update_for_existing_entry_with_null_value() {
        let mut hash = default_level_hash("update-existing-with-null-value");

        assert!(hash.insert(b"k", &[]).is_ok());
        let entry = hash.find_slot(b"k").unwrap();
        assert_eq!(entry.0.key(&mut hash.io.values), b"k".to_vec());
        assert_eq!(entry.0.value(&mut hash.io.values), vec![]);

        assert_eq!(
            hash.update(b"k", b"newV").expect("failed to update entry"),
            vec![]
        );

        let entry = hash.find_slot(b"k").unwrap();
        assert_eq!(entry.0.key(&mut hash.io.values), b"k".to_vec());
        assert_eq!(entry.0.value(&mut hash.io.values), b"newV".to_vec());
    }

    #[test]
    fn existing_level_init() {
        let mut hash = default_level_hash("init-existing");
        hash.insert(b"key", b"value").unwrap();
        hash.insert(b"null", &[]).unwrap();
        hash.insert(
            b"long",
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
        )
        .unwrap();

        assert_eq!(hash.get_value(b"key"), b"value".to_vec());
        assert_eq!(hash.get_value(b"null"), vec![]);
        assert_eq!(
            hash.get_value(b"long"),
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".to_vec()
        );

        let mut hash = create_level_hash("init-existing", false, |options| {
            options.level_size(2).bucket_size(4).auto_expand(false);
        });

        assert_eq!(hash.get_value(b"key"), b"value".to_vec());
        assert_eq!(hash.get_value(b"null"), vec![]);
        assert_eq!(
            hash.get_value(b"long"),
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".to_vec()
        );
    }

    #[test]
    fn level_hash_expand() {
        let mut hash = create_level_hash("expand", true, |options| {
            options.level_size(5).bucket_size(10).auto_expand(false);
        });

        let slots = hash.total_slots() - hash.io.meta.km_bucket_size() as u32;
        for i in 0..slots {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            hash.insert(&key, &value).expect("failed to insert entry");
        }

        println!("Inserted {} entries", slots);

        for i in 0..slots {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            assert_eq!(hash.get_value(&key), value);
        }

        hash.expand().expect("failed to expand level hash");

        for i in 0..slots {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            assert_eq!(hash.get_value(key.as_slice()), value.clone());
        }
    }

    #[test]
    fn expansion_with_huge_slot_count() {
        let mut hash = create_level_hash("expand-huge-slot-count", true, |options| {
            options.level_size(15).bucket_size(4).auto_expand(false);
        });

        let mut i = 0u32;
        while hash
            .insert(
                format!("key{}", i).as_bytes(),
                format!("value{}", i).as_bytes(),
            )
            .is_ok()
        {
            i += 1;
        }

        println!("inserted {} entries", i);
        println!("load factor: {}", hash.load_factor());

        hash.expand().expect("failed to expand level hash");

        println!("load factor after expansion: {}", hash.load_factor());

        for j in 0..i {
            let key = format!("key{}", j).as_bytes().to_vec();
            let value = format!("value{}", j).as_bytes().to_vec();
            assert_eq!(hash.get_value(&key), value);
        }
    }

    #[test]
    fn meta_after_expand() {
        let mut hash = create_level_hash("meta-after-expand", true, |options| {
            options.level_size(5).bucket_size(10).auto_expand(false);
        });

        let l0_size: u64 = hash.top_level_bucket_count() as u64
            * hash.io.meta.km_bucket_size() as u64
            * LevelHashIO::KEYMAP_ENTRY_SIZE_BYTES;

        assert_eq!(hash.io.meta.km_level_size(), 5);
        assert_eq!(hash.io.meta.km_bucket_size(), 10);
        assert_eq!(hash.io.meta.km_l0_addr(), 0);
        assert_eq!(hash.io.meta.km_l1_addr(), l0_size);

        hash.expand().expect("failed to expand level hash");

        assert_eq!(hash.io.meta.km_level_size(), 6);
        assert_eq!(hash.io.meta.km_bucket_size(), 10);
        assert_eq!(hash.io.meta.km_l0_addr(), l0_size + (l0_size >> 1));
        assert_eq!(hash.io.meta.km_l1_addr(), 0);
    }

    #[test]
    fn values_file_binary_repr() {
        let file_name = "values-binary-repr";
        let mut hash = create_level_hash(file_name, true, |options| {
            options.auto_expand(false);
        });

        // min_size + "key1".len() + "value1".len()
        let entry_size = ValuesEntry::ENTRY_SIZE_MIN + 4 + 6;

        for i in 0..10 {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            assert!(hash.insert(&key, &value).is_ok());
        }

        let index_file = &format!(
            "target/tests/level-hash/index-{}/{}.index",
            file_name, file_name
        );
        let mut input = fs::read(&index_file).expect("Unable to read index file");
        let input = input.as_mut_slice();

        assert_eq!(
            IOEndianness::read_u64(input),
            LevelHashIO::VALUES_MAGIC_NUMBER
        );

        let mut pos = SIZE_U64; // magic number
        let input = &mut input[pos as usize..];

        let mut prev_entry = 0;
        for i in 0..10 {
            let p = pos - SIZE_U64; // magic number len
            let data = unsafe { &*(input[p as usize..].as_ptr() as *const ValuesData) };

            assert_eq!(data.entry_size, entry_size);
            assert_eq!(data.prev_entry, prev_entry);
            assert_eq!(data.next_entry, align_8(p + entry_size) + 1);
            assert_eq!(data.key_size, 4);
            assert_eq!(data.value_size, 6);
            assert_eq!(
                &input[(p + ValuesEntry::OFF_KEY) as usize
                    ..(p + ValuesEntry::OFF_KEY + data.key_size as u64) as usize],
                format!("key{}", i).as_bytes()
            );
            assert_eq!(
                &input[(p + ValuesEntry::OFF_KEY + data.key_size as u64) as usize
                    ..(p + ValuesEntry::OFF_KEY + data.key_size as u64 + data.value_size as u64)
                        as usize],
                format!("value{}", i).as_bytes()
            );

            prev_entry = p + 1;
            pos = align_8(p + entry_size) + SIZE_U64; // + magic number len
        }
    }

    #[test]
    fn values_file_binary_repr_when_head_is_removed() {
        let file_name = "values-binary-repr-rem-head";
        let mut hash = create_level_hash(file_name, true, |options| {
            options.auto_expand(false);
        });

        // min_size + "key1".len() + "value1".len()
        let entry_size = ValuesEntry::ENTRY_SIZE_MIN + 4 + 6;
        let count = 10;

        for i in 0..count {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            assert!(hash.insert(&key, &value).is_ok());
        }

        let index_file = format!(
            "target/tests/level-hash/index-{}/{}.index",
            file_name, file_name
        );
        let meta_file = format!(
            "target/tests/level-hash/index-{}/{}.index._meta",
            file_name, file_name
        );

        let val_bytes = fs::read(&index_file).expect("Unable to read index file");
        let input = val_bytes.as_slice();
        let meta_io = MetaIO::new(
            Path::new(&meta_file),
            hash.io.meta.km_level_size(),
            hash.io.meta.km_bucket_size(),
        )
        .expect("failed to create meta file");

        assert_eq!(meta_io.val_head_addr(), 1);
        assert_eq!(
            meta_io.val_tail_addr(),
            (align_8(entry_size) * (count - 1)) + 1
        );

        assert_eq!(
            IOEndianness::read_u64(input),
            LevelHashIO::VALUES_MAGIC_NUMBER
        );

        let pos = SIZE_U64; // magic number
        let input = &input[pos as usize..];

        let data = unsafe { &*(input.as_ptr() as *mut ValuesData) };
        assert_eq!(data.entry_size, entry_size);
        assert_eq!(data.prev_entry, 0);
        assert_eq!(data.next_entry, align_8(entry_size) + 1);
    }

    #[test]
    fn values_file_binary_repr_when_tail_is_removed() {
        let file_name = "values-binary-repr-rem-tail";
        let mut hash = create_level_hash(file_name, true, |options| {
            options.auto_expand(false);
        });

        // min_size + "key1".len() + "value1".len()
        let entry_size = ValuesEntry::ENTRY_SIZE_MIN + 4 + 6;
        let count = 10;

        for i in 0..count {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            assert!(hash.insert(&key, &value).is_ok());
        }

        let index_file = &format!(
            "target/tests/level-hash/index-{}/{}.index",
            file_name, file_name
        );
        let meta_file = &format!(
            "target/tests/level-hash/index-{}/{}.index._meta",
            file_name, file_name
        );

        {
            let val_bytes = fs::read(index_file).expect("Unable to read index file");
            let input = val_bytes.as_slice();
            let meta_io = MetaIO::new(
                Path::new(&meta_file),
                hash.io.meta.km_level_size(),
                hash.io.meta.km_bucket_size(),
            )
            .expect("failed to create meta file");

            assert_eq!(meta_io.val_head_addr(), 1);
            assert_eq!(
                meta_io.val_tail_addr(),
                (align_8(entry_size) * (count - 1)) + 1
            );

            assert_eq!(
                IOEndianness::read_u64(input),
                LevelHashIO::VALUES_MAGIC_NUMBER
            );

            let pos = SIZE_U64; // magic number
            let input = &input[pos as usize..];

            let pos = (align_8(entry_size) * (count - 1)) as usize;
            let data = unsafe { &*(input[pos as usize..].as_ptr() as *const ValuesData) };
            assert_eq!(data.entry_size, entry_size);
            assert_eq!(data.prev_entry, (align_8(entry_size) * (count - 2)) + 1);
            assert_eq!(data.next_entry, (align_8(entry_size) * count) + 1);
        }

        hash.remove(&format!("key{}", count - 1).as_bytes());

        {
            let val_bytes = fs::read(index_file).expect("Unable to read index file");
            let input = val_bytes.as_slice();
            let meta_io = MetaIO::new(
                Path::new(&meta_file),
                hash.io.meta.km_level_size(),
                hash.io.meta.km_bucket_size(),
            )
            .expect("failed to create meta file");

            assert_eq!(meta_io.val_head_addr(), 1);
            assert_eq!(
                meta_io.val_tail_addr(),
                (align_8(entry_size) * (count - 2)) + 1
            );

            assert_eq!(
                IOEndianness::read_u64(input),
                LevelHashIO::VALUES_MAGIC_NUMBER
            );

            let pos = SIZE_U64 as usize + (align_8(entry_size) * (count - 1)) as usize;
            let input = &val_bytes[pos..];
            let data = unsafe { &*(input.as_ptr() as *const ValuesData) };
            assert_eq!(data.entry_size, 0);

            let pos = SIZE_U64 as usize + (align_8(entry_size) * (count - 2)) as usize;
            let input = &val_bytes[pos..];
            let data = unsafe { &*(input.as_ptr() as *const ValuesData) };
            assert_eq!(data.entry_size, entry_size);
            assert_eq!(data.prev_entry, (align_8(entry_size) * (count - 3)) + 1);
            assert_eq!(data.next_entry, (align_8(entry_size) * (count - 1)) + 1);
        }
    }

    #[test]
    fn values_file_binary_repr_when_entry_is_removed_in_the_middle() {
        let file_name = "values-binary-repr-rem-tail";
        let mut hash = create_level_hash(file_name, true, |options| {
            options.auto_expand(false);
        });

        // min_size + "key1".len() + "value1".len()
        let entry_size = ValuesEntry::ENTRY_SIZE_MIN + 4 + 6;
        let entry_size_aligned = align_8(entry_size);
        let count = 10;

        for i in 0..count {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            assert!(hash.insert(&key, &value).is_ok());
        }

        let index_file = &format!(
            "target/tests/level-hash/index-{}/{}.index",
            file_name, file_name
        );
        let meta_file = &format!(
            "target/tests/level-hash/index-{}/{}.index._meta",
            file_name, file_name
        );

        // let to_remove_idx = rand::thread_rng().gen_range(2..count - 2);
        let to_remove_idx = 5;

        {
            let val_bytes = fs::read(index_file).expect("Unable to read index file");
            let input = val_bytes.as_slice();
            let meta_io = MetaIO::new(
                Path::new(&meta_file),
                hash.io.meta.km_level_size(),
                hash.io.meta.km_bucket_size(),
            )
            .expect("failed to create meta file");

            assert_eq!(meta_io.val_head_addr(), 1);
            assert_eq!(
                meta_io.val_tail_addr(),
                (entry_size_aligned * (count - 1)) + 1
            );

            assert_eq!(
                IOEndianness::read_u64(input),
                LevelHashIO::VALUES_MAGIC_NUMBER
            );

            let pos = SIZE_U64 as usize + (entry_size_aligned * to_remove_idx) as usize;
            let input = &val_bytes[pos..];
            let data = unsafe { &*(input.as_ptr() as *const ValuesData) };
            assert_eq!(data.entry_size, entry_size);
            assert_eq!(
                data.prev_entry,
                (entry_size_aligned * (to_remove_idx - 1)) + 1
            );
            assert_eq!(
                data.next_entry,
                (entry_size_aligned * (to_remove_idx + 1)) + 1
            );
            assert_eq!(data.key_size, 4);

            assert_eq!(
                &val_bytes[(pos + ValuesEntry::OFF_KEY as usize)
                    ..(pos + ValuesEntry::OFF_KEY as usize + data.key_size as usize)],
                format!("key{}", to_remove_idx).as_bytes()
            );
        }

        hash.remove(&format!("key{}", to_remove_idx).as_bytes());

        {
            let val_bytes = fs::read(index_file).expect("Unable to read index file");
            let input = val_bytes.as_slice();
            let meta_io = MetaIO::new(
                Path::new(&meta_file),
                hash.io.meta.km_level_size(),
                hash.io.meta.km_bucket_size(),
            )
            .expect("failed to create meta file");

            assert_eq!(meta_io.val_head_addr(), 1);
            assert_eq!(
                meta_io.val_tail_addr(),
                (entry_size_aligned * (count - 1)) as u64 + 1
            );

            assert_eq!(
                IOEndianness::read_u64(input),
                LevelHashIO::VALUES_MAGIC_NUMBER
            );

            let pos = SIZE_U64 as usize + (entry_size_aligned * to_remove_idx) as usize;
            let input = &val_bytes[pos..];
            let data = unsafe { &*(input.as_ptr() as *const ValuesData) };
            assert_eq!(data.entry_size, 0);

            let pos = SIZE_U64 as usize + (entry_size_aligned * (to_remove_idx - 1)) as usize;
            let input = &val_bytes[pos..];
            let data = unsafe { &*(input.as_ptr() as *const ValuesData) };
            assert_eq!(data.entry_size, entry_size);
            assert_eq!(
                data.prev_entry,
                (entry_size_aligned * (to_remove_idx - 2)) + 1
            );
            assert_eq!(
                data.next_entry,
                (entry_size_aligned * (to_remove_idx + 1)) + 1
            );
            assert_eq!(data.key_size, 4);

            assert_eq!(
                &val_bytes[(pos + ValuesEntry::OFF_KEY as usize)
                    ..(pos + ValuesEntry::OFF_KEY as usize + data.key_size as usize)],
                format!("key{}", to_remove_idx - 1).as_bytes()
            );

            let pos = SIZE_U64 as usize + (entry_size_aligned * (to_remove_idx + 1)) as usize;
            let input = &val_bytes[pos..];
            let data = unsafe { &*(input.as_ptr() as *const ValuesData) };
            assert_eq!(data.entry_size, entry_size);
            assert_eq!(
                data.prev_entry,
                (entry_size_aligned * (to_remove_idx - 1)) + 1
            );
            assert_eq!(
                data.next_entry,
                (entry_size_aligned * (to_remove_idx + 2)) + 1
            );
            assert_eq!(data.key_size, 4);

            assert_eq!(
                &val_bytes[(pos + ValuesEntry::OFF_KEY as usize)
                    ..(pos + ValuesEntry::OFF_KEY as usize + data.key_size as usize)],
                format!("key{}", to_remove_idx + 1).as_bytes()
            );
        }
    }
}
