# Level Hash

Level Hash is write-optimized and high-performance hashing index scheme for persistent memory, based on
the work of Pengfei Zuo, Yu Hua, and Jie Wu, Huazhong University of Science and Technology.

## Limitations

- Linux/Android only!
- Only supported on `aarch64` and `x86_64`!

## Structure

The peristent level hash is backed by three files :

- `{name}.index._meta` - metadata for the level hash.
- `{name}.index._keymap` - maps the slot position
   `level_num, bucket_idx, slot_idx` to the address of entry in the
   `${name}` file. This is known as the keymap.
- `{name}.index` - Contains all the variable-length entry values of the level hash.

### Keymap

Structure of the keymap file:

```
keymap {
  u64 magic_number;
  level levels[2];
  level interim_level?;
}
level {
   bucket buckets[2^(level_size-level_idx)];
}
bucket {
   u64 slots[bucket_size];
}
```

The `keymap` struct contains fields :
- `magic_number` - Magic number for uniquely identifying the keymap file.
- `levels` - The levels of the keymap. There are 2 levels, the top level (index 0)
   and the bottom level (index 1).
- `interim_level` - The temporary level that is used to move the slots from
   the bottom level to the yet-to-be top level during expansion process.

The `level` struct contains fields :
- `buckets` - The buckets of the level. The number of buckets depends on the
   index of the level and can be calculated using `2^(level_size-level_idx)`
   where `level_idx` is the index of the level for which the number of buckets
   is being calculated.

The `bucket` struct contains fields :
- `slots` - An array of size `bucket_size` that contains the 1-based, 64-bit
   address of the value entry in the values file. Note that the offset is
   1-based because 0 is considered as an invalid value for the address (hence,
   if the value is 0, then that slot is considered as an empty slot). This
   helps in recognizing if an entry in the keymap points to a valid address
   or not. We use `fallocate(3)` with `FALLOC_FL_PUNCH_HOLE` to punch holes in
   the files for regions which are unused. After this, reading that region of
   the file returns `0`.


### Values

Structure of the values file :

```
values {
   u64 magic_number;
   value values[];
   value {
     u64 entry_size;
     u64 prev_entry;
     u64 next_entry;
     u32 key_size;
     u32 value_size;
     u8 key[key_size];
     u8 value[value_size];
   }
}
```

The `values` structure constains fields :
- `magic_number` - A magic number that is used to identify the values file.
- `values` - The value entries.

Each `value` entry contains fields :
- `entry_size` - The size of the entry in bytes (all fields inclusive).
- `prev_entry` - The address of the previous entry in the values file.
- `next_entry` - The address of the next entry in the values file.
- `key_size` - The size of the key in bytes.
- `key` - The key of `key_size` 8-bit bytes.
- `value_size` - The size of the value in bytes.
- `value` - The value of `value_size` 8-bit bytes.

### Metadata

Structure of the metadata file :

```
meta {
   u32 values_version;
   u32 keymap_version;
   u64 values_head_entry;
   u64 values_tail_entry;
   u64 values_file_size_bytes;
   u8 km_level_size;
   u8 km_bucket_size;
   u64 km_l0_addr;
   u64 km_l1_addr;
}
```

The `meta` structure contains the fields :
- `values_version` - The version of the values file.
- `keymap_version` - The version of the keymap file.
- `values_head_entry` - The address of the first entry in the values file.
- `values_tail_entry` - The address of the last entry in the values file.
- `values_file_size_bytes` - The size of the values file in bytes.
- `km_level_size` - The level size of the level hash.
- `km_bucket_size` - The bucket size of the level hash.
- `km_l0_addr` - Address of the level 0 (top level) in the keymap.
- `km_l1_addr` - Address of the level 1 (bottom level) in the keymap.

## License

```
This project is part of AndroidIDE.

AndroidIDE is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

AndroidIDE is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
 along with AndroidIDE.  If not, see <https://www.gnu.org/licenses/>.
```
