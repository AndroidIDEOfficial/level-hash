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

//! The structs in this module are sensitive. The layout of these
//! structures determine how the data is stored in file. Modifications
//! in this file must be made carefully.
//!
//! - All `struct`s must have `#[repr(C)]`

use crate::types::BucketSizeT;
use crate::types::LevelSizeT;
use crate::types::OffT;

macro_rules! def_layout {
    (struct $name:ident {
        $($prop_name:ident: $prop_typ:ty $(,)?)+
    }) => {
        #[repr(C)]
        #[derive(Debug)]
        pub(crate) struct $name {
            $(pub $prop_name: $prop_typ,)+
        }

        paste::paste! {
            #[allow(dead_code)]
            #[allow(non_upper_case_globals)]
            impl $name {
                $(
                    pub const [<SIZE_ $prop_name>]: usize = size_of::<$prop_typ>();
                )+
                pub const [<SIZE_ $name>]: usize = size_of::<$name>();
            }

            impl From<&[u8]> for &$name {
                fn from(value: &[u8]) -> Self {
                    assert!(value.len() >= $name::[<SIZE_ $name>]);
                    unsafe {
                        &*(value.as_ptr() as *const $name)
                    }
                }
            }

            impl From<&mut [u8]> for &mut $name {
                fn from(value: &mut [u8]) -> Self {
                    assert!(value.len() >= $name::[<SIZE_ $name>]);
                    unsafe {
                        &mut *(value.as_mut_ptr() as *mut $name)
                    }
                }
            }
        }
    };
}

def_layout!(
    struct LevelMeta {
        val_version: u32,
        km_version: u32,
        val_tail_addr: OffT,
        val_next_addr: OffT,
        val_file_size: OffT,
        km_level_size: LevelSizeT,
        km_bucket_size: BucketSizeT,
        km_l0_addr: OffT,
        km_l1_addr: OffT,
    }
);

def_layout!(
    struct ValuesData {
        // we store the key_size and value_size sequentially
        // so that we don't need to store an extra field 'entry_size' as
        // it can be calculated by adding the value of the below two fields
        // to the size of the fields itself (4 + 4)
        //
        // For example, for key=key1 and value=value1
        // len(key) = 4
        // len(value) = 6
        // sizeof(key_size) = 4
        // sizeof(value_size) = 4
        // entry_size = 4 + 6 + 4 + 4 = 18 bytes
        key_size: u32,
        value_size: u32,
        // key_size bytes of key
        // value_size bytes of value
    }
);
