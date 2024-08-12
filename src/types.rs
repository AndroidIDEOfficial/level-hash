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

/// Type used to represent offsets in file.
pub type OffT = u64;

pub type LevelKeyT = [u8];
pub type LevelValueT = [u8];

pub type LevelSizeT = u8;
pub type BucketSizeT = u8;

pub(crate) type _LevelIdxT = u32;
pub(crate) type _BucketIdxT = u32;
pub(crate) type _SlotIdxT = u32;
