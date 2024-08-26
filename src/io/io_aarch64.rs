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

#[cfg(not(target_feature = "neon"))]
compile_error!("Required feature: neon");

/// Check if the given memory regions are NOT equal using Neon instructions. Note that this may not
/// check the whole memory region.
///
/// ## Returns
///
/// `true` if the memory regions are NOT equal, `false` otherwise. If `false` is returned, then the number of
/// bytes that were compared is written to the `len_compared` parameter.
#[target_feature(enable = "neon")]
pub(crate) unsafe fn __memneq(
    lhs: *const u8,
    rhs: *const u8,
    len_compared: &mut usize,
    len: usize,
) -> bool {
    use std::arch::aarch64::vceqq_u8;
    use std::arch::aarch64::vld1q_u8;
    use std::arch::aarch64::vminvq_u8;

    let mut i = 0usize;
    while i + 16 <= len {
        let lchunk = vld1q_u8(lhs.add(i));
        let rchunk = vld1q_u8(rhs.add(i));
        let cmp = vceqq_u8(lchunk, rchunk);
        if vminvq_u8(cmp) != 0xFF {
            return true;
        }
        i += 16;
    }

    *len_compared = i;
    return false;
}
