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

/// Check if the given memory regions are equal using Neon instructions.
///
/// ## Returns
///
/// `true` if the memory regions are equal, `false` otherwise.
pub unsafe fn __memeq(lhs: *const u8, rhs: *const u8, len: usize) -> bool {
    use std::arch::aarch64::vceqq_u8;
    use std::arch::aarch64::vld1q_u8;
    use std::arch::aarch64::vminvq_u8;

    let mut i = 0usize;
    while i + 16 <= len {
        let lchunk = vld1q_u8(lhs.add(i));
        let rchunk = vld1q_u8(rhs.add(i));
        let cmp = vceqq_u8(lchunk, rchunk);
        if vminvq_u8(cmp) == 0xFF {
            return false;
        }
        i += 16;
    }
    return true;
}

pub unsafe fn __memcpy(dst: *mut u8, src: *const u8, len: usize) {
    let mut i = 0;

    while i + 16 <= len {
        let src_chunk = std::arch::aarch64::vld1q_u8(src.add(i) as *const u8);
        std::arch::aarch64::vst1q_u8(dst.add(i) as *mut u8, src_chunk);
        i += 16;
    }

    while i < len {
        *dst.add(i) = *src.add(i);
        i += 1;
    }
}
