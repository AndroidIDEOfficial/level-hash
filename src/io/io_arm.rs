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

// Feature check for neon,v7 is not available for arm target.
// As a result, we perform a check at runtime.

use crate::GetArmInfo;

/// Check if the given memory regions are NOT equal using Neon instructions. Note that this may not
/// check the whole memory region.
///
/// ## Returns
///
/// `true` if the memory regions are NOT equal, `false` otherwise. If `false` is returned, then the number of
/// bytes that were compared is written to the `len_compared` parameter.
pub(crate) unsafe fn __memneq(
    lhs: *const u8,
    rhs: *const u8,
    len_compared: &mut usize,
    len: usize,
) -> bool {
    static mut has_neon: Option<bool> = None;
    if let None = has_neon {
        has_neon = Some(GetArmInfo().features.neon() != 0)
    }

    if !has_neon.is_some_and(|has| has) {
        // No Neon support, fall back to memcmp
        let result = unsafe {
            libc::memcmp(lhs as *const libc::c_void, rhs as *const libc::c_void, len) == 0
        };
        *len_compared = len;
        return result;
    }

    use core::arch::arm::vceqq_u8;
    use core::arch::arm::vgetq_lane_u64;
    use core::arch::arm::vld1q_u8;
    use core::arch::arm::vreinterpretq_u64_u8;

    let mut i = 0usize;
    while i + 16 <= len {
        let lchunk = vld1q_u8(lhs.add(i));
        let rchunk = vld1q_u8(rhs.add(i));
        let cmp = vceqq_u8(lchunk, rchunk);
        let cast = vreinterpretq_u64_u8(cmp);
        if vgetq_lane_u64(cast, 0) != 0xFF || vgetq_lane_u64(cast, 1) != 0xFF {
            return false;
        }
        i += 16;
    }

    *len_compared = i;
    return false;
}
