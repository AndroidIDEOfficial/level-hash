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

#[cfg(not(target_feature = "sse2"))]
compile_error!("Required feature: sse2");

/// Check if the given memory regions are NOT equal using Neon instructions. Note that this may not
/// check the whole memory region.
///
/// ## Returns
///
/// `true` if the memory regions are NOT equal, `false` otherwise. If `false` is returned, then the number of
/// bytes that were compared is written to the `len_compared` parameter.
#[target_feature(enable = "sse2")]
pub(crate) unsafe fn __memneq(
    lhs: *const u8,
    rhs: *const u8,
    len_compared: &mut usize,
    len: usize,
) -> bool {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8};

    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{__m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8};

    let mut i = 0usize;
    while i + 16 <= len {
        let mem_chunk = _mm_loadu_si128(lhs.add(i) as *const __m128i);
        let arr_chunk = _mm_loadu_si128(rhs.add(i) as *const __m128i);
        let cmp = _mm_cmpeq_epi8(mem_chunk, arr_chunk);
        if _mm_movemask_epi8(cmp) != 0xFFFF {
            return true;
        }
        i += 16;
    }

    *len_compared = i;
    return false;
}
