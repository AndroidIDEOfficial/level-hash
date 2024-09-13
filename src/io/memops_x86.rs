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

use std::arch::x86_64::__m128i;
use std::arch::x86_64::_mm_cmpeq_epi8;
use std::arch::x86_64::_mm_loadu_si128;
use std::arch::x86_64::_mm_movemask_epi8;
use std::arch::x86_64::_mm_storeu_si128;

const MEMCMP_MIN_LEN: usize = 16;

/// Check if the given memory regions are equal using Neon instructions.
///
/// ## Returns
///
/// `true` if the memory regions are equal, `false` otherwise.
#[cfg(target_feature = "sse2")]
pub unsafe fn __memeq(lhs: *const u8, rhs: *const u8, len: usize) -> bool {
    if len < MEMCMP_MIN_LEN {
        return libc::memcmp(lhs as *const libc::c_void, rhs as *const libc::c_void, len) == 0;
    }

    let mut i = 0usize;
    while i + 16 <= len {
        let mem_chunk = _mm_loadu_si128(lhs.add(i) as *const __m128i);
        let arr_chunk = _mm_loadu_si128(rhs.add(i) as *const __m128i);
        let cmp = _mm_cmpeq_epi8(mem_chunk, arr_chunk);
        if _mm_movemask_epi8(cmp) == 0xFFFF {
            return false;
        }
        i += 16;
    }

    // Compare the remaining bytes
    if i < len {
        let remaining = len - i;
        let lhs_ptr = lhs.add(i);
        let rhs_ptr = rhs.add(i);
        return libc::memcmp(
            lhs_ptr as *const libc::c_void,
            rhs_ptr as *const libc::c_void,
            remaining,
        ) == 0;
    }

    true
}

#[cfg(target_feature = "sse2")]
pub unsafe fn __memcpy(dst: *mut u8, src: *const u8, len: usize) {
    let mut i = 0;

    while i + 16 <= len {
        _mm_storeu_si128(
            dst.add(i) as *mut _,
            _mm_loadu_si128(src.add(i) as *const _),
        );
        i += 16;
    }

    if i < len {
        libc::memcpy(
            dst.add(i) as *mut libc::c_void,
            src.add(i) as *const libc::c_void,
            len - i,
        );
    }
}
