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

pub unsafe fn __memeq(lhs: *const u8, rhs: *const u8, len: usize) -> bool {
    libc::memcmp(lhs as *const libc::c_void, rhs as *const libc::c_void, len) == 0
}

pub unsafe fn __memcpy(dst: *mut u8, src: *const u8, len: usize) {
    libc::memcpy(dst as *mut libc::c_void, src as *const libc::c_void, len);
}
