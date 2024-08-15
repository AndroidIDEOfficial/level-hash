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

use crate::io::MappedFile;
use crate::types::OffT;

impl MappedFile {
    pub(crate) fn remap(&mut self, size: OffT) {
        // reassigning drops the previous mmap which unmaps the file
        // then we map the file again with the new size
        self.map = MappedFile::do_map(&self.fd, self.off, size);
        self.size = size;
    }
}
