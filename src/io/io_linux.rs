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

use memmap2::RemapOptions;

use crate::io::MappedFile;
use crate::result::{IntoLevelIOErr, IntoLevelMapErr, LevelMapError, LevelResult};
use crate::types::OffT;

impl MappedFile {
    pub(crate) fn remap(&mut self, size: OffT) -> LevelResult<(), LevelMapError> {
        unsafe {
            self.map
                .remap(size as usize, RemapOptions::new().may_move(true))
        }
        .into_lvl_io_e_msg("failed to remap file".to_string())
        .into_lvl_mmap_err()?;

        self.size = size;

        Ok(())
    }
}
