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
use std::fs::File;
use std::path::Path;

use rand::rngs::StdRng;
use rand::RngCore;
use rand::SeedableRng;

/// Open the file in read-write mode, or panic.
pub(crate) fn file_open_or_panic(path: &Path, read: bool, write: bool, create: bool) -> File {
    let file = File::options()
        .read(read)
        .write(write)
        .create(create)
        .open(path);

    match file {
        Ok(file) => file,
        Err(why) => panic!("couldn't open {}: {}", path.display(), why),
    }
}

/// Generate a random seed pair.
pub fn generate_seeds() -> (u64, u64) {
    let mut rand = StdRng::seed_from_u64(6248403840530382848);

    let mut fseed: u64;
    let mut sseed: u64;

    loop {
        fseed = rand.next_u64();
        sseed = rand.next_u64();
        fseed = fseed << (rand.next_u64() % 63);
        sseed = sseed << (rand.next_u64() % 63);

        if fseed != sseed {
            break;
        }
    }

    (fseed, sseed)
}
