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
use gxhash::GxHasher;

use std::fs;
use std::hash::Hasher;
use std::path::Path;
use std::time::Duration;

use criterion::black_box;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;

use level_hash::util::generate_seeds;
use level_hash::LevelHash;

fn gxhash(seed: u64, data: &[u8]) -> u64 {
    let mut hasher = GxHasher::with_seed(seed as i64);
    hasher.write(data);
    hasher.finish()
}

fn create_level_hash(
    name: &str,
    create_new: bool,
    conf: impl Fn(&mut level_hash::LevelHashOptions),
) -> LevelHash {
    let dir_path = &format!("target/tests/level-hash-benchmarks/{}", name);
    let index_dir = Path::new(dir_path);
    if index_dir.exists() && create_new {
        fs::remove_dir_all(&index_dir).expect("Failed to delete existing directory");
    } else {
        fs::create_dir_all(&index_dir).expect("Failed to create directories");
    }

    let (s1, s2) = generate_seeds();

    let mut options = LevelHash::options();
    options
        .index_dir(index_dir)
        .index_name(name)
        .seeds(s1, s2)
        .hash_fns(self::gxhash, self::gxhash);

    conf(&mut options);

    options.build().expect("failed to crate level hash")
}

fn bench_level_insert(c: &mut Criterion) {
    c.bench_function("insert", |b| {
        let mut hash = create_level_hash("insert", true, |ops| {
            ops.level_size(13)
                .bucket_size(10)
                .auto_expand(false)
                .unique_keys(false);
        });
        b.iter(|| {
            for i in 0..100000 {
                let key = black_box([i as u8]);
                let value = black_box([i as u8]);
                let _ = hash.insert(&key, &value);
            }
        })
    });
}

fn bench_level_lookup(c: &mut Criterion) {
    c.bench_function("lookup", |b| {
        let mut hash = create_level_hash("lookup", true, |ops| {
            ops.level_size(13)
                .bucket_size(10)
                .auto_expand(false)
                .unique_keys(false);
        });
        for i in 0..100000 {
            let key = [i as u8];
            let value = [i as u8];
            let _ = hash.insert(&key, &value);
        }
        b.iter(|| {
            for i in 0..100000 {
                let key = black_box([i as u8]);
                hash.get_value(&key);
            }
        })
    });
}

fn bench_level_delete(c: &mut Criterion) {
    c.bench_function("delete", |b| {
        let mut hash = create_level_hash("delete", true, |ops| {
            ops.level_size(13)
                .bucket_size(10)
                .auto_expand(false)
                .unique_keys(false);
        });
        for i in 0..100000 {
            let key = [i as u8];
            let value = [i as u8];
            let _ = hash.insert(&key, &value);
        }
        b.iter(|| {
            for i in 0..100000 {
                let key = black_box([i as u8]);
                hash.remove(&key);
            }
        })
    });
}

fn bench_level_insert_auto_expand(c: &mut Criterion) {
    c.bench_function("insert_auto_expand", |b| {
        let mut hash = create_level_hash("insert_auto_expand", true, |ops| {
            ops.level_size(10)
                .bucket_size(10)
                .auto_expand(true)
                .unique_keys(false);
        });
        b.iter(|| {
            for i in 0..100000 {
                let key = black_box([i as u8]);
                let value = black_box([i as u8]);
                let _ = hash.insert(&key, &value);
            }
        })
    });
}

criterion_group!(
    name = crud_benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(30));
    targets = bench_level_insert, bench_level_lookup, bench_level_delete, bench_level_insert_auto_expand
);
criterion_main!(crud_benches);
