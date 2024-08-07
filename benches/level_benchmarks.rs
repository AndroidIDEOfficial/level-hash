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
use std::fs;
use std::path::Path;
use std::time::Duration;

use criterion::black_box;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;

use level_hash::LevelHash;
use level_hash::util::generate_seeds;

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
        options.index_dir(index_dir)
        .index_name(name)
        .seeds(s1, s2);
    
    conf(&mut options);
    
    options.build()
}

fn bench_level_insert(c: &mut Criterion) {
    c.bench_function("insert", |b| {
        let mut hash = create_level_hash("insert", true, |ops| {
            ops.level_size(11)
                .bucket_size(4)
                .auto_expand(false)
                .unique_keys(false);
        });
        b.iter(|| {
            for i in 0..10000 {
                let key = black_box([i as u8]);
                let value = black_box([i as u8]);
                hash.insert(&key, &value);
            }
        })
    });
}

fn bench_level_lookup(c: &mut Criterion) {
    c.bench_function("lookup", |b| {
        let mut hash = create_level_hash("lookup", true, |ops| {
            ops.level_size(11)
                .bucket_size(4)
                .auto_expand(false)
                .unique_keys(false);
        });
        for i in 0..10000 {
            let key = [i as u8];
            let value = [i as u8];
            hash.insert(&key, &value);
        }
        b.iter(|| {
            for i in 0..10000 {
                let key = black_box([i as u8]);
                hash.get_value(&key);
            }
        })
    });
}

fn bench_level_delete(c: &mut Criterion) {
    c.bench_function("delete", |b| {
        let mut hash = create_level_hash("delete", true, |ops| {
            ops.level_size(11)
                .bucket_size(4)
                .auto_expand(false)
                .unique_keys(false);
        });
        for i in 0..10000 {
            let key = [i as u8];
            let value = [i as u8];
            hash.insert(&key, &value);
        }
        b.iter(|| {
            for i in 0..10000 {
                let key = black_box([i as u8]);
                hash.remove(&key);
            }
        })
    });
}

fn bench_level_insert_auto_expand(c: &mut Criterion) {
    c.bench_function("insert_auto_expand", |b| {
        let mut hash = create_level_hash("insert_auto_expand", true, |ops| {
            // 2^10*4+2^9*4 = 6144
            // we're inserting 10,000 entries
            // so this should expand
            ops.level_size(10)
                .bucket_size(4)
                .auto_expand(true)
                .unique_keys(false);
        });
        b.iter(|| {
            for i in 0..10000 {
                let key = black_box([i as u8]);
                let value = black_box([i as u8]);
                hash.insert(&key, &value);
            }
        })
    });
}

fn bench_level_mixed(c: &mut Criterion) {
    c.bench_function("CRD", |b| {
        let mut hash = create_level_hash("CRD", true, |ops| {
            ops.level_size(10)
                .bucket_size(4)
                .auto_expand(true)
                .unique_keys(false);
        });
        b.iter(|| {
            for i in 0..10000 {
                let key = black_box([i as u8]);
                let value = black_box([i as u8]);
                hash.insert(&key, &value);
                hash.get_value(&key);
                hash.remove(&key);
            }
        })
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(20).measurement_time(Duration::from_secs(40));
    targets = bench_level_insert, bench_level_lookup, bench_level_delete, bench_level_insert_auto_expand, bench_level_mixed
);
criterion_main!(benches);