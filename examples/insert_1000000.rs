use std::path::Path;

use std::hash::Hasher;
use gxhash::GxHasher;
use level_hash::{generate_seeds, LevelHash};

fn gxhash(seed: u64, data: &[u8]) -> u64 {
    let mut hasher = GxHasher::with_seed(seed as i64);
    hasher.write(data);
    hasher.finish()
}

fn main() {
    let index_dir = Path::new("target");
    let (seed_1, seed_2) = generate_seeds();
    let mut hash = LevelHash::options()
        .auto_expand(true)
        .bucket_size(10)
        .level_size(13)
        .index_dir(&index_dir)
        .index_name("insert-1000000")
        .seeds(seed_1, seed_2)
        .hash_fns(self::gxhash, self::gxhash)
        .build()
        .expect("failed to create level hash");

    let start = std::time::Instant::now();
    for i in 0..1_000_000 {
        assert!(hash
            .insert(
                format!("longlonglongkey{}", i).as_bytes(),
                format!("longlonglongvalue{}", i).as_bytes()
            )
            .is_ok());
    }
    let end = std::time::Instant::now();
    let duration = end.duration_since(start).as_millis();
    println!("Inserted in {}ms", duration);
}
