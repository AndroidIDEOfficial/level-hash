use std::path::Path;

use level_hash::{generate_seeds, LevelHash};

fn main() {
    let index_dir = Path::new(".");
    let (seed_1, seed_2) = generate_seeds();
    let mut hash = LevelHash::options()
        .auto_expand(true)
        .bucket_size(10)
        .level_size(13)
        .index_dir(&index_dir)
        .index_name("test")
        .seeds(seed_1, seed_2)
        .build();

    for i in 0..100_000 {
        assert!(hash.insert(
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes()
        ));
    }
}
