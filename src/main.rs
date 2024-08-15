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

    let start = std::time::Instant::now();
    for i in 0..100_000 {
        assert!(hash.insert(
            format!("longlonglongkey{}", i).as_bytes(),
            format!("longlonglongvalue{}", i).as_bytes()
        ));
    }
    let end = std::time::Instant::now();
    let duration = end.duration_since(start).as_millis();
    println!("Inserted in {}ms", duration);
}
