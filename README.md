# photon: a modern experiment tracker in pure Rust

[![Tensaur](https://img.shields.io/badge/tensaur-grey.svg?logo=data:image/png%2bxml;base64,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)](https://tensaur.ai)
[![GitHub repo](https://img.shields.io/badge/github-tensaur/photon-blue?logo=github)](https://github.com/tensaur/photon)
[![Latest version](https://img.shields.io/crates/v/photon.svg)](https://crates.io/crates/photon)
[![Documentation](https://docs.rs/photon/badge.svg)](https://docs.rs/photon)
[![MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/tensaur/photon/blob/main/LICENSE-MIT)
[![Apache](https://img.shields.io/badge/license-Apache-blue.svg)](https://github.com/tensaur/photon/blob/main/LICENSE-APACHE)
[![Discord](https://img.shields.io/discord/1239275597704855572?label=discord)](https://discord.gg/PZ4GqDTMgv)

photon is a fast and modern experiment tracker written in Rust.

## Example

``` rust
fn main() {
    let mut run = photon::Run::builder()
        .start()
        .expect("failed to start run");

    println!("Run: {}", run.id());

    // Simulate a training loop
    for step in 0..200 {
        let loss = 1.0 / (1.0 + step as f64 * 0.05);
        let accuracy = 1.0 - loss;

        run.log("train/loss", loss, step).unwrap();
        run.log("train/accuracy", accuracy, step).unwrap();

        if step % 10 == 0 {
            let lr = 0.001 * 0.95_f64.powi(step as i32 / 10);
            run.log("train/lr", lr, step).unwrap();
        }
    }

    println!("Logged: {} points", run.points_logged());

    let stats = run.finish().expect("finish failed");
    // etc
}
```

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.

