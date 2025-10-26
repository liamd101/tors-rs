# Tors RS

A BitTorrent client implementation written in Rust for downloading and sharing files via the BitTorrent protocol.

## Features

- Download files from `.torrent` files
- Multi-file torrent support
- Concurrent peer connections
- Automatic piece verification using SHA-1 hashing
- Configurable peer limits and logging

## Installation

### Building from source
```bash
git clone https://github.com/liamd101/tors-rs.git
cd tors-rs
cargo build --release
```

The compiled binary will be available at `target/release/tors-rs`.

## Usage

### Basic usage

Download a torrent file:
```bash
tors-rs --file path/to/file.torrent
```

### Examples

Download with more concurrent peer connections:
```bash
tors-rs --file ubuntu-iso.torrent --max-peers 10
```

## Demo

<video src="https://raw.githubusercontent.com/liamd101/tors-rs/main/assets/demo.mp4" controls></video>

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.
