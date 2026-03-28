#![no_main]

use libfuzzer_sys::fuzz_target;

#[cfg(feature = "compression")]
use celers_protocol::compression::{Compressor, CompressionType};

fuzz_target!(|data: &[u8]| {
    #[cfg(feature = "compression")]
    {
        // Test gzip compression
        #[cfg(feature = "gzip")]
        {
            let compressor = Compressor::new(CompressionType::Gzip).with_level(6);
            if let Ok(compressed) = compressor.compress(data) {
                let _ = compressor.decompress(&compressed);
            }
        }

        // Test zstd compression
        #[cfg(feature = "zstd-compression")]
        {
            let compressor = Compressor::new(CompressionType::Zstd).with_level(3);
            if let Ok(compressed) = compressor.compress(data) {
                let _ = compressor.decompress(&compressed);
            }
        }

        // Test auto-detection
        let _ = celers_protocol::compression::auto_decompress(data);
    }
});
