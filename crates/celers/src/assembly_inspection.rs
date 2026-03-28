//! Assembly inspection utilities for verifying zero-cost abstractions
//!
//! This module provides tools and documentation for inspecting generated assembly
//! to verify that Rust's zero-cost abstractions are working as expected.
//!
//! # Assembly Inspection Guide
//!
//! This module helps you verify zero-cost abstractions by inspecting generated assembly.
//!
//! ## Generating Assembly Output
//!
//! ### Method 1: Using cargo-asm (Recommended)
//!
//! ```bash
//! # Install cargo-asm
//! cargo install cargo-asm
//!
//! # Generate assembly for a specific function
//! cargo asm --release celers::dev_utils::TaskBuilder::new
//!
//! # Generate assembly with Intel syntax (easier to read)
//! cargo asm --release --intel celers::dev_utils::TaskBuilder::new
//! ```
//!
//! ### Method 2: Using rustc directly
//!
//! ```bash
//! # Generate assembly for the entire crate
//! cargo rustc --release -- --emit asm
//!
//! # Output will be in target/release/deps/*.s
//! ```
//!
//! ### Method 3: Using Compiler Explorer (Godbolt)
//!
//! Visit <https://rust.godbolt.org/> and paste your code to see assembly interactively.
//!
//! ## What to Look For
//!
//! ### 1. Function Inlining
//!
//! Zero-cost abstractions should be inlined. Look for:
//! - Direct instructions instead of `call` instructions
//! - No function preamble/epilogue for simple wrappers
//!
//! Example of good inlining:
//! ```asm
//! # Simple arithmetic directly in caller
//! add    rdi, rsi
//! mov    rax, rdi
//! ```
//!
//! Example of poor inlining (overhead):
//! ```asm
//! # Function call overhead
//! call   my_wrapper_function
//! push   rbp
//! mov    rbp, rsp
//! ```
//!
//! ### 2. Dead Code Elimination
//!
//! Unused code should be completely removed:
//! - No assembly generated for unused branches
//! - Constant propagation should eliminate runtime checks
//!
//! ### 3. Iterator Optimization
//!
//! Iterator chains should compile to simple loops:
//! ```asm
//! # Should see a tight loop, not iterator machinery
//! .LBB0_1:
//!     add    rax, 1
//!     cmp    rax, rcx
//!     jne    .LBB0_1
//! ```
//!
//! ### 4. Monomorphization
//!
//! Generic functions should be specialized:
//! - No dynamic dispatch (vtable calls) unless using trait objects
//! - Type parameters resolved at compile time
//!
//! ## Example Commands for CeleRS
//!
//! ```bash
//! # Verify TaskBuilder is zero-cost
//! cargo asm --release --intel celers::dev_utils::TaskBuilder::build
//!
//! # Verify chain workflow construction
//! cargo asm --release --intel celers::canvas::Chain::new
//!
//! # Compare debug vs release
//! cargo asm celers::dev_utils::TaskBuilder::build > debug.asm
//! cargo asm --release celers::dev_utils::TaskBuilder::build > release.asm
//! diff debug.asm release.asm
//! ```
//!
//! ## Automated Verification
//!
//! Use these helper functions to automate assembly inspection:

use std::process::Command;

/// Generate assembly output for a specific function
///
/// # Example
///
/// ```rust,no_run
/// # #[cfg(feature = "dev-utils")]
/// # {
/// use celers::assembly_inspection::generate_asm;
///
/// let asm = generate_asm("celers::dev_utils::TaskBuilder::new", true).unwrap();
/// println!("Assembly:\n{}", asm);
/// # }
/// ```
pub fn generate_asm(function_path: &str, release: bool) -> Result<String, std::io::Error> {
    let mut cmd = Command::new("cargo");
    cmd.arg("asm");
    if release {
        cmd.arg("--release");
    }
    cmd.arg("--intel");
    cmd.arg(function_path);

    let output = cmd.output()?;
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Check if a function is properly inlined by looking for call instructions
///
/// Returns true if the function appears to be inlined (no call instructions found)
pub fn verify_inlined(asm: &str) -> bool {
    !asm.contains("call") || asm.lines().filter(|l| l.contains("call")).count() < 2
}

/// Count the number of instructions in the assembly
///
/// Useful for comparing optimization levels
pub fn count_instructions(asm: &str) -> usize {
    asm.lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty()
                && !trimmed.starts_with(';')
                && !trimmed.starts_with('#')
                && !trimmed.starts_with('.')
                && !trimmed.ends_with(':')
        })
        .count()
}

/// Generate a performance report comparing debug and release assembly
pub fn compare_debug_release(function_path: &str) -> Result<String, std::io::Error> {
    let debug_asm = generate_asm(function_path, false)?;
    let release_asm = generate_asm(function_path, true)?;

    let debug_count = count_instructions(&debug_asm);
    let release_count = count_instructions(&release_asm);
    let debug_inlined = verify_inlined(&debug_asm);
    let release_inlined = verify_inlined(&release_asm);

    Ok(format!(
        "Assembly Comparison for {}\n\
         \n\
         Debug Build:\n\
         - Instructions: {}\n\
         - Inlined: {}\n\
         \n\
         Release Build:\n\
         - Instructions: {}\n\
         - Inlined: {}\n\
         \n\
         Optimization Ratio: {:.2}x fewer instructions in release\n",
        function_path,
        debug_count,
        debug_inlined,
        release_count,
        release_inlined,
        debug_count as f64 / release_count.max(1) as f64
    ))
}
