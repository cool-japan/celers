//! Message transactions example
//!
//! This example demonstrates message transaction support with ACID guarantees:
//! - Transaction lifecycle (begin, commit, rollback)
//! - Isolation levels (ReadUncommitted, ReadCommitted, RepeatableRead, Serializable)
//! - Transaction states (Active, Committed, RolledBack)
//! - Error handling and recovery
//!
//! Run with: cargo run --example transactions

use celers_kombu::*;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    println!("💳 Message Transactions Example\n");

    // =========================================================================
    // 1. Transaction Basics
    // =========================================================================
    println!("📝 Transaction Basics");
    println!("   ACID guarantees for message operations\n");

    println!("   ACID Properties:");
    println!("     Atomicity: All operations succeed or all fail");
    println!("     Consistency: System state remains valid");
    println!("     Isolation: Transactions don't interfere");
    println!("     Durability: Committed changes persist");

    println!("\n   Transaction Lifecycle:");
    println!("     1. Begin: Start transaction with isolation level");
    println!("     2. Execute: Perform publish/consume operations");
    println!("     3. Commit: Make changes permanent");
    println!("     4. Rollback: Undo all changes on error");

    // =========================================================================
    // 2. Isolation Levels
    // =========================================================================
    println!("\n🔒 Isolation Levels");
    println!("   Control transaction visibility and consistency\n");

    let isolation_levels = vec![
        (
            IsolationLevel::ReadUncommitted,
            "Lowest isolation - may read uncommitted changes (dirty reads)",
            "Highest performance, minimal locking",
            "Analytics, non-critical data",
        ),
        (
            IsolationLevel::ReadCommitted,
            "Read only committed data, prevent dirty reads",
            "Balanced performance and consistency",
            "Most applications, default choice",
        ),
        (
            IsolationLevel::RepeatableRead,
            "Consistent reads within transaction, prevent phantom reads",
            "Lower performance, more locking",
            "Financial transactions, reporting",
        ),
        (
            IsolationLevel::Serializable,
            "Highest isolation - full serializability",
            "Lowest performance, maximum locking",
            "Critical operations, strict consistency",
        ),
    ];

    for (level, description, performance, use_case) in isolation_levels {
        println!("   {:?}", level);
        println!("     Description: {}", description);
        println!("     Performance: {}", performance);
        println!("     Use case: {}", use_case);
        println!();
    }

    // =========================================================================
    // 3. Transaction States
    // =========================================================================
    println!("📊 Transaction States");
    println!("   Track transaction progress\n");

    let states = vec![
        (
            TransactionState::Active,
            "Transaction in progress",
            "Can execute operations",
        ),
        (
            TransactionState::Committed,
            "Transaction completed successfully",
            "Changes are permanent",
        ),
        (
            TransactionState::RolledBack,
            "Transaction cancelled",
            "All changes undone",
        ),
    ];

    for (state, description, meaning) in states {
        println!("   {:?}", state);
        println!("     Description: {}", description);
        println!("     Meaning: {}", meaning);
        println!();
    }

    // =========================================================================
    // 4. Basic Transaction Example
    // =========================================================================
    println!("💼 Basic Transaction Example");
    println!("   Simple publish transaction with commit\n");

    let tx_id = Uuid::new_v4();
    println!("   Transaction ID: {}", tx_id);
    println!("   Isolation Level: {:?}", IsolationLevel::ReadCommitted);

    println!("\n   Steps:");
    println!("     1. BEGIN TRANSACTION");
    println!("        State: {:?}", TransactionState::Active);

    println!("\n     2. PUBLISH messages");
    println!("        • Message 1: order_created");
    println!("        • Message 2: payment_initiated");
    println!("        • Message 3: inventory_reserved");
    println!("        State: {:?}", TransactionState::Active);

    println!("\n     3. COMMIT TRANSACTION");
    println!("        All messages published atomically");
    println!("        State: {:?}", TransactionState::Committed);

    println!("\n   Result: ✅ Success");
    println!("     All 3 messages visible to consumers");
    println!("     Changes are durable and permanent");

    // =========================================================================
    // 5. Transaction with Rollback
    // =========================================================================
    println!("\n🔄 Transaction with Rollback");
    println!("   Handle errors with automatic rollback\n");

    let tx_id = Uuid::new_v4();
    println!("   Transaction ID: {}", tx_id);

    println!("\n   Steps:");
    println!("     1. BEGIN TRANSACTION");
    println!("        State: {:?}", TransactionState::Active);

    println!("\n     2. PUBLISH messages");
    println!("        • Message 1: transfer_initiated ✅");
    println!("        • Message 2: account_debited ✅");
    println!("        • Message 3: account_credited ❌ ERROR");
    println!("        State: {:?}", TransactionState::Active);

    println!("\n     3. ROLLBACK TRANSACTION");
    println!("        Error detected - rolling back all changes");
    println!("        State: {:?}", TransactionState::RolledBack);

    println!("\n   Result: 🔄 Rolled Back");
    println!("     No messages visible to consumers");
    println!("     System state unchanged");
    println!("     Can retry entire transaction");

    // =========================================================================
    // 6. Complex Transaction Scenario
    // =========================================================================
    println!("\n🏢 Complex Transaction Scenario");
    println!("   E-commerce order processing with multiple steps\n");

    let order_id = Uuid::new_v4();
    println!("   Order ID: {}", order_id);
    println!("   Isolation Level: {:?}", IsolationLevel::Serializable);

    println!("\n   Business Logic:");
    println!("     Goal: Process order atomically");
    println!("     Steps: Validate → Reserve → Charge → Ship");
    println!("     Requirement: All steps succeed or all fail");

    println!("\n   Transaction Execution:");
    println!("     1. BEGIN TRANSACTION (Serializable)");

    let steps = vec![
        ("Validate Order", "Check inventory availability", true, "✅"),
        ("Reserve Inventory", "Lock items for this order", true, "✅"),
        ("Charge Payment", "Process credit card", true, "✅"),
        ("Create Shipment", "Generate shipping label", true, "✅"),
        ("Update Status", "Mark order as processing", true, "✅"),
    ];

    for (i, (step, action, success, icon)) in steps.iter().enumerate() {
        println!("\n     {}. {} {}", i + 2, step, icon);
        println!("        Action: {}", action);
        if *success {
            println!("        Status: Success");
        }
    }

    println!("\n     7. COMMIT TRANSACTION");
    println!("        All operations succeeded");

    println!("\n   Result: ✅ Order Processed");
    println!("     Inventory: Reserved and decremented");
    println!("     Payment: Charged successfully");
    println!("     Shipment: Created and queued");
    println!("     Status: Order confirmed to customer");

    // =========================================================================
    // 7. Transaction Failure Scenario
    // =========================================================================
    println!("\n❌ Transaction Failure Scenario");
    println!("   Payment failure with automatic rollback\n");

    let order_id = Uuid::new_v4();
    println!("   Order ID: {}", order_id);

    println!("\n   Transaction Execution:");
    println!("     1. BEGIN TRANSACTION (Serializable)");

    let failure_steps = [
        ("Validate Order", "Check inventory availability", true, "✅"),
        ("Reserve Inventory", "Lock items for this order", true, "✅"),
        ("Charge Payment", "Process credit card", false, "❌"),
    ];

    for (i, (step, action, success, icon)) in failure_steps.iter().enumerate() {
        println!("\n     {}. {} {}", i + 2, step, icon);
        println!("        Action: {}", action);
        if *success {
            println!("        Status: Success");
        } else {
            println!("        Status: FAILED - Insufficient funds");
            println!("        Error: Payment declined by processor");
        }
    }

    println!("\n     5. ROLLBACK TRANSACTION");
    println!("        Payment failed - undoing all changes");

    println!("\n   Result: 🔄 Transaction Rolled Back");
    println!("     Inventory: Reservation released");
    println!("     Payment: No charge attempted after failure");
    println!("     Shipment: Not created");
    println!("     Status: Order cancelled, customer notified");

    println!("\n   Consistency Maintained:");
    println!("     ✓ No partial state changes");
    println!("     ✓ Inventory not incorrectly reserved");
    println!("     ✓ No orphaned shipments");
    println!("     ✓ Customer sees accurate status");

    // =========================================================================
    // 8. Concurrent Transactions
    // =========================================================================
    println!("\n👥 Concurrent Transactions");
    println!("   Isolation level comparison\n");

    println!("   Scenario: Two users buying last item");
    println!("     Initial inventory: 1 item");
    println!("     Transaction A: User 1 attempts purchase");
    println!("     Transaction B: User 2 attempts purchase");

    println!("\n   With ReadCommitted:");
    println!("     • Both transactions read inventory: 1 item");
    println!("     • Both proceed with purchase");
    println!("     • Result: ⚠️  Race condition - oversell!");

    println!("\n   With Serializable:");
    println!("     • Transaction A: Begin, read inventory: 1");
    println!("     • Transaction B: Begin, blocked waiting");
    println!("     • Transaction A: Reserve item, commit");
    println!("     • Transaction B: Read inventory: 0, cancel");
    println!("     • Result: ✅ Consistent - no oversell");

    println!("\n   Key Takeaway:");
    println!("     Higher isolation = Better consistency");
    println!("     Higher isolation = Lower concurrency");
    println!("     Choose based on requirements");

    // =========================================================================
    // 9. Best Practices
    // =========================================================================
    println!("\n💡 Transaction Best Practices\n");

    let practices = vec![
        (
            "Keep transactions short",
            "Minimize lock duration, improve throughput",
        ),
        (
            "Use appropriate isolation",
            "Balance consistency vs performance",
        ),
        (
            "Handle failures gracefully",
            "Always handle rollback scenarios",
        ),
        ("Avoid nested transactions", "Keep transaction logic simple"),
        ("Test rollback paths", "Ensure cleanup works correctly"),
        (
            "Monitor transaction time",
            "Detect and fix slow transactions",
        ),
        ("Use idempotent operations", "Safe to retry on failure"),
    ];

    for (practice, reason) in practices {
        println!("   ✓ {}", practice);
        println!("     {}", reason);
        println!();
    }

    // =========================================================================
    // 10. Use Cases
    // =========================================================================
    println!("🎯 Common Use Cases\n");

    let use_cases = vec![
        (
            "Financial Transactions",
            IsolationLevel::Serializable,
            "Money transfers, payment processing, account updates",
        ),
        (
            "Order Processing",
            IsolationLevel::Serializable,
            "E-commerce orders, inventory management, fulfillment",
        ),
        (
            "Audit Logging",
            IsolationLevel::ReadCommitted,
            "Activity logs, event tracking, compliance records",
        ),
        (
            "Batch Processing",
            IsolationLevel::ReadCommitted,
            "Bulk imports, data migrations, periodic jobs",
        ),
        (
            "Analytics",
            IsolationLevel::ReadUncommitted,
            "Reports, dashboards, non-critical metrics",
        ),
    ];

    for (use_case, recommended_level, examples) in use_cases {
        println!("   {}", use_case);
        println!("     Recommended: {:?}", recommended_level);
        println!("     Examples: {}", examples);
        println!();
    }

    println!("✨ Transactions example completed successfully!");
    Ok(())
}
