// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! A [`celers_core::Broker`] over this crate's transports.
//!
//! # The gap this closes
//!
//! Two different broker traits live in this workspace:
//!
//! * [`celers_core::Broker`] is the **task-queue** abstraction —
//!   `enqueue`/`dequeue`/`ack`/`reject`/`defer`/`cancel` over
//!   [`celers_core::SerializedTask`]. It is what `celers_worker::Worker`
//!   consumes from.
//! * [`crate::Broker`] (with [`Producer`](crate::Producer),
//!   [`Consumer`](crate::Consumer) and [`Transport`](crate::Transport)) is the
//!   **message-transport** abstraction — `publish`/`consume`/`purge`/
//!   `create_queue` over [`celers_protocol::Message`].
//!
//! `celers_broker_amqp::AmqpBroker` and `celers_broker_sqs::SqsBroker`
//! implement the second one only, so until this module existed a worker could
//! not consume from RabbitMQ or SQS at all — the transports could move
//! messages, but nothing turned those messages into tasks a worker would run.
//!
//! # The pieces
//!
//! * [`task_to_message`] / [`message_to_task`] convert between the two models
//!   without losing metadata (see [`convert`] for exactly how, and for what
//!   happens to a message from a foreign producer).
//! * [`CoreBrokerTransport`] is the seam a transport implements to lend the
//!   adapter its native batch, defer and delayed-publish operations. Every
//!   method has a working default, so opting in costs one empty `impl`.
//! * [`KombuBrokerAdapter`] is the [`celers_core::Broker`] itself.
//!
//! # Using it
//!
//! The broker crates each expose a ready-made alias and constructor
//! (`AmqpBroker::into_core_broker`, `SqsBroker::into_core_broker`), so a caller
//! normally never names this type:
//!
//! ```no_run
//! # use celers_kombu::{MockBroker, core_adapter::KombuBrokerAdapter};
//! # use celers_core::{Broker, SerializedTask};
//! # async fn example() -> celers_core::Result<()> {
//! let broker = KombuBrokerAdapter::new(MockBroker::new(), "celery");
//!
//! broker
//!     .enqueue(SerializedTask::new("tasks.add".to_string(), vec![1, 2]))
//!     .await?;
//!
//! if let Some(message) = broker.dequeue().await? {
//!     // ... run it ...
//!     broker
//!         .ack(&message.task.metadata.id, message.receipt_handle.as_deref())
//!         .await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Availability
//!
//! Behind the `core-adapter` feature, which pulls in `celers-core`. The AMQP
//! and SQS broker crates enable it for you.

mod adapter;
mod convert;
mod transport;

pub use adapter::{KombuBrokerAdapter, DEFAULT_POLL_TIMEOUT};
pub use convert::{message_to_task, task_to_message, wire_priority, TASK_METADATA_HEADER};
pub use transport::CoreBrokerTransport;

/// The in-memory transport is adaptable too, which is what makes an end-to-end
/// worker test possible without a broker on the machine.
///
/// It takes the defaults wholesale: a mock has no native batch API to lend, and
/// exercising the *defaults* through it is exactly what the hermetic tests
/// want.
impl CoreBrokerTransport for crate::MockBroker {}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
