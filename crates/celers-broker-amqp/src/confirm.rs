//! Publisher-confirm result classification.
//!
//! AMQP publisher confirms only exist on a channel that issued
//! `confirm.select`. On a plain channel `lapin` resolves the returned
//! [`PublisherConfirm`](lapin::PublisherConfirm) immediately with
//! [`Confirmation::NotRequested`], which is *not* a broker acknowledgement.
//! [`classify_confirmation`] turns the three possible outcomes into a
//! `Result` so that a negative acknowledgement or an unroutable (returned)
//! message can never be recorded as a successful publish.

use celers_kombu::{BrokerError, Result};
use lapin::Confirmation;

/// Classify a resolved publisher confirmation.
///
/// * `Ack(None)` - the broker took responsibility for the message.
/// * `Ack(Some(returned))` - the broker accepted the publish but the message
///   was unroutable and came back via `basic.return` (only possible for a
///   `mandatory` publish). This is a delivery failure.
/// * `Nack(_)` - the broker refused the message; it was **not** stored.
/// * `NotRequested` - confirms are off on this channel. That is an error when
///   the broker is configured to use publisher confirms (the confirmation is
///   meaningless), and a success otherwise.
///
/// # Errors
///
/// Returns [`BrokerError::OperationFailed`] for every outcome that is not a
/// genuine broker acknowledgement.
pub(crate) fn classify_confirmation(confirmation: Confirmation, confirms_enabled: bool) -> Result<()> {
    match confirmation {
        Confirmation::Ack(None) => Ok(()),
        Confirmation::Ack(Some(returned)) => Err(BrokerError::OperationFailed(format!(
            "Message returned as unroutable by the broker ({} {}): exchange='{}' routing_key='{}'",
            returned.reply_code,
            returned.reply_text,
            returned.delivery.exchange,
            returned.delivery.routing_key
        ))),
        Confirmation::Nack(returned) => {
            let detail = returned
                .map(|returned| {
                    format!(
                        " (exchange='{}' routing_key='{}')",
                        returned.delivery.exchange, returned.delivery.routing_key
                    )
                })
                .unwrap_or_default();
            Err(BrokerError::OperationFailed(format!(
                "Broker sent a negative acknowledgement (nack); the message was not stored{}",
                detail
            )))
        }
        Confirmation::NotRequested => {
            if confirms_enabled {
                Err(BrokerError::OperationFailed(
                    "Publisher confirms are enabled but the channel is not in confirm mode; \
                     the publish was not acknowledged by the broker"
                        .to_string(),
                ))
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_without_return_is_success() {
        assert!(classify_confirmation(Confirmation::Ack(None), true).is_ok());
        assert!(classify_confirmation(Confirmation::Ack(None), false).is_ok());
    }

    #[test]
    fn nack_is_a_failure() {
        let err = classify_confirmation(Confirmation::Nack(None), true)
            .expect_err("nack must not be treated as success");
        assert!(err.to_string().contains("negative acknowledgement"));
        assert!(classify_confirmation(Confirmation::Nack(None), false).is_err());
    }

    #[test]
    fn not_requested_is_a_failure_only_when_confirms_are_enabled() {
        let err = classify_confirmation(Confirmation::NotRequested, true)
            .expect_err("unconfirmed publish must not count as confirmed");
        assert!(err.to_string().contains("confirm mode"));
        assert!(classify_confirmation(Confirmation::NotRequested, false).is_ok());
    }

    #[test]
    fn returned_message_is_a_failure() {
        let returned = lapin::message::BasicReturnMessage {
            delivery: lapin::message::Delivery::mock(
                1,
                "celery".into(),
                "missing_queue".into(),
                false,
                b"{}".to_vec(),
            ),
            reply_code: 312,
            reply_text: "NO_ROUTE".into(),
        };
        let err = classify_confirmation(Confirmation::Ack(Some(returned)), true)
            .expect_err("returned message must not count as delivered");
        assert!(err.to_string().contains("unroutable"));
        assert!(err.to_string().contains("missing_queue"));
    }
}
