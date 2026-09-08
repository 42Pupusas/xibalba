//! A point in time by which an operation must have returned.

use std::time::{Duration, Instant};

use xibalba_proto::error::{ConnectionError, Error};

/// What is left of a [`Deadline`] when it is asked.
///
/// The three cases are kept apart because collapsing them loses the
/// distinction that matters at a socket API: `SO_RCVTIMEO` and
/// `SO_SNDTIMEO` read a zero duration as *no timeout*, so a remaining
/// time of zero handed to one of them produces an unbounded wait —
/// the exact opposite of what an expired deadline asks for.
/// [`Self::Remaining`] is therefore never zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeLeft {
    /// No deadline was set; the operation may take as long as it needs.
    Unbounded,
    /// Time remains. Never [`Duration::ZERO`].
    Remaining(Duration),
    /// The deadline has passed.
    Expired,
}

/// The instant by which a [`Connector::connect`](crate::connector::Connector::connect)
/// must return, whether it has a stream or not.
///
/// A duration cannot express this: connecting is several operations —
/// resolution, then one attempt per resolved address, then a handshake —
/// and giving each of them the same duration multiplies the bound by the
/// number of steps. An instant is shared by all of them and does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadline {
    expires_at: Option<Instant>,
}

impl Deadline {
    /// A deadline `budget` from now.
    #[must_use]
    pub fn after(budget: Duration) -> Self {
        Self {
            expires_at: Instant::now().checked_add(budget),
        }
    }

    /// No deadline. The caller accepts that the operation is bounded only
    /// by the OS, which for a blackholed address is on the order of two
    /// minutes and for DNS is unbounded.
    #[must_use]
    pub const fn never() -> Self {
        Self { expires_at: None }
    }

    #[must_use]
    pub fn time_left(&self) -> TimeLeft {
        let Some(expires_at) = self.expires_at else {
            return TimeLeft::Unbounded;
        };
        match expires_at.checked_duration_since(Instant::now()) {
            Some(left) if !left.is_zero() => TimeLeft::Remaining(left),
            _ => TimeLeft::Expired,
        }
    }

    #[must_use]
    pub fn is_expired(&self) -> bool {
        matches!(self.time_left(), TimeLeft::Expired)
    }

    /// Whichever of two deadlines expires first.
    ///
    /// An unbounded deadline never wins this comparison: any bounded
    /// deadline is sooner than one that never expires.
    #[must_use]
    pub fn sooner(a: Self, b: Self) -> Self {
        match (a.expires_at, b.expires_at) {
            (Some(x), Some(y)) => Self {
                expires_at: Some(x.min(y)),
            },
            (Some(_), None) => a,
            (None, Some(_)) => b,
            (None, None) => Self::never(),
        }
    }

    /// # Errors
    /// Returns [`ConnectionError::ConnectDeadlineExceeded`] once the deadline
    /// has passed, for a connector to return before starting further work.
    pub fn check(&self) -> Result<(), Error> {
        if self.is_expired() {
            return Err(Error::Connection(ConnectionError::ConnectDeadlineExceeded));
        }
        Ok(())
    }

    /// The timeout for one attempt that carries its own: `step`, or the time
    /// left when that is shorter.
    ///
    /// The result is never zero, so it is safe to hand to
    /// [`TcpStream::connect_timeout`](std::net::TcpStream::connect_timeout),
    /// which rejects a zero duration, and to the socket timeout setters,
    /// which read one as *no timeout*.
    ///
    /// # Errors
    /// Returns [`ConnectionError::ConnectDeadlineExceeded`] if no time is left,
    /// which is the signal to stop rather than to attempt with a zero timeout.
    pub fn clamp(&self, step: Duration) -> Result<Duration, Error> {
        match self.time_left() {
            TimeLeft::Unbounded => Ok(step),
            TimeLeft::Remaining(left) => Ok(step.min(left)),
            TimeLeft::Expired => Err(Error::Connection(ConnectionError::ConnectDeadlineExceeded)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_deadline_already_in_the_past_is_expired() {
        let deadline = Deadline::after(Duration::ZERO);
        assert!(deadline.is_expired());
        assert_eq!(deadline.time_left(), TimeLeft::Expired);
        assert!(deadline.check().is_err());
    }

    #[test]
    fn an_absent_deadline_never_expires() {
        let deadline = Deadline::never();
        assert!(!deadline.is_expired());
        assert_eq!(deadline.time_left(), TimeLeft::Unbounded);
        deadline.check().expect("an unbounded deadline is not due");
    }

    #[test]
    fn a_future_deadline_reports_time_remaining() {
        let deadline = Deadline::after(Duration::from_secs(30));
        match deadline.time_left() {
            TimeLeft::Remaining(left) => assert!(left <= Duration::from_secs(30)),
            other => panic!("expected time remaining, got {other:?}"),
        }
    }

    /// The whole point of an instant over a duration: three attempts against
    /// a 5s deadline get 5s between them, not 5s each.
    #[test]
    fn a_step_is_shortened_to_what_the_deadline_leaves() {
        let deadline = Deadline::after(Duration::from_millis(50));
        let allowed = deadline
            .clamp(Duration::from_secs(10))
            .expect("time remains");
        assert!(
            allowed <= Duration::from_millis(50),
            "an attempt may not outlive the deadline, got {allowed:?}"
        );
    }

    #[test]
    fn a_step_shorter_than_the_deadline_is_left_alone() {
        let deadline = Deadline::after(Duration::from_secs(30));
        let allowed = deadline
            .clamp(Duration::from_secs(1))
            .expect("time remains");
        assert_eq!(allowed, Duration::from_secs(1));
    }

    #[test]
    fn an_unbounded_deadline_leaves_the_step_alone() {
        let allowed = Deadline::never()
            .clamp(Duration::from_secs(7))
            .expect("unbounded");
        assert_eq!(allowed, Duration::from_secs(7));
    }

    /// A zero timeout means *no timeout* to `SO_RCVTIMEO`, so an expired
    /// deadline that reported `Ok(ZERO)` would produce an unbounded wait
    /// at exactly the moment it was supposed to stop one.
    #[test]
    fn an_expired_deadline_refuses_a_step_rather_than_allowing_zero() {
        let error = Deadline::after(Duration::ZERO)
            .clamp(Duration::from_secs(10))
            .expect_err("an expired deadline has no time to give");
        assert_eq!(
            error,
            Error::Connection(ConnectionError::ConnectDeadlineExceeded)
        );
    }

    #[test]
    fn sooner_picks_the_nearer_of_two_bounded_deadlines() {
        let near = Deadline::after(Duration::from_millis(50));
        let far = Deadline::after(Duration::from_secs(30));
        assert_eq!(Deadline::sooner(near, far), near);
        assert_eq!(Deadline::sooner(far, near), near);
    }

    #[test]
    fn sooner_prefers_a_bounded_deadline_over_an_unbounded_one() {
        let bounded = Deadline::after(Duration::from_secs(5));
        assert_eq!(Deadline::sooner(bounded, Deadline::never()), bounded);
        assert_eq!(Deadline::sooner(Deadline::never(), bounded), bounded);
    }

    #[test]
    fn sooner_of_two_unbounded_deadlines_is_unbounded() {
        assert_eq!(
            Deadline::sooner(Deadline::never(), Deadline::never()),
            Deadline::never()
        );
    }

    #[test]
    fn a_far_future_deadline_does_not_overflow_into_the_past() {
        let deadline = Deadline::after(Duration::MAX);
        assert!(
            !deadline.is_expired(),
            "an unrepresentable deadline must not read as already due"
        );
    }
}
