use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Circuit Breaker States
#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    /// Normal operation — requests flow through
    Closed,
    /// Tripped — all requests are instantly rejected
    Open,
    /// Recovery — allows one test request to determine if upstream is healthy
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "CLOSED"),
            CircuitState::Open => write!(f, "OPEN"),
            CircuitState::HalfOpen => write!(f, "HALF-OPEN"),
        }
    }
}

/// Thread-safe Circuit Breaker
///
/// Protects downstream services from cascading failures.
/// When consecutive failures exceed `failure_threshold`, the circuit trips OPEN
/// and rejects all requests for `cooldown` duration. After cooldown, it
/// transitions to HALF-OPEN and allows a single probe request.
pub struct CircuitBreaker {
    inner: Mutex<CircuitBreakerState>,
}

struct CircuitBreakerState {
    state: CircuitState,
    failure_count: usize,
    failure_threshold: usize,
    cooldown: Duration,
    last_failure_time: Option<Instant>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker.
    ///
    /// - `failure_threshold`: Number of consecutive failures before tripping to Open
    /// - `cooldown_seconds`: Seconds to wait in Open state before trying Half-Open
    pub fn new(failure_threshold: usize, cooldown_seconds: u64) -> Self {
        CircuitBreaker {
            inner: Mutex::new(CircuitBreakerState {
                state: CircuitState::Closed,
                failure_count: 0,
                failure_threshold,
                cooldown: Duration::from_secs(cooldown_seconds),
                last_failure_time: None,
            }),
        }
    }

    /// Check if a request is allowed to pass through.
    ///
    /// Returns `Ok(())` if allowed, `Err(state_description)` if blocked.
    pub fn check(&self) -> Result<(), String> {
        let mut state = self.inner.lock().unwrap();

        match state.state {
            CircuitState::Closed => Ok(()),
            CircuitState::Open => {
                // Check if cooldown period has elapsed
                if let Some(last_failure) = state.last_failure_time {
                    if last_failure.elapsed() >= state.cooldown {
                        // Transition to Half-Open — allow one probe request
                        state.state = CircuitState::HalfOpen;
                        Ok(())
                    } else {
                        let remaining = state.cooldown - last_failure.elapsed();
                        Err(format!(
                            "Circuit breaker is OPEN. Retry in {:.1}s",
                            remaining.as_secs_f64()
                        ))
                    }
                } else {
                    // No recorded failure time — shouldn't happen, but recover gracefully
                    state.state = CircuitState::HalfOpen;
                    Ok(())
                }
            }
            CircuitState::HalfOpen => {
                // Already in half-open, allow the probe request
                Ok(())
            }
        }
    }

    /// Record a successful request. Resets the circuit to Closed.
    pub fn record_success(&self) {
        let mut state = self.inner.lock().unwrap();
        state.failure_count = 0;
        state.state = CircuitState::Closed;
    }

    /// Record a failed request. May trip the circuit to Open.
    pub fn record_failure(&self) {
        let mut state = self.inner.lock().unwrap();
        state.failure_count += 1;
        state.last_failure_time = Some(Instant::now());

        if state.failure_count >= state.failure_threshold {
            state.state = CircuitState::Open;
        }
    }

    /// Get the current state (for telemetry/dashboard)
    pub fn get_state(&self) -> CircuitState {
        let state = self.inner.lock().unwrap();
        state.state.clone()
    }

    /// Get the current consecutive failure count
    pub fn failure_count(&self) -> usize {
        let state = self.inner.lock().unwrap();
        state.failure_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_starts_closed() {
        let cb = CircuitBreaker::new(3, 5);
        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert!(cb.check().is_ok());
    }

    #[test]
    fn test_trips_after_threshold() {
        let cb = CircuitBreaker::new(3, 5);

        // 2 failures — still closed
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert!(cb.check().is_ok());

        // 3rd failure — trips open
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);
        assert!(cb.check().is_err());
    }

    #[test]
    fn test_success_resets() {
        let cb = CircuitBreaker::new(3, 5);
        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // Reset
        assert_eq!(cb.failure_count(), 0);
        assert_eq!(cb.get_state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_after_cooldown() {
        let cb = CircuitBreaker::new(2, 1); // 1 second cooldown

        // Trip the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);

        // Wait for cooldown
        thread::sleep(Duration::from_millis(1100));

        // Should transition to half-open and allow the probe
        assert!(cb.check().is_ok());
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_half_open_success_closes() {
        let cb = CircuitBreaker::new(2, 1);

        cb.record_failure();
        cb.record_failure();
        thread::sleep(Duration::from_millis(1100));

        // Probe request allowed (half-open)
        assert!(cb.check().is_ok());
        
        // Probe succeeds — should close the circuit
        cb.record_success();
        assert_eq!(cb.get_state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_failure_reopens() {
        let cb = CircuitBreaker::new(2, 1);

        cb.record_failure();
        cb.record_failure();
        thread::sleep(Duration::from_millis(1100));

        // Probe request allowed (half-open)
        assert!(cb.check().is_ok());
        
        // Probe fails — should re-open the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.get_state(), CircuitState::Open);
    }
}
