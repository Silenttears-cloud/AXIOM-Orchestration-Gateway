use std::sync::Mutex;
use std::time::Instant;

/// Thread-safe Token Bucket Rate Limiter
///
/// Implements the standard token bucket algorithm:
/// - Bucket starts full with `capacity` tokens
/// - Tokens refill at `refill_rate` tokens per second
/// - Each request consumes 1 token
/// - When empty → request is rejected (HTTP 429)
pub struct RateLimiter {
    inner: Mutex<RateLimiterState>,
}

struct RateLimiterState {
    capacity: f64,
    tokens: f64,
    refill_rate: f64, // tokens per second
    last_refill: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter.
    ///
    /// - `capacity`: Maximum burst size (how many tokens the bucket can hold)
    /// - `tokens_per_minute`: Steady-state refill rate
    pub fn new(capacity: f64, tokens_per_minute: f64) -> Self {
        let refill_rate = tokens_per_minute / 60.0;
        RateLimiter {
            inner: Mutex::new(RateLimiterState {
                capacity,
                tokens: capacity, // Start full
                refill_rate,
                last_refill: Instant::now(),
            }),
        }
    }

    /// Attempt to consume 1 token. Returns `true` if allowed, `false` if rate limited.
    pub fn try_acquire(&self) -> bool {
        let mut state = self.inner.lock().unwrap();

        // Refill tokens based on elapsed time
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        let new_tokens = elapsed * state.refill_rate;
        state.tokens = (state.tokens + new_tokens).min(state.capacity);
        state.last_refill = now;

        // Try to consume 1 token
        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Get the current number of available tokens (for telemetry/dashboard)
    pub fn available_tokens(&self) -> f64 {
        let state = self.inner.lock().unwrap();
        state.tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_burst_capacity() {
        // 5 burst capacity, 60 tokens/min
        let limiter = RateLimiter::new(5.0, 60.0);
        
        // Should allow 5 requests (burst)
        for _ in 0..5 {
            assert!(limiter.try_acquire());
        }
        // 6th should be denied
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_refill() {
        // 1 burst capacity, 600 tokens/min = 10/sec
        let limiter = RateLimiter::new(1.0, 600.0);
        
        // Consume the single token
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
        
        // Wait 150ms — should refill ~1.5 tokens, capped at 1.0
        thread::sleep(Duration::from_millis(150));
        assert!(limiter.try_acquire());
    }

    #[test]
    fn test_capacity_cap() {
        // 3 burst, 6000 tokens/min = 100/sec
        let limiter = RateLimiter::new(3.0, 6000.0);
        
        // Wait a bit (tokens should NOT exceed capacity)
        thread::sleep(Duration::from_millis(100));
        
        // Should allow exactly 3 (capped at capacity)
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());
    }
}
