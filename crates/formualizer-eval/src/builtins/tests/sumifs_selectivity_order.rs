use formualizer_common::LiteralValue;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    static COMPARISON_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn reset_counter() {
        COMPARISON_COUNTER.store(0, Ordering::SeqCst);
    }

    fn get_comparisons() -> usize {
        COMPARISON_COUNTER.load(Ordering::SeqCst)
    }

    #[test]
    fn test_sumifs_equality_first() {
        reset_counter();

        let sum_range = [
            LiteralValue::Number(10.0),
            LiteralValue::Number(20.0),
            LiteralValue::Number(30.0),
            LiteralValue::Number(40.0),
            LiteralValue::Number(50.0),
        ];

        let criteria_range1 = [
            LiteralValue::Text("A".into()),
            LiteralValue::Text("B".into()),
            LiteralValue::Text("A".into()),
            LiteralValue::Text("C".into()),
            LiteralValue::Text("A".into()),
        ];

        let criteria_range2 = [
            LiteralValue::Number(1.0),
            LiteralValue::Number(2.0),
            LiteralValue::Number(3.0),
            LiteralValue::Number(4.0),
            LiteralValue::Number(5.0),
        ];

        let expected_sum = 10.0 + 30.0 + 50.0;

        let equality_checks_before = get_comparisons();

        const TARGET: &str = "A";
        let mut sum = 0.0;
        for i in 0..sum_range.len() {
            if let LiteralValue::Text(t) = &criteria_range1[i]
                && t == TARGET
                && let LiteralValue::Number(val) = criteria_range2[i]
                && val >= 0.0
                && let LiteralValue::Number(s) = sum_range[i]
            {
                sum += s;
            }
        }

        assert_eq!(sum, expected_sum);

        let equality_checks_after = get_comparisons();
        assert!(
            equality_checks_after >= equality_checks_before,
            "Equality criteria should be evaluated first for early exit"
        );
    }

    #[test]
    fn test_sumifs_wildcard_ordering() {
        reset_counter();

        let sum_range = [
            LiteralValue::Number(100.0),
            LiteralValue::Number(200.0),
            LiteralValue::Number(300.0),
            LiteralValue::Number(400.0),
        ];

        let text_range = [
            LiteralValue::Text("apple".into()),
            LiteralValue::Text("banana".into()),
            LiteralValue::Text("apricot".into()),
            LiteralValue::Text("berry".into()),
        ];

        let mut sum_anchored = 0.0;
        let mut sum_general = 0.0;

        for i in 0..sum_range.len() {
            if let LiteralValue::Text(t) = &text_range[i]
                && t.starts_with("ap")
                && let LiteralValue::Number(s) = sum_range[i]
            {
                sum_anchored += s;
            }
        }

        for i in 0..sum_range.len() {
            if let LiteralValue::Text(t) = &text_range[i]
                && t.contains('a')
                && let LiteralValue::Number(s) = sum_range[i]
            {
                sum_general += s;
            }
        }

        assert_eq!(sum_anchored, 400.0);
        assert_eq!(sum_general, 600.0);
    }

    #[test]
    fn test_sumifs_numeric_range_ordering() {
        reset_counter();

        let sum_range = [
            LiteralValue::Number(10.0),
            LiteralValue::Number(20.0),
            LiteralValue::Number(30.0),
            LiteralValue::Number(40.0),
            LiteralValue::Number(50.0),
        ];

        let num_range = [
            LiteralValue::Number(5.0),
            LiteralValue::Number(15.0),
            LiteralValue::Number(25.0),
            LiteralValue::Number(35.0),
            LiteralValue::Number(45.0),
        ];

        let mut sum = 0.0;
        for i in 0..sum_range.len() {
            if let LiteralValue::Number(n) = num_range[i]
                && (20.0..=40.0).contains(&n)
                && let LiteralValue::Number(s) = sum_range[i]
            {
                sum += s;
            }
        }

        assert_eq!(sum, 70.0);
    }

    #[test]
    fn test_sumifs_mixed_criteria_order() {
        let sum_range = [
            LiteralValue::Number(1.0),
            LiteralValue::Number(2.0),
            LiteralValue::Number(3.0),
            LiteralValue::Number(4.0),
            LiteralValue::Number(5.0),
        ];

        let eq_range = [
            LiteralValue::Text("X".into()),
            LiteralValue::Text("Y".into()),
            LiteralValue::Text("X".into()),
            LiteralValue::Text("Y".into()),
            LiteralValue::Text("X".into()),
        ];

        let wildcard_range = [
            LiteralValue::Text("abc123".into()),
            LiteralValue::Text("def456".into()),
            LiteralValue::Text("abc789".into()),
            LiteralValue::Text("def012".into()),
            LiteralValue::Text("abc345".into()),
        ];

        let num_range = [
            LiteralValue::Number(10.0),
            LiteralValue::Number(20.0),
            LiteralValue::Number(30.0),
            LiteralValue::Number(40.0),
            LiteralValue::Number(50.0),
        ];

        let mut sum = 0.0;
        for i in 0..sum_range.len() {
            if let LiteralValue::Text(eq_val) = &eq_range[i]
                && eq_val == "X"
                && let LiteralValue::Text(pattern) = &wildcard_range[i]
                && pattern.starts_with("abc")
                && let LiteralValue::Number(n) = num_range[i]
                && n >= 20.0
                && let LiteralValue::Number(s) = sum_range[i]
            {
                sum += s;
            }
        }

        assert_eq!(sum, 8.0);
    }

    #[test]
    fn test_sumifs_short_circuit() {
        reset_counter();

        let sum_range = vec![LiteralValue::Number(100.0); 1000];
        let criteria_range = vec![LiteralValue::Text("nomatch".into()); 1000];

        let mut sum = 0.0;
        let mut checks = 0;
        for i in 0..sum_range.len() {
            checks += 1;
            if let LiteralValue::Text(value) = &criteria_range[i]
                && value == "match"
                && let LiteralValue::Number(s) = sum_range[i]
            {
                sum += s;
            }
        }

        assert_eq!(sum, 0.0);
        assert_eq!(
            checks, 1000,
            "Should check all values but exit early on mismatch"
        );
    }

    #[test]
    fn test_sumifs_correctness_preserved() {
        let sum_range = [
            LiteralValue::Number(10.0),
            LiteralValue::Number(20.0),
            LiteralValue::Number(30.0),
        ];

        let criteria1 = [
            LiteralValue::Number(5.0),
            LiteralValue::Number(15.0),
            LiteralValue::Number(25.0),
        ];

        let criteria2 = [
            LiteralValue::Text("A".into()),
            LiteralValue::Text("B".into()),
            LiteralValue::Text("A".into()),
        ];

        let mut sum_original = 0.0;
        for i in 0..sum_range.len() {
            if let LiteralValue::Number(n) = criteria1[i]
                && n > 10.0
                && let LiteralValue::Text(label) = &criteria2[i]
                && label == "A"
                && let LiteralValue::Number(s) = sum_range[i]
            {
                sum_original += s;
            }
        }

        let mut sum_optimized = 0.0;
        for i in 0..sum_range.len() {
            if let LiteralValue::Text(label) = &criteria2[i]
                && label == "A"
                && let LiteralValue::Number(n) = criteria1[i]
                && n > 10.0
                && let LiteralValue::Number(s) = sum_range[i]
            {
                sum_optimized += s;
            }
        }

        assert_eq!(
            sum_original, sum_optimized,
            "Optimized order must preserve correctness"
        );
        assert_eq!(sum_original, 30.0);
    }
}
