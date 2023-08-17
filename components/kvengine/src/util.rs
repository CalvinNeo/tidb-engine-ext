// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

/// A helper function to evenly distribute `total` into `count` parts.
/// Note: when `total` <= `count`, return `[1; total]`.
pub fn evenly_distribute(total: usize, count: usize) -> Vec<usize> {
    debug_assert!(count > 0);
    if total <= count {
        return vec![1; total];
    }
    let quotient = total / count;
    let remainder = total % count;
    let mut ret = vec![quotient; count];
    for i in 0..remainder {
        ret[i] += 1;
    }
    ret
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evenly_distribute() {
        assert_eq!(evenly_distribute(0, 1), Vec::<usize>::new());
        assert_eq!(evenly_distribute(10, 1), vec![10]);
        assert_eq!(evenly_distribute(10, 2), vec![5, 5]);
        assert_eq!(evenly_distribute(10, 3), vec![4, 3, 3]);
        assert_eq!(evenly_distribute(10, 4), vec![3, 3, 2, 2]);
        assert_eq!(evenly_distribute(10, 5), vec![2, 2, 2, 2, 2]);
        assert_eq!(evenly_distribute(10, 6), vec![2, 2, 2, 2, 1, 1]);
        assert_eq!(evenly_distribute(10, 7), vec![2, 2, 2, 1, 1, 1, 1]);
        assert_eq!(evenly_distribute(10, 8), vec![2, 2, 1, 1, 1, 1, 1, 1]);
        assert_eq!(evenly_distribute(10, 9), vec![2, 1, 1, 1, 1, 1, 1, 1, 1]);
        assert_eq!(evenly_distribute(10, 10), vec![1; 10]);
        assert_eq!(evenly_distribute(10, 11), vec![1; 10]);
        assert_eq!(evenly_distribute(10, 100), vec![1; 10]);
    }
}
