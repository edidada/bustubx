use crate::{BustubxError, BustubxResult};

/// Evaluate contiguous partitions concurrently and gather in input order.
/// Every worker is joined, including when spawning or evaluation fails.
pub(super) fn ordered_map<T: Sync, R: Send>(
    input: &[T],
    workers: usize,
    evaluate: impl Fn(&T) -> R + Sync,
) -> BustubxResult<Vec<R>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    let workers = workers.max(1).min(input.len());
    if workers == 1 {
        return Ok(input.iter().map(evaluate).collect());
    }
    let chunk_size = input.len().div_ceil(workers);
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        let mut failure = None;
        for chunk in input.chunks(chunk_size) {
            let evaluate = &evaluate;
            match std::thread::Builder::new()
                .name("bustubx-project".into())
                .spawn_scoped(scope, move || {
                    chunk.iter().map(evaluate).collect::<Vec<R>>()
                }) {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    failure = Some(BustubxError::Execution(format!(
                        "Cannot start projection worker: {error}"
                    )));
                    break;
                }
            }
        }
        let mut output = Vec::with_capacity(input.len());
        for handle in handles {
            match handle.join() {
                Ok(rows) => output.extend(rows),
                Err(_) => {
                    failure = Some(BustubxError::Execution("Projection worker panicked".into()));
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(output),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};

    #[test]
    fn concurrent_workers_preserve_order() {
        let barrier = Barrier::new(4);
        let threads = Mutex::new(HashSet::new());
        let output = ordered_map(&[0, 1, 2, 3], 4, |value| {
            threads.lock().unwrap().insert(std::thread::current().id());
            barrier.wait();
            value * 2
        })
        .unwrap();
        assert_eq!(output, vec![0, 2, 4, 6]);
        assert_eq!(threads.into_inner().unwrap().len(), 4);
        assert_eq!(
            ordered_map(&[1, 2, 3, 4, 5], 3, |x| *x).unwrap(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(ordered_map(&[7], 64, |x| *x).unwrap(), vec![7]);
        assert!(ordered_map::<u8, u8>(&[], 4, |_| panic!())
            .unwrap()
            .is_empty());
        let caller = std::thread::current().id();
        ordered_map(&[1, 2], 1, |_| {
            assert_eq!(std::thread::current().id(), caller)
        })
        .unwrap();
    }

    #[test]
    fn joins_remaining_workers_after_panic() {
        let completed = AtomicUsize::new(0);
        let result = ordered_map(&[0, 1, 2, 3], 4, |value| {
            if *value == 0 {
                panic!("test worker failure");
            }
            completed.fetch_add(1, Ordering::SeqCst);
        });
        assert!(matches!(result, Err(BustubxError::Execution(_))));
        assert_eq!(completed.load(Ordering::SeqCst), 3);
    }
}
