#![allow(dead_code)]

use std::thread;
use std::sync::atomic::{ AtomicBool, Ordering };
use std::ops::{ Deref, DerefMut };
use std::cell::UnsafeCell;

/// A mutable memory location similar to `RefCell` but which allows at most one mutable borrow concurrently with any number of immutable borrows
///
/// Additional mutable borrows force a wait condition on the additional borrowing thread(s). Lock-free behavior is guaranteed as long as only one thread
/// ever writes, but locking may occur with multiple writing threads. All reads are guaranteed to be wait-free.
///
/// Because `SwmrCell` breaks Rusts aliasing rules by design it is inherently unsafe and is not suitable for use just anywhere
#[derive(Debug)]
pub struct SwmrCell<T> {
    writing: AtomicBool,
    value: UnsafeCell<T>,
}

unsafe impl<T> Send for SwmrCell<T> {}
unsafe impl<T> Sync for SwmrCell<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct Ref<'b, T: ?Sized + 'b> {
    value: &'b T,
}

impl<T: ?Sized> Deref for Ref<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        self.value
    }
}

#[derive(Debug)]
pub struct RefMut<'b, T: ?Sized + 'b> {
    value: &'b mut T,
    writing: &'b AtomicBool,
}

impl<T: ?Sized> Deref for RefMut<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        self.value
    }
}

impl <T: ?Sized> DerefMut for RefMut<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        self.value
    }
}

impl<'b, T> RefMut<'b, T> {
    fn new(value: &'b mut T, writing: &'b AtomicBool) -> RefMut<'b, T> {
        while writing.compare_and_swap(false, true, Ordering::Acquire) {
            thread::yield_now(); // todo: explore different spinning and yield techniques to optimize performance
        }

        RefMut {
            value: value,
            writing: writing,
        }
    }
}

impl<'b, T: ?Sized> Drop for RefMut<'b, T> {
    fn drop(&mut self) {
        self.writing.store(false, Ordering::Release);
    }
}

impl<T> SwmrCell<T> {
    pub fn new(value: T) -> SwmrCell<T> {
        SwmrCell {
            writing: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }
    
    /// Retrieve an immutable reference to the contents of this `SwmrCell`
    pub fn borrow(&self) -> Ref<T> {
        // We don't actually keep track of immutable references, but still wrap them in a `Ref`
        Ref {
            value: unsafe { &*self.value.get() }
        }
    }

    /// Retrieve a mutable reference to the contents of this `SwmrCell`
    /// 
    /// Blocks while there is already an active mutable borrow, guaranteeing that there can only ever be one mutable borrow at any given time
    /// 
    /// # Safety
    /// `SwmrCell` allows one mutable borrow concurrently with any number of immutable borrows, breaking Rusts aliasing rules. This is inherently
    /// unsafe, and special care must be taken by the user to properly synchronize access reads and writes over the stored data. `SwmrCell`'s are intended
    /// as a building block for implementing more complex synchronization mechanisms
    pub unsafe fn borrow_mut(&self) -> RefMut<T> {
        RefMut::new(&mut *self.value.get(), &self.writing)        
    }
}

#[cfg(test)]
mod tests {
    // Note: Stress tests can only confirm the presence of a concurrent bugs - they can not confirm the absence of them
    
    use super::*;
    use std::sync::{ Arc, atomic::{ AtomicU64, Ordering } };
    
    
    #[test]
    #[ignore]
    fn stress_single_writer() {
        const TEST_THREAD_COUNT: usize = 4;
        const TEST_DURATION_MILLIS: u64 = 1000 * 60;
        const TEST_INCREMENT_RANGE: u64 = 100;

        println!("running stress test for {} milliseconds using {} threads", TEST_DURATION_MILLIS, TEST_THREAD_COUNT);

        let stop = Arc::new(AtomicBool::new(false));
        let tops = Arc::new(AtomicU64::new(0));

        // spin up some threads, have each thread acquire read or write access, but guarantee that there is only every one writer 
        let mut threads = Vec::new();
        
        let cell = Arc::new(SwmrCell::new(AtomicU64::new(0)));
        
        for _ in 0..TEST_THREAD_COUNT {
            let this_stop = stop.clone();
            let this_tops = tops.clone();
            let this_cell = cell.clone();

            let thread = thread::spawn(move || {

                while !this_stop.load(Ordering::Acquire) {
                    {
                        // We want to ensure that only one thread is able to have one of these at a time
                        let mut_ref = unsafe { this_cell.borrow_mut() };
                        
                        // Whenever a thread acquires write access, we'll have it increment the stored value up to a certain value, then
                        // it will spend some time asserting that the stored value remains constant, before it decrements the value back
                        // to zero, then we spend some time asserting the value remains constant again, before finally releasing write
                        // access
                        {
                            for _ in 0..TEST_INCREMENT_RANGE {
                                mut_ref.fetch_add(1, Ordering::SeqCst);
                            }

                            for _ in 0..TEST_INCREMENT_RANGE {
                                let now = mut_ref.load(Ordering::SeqCst);
                                assert!(now == TEST_INCREMENT_RANGE);
                            }

                            for _ in 0..TEST_INCREMENT_RANGE {
                                mut_ref.fetch_sub(1, Ordering::SeqCst);
                            }

                            for _ in 0..TEST_INCREMENT_RANGE {
                                let now = mut_ref.load(Ordering::SeqCst);
                                assert!(now == 0);
                            }

                            this_tops.fetch_add(TEST_INCREMENT_RANGE + TEST_INCREMENT_RANGE, Ordering::SeqCst);
                        }
                    }
                }
            });

            threads.push(thread);
        }

        let time_start = std::time::Instant::now();
        let time_end = time_start + std::time::Duration::from_millis(TEST_DURATION_MILLIS);

        // Here our main thread will continuously assert that the test value remains in our desired range
        let imm_ref = cell.borrow();
        while std::time::Instant::now() < time_end {
            let now = imm_ref.load(Ordering::SeqCst);
            assert!(now <= TEST_INCREMENT_RANGE);
        }

        stop.store(true, Ordering::SeqCst);

        for thread in threads {
            let _ = thread.join();
        }

        let tdir = (time_end - time_start).as_millis();
        println!("stress test completed: test duration {} milliseconds, total ops {}", tdir, tops.load(Ordering::Acquire));
    }
}

//test duration 10000, total ops 273,122,800, seqcst, 8 threads
//test duration 10000, total ops 295,467,200, relaxed, 8 threads
//
//test duration 10000, total ops 336,822,800, seqcst, 4 threads
//test duration 10000, total ops 451,297,400, relaxed, 4 threads
//
//test duration 60000, total ops 2,289,817,200, relaxed, 4 threads
//test duration 60000, total ops 3,318,365,600, acquire/release, 4 threads
