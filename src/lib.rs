//! A lock free single-writer, many-reader concurrent array

use std::ptr;
use std::mem;
use std::alloc;
use std::thread;
use std::sync::{ Arc };
use std::sync::atomic::{ AtomicPtr, AtomicI64, Ordering };
use std::ops::{ Deref, DerefMut, Mul };
use swmr::{ Ref, RefMut, SwmrCell };

mod swmr;

/// A contiguous, heap allocated, fixed sized, concurrent array which supports a *single writer and multiple readers* in lock-free mode,
/// and multiple writers which share a single lock. Reads are always wait-free, while writes may lock.
/// 
/// Due to the way `ConcurrentArray` necessarily handles concurrent reads and writes, it is limited to storing trivially copyable types
/// or types which implement `Copy`. This may make it unsuitable for certain use cases
/// 
/// # Reading threads vs. Writing threads
/// 
/// Any thread which holds an instance of the same `ConcurrentArray` can function as either a reader or a writer or both. The only
/// differentiating factor between reading threads and writing threads are the functions each thread calls. Functions which are guaranteed
/// to be "read-safe" so to speak, meaning that they never invoke a write lock, are marked as such. Any other function is assumed to lock
/// writes on the `ConcurrentArray`, blocking calls from any thread on functions which *are not* marked as "read-safe".
/// 
/// Read-safe functions are *guaranteed* to be wait free, while all other functions are guaranteed to be lock free *only if there is a
/// single writing thread.* 
#[derive(Debug)]
pub struct ConcurrentArray<T: Copy> {
    inner: Arc<SwmrCell<Inner<T>>>,
    epoch: usize,
}

impl<T: Copy> ConcurrentArray<T> {
    /// Returns a `ReadGuard` which provides read access over the `ConcurrentArray`
    /// 
    /// Creating a read guard is guaranteed to be wait-free. Any attempts by writing threads to commit new writes to
    /// the `ConcurrentArray` will block as long as any `ReadGuards` that existed before the call to commit are not dropped.
    /// For this reason `ReadGuards` should be dropped as soon as they are no longer necessary, otherwise it is possible to
    /// unnecessarily stall writing threads
    /// 
    /// # Examples
    /// 
    /// ```
    /// use conarray::ConcurrentArray;
    /// 
    /// let array: ConcurrentArray<i32> = ConcurrentArray::new(10);
    /// let guard = array.read();
    /// 
    /// guard.iter().for_each(|item| assert_eq!(*item, i32::default()));
    /// ```
    pub fn read(&self) -> ReadGuard<T> {
        ReadGuard::from(self)
    }
    
    /// Locks this `ConcurrentArray` with single writer write access, blocking the current thread until it can be acquired, but does
    /// *not* block other threads from acquiring read access.
    /// 
    /// This function will not return while any other thread is performing any type of write operation on this `ConcurrentArray`,
    /// this includes any thread which calls any function which requires *mutable* access to the `ConcurrentArray`.
    /// 
    /// # Note
    /// 
    /// Writes made in a writing thread through a `WriteGuard` are *not* immediately visible to reading threads. Instead the
    /// written state is considered pending until a call to `commit` is made. These writes are only made visible *after* any
    /// thread calls `commit` on the `ConcurrentArray`. Pending writes can be destroyed at any time and by any thread by calling `cancel`
    /// on the `ConcucrentArray`
    /// 
    /// # Examples
    /// 
    /// ```
    /// use conarray::ConcurrentArray;
    /// 
    /// let mut array: ConcurrentArray<i32> = ConcurrentArray::new(10);
    /// let mut guard = array.write();
    /// 
    /// guard.iter_mut().for_each(|item| assert_eq!(*item, i32::default()));
    /// ```
    pub fn write(&self) -> WriteGuard<T> {
        WriteGuard::from(self)
    }

    /// Commits a set of pending writes, atomically making them visible to any *new* read operation
    /// 
    /// Any threads which were already reading during a call to `commit` will continue to see the old data as if the commit never
    /// happened.
    /// A call to `commit` is guaranteed to be lock-free *only* if there is at most a single thread with *mutable* access to
    /// the `ConcurrentArray`. Calling commit will cause the current thread to wait until any read operations which started before
    /// the call are completed (by dropping their respective `ReadGuard`'s)
    /// 
    /// # Examples
    /// 
    /// ```
    /// use conarray::ConcurrentArray;
    /// 
    /// let mut array: ConcurrentArray<i32> = ConcurrentArray::new(10);
    /// 
    /// array.write().iter_mut().enumerate().for_each(|(i, item)| *item = (i as i32) - 5);
    /// 
    /// // we've mutated data, but haven't committed it
    /// array.read().iter().enumerate().for_each(|(i, item)| assert_eq!(*item, i32::default()));
    /// 
    /// // all of our mutated data is immediately and atomically made visible here
    /// array.commit();
    /// array.read().iter().enumerate().for_each(|(i, item)| assert_eq!(*item, (i as i32) - 5));
    /// 
    pub fn commit(&self) {
        let mut inner: RefMut<Inner<T>> = unsafe { self.inner.borrow_mut() };
        
        inner.swap();
        inner.wait();
        inner.sync();
    }

    /// Cancels all pending written data and resets it to its original state
    /// 
    /// # Examples
    /// 
    /// ```
    /// use conarray::ConcurrentArray;
    /// 
    /// let mut array: ConcurrentArray<i32> = ConcurrentArray::new(10);
    /// array.read().iter().enumerate().for_each(|(i, item)| assert_eq!(*item, i32::default()));
    /// array.write().iter_mut().enumerate().for_each(|(i, item)| *item = (i as i32) - 5);
    /// 
    /// // reading the written data again should yield what we wrote
    /// array.write().iter_mut().enumerate().for_each(|(i, item)| assert_eq!(*item, (i as i32) - 5));
    /// 
    /// // even though we wrote data, it hasn't been committed
    /// array.read().iter().enumerate().for_each(|(i, item)| assert_eq!(*item, i32::default()));
    /// 
    /// // cancel everything we wrote, this resets the state of written data to reflect the readable data
    /// array.cancel();
    /// 
    /// // read and write are now the same
    /// array.read().iter().enumerate().for_each(|(i, item)| assert_eq!(*item, i32::default()));
    /// array.write().iter_mut().enumerate().for_each(|(i, item)| assert_eq!(*item, i32::default()));
    /// ```
    pub fn cancel(&self) {
        let mut inner: RefMut<Inner<T>> = unsafe { self.inner.borrow_mut() };
        inner.sync();
    }

    fn increment_epoch(&self) {
        self.inner.borrow().increment_epoch(self.epoch);
    }
}

impl<T: Default + Copy> ConcurrentArray<T> {
    /// Creates an empty `ConcurrentArray` of size `size`
    /// 
    /// The array is immediately allocated on the heap and filled with `T::Default`
    /// 
    /// # Examples
    /// 
    /// ```
    /// use conarray::ConcurrentArray;
    /// let mut array: ConcurrentArray<i32> = ConcurrentArray::new(32);
    /// ```
    pub fn new(size: usize) -> Self {
        ConcurrentArray {
            inner: Arc::new(SwmrCell::new(Inner::new(size))),
            epoch: 0usize,
        }
    }
}

impl<T: Copy> Clone for ConcurrentArray<T> {
    fn clone(&self) -> Self {
        let mut inner: RefMut<Inner<T>> = unsafe { self.inner.borrow_mut() };
        let mut epoch: usize = inner.epochs.len();
        
        if let Some(e) = inner.fepoch.pop() {
            inner.epochs[e] = (AtomicI64::from(0i64), AtomicI64::from(0i64));
            epoch = e;
        } else {
            inner.epochs.push((AtomicI64::from(0i64), AtomicI64::from(0i64)));
        }

        ConcurrentArray {
            inner: self.inner.clone(),
            epoch,
        }
    }
}

impl<T: Copy> Drop for ConcurrentArray<T> {
    fn drop(&mut self) {
        unsafe { self.inner.borrow_mut().fepoch.push(self.epoch) };
    }
}

#[derive(Debug)]
struct Inner<T> {
    reader_ptr: AtomicPtr<T>,
    writer_ptr: AtomicPtr<T>,
    epochs: Vec<(AtomicI64, AtomicI64)>,
    fepoch: Vec<usize>,
    bslice: Box<[T]>,
    length: usize,
}

impl<T> Inner<T> {
    pub fn swap(&mut self) {
        let new_writer_ptr: *mut T = self.reader_ptr.swap(self.writer_ptr.load(Ordering::SeqCst), Ordering::SeqCst);
        self.writer_ptr.store(new_writer_ptr, Ordering::SeqCst);
    }

    pub fn wait(&mut self) {
        let mut headcount: usize = 0;
        let readers: usize = self.epochs.len();

        loop {
            for (epoch, epoch_last) in self.epochs.iter().skip(headcount) {
                let now = epoch.load(Ordering::SeqCst);
                let last = epoch_last.load(Ordering::SeqCst);

                if ((now % 2) == 0) | (now == last) | (now == 0) {
                    headcount += 1;
                    epoch_last.store(now, Ordering::SeqCst);
                }
            }

            if headcount >= readers {
                return;
            } else {
                thread::yield_now();
            }
        }
    }

    pub fn sync(&mut self) {
        let reader_ptr: *mut T = self.reader_ptr.load(Ordering::SeqCst);
        let writer_ptr: *mut T = self.writer_ptr.load(Ordering::SeqCst);

        unsafe { ptr::copy_nonoverlapping(reader_ptr, writer_ptr, self.length) };
    }

    #[inline]
    pub fn increment_epoch(&self, epoch: usize) {
        let last = self.epochs[epoch].0.fetch_add(1i64, Ordering::SeqCst);
        self.epochs[epoch].1.store(last, Ordering::SeqCst);
    }
}

impl<T: Default + Clone> Inner<T> {
    fn new(size: usize) -> Self {
        let mut bslice: Box<[T]> = vec![T::default(); size.mul(2usize)].into_boxed_slice();
        let epochs: Vec<(AtomicI64, AtomicI64)> = vec![(AtomicI64::from(0i64), AtomicI64::from(0i64))];

        Inner {
            reader_ptr: AtomicPtr::new(bslice.as_mut_ptr()),
            writer_ptr: AtomicPtr::new(unsafe { bslice.as_mut_ptr().offset(size as isize) }),
            epochs: epochs,
            fepoch: Vec::new(),
            bslice: bslice,
            length: size,
        }
    }
}

//impl<T> Drop for Inner<T> {
//    fn drop(&mut self) {
//        let ptr: *mut T = self.reader_ptr.load(Ordering::SeqCst);
//        
//        unsafe {
//            let slice: &mut [T] = std::slice::from_raw_parts_mut(ptr, self.length);
//            ptr::drop_in_place(slice)
//        };
//        
//        let ptr: *mut [T] = Box::into_raw(mem::take(&mut self.bslice));
//        unsafe { alloc::dealloc(ptr as *mut u8, alloc::Layout::new::<T>()) };
//    }
//}

/// An RAII structure used to correctly synchronize concurrent read access of a `ConcurrentArray`.
/// 
/// Produced by the `read` method on `ConcurrentArray`.
pub struct ReadGuard<'a, T> {
    epoch: usize,
    inner: Ref<'a, Inner<T>>,
}

impl<'a, T: Copy> From<&'a ConcurrentArray<T>> for ReadGuard<'a, T> {
    fn from(array: &'a ConcurrentArray<T>) -> Self {
        array.increment_epoch();

        let epoch: usize = array.epoch;
        let inner: Ref<Inner<T>> = array.inner.borrow();

        ReadGuard { inner, epoch }
    }
}

impl<'a, T> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) {
        self.inner.increment_epoch(self.epoch);
    }
}

impl<'a, T> Deref for ReadGuard<'a, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        let ptr: *mut T = self.inner.reader_ptr.load(Ordering::SeqCst);

        unsafe { std::slice::from_raw_parts(ptr, self.inner.length) }
    }
}

/// An RAII structure used to correctly synchronize concurrent write access of a `ConcurrentArray`.
/// 
/// Produced by the `write` method on `ConcurrentArray`.
#[derive(Debug)]
pub struct WriteGuard<'a, T> {
    inner: RefMut<'a, Inner<T>>,
}

//impl<'a, T> WriteGuard<'a, T> {
//    pub fn set(&mut self, idx: usize, value: T) {
//        // todo: Change this so that the atomic pointer isn't read every time if it's not necessary
//
//        let ptr: *mut T = self.inner.writer_ptr.load(Ordering::SeqCst);
//        let slice: &mut [T] = unsafe { std::slice::from_raw_parts_mut(ptr, self.inner.length) };
//
//        slice[idx] = value;
//    }
//}

impl<'a, T: Copy> From<&'a ConcurrentArray<T>> for WriteGuard<'a, T> {
    fn from(array: &'a ConcurrentArray<T>) -> Self {
        let inner: RefMut<'a, Inner<T>> = unsafe { array.inner.borrow_mut() };

        WriteGuard { inner }
    }
}

impl<'a, T> Deref for WriteGuard<'a, T> {
    type Target = [T];
    
    fn deref(&self) -> &Self::Target {
        let ptr: *mut T = self.inner.writer_ptr.load(Ordering::SeqCst);

        unsafe { std::slice::from_raw_parts(ptr, self.inner.length) }
    }
}

impl<'a, T> DerefMut for WriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let ptr: *mut T = self.inner.writer_ptr.load(Ordering::SeqCst);

        unsafe { std::slice::from_raw_parts_mut(ptr, self.inner.length) }
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    type TT = u32;
    const SIZE: usize = 100;
    const INITIAL_VALUE: TT = 0xA4420810; // (0b10100100010000100000100000010000)
    const WRITTEN_VALUE: TT = 0x5BBDF7EF; // (0b01011011101111011111011111101111)

    fn init_array(s: usize, v: TT) -> ConcurrentArray<TT> {
        let array: ConcurrentArray<TT> = ConcurrentArray::new(s);

        for value in array.write().iter_mut() {
            *value = v;
        }

        array.commit();

        array
    }
    
    #[test]
    fn test_readguard_deref() {
        let array = init_array(SIZE, INITIAL_VALUE);
        let guard = array.read();
        let slice = [INITIAL_VALUE; SIZE];
        
        assert_eq!(guard.len(), slice.len());

        for (i, item) in guard.iter().enumerate() {
            assert_eq!(*item, slice[i]);
        }

        for (i, item) in slice.iter().enumerate() {
            assert_eq!(*item, guard[i]);
        }
    }
    
    #[test]
    fn test_new_default_clone() {
        let array: ConcurrentArray<TT> = ConcurrentArray::new(SIZE);
        let mut inner: RefMut<Inner<TT>> = unsafe { array.inner.borrow_mut() };

        assert_eq!(array.epoch, 0usize);
        assert_eq!(inner.epochs.len(), 1usize);
        assert_eq!(inner.epochs[0].0.load(Ordering::SeqCst), 0i64);
        assert_eq!(inner.epochs[0].1.load(Ordering::SeqCst), 0i64);
        assert_eq!(inner.reader_ptr.load(Ordering::SeqCst), inner.bslice.as_mut_ptr());
        assert_eq!(inner.writer_ptr.load(Ordering::SeqCst), unsafe { inner.bslice.as_mut_ptr().offset(SIZE as isize) });
        assert_eq!(inner.length, SIZE);
        assert_eq!(inner.bslice.len(), SIZE.mul(2usize));
    }

    #[test]
    fn test_clone_array() {
        let array: ConcurrentArray<TT> = ConcurrentArray::new(SIZE);
        let clone: ConcurrentArray<TT> = array.clone();

        assert_eq!(array.epoch, 0usize);
        assert_eq!(clone.epoch, 1usize);

        let array_inner: Ref<Inner<TT>> = array.inner.borrow();
        let clone_inner: Ref<Inner<TT>> = clone.inner.borrow();
        
        assert_eq!(array_inner.length, clone_inner.length);
        assert_eq!(array_inner.bslice.as_ptr(), clone_inner.bslice.as_ptr());
        assert_eq!(array_inner.epochs.len(), 2usize);
        assert_eq!(clone_inner.epochs.len(), 2usize);
        assert_eq!(array_inner.reader_ptr.load(Ordering::SeqCst), clone_inner.reader_ptr.load(Ordering::SeqCst));
        assert_eq!(array_inner.writer_ptr.load(Ordering::SeqCst), clone_inner.writer_ptr.load(Ordering::SeqCst));

        let array_ptr: *const Inner<TT> = &(*array_inner);
        let clone_ptr: *const Inner<TT> = &(*clone_inner);

        // The cloned structures should reference the exact same object in memory
        assert_eq!(array_ptr, clone_ptr);
    }

    #[test]
    fn test_inner_swap() {
        let array: ConcurrentArray<TT> = ConcurrentArray::new(SIZE);
        let mut inner: RefMut<Inner<TT>> = unsafe { array.inner.borrow_mut() };

        assert_eq!(inner.reader_ptr.load(Ordering::SeqCst), inner.bslice.as_mut_ptr());
        assert_eq!(inner.writer_ptr.load(Ordering::SeqCst), unsafe { inner.bslice.as_mut_ptr().offset(SIZE as isize) });

        let old_reader_ptr: *mut TT = inner.reader_ptr.load(Ordering::SeqCst);
        let old_writer_ptr: *mut TT = inner.writer_ptr.load(Ordering::SeqCst);

        inner.swap();

        assert_eq!(inner.reader_ptr.load(Ordering::SeqCst), old_writer_ptr);
        assert_eq!(inner.writer_ptr.load(Ordering::SeqCst), old_reader_ptr);
        assert_eq!(inner.writer_ptr.load(Ordering::SeqCst), inner.bslice.as_mut_ptr());
        assert_eq!(inner.reader_ptr.load(Ordering::SeqCst), unsafe { inner.bslice.as_mut_ptr().offset(SIZE as isize) });
    }

    #[test]
    fn test_inner_sync() {
        let array: ConcurrentArray<u32> = ConcurrentArray::new(SIZE);

        // Manually set all values in the array to the INITIAL_VALUE
        {
            let mut inner: RefMut<Inner<u32>> = unsafe { array.inner.borrow_mut() };
            for value in inner.bslice.iter_mut() {
                *value = INITIAL_VALUE;
            }
        }

        // Directly write into the read section, check each section, then sync, then assert all data in both sections is the same
        {   
            let mut inner: RefMut<Inner<u32>> = unsafe { array.inner.borrow_mut() };
            for value in unsafe { std::slice::from_raw_parts_mut(inner.reader_ptr.load(Ordering::SeqCst), inner.length) } {
                *value = WRITTEN_VALUE;
            }

            for value in unsafe { std::slice::from_raw_parts_mut(inner.writer_ptr.load(Ordering::SeqCst), inner.length) } {
                assert_eq!(*value, INITIAL_VALUE);
            }

            for value in unsafe { std::slice::from_raw_parts_mut(inner.reader_ptr.load(Ordering::SeqCst), inner.length) } {
                assert_eq!(*value, WRITTEN_VALUE);
            }

            inner.sync();

            for value in inner.bslice.iter() {
                assert_eq!(*value, WRITTEN_VALUE);
            }
        }
    }   
}

// Notes for future development
//
// 1. When a commit is made the read block does not have to be immediately copied onto the write block if it would be inefficient to do so
//      rather we can simply *mark* the array has needing to be synchronized, and this synchronization can happen at any time before any new
//      write operations occur. It can even happen through the action of some other thread that's busy waiting on the same structure
// 
// 2. What happens when one item in the structure points to another item in the structure? Probably should rely on indices instead of pointers
//      Pointers shouldn't be acceptable or considered stable, similar to Vec
//
// 3. Explore adding a `refresh` function to `ReadGuard` which refreshes state as if the read guard were dropped and then created again
