
use std::time::Duration;
use std::thread::{ self, JoinHandle };
use std::sync::{ atomic::{ Ordering, AtomicBool }, Arc };

extern crate conarray;
use conarray::ConcurrentArray;

#[test]
fn test_swmr_integration() {
    println!();
    
    const SIZE: usize = 100;
    const THREADS: usize = 4;

    let mut array: ConcurrentArray<i32> = ConcurrentArray::new(SIZE);
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    let stop: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

    for (i, item) in array.write().iter_mut().enumerate() {
        *item = i as i32;
    }
    
    array.commit();

    let reader_threads: usize = std::cmp::max(1, THREADS - 1);
    let writer_threads: usize = 1usize;

    println!("starting {} reading threads...", reader_threads);
    for _ in 0..reader_threads {

        let local_array = array.clone();
        let local_stop = stop.clone();

        // Start reading threads
        handles.push(thread::spawn(move || {
            while !local_stop.load(Ordering::SeqCst) {
                for (i, item) in local_array.read().iter().enumerate() {
                    assert_eq!(*item, i as i32);
                }
            }
        }));
    }
    
    println!("starting {} writing threads...", writer_threads);
    for _ in 0..writer_threads {

        let mut local_array = array.clone();
        let local_stop = stop.clone();

        // Start reading threads
        handles.push(thread::spawn(move || {
            while !local_stop.load(Ordering::SeqCst) {
                local_array.commit();
            }
        }));
    }


    // This is the control thread
    thread::sleep(Duration::from_millis(1000));
    stop.store(true, Ordering::SeqCst);
}


fn rand(min: isize, max: isize) -> isize {
    const RANDOM_SEED: i64 = 61964112671;

    unsafe {
        static mut A: i64 = 273901;
        static mut B: i64 = 997973;
        static mut C: i64 = 825791;
        
        // four rounds of convolution
        for i in 0..3 {
            A ^= B.wrapping_shl((B % (i + 7)) as u32);
            B ^= A;
            C ^= B.wrapping_shl((B % (i + 11)) as u32);
            C ^= A;
            A ^= C + RANDOM_SEED;
        }

        let clamped = std::cmp::max(0, min);
        return min + (clamped + ((A as isize).abs() % (((max - min).abs() + 1) - clamped)));
    }
}
