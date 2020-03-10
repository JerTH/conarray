
use std::time::Duration;
use std::thread::{ self, JoinHandle };
use std::sync::{ atomic::{ Ordering, AtomicBool }, Arc };

extern crate conarray;
use conarray::ConcurrentArray;

#[allow(unused_macros)]
macro_rules! swmr_contention_tests {
    ($($name:ident: ($rthreads:expr, $wthreads:expr),)*) => {
        $(
            #[test]
            fn $name() {
                println!();

                const DURATION: usize = 100;
                const SIZE: usize = 1024*1024;
                const READER_THREADS: usize = $rthreads;
                const WRITER_THREADS: usize = $wthreads;

                let array: ConcurrentArray<i32> = ConcurrentArray::new(SIZE);
                let mut handles: Vec<JoinHandle<()>> = Vec::new();
                let stop: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
            
                for (i, item) in array.write().iter_mut().enumerate() {
                    *item = i as i32;
                }

                array.commit();
            
                //let reader_threads: usize = std::cmp::max(1, THREADS - (THREADS / 4));
                //let writer_threads: usize = std::cmp::max(1, THREADS - reader_threads);

                let reader_threads = READER_THREADS;
                let writer_threads = WRITER_THREADS;

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
                
                    let local_array = array.clone();
                    let local_stop = stop.clone();
                
                    // Start reading threads
                    handles.push(thread::spawn(move || {
                        while !local_stop.load(Ordering::SeqCst) {
                            for (i, item) in local_array.write().iter_mut().enumerate() {
                                assert_eq!(*item, i as i32);
                            }
                            local_array.commit();
                        }
                    }));
                }

                // This is the control thread
                thread::sleep(Duration::from_millis(DURATION as u64));
                stop.store(true, Ordering::SeqCst);
            }
        )*
    };
}

swmr_contention_tests! {
    test_contention_1r_1w: (1, 1),
    test_contention_2r_2w: (2, 2),
    test_contention_4r_4w: (4, 4),
    test_contention_8r_4w: (8, 4),
    test_contention_16r_4w: (16, 4),
    test_contention_32r_4w: (32, 4),
    test_contention_48r_4w: (48, 4),
    test_contention_64r_4w: (64, 4),
    test_contention_96r_4w: (96, 4),
    test_contention_128r_4w: (128, 4),
}
