//! Shared integration testing tools

pub fn rand(min: isize, max: isize) -> isize {
    const RANDOM_SEED: i64 = 61964112671;

    unsafe {
        static mut A: i64 = 4611686018428264157;
        static mut B: i64 = 4611694780817250319;
        static mut C: i64 = 4604505837158266799;
        
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