use super::{
    bessel_i::bessel_i,
    bessel_j0_y0::{j0, y0},
    bessel_j1_y1::j1,
    bessel_jn_yn::{jn, yn},
    bessel_k::bessel_k,
};

const EPS: f64 = 1e-13;
const EPS_TIGHT: f64 = 1e-14;

// Known values computed with Arb via Nemo.jl in Julia or 50-digit mpmath
// You can also use Mathematica
// But please do not use Excel or any other software without arbitrary precision

fn numbers_are_close(a: f64, b: f64) -> bool {
    if a == b {
        // avoid underflow if a = b = 0.0
        return true;
    }
    (a - b).abs() / ((a * a + b * b).sqrt()) < EPS
}

fn rel_close(actual: f64, expected: f64, tol: f64) -> bool {
    if actual == expected {
        return true;
    }
    if expected == 0.0 {
        return actual.abs() <= tol;
    }
    ((actual - expected) / expected).abs() < tol
}

#[test]
fn bessel_j0_known_values() {
    let cases = [
        (2.4, 0.002507683297243813),
        (0.5, 0.9384698072408129),
        (1.0, 0.7651976865579666),
        (1.12345, 0.7084999488947348),
        (27.0, 0.07274191800588709),
        (33.0, 0.09727067223550946),
        (2e-4, 0.9999999900000001),
        (0.0, 1.0),
        (1e10, 2.175591750246892e-6),
    ];
    for (value, known) in cases {
        let f = j0(value);
        assert!(
            numbers_are_close(f, known),
            "Got: {f}, expected: {known} for j0({value})"
        );
    }
}

#[test]
fn bessel_y0_known_values() {
    let cases = [
        (2.4, 0.5104147486657438),
        (0.5, -0.4445187335067065),
        (1.0, 0.08825696421567692),
        (1.12345, 0.1783162909790613),
        (27.0, 0.1352149762078722),
        (33.0, 0.0991348255208796),
        (2e-4, -5.496017824512429),
        (1e10, -7.676508175792937e-6),
        (1e-300, -439.8351636227653),
    ];
    for (value, known) in cases {
        let f = y0(value);
        assert!(
            numbers_are_close(f, known),
            "Got: {f}, expected: {known} for y0({value})"
        );
    }
    assert!(y0(0.0).is_infinite());
}

#[test]
fn bessel_j1_known_values() {
    // Values computed with Maxima, the computer algebra system
    // TODO: Recompute
    let cases = [
        (2.4, 0.5201852681819311),
        (0.5, 0.2422684576748738),
        (1.0, 0.4400505857449335),
        (1.17232, 0.4910665691824317),
        (27.5, 0.1521418932046569),
        (42.0, -0.04599388822188721),
        (3e-5, 1.499999999831249E-5),
        (350.0, -0.02040531295214455),
        (0.0, 0.0),
        (1e12, -7.913802683850441e-7),
    ];
    for (value, known) in cases {
        let f = j1(value);
        assert!(
            numbers_are_close(f, known),
            "Got: {f}, expected: {known} for j1({value})"
        );
    }
}

#[test]
fn bessel_jn_known_values() {
    // Values computed with Maxima, the computer algebra system
    // TODO: Recompute
    let cases = [
        (3, 0.5, 0.002_563_729_994_587_244),
        (4, 0.5, 0.000_160_736_476_364_287_6),
        (-3, 0.5, -0.002_563_729_994_587_244),
        (-4, 0.5, 0.000_160_736_476_364_287_6),
        (3, 30.0, 0.129211228759725),
        (-3, 30.0, -0.129211228759725),
        (4, 30.0, -0.052609000321320355),
        (20, 30.0, 0.0048310199934040645),
        (7, 0.0, 0.0),
    ];
    for (n, value, known) in cases {
        let f = jn(n, value);
        assert!(
            numbers_are_close(f, known),
            "Got: {f}, expected: {known} for jn({n}, {value})"
        );
    }
}

#[test]
fn bessel_yn_known_values() {
    let cases = [
        (3, 0.5, -42.059494304723883),
        (4, 0.5, -499.272_560_819_512_3),
        (-3, 0.5, 42.059494304723883),
        (-4, 0.5, -499.272_560_819_512_3),
        (3, 35.0, -0.13191405300596323),
        (-12, 12.2, -0.310438011314211),
        (7, 1e12, 1.016_712_505_197_956_3e-7),
        (35, 3.0, -6.895_879_073_343_495e31),
    ];
    for (n, value, known) in cases {
        let f = yn(n, value);
        assert!(
            numbers_are_close(f, known),
            "Got: {f}, expected: {known} for yn({n}, {value})"
        );
    }
}

#[test]
fn bessel_in_known_values() {
    let cases = [
        (1, 0.5, 0.2578943053908963),
        (3, 0.5, 0.002645111968990286),
        (7, 0.2, 1.986608521182497e-11),
        (7, 0.0, 0.0),
        (0, -0.5, 1.0634833707413236),
        (0, 3.7499, 9.118167894541882),
        (0, 3.7501, 9.119723897590003),
        // Issue #596 reference cases
        (1, 2.0, 1.590636854637329),
        (2, 2.0, 0.6889484476987382),
    ];
    for (n, value, known) in cases {
        let f = bessel_i(n, value);
        assert!(
            rel_close(f, known, EPS_TIGHT),
            "Got: {f:.17e}, expected: {known:.17e} for in({n}, {value})"
        );
    }
}

#[test]
fn bessel_kn_known_values() {
    let cases = [
        (1, 0.5, 1.656441120003301),
        (0, 0.5, 0.9244190712276659),
        (3, 0.5, 62.05790952993026),
        // Issue #596 reference cases
        (0, 0.1, 2.4270690247020163),
        (1, 2.0, 0.13986588181652243),
        (2, 2.0, 0.2537597545660559),
    ];
    for (n, value, known) in cases {
        let f = bessel_k(n, value);
        assert!(
            rel_close(f, known, EPS_TIGHT),
            "Got: {f:.17e}, expected: {known:.17e} for kn({n}, {value})"
        );
    }
}

#[test]
fn bessel_in_precision_grid_and_parity() {
    // 50-digit mpmath references across [1e-6 .. 500] and orders 0..=20
    let cases = [
        (0, 1e-6, 1.000_000_000_000_25_e0),
        (1, 1e-6, 5.000_000_000_000_624e-7),
        (2, 1e-6, 1.250_000_000_000_104e-13),
        (3, 1e-6, 2.083_333_333_333_463_4e-20),
        (5, 1e-6, 2.604_166_666_666_774_5e-34),
        (10, 1e-6, 2.691_144_455_467_432_2e-70),
        (15, 1e-6, 2.333_729_166_204_813e-107),
        (20, 1e-6, 3.919_904_349_624_834e-145),
        (0, 0.1, 1.002_501_562_934_095_7e0),
        (1, 0.1, 5.006_252_604_709_269_4e-2),
        (2, 0.1, 1.251_041_992_241_759_3e-3),
        (3, 0.1, 2.084_635_742_232_715_5e-5),
        (5, 0.1, 2.605_251_929_893_698e-9),
        (10, 0.1, 2.691_756_142_922_143e-20),
        (15, 0.1, 2.334_093_838_200_386_5e-32),
        (20, 0.1, 3.920_371_031_419_978e-45),
        (0, 0.5, 1.063_483_370_741_323_6e0),
        (1, 0.5, 2.578_943_053_908_963e-1),
        (2, 0.5, 3.190_614_917_773_825_6e-2),
        (3, 0.5, 2.645_111_968_990_286e-3),
        (5, 0.5, 8.223_171_313_109_265e-6),
        (10, 0.5, 2.643_041_925_881_279_4e-13),
        (15, 0.5, 7.149_847_634_163_453e-22),
        (20, 0.5, 3.749_453_848_079_019_4e-31),
        (0, 1.0, 1.266_065_877_752_008_4e0),
        (1, 1.0, 5.651_591_039_924_85e-1),
        (2, 1.0, 1.357_476_697_670_382_8e-1),
        (3, 1.0, 2.216_842_492_433_19e-2),
        (5, 1.0, 2.714_631_559_569_719e-4),
        (10, 1.0, 2.752_948_039_836_873_7e-10),
        (15, 1.0, 2.370_463_051_280_748e-17),
        (20, 1.0, 3.966_835_985_819_02e-25),
        (0, 2.0, 2.279_585_302_336_067_3e0),
        (1, 2.0, 1.590_636_854_637_329e0),
        (2, 2.0, 6.889_484_476_987_382e-1),
        (3, 2.0, 2.127_399_592_398_526_4e-1),
        (5, 2.0, 9.825_679_323_131_702e-3),
        (10, 2.0, 3.016_963_879_350_684_5e-7),
        (15, 2.0, 8.139_432_531_237_38e-13),
        (20, 2.0, 4.310_560_576_109_548_4e-19),
        (0, 3.75, 9.118_945_860_844_567e0),
        (1, 3.75, 7.780_015_229_824_416e0),
        (2, 3.75, 4.969_604_404_938_211e0),
        (3, 3.75, 2.479_103_864_556_990_3e0),
        (5, 3.75, 3.392_889_917_099_987_7e-1),
        (10, 3.75, 2.028_901_121_006_349e-4),
        (15, 3.75, 1.183_964_364_275_969_2e-8),
        (20, 3.75, 1.400_658_929_485_067_7e-13),
        (0, 5.0, 2.723_987_182_360_444_6e1),
        (1, 5.0, 2.433_564_214_245_052_8e1),
        (2, 5.0, 1.750_561_496_662_423_6e1),
        (3, 5.0, 1.033_115_016_915_113_9e1),
        (5, 5.0, 2.157_974_547_322_546_7e0),
        (10, 5.0, 4.580_044_419_176_052e-3),
        (15, 5.0, 1.047_977_675_417_918_8e-6),
        (20, 5.0, 5.024_239_357_971_806e-11),
        (0, 10.0, 2.815_716_628_466_254_4e3),
        (1, 10.0, 2.670_988_303_701_254_6e3),
        (2, 10.0, 2.281_518_967_726_003_7e3),
        (3, 10.0, 1.758_380_716_610_853_1e3),
        (5, 10.0, 7.771_882_864_032_6e2),
        (10, 10.0, 2.189_170_616_372_337e1),
        (15, 10.0, 1.043_714_907_060_010_9e-1),
        (20, 10.0, 1.250_799_735_644_947_6e-4),
        (0, 20.0, 4.355_828_255_955_353_4e7),
        (1, 20.0, 4.245_497_338_512_777e7),
        (2, 20.0, 3.931_278_522_104_075_5e7),
        (3, 20.0, 3.459_241_634_091_962e7),
        (5, 20.0, 2.301_839_221_341_367e7),
        (10, 20.0, 3.540_200_209_019_521_3e6),
        (15, 20.0, 1.756_697_432_691_592_3e5),
        (20, 20.0, 3.188_750_328_853_615e3),
        (0, 50.0, 2.932_553_783_849_336e20),
        (1, 50.0, 2.903_078_590_103_557e20),
        (2, 50.0, 2.816_430_640_245_194e20),
        (3, 50.0, 2.677_764_138_883_941_3e20),
        (5, 50.0, 2.278_548_307_911_282e20),
        (10, 50.0, 1.071_597_159_477_637_1e20),
        (15, 50.0, 3.073_764_549_487_122e19),
        (20, 50.0, 5.442_008_402_752_997e18),
        (0, 100.0, 1.073_751_707_131_073_8e42),
        (1, 100.0, 1.068_369_390_338_162_5e42),
        (2, 100.0, 1.052_384_319_324_310_6e42),
        (3, 100.0, 1.026_274_017_565_19e42),
        (5, 100.0, 9.470_093_873_035_58e41),
        (10, 100.0, 6.498_975_524_720_148e41),
        (15, 100.0, 3.473_686_381_500_949e41),
        (20, 100.0, 1.448_346_125_642_717_2e41),
        (0, 500.0, 2.504_809_476_570_078e215),
        (1, 500.0, 2.502_303_412_176_1e215),
        (2, 500.0, 2.494_800_262_921_373_7e215),
        (3, 500.0, 2.482_345_010_072_729e215),
        (5, 500.0, 2.442_904_816_107_921e215),
        (10, 500.0, 2.266_225_812_384_004e215),
        (15, 500.0, 1.999_713_974_385_241e215),
        (20, 500.0, 1.678_441_080_193_862e215),
    ];
    for (n, x, expected) in cases {
        let got = bessel_i(n, x);
        assert!(
            rel_close(got, expected, EPS_TIGHT),
            "bessel_i({n}, {x}) = {got:.17e}, expected {expected:.17e}"
        );
        // Parity: I_n(-x) = (-1)^n I_n(x)
        let neg = bessel_i(n, -x);
        let expected_neg = if n % 2 == 1 { -expected } else { expected };
        assert!(
            rel_close(neg, expected_neg, EPS_TIGHT),
            "bessel_i({n}, -{x}) = {neg:.17e}, expected {expected_neg:.17e}"
        );
    }
}

#[test]
fn bessel_kn_precision_grid() {
    // 50-digit mpmath references across [1e-6 .. 500] and orders 0..=20
    let cases = [
        (0, 1e-6, 1.393_144_207_362_642e1),
        (1, 1e-6, 9.999_999_999_927_843e5),
        (2, 1e-6, 1.999_999_999_999_500_2e12),
        (3, 1e-6, 7.999_999_999_999e18),
        (5, 1e-6, 3.839_999_999_999_761e32),
        (10, 1e-6, 1.857_945_599_999_949_3e68),
        (15, 1e-6, 1.428_329_123_020_775_4e105),
        (20, 1e-6, 6.377_706_640_314_493e142),
        (0, 0.1, 2.427_069_024_702_016_4e0),
        (1, 0.1, 9.853_844_780_870_606e0),
        (2, 0.1, 1.995_039_646_421_141e2),
        (3, 0.1, 7.990_012_430_465_435e3),
        (5, 0.1, 3.837_600_999_583_591_5e7),
        (10, 0.1, 1.857_429_584_630_4e18),
        (15, 0.1, 1.428_074_088_772_009_8e30),
        (20, 0.1, 6.376_867_526_661_179e42),
        (0, 0.5, 9.244_190_712_276_659e-1),
        (1, 0.5, 1.656_441_120_003_301e0),
        (2, 0.5, 7.550_183_551_240_869_5e0),
        (3, 0.5, 6.205_790_952_993_026e1),
        (5, 0.5, 1.209_797_947_609_639_4e4),
        (10, 0.5, 1.889_375_693_199_002_7e11),
        (15, 0.5, 4.659_504_595_597_239e19),
        (20, 0.5, 6.665_549_874_417_156e28),
        (0, 1.0, 4.210_244_382_407_083_4e-1),
        (1, 1.0, 6.019_072_301_972_346e-1),
        (2, 1.0, 1.624_838_898_635_177_4e0),
        (3, 1.0, 7.101_262_824_737_945e0),
        (5, 1.0, 3.609_605_896_012_407e2),
        (10, 1.0, 1.807_132_899_010_294_7e8),
        (15, 1.0, 1.403_066_801_155_039e15),
        (20, 1.0, 6.294_369_360_424_535e22),
        (0, 2.0, 1.138_938_727_495_334_4e-1),
        (1, 2.0, 1.398_658_818_165_224_3e-1),
        (2, 2.0, 2.537_597_545_660_559e-1),
        (3, 2.0, 6.473_853_909_486_341e-1),
        (5, 2.0, 9.431_049_100_596_468e0),
        (10, 2.0, 1.624_824_039_795_591_4e5),
        (15, 2.0, 4.059_213_331_550_876_6e10),
        (20, 2.0, 5.770_856_852_700_241e16),
        (0, 3.75, 1.477_425_087_712_870_4e-2),
        (1, 3.75, 1.663_819_175_468_891e-2),
        (2, 3.75, 2.364_795_314_629_612e-2),
        (3, 3.75, 4.186_267_511_073_81e-2),
        (5, 3.75, 2.352_029_062_008_225_7e-1),
        (10, 3.75, 2.306_424_931_499_377_4e2),
        (15, 3.75, 2.731_027_396_142_346_3e6),
        (20, 3.75, 1.754_234_219_583_592_5e11),
        (0, 5.0, 3.691_098_334_042_594e-3),
        (1, 5.0, 4.044_613_445_452_165e-3),
        (2, 5.0, 5.308_943_712_223_46e-3),
        (3, 5.0, 8.291_768_415_230_933e-3),
        (5, 5.0, 3.270_627_371_203_186e-2),
        (10, 5.0, 9.758_562_829_177_81e0),
        (15, 5.0, 3.016_976_630_067_320_4e4),
        (20, 5.0, 4.827_000_520_621_484_5e8),
        (0, 10.0, 1.778_006_231_616_765e-5),
        (1, 10.0, 1.864_877_345_382_558_6e-5),
        (2, 10.0, 2.150_981_700_693_277e-5),
        (3, 10.0, 2.725_270_025_659_869e-5),
        (5, 10.0, 5.754_184_998_531_228e-5),
        (10, 10.0, 1.614_255_300_390_67e-3),
        (15, 10.0, 2.656_563_848_552_397e-1),
        (20, 10.0, 1.787_442_782_077_055e2),
        (0, 20.0, 5.741_237_815_336_525e-10),
        (1, 20.0, 5.883_057_969_557_038e-10),
        (2, 20.0, 6.329_543_612_292_228e-10),
        (3, 20.0, 7.148_966_692_015_483e-10),
        (5, 20.0, 1.053_866_013_997_423_3e-9),
        (10, 20.0, 6.316_214_528_321_58e-9),
        (15, 20.0, 1.138_383_165_981_996_5e-7),
        (20, 20.0, 5.543_111_636_125_816e-6),
        (0, 50.0, 3.410_167_749_789_495_6e-23),
        (1, 50.0, 3.444_102_226_717_555_5e-23),
        (2, 50.0, 3.547_931_838_858_198e-23),
        (3, 50.0, 3.727_936_773_826_211e-23),
        (5, 50.0, 4.367_182_254_100_986_5e-23),
        (10, 50.0, 9.150_988_209_987_996e-23),
        (15, 50.0, 3.116_211_179_676_359e-22),
        (20, 50.0, 1.706_148_379_722_035_2e-21),
        (0, 100.0, 4.656_628_229_175_902e-45),
        (1, 100.0, 4.679_853_735_636_909_5e-45),
        (2, 100.0, 4.750_225_303_888_64e-45),
        (3, 100.0, 4.869_862_747_792_455e-45),
        (5, 100.0, 5.273_256_113_292_95e-45),
        (10, 100.0, 7.655_427_977_388_1e-45),
        (15, 100.0, 1.423_483_251_144_714_1e-44),
        (20, 100.0, 3.385_205_414_890_17e-44),
        (0, 500.0, 3.992_321_609_117_793e-219),
        (1, 500.0, 3.996_311_938_546_003_5e-219),
        (2, 500.0, 4.008_306_856_871_977e-219),
        (3, 500.0, 4.028_378_393_400_979e-219),
        (5, 500.0, 4.093_284_751_762_463_5e-219),
        (10, 500.0, 4.411_742_876_731_852e-219),
        (15, 500.0, 4.998_468_845_859_806e-219),
        (20, 500.0, 5.953_151_793_910_654e-219),
    ];
    for (n, x, expected) in cases {
        let got = bessel_k(n, x);
        assert!(
            rel_close(got, expected, EPS_TIGHT),
            "bessel_k({n}, {x}) = {got:.17e}, expected {expected:.17e}"
        );
    }
}

#[test]
fn bessel_ik_extremes_boundaries_and_domain_contracts() {
    // High-order / large-x scaling & subnormals (50-digit mpmath references)
    assert!(rel_close(
        bessel_i(0, 713.9),
        1.648_155_186_695_138e308,
        EPS_TIGHT
    ));
    assert_eq!(bessel_i(0, 714.0), f64::INFINITY);
    assert!(rel_close(
        bessel_i(300, 750.0),
        1.387_920_365_488_94e298,
        EPS_TIGHT
    ));
    assert!(rel_close(
        bessel_i(100, 0.06),
        5.522_372_366_367e-311,
        1e-12
    ));
    assert!(rel_close(
        bessel_i(50, 50.0),
        1.765_080_243_001_671e10,
        EPS_TIGHT
    ));
    assert!(rel_close(
        bessel_i(500, 500.0),
        7.601_701_925_731_382e113,
        EPS_TIGHT
    ));

    // Crossover continuity around x = 25 (I) and x = 1 (K)
    assert!(rel_close(
        bessel_i(0, 25.0),
        5.774_560_606_466_310_5e9,
        EPS_TIGHT
    ));
    assert!(rel_close(
        bessel_i(0, 25.0001),
        5.775_126_420_721_435e9,
        EPS_TIGHT
    ));
    assert!(rel_close(
        bessel_k(0, 1.0001),
        4.209_642_526_319_758e-1,
        EPS_TIGHT
    ));
    assert!(rel_close(
        bessel_k(1, 1.0001),
        6.018_049_481_630_383e-1,
        EPS_TIGHT
    ));

    // Bessel K large-x subnormals, underflow, scaled recurrence, and tiny-x
    assert!(rel_close(
        bessel_k(0, 710.0),
        2.105_097_455_568_84e-310,
        1e-12
    ));
    assert!(rel_close(bessel_k(0, 740.0), 2e-323, 0.5));
    assert_eq!(bessel_k(0, 746.0), 0.0);
    assert_eq!(bessel_k(0, 1000.0), 0.0);
    assert!(rel_close(
        bessel_k(400, 750.0),
        1.669_169_371_588_151e-282,
        EPS_TIGHT
    ));
    assert!(rel_close(
        bessel_k(500, 500.0),
        9.301_951_793_329_595e-118,
        EPS_TIGHT
    ));
    assert!(rel_close(
        bessel_k(0, 1e-300),
        6.908_914_594_138_721e2,
        EPS_TIGHT
    ));
    assert!(rel_close(bessel_k(1, 1e-300), 1.0e300, EPS_TIGHT));
    assert_eq!(bessel_k(2, 1e-200), f64::INFINITY);

    // Special values & error contracts
    assert_eq!(bessel_i(0, 0.0), 1.0);
    assert_eq!(bessel_i(0, -0.0), 1.0);
    assert_eq!(bessel_i(1, 0.0), 0.0);
    assert_eq!(bessel_i(5, -0.0), 0.0);
    assert!(bessel_i(-1, 1.0).is_nan());
    assert!(bessel_i(0, f64::NAN).is_nan());
    assert!(bessel_i(2, f64::NAN).is_nan());
    assert_eq!(bessel_i(0, f64::INFINITY), f64::INFINITY);
    assert_eq!(bessel_i(1, f64::NEG_INFINITY), f64::NEG_INFINITY);
    assert_eq!(bessel_i(2, f64::NEG_INFINITY), f64::INFINITY);
    assert!(bessel_i(1_000_001, 1.0).is_nan());
    assert!(bessel_i(1_000_000_000, 1.0).is_nan());

    assert!(bessel_k(0, 0.0).is_nan());
    assert!(bessel_k(1, 0.0).is_nan());
    assert!(bessel_k(0, -1.0).is_nan());
    assert!(bessel_k(-1, 1.0).is_nan());
    assert!(bessel_k(0, f64::NAN).is_nan());
    assert_eq!(bessel_k(0, f64::INFINITY), 0.0);
    assert_eq!(bessel_k(5, f64::INFINITY), 0.0);
    assert!(bessel_k(1_000_001, 1.0).is_nan());
}

/// Regression: the tiny-`x` Taylor branch of `jn` accumulated `n!` in an `i32`.
/// `13!` already overflows `i32`, so any order in `13..=33` combined with a
/// very small `x` panicked in debug builds (and silently wrapped, producing
/// garbage, in release builds).
#[test]
fn bessel_jn_tiny_x_high_order_does_not_overflow() {
    // x < 2^-29 selects the Taylor branch.
    let x = 1e-12;
    for n in 2..=33 {
        let f = jn(n, x);
        assert!(f.is_finite(), "jn({n}, {x}) must be finite, got {f}");
        // (x/2)^n / n! underflows to +0 well before n = 33.
        assert!(f >= 0.0, "jn({n}, {x}) must be non-negative, got {f}");
    }
}

/// Regression: `jn`/`yn` negated `n` with `-n`, which overflows for
/// `i32::MIN`, and `jn` negated the high word with `-hx`, which overflows for
/// the high word of `-0.0`.
#[test]
fn bessel_extreme_orders_do_not_panic() {
    for x in [0.5f64, 1.0, 30.0] {
        let jn_result = jn(i32::MIN, x);
        let yn_result = yn(i32::MIN, x);
        assert!(
            jn_result.is_nan(),
            "excessive recurrence must fail explicitly"
        );
        assert!(
            yn_result.is_nan(),
            "excessive recurrence must fail explicitly"
        );
    }
}

/// Regression: `jn` used `-hx` on the high word to flip the sign of `x`.
/// For `x == -0.0` the high word is `i32::MIN`, whose arithmetic negation
/// overflows.
#[test]
fn bessel_jn_handles_negative_zero() {
    for (n, x, negative) in [
        (2, -0.0, false),
        (3, -0.0, true),
        (-3, 0.0, true),
        (-3, -0.0, false),
        (i32::MIN, -0.0, false),
    ] {
        let result = jn(n, x);
        assert_eq!(result, 0.0);
        assert_eq!(result.is_sign_negative(), negative);
    }
}

/// `y0`/`yn` rely on the *high* word of the IEEE bit pattern to detect zero,
/// infinity and the sign. Sanity-check those boundaries, which silently broke
/// when the words were extracted via native byte order.
#[test]
fn bessel_y_special_values() {
    assert!(yn(2, 0.0).is_infinite() && yn(2, 0.0) < 0.0);
    assert!(yn(2, -1.0).is_nan());
    assert_eq!(yn(2, f64::INFINITY), 0.0);
    assert!(yn(2, f64::NAN).is_nan());
}

/// Genuine underflow/overflow inside the admitted recurrence still has its
/// ordinary IEEE result. This is not a reason to synthesize limits elsewhere.
#[test]
fn bessel_moderate_orders_run_real_recurrence() {
    let j = jn(2000, 100.0);
    let y = yn(2000, 100.0);
    // J underflows toward zero from above: a non-negative, non-NaN result.
    assert!(!j.is_nan(), "jn(2000, 100) must not be NaN, got {j}");
    assert!(j >= 0.0, "jn(2000, 100) = {j} must be non-negative");
    // Y diverges negative: either a large finite negative or -inf, never NaN/positive.
    assert!(!y.is_nan(), "yn(2000, 100) must not be NaN, got {y}");
    assert!(y < 0.0, "yn(2000, 100) = {y} must be negative");
}

#[test]
fn bessel_excessive_recurrence_returns_failure_not_fabricated_limits() {
    for n in [1_000_001, i32::MAX, i32::MIN] {
        for x in [1.0, 3.5, 3e9] {
            assert!(jn(n, x).is_nan());
            assert!(yn(n, x).is_nan());
        }
    }
}

#[test]
fn bessel_constant_time_paths_precede_work_limit_and_preserve_parity() {
    for n in [i32::MIN, i32::MAX, 2_000_000] {
        assert_eq!(jn(n, f64::INFINITY), 0.0);
        assert_eq!(jn(n, f64::NEG_INFINITY), 0.0);
        assert_eq!(yn(n, f64::INFINITY), 0.0);
        assert!(jn(n, f64::NAN).is_nan());
        assert!(yn(n, f64::NAN).is_nan());
        assert!(yn(n, -1.0).is_nan());
        // The existing tiny-x bound is distinct from an order-only heuristic.
        assert_eq!(jn(n, 1e-12), 0.0);
        // Huge x uses the existing constant-time phase approximation. With
        // i32::MIN the magnitude is even, not the odd saturated i32::MAX.
        let parity = n.unsigned_abs() % 4;
        let sign = if n < 0 && parity % 2 != 0 { -1.0 } else { 1.0 };
        assert_eq!(jn(n, 1e100), sign * jn(parity as i32, 1e100));
        assert_eq!(yn(n, 1e100), sign * yn(parity as i32, 1e100));
    }
    assert_eq!(yn(-3, 0.0), f64::INFINITY);
    assert_eq!(yn(-2, -0.0), f64::NEG_INFINITY);
    assert_eq!(yn(i32::MIN, 0.0), f64::NEG_INFINITY);
}

#[test]
fn bessel_representable_values_are_not_discarded() {
    // mpmath 80-digit references; compare ratios to retain subnormal sensitivity
    // and avoid overflowing a subtraction of large opposite-sign values.
    for (actual, expected) in [
        (jn(100, 0.06), 5.522_273_948_726_5e-311),
        (yn(100, 0.06), -5.76410997427483e307),
        (jn(13, 1e-12), 1.9603324996120135e-170),
    ] {
        assert!(
            (actual / expected - 1.0).abs() < 2e-12,
            "{actual:e} vs {expected:e}"
        );
    }
    // Independent SciPy 1.18.1/AMOS references around the old cutoff and at
    // the new work boundary. These assert values, not merely finite/nonzero.
    for (n, x, j, y) in [
        (
            200_000,
            200_000.0,
            0.007648847543722423,
            -0.013248192594800937,
        ),
        (
            200_001,
            200_000.0,
            0.007528721867113492,
            -0.013456282866366336,
        ),
        (
            250_000,
            250_000.0,
            0.00710056107183997,
            -0.012298532559165735,
        ),
        (
            1_000_000,
            1_000_000.0,
            0.004473073183377776,
            -0.007747590021617347,
        ),
    ] {
        assert!((jn(n, x) / j - 1.0).abs() < 1e-10);
        assert!((yn(n, x) / y - 1.0).abs() < 1e-10);
    }
    // Large oscillatory arguments retain the existing recurrence. The wider
    // tolerance acknowledges disagreement with AMOS here, not a precision fix.
    assert!((jn(300_000, 1e8) / 2.6539905910125003e-5 - 1.0).abs() < 1e-6);
    assert!((yn(300_000, 1e8) / -7.524532638187288e-5 - 1.0).abs() < 1e-6);
}
