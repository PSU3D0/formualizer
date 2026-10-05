use formualizer_parse::{parse, pretty::canonical_formula};

#[test]
fn unions_nested_in_delimited_expressions_keep_their_grouping() {
    for (source, expected) in [
        ("=SUM((A1,B1)+1)", "=SUM((A1, B1) + 1)"),
        ("=SUM(1+(A1,B1))", "=SUM(1 + (A1, B1))"),
        ("=SUM(-(A1,B1))", "=SUM(-(A1, B1))"),
        ("=SUM((A1,B1)%,C1)", "=SUM((A1, B1)%, C1)"),
        ("=SUM((A1,B1,C1)+D1)", "=SUM((A1, B1, C1) + D1)"),
        ("=SUM(((A1,B1)+C1)*D1)", "=SUM((A1, B1 + C1) * D1)"),
    ] {
        let original = parse(source).unwrap();
        let printed = canonical_formula(&original);
        assert_eq!(printed, expected, "{source}");
        assert_eq!(
            parse(&printed).unwrap().fingerprint(),
            original.fingerprint(),
            "{source} -> {printed}"
        );
    }
}
