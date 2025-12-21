import formualizer as fz


def test_sumifs_cross_sheet_with_dependent_multiplier(xlsx_builder):
    def populate(wb):
        # Calculations sheet with criteria and dependent cells
        calc = wb.active
        calc.title = "Calculations"
        calc.cell(row=3, column=4, value="X")  # D3 = "X"
        # D5 = SUMIFS('MONTHLY.POLR'!W:W, 'MONTHLY.POLR'!AB:AB, D3, 'MONTHLY.POLR'!T:T, ">=5")
        calc.cell(
            row=5,
            column=4,
            value="=SUMIFS('MONTHLY.POLR'!W:W, 'MONTHLY.POLR'!AB:AB, D3, 'MONTHLY.POLR'!T:T, \">=5\")",
        )
        # E5 depends on D5 via multiplier
        calc.cell(row=5, column=5, value="=D5*2")

        # Data sheet MONTHLY.POLR with columns: T (20), W (23), AB (28)
        polr = wb.create_sheet("MONTHLY.POLR")
        # Rows: (AB, T, W)
        rows = [
            ("X", 4, 100.0),  # filtered out by T<5
            ("X", 5, 200.0),  # included
            ("Y", 5, 300.0),  # depends on AB
            ("X", 7, 400.0),  # included
        ]
        start_row = 5
        for i, (ab, t, w) in enumerate(rows):
            r = start_row + i
            polr.cell(row=r, column=28, value=ab)  # AB
            polr.cell(row=r, column=20, value=t)  # T
            polr.cell(row=r, column=23, value=w)  # W

    path = xlsx_builder(populate)

    wb = fz.Workbook.from_path(str(path), backend="calamine")

    # Initial evaluation: D3="X", expect D5 = 200 + 400 = 600, E5 = 1200
    wb.evaluate_all()
    assert wb.evaluate_cell("Calculations", 5, 4) == 600.0
    assert wb.evaluate_cell("Calculations", 5, 5) == 1200.0

    # Change D3 to "Y" via value edit; D5 should become 300; E5 = 600
    wb.set_value("Calculations", 3, 4, "Y")
    wb.evaluate_all()
    assert wb.evaluate_cell("Calculations", 5, 4) == 300.0
    assert wb.evaluate_cell("Calculations", 5, 5) == 600.0


def test_sumifs_demand_driven_single_eval(xlsx_builder):
    """Change criteria and evaluate dependent without a full evaluate_all."""

    def populate(wb):
        calc = wb.active
        calc.title = "Calculations"
        calc.cell(row=3, column=4, value="X")  # D3
        calc.cell(
            row=5,
            column=4,
            value="=SUMIFS('MONTHLY.POLR'!W:W, 'MONTHLY.POLR'!AB:AB, D3, 'MONTHLY.POLR'!T:T, \">=5\")",
        )
        calc.cell(row=5, column=5, value="=D5*2")

        polr = wb.create_sheet("MONTHLY.POLR")
        rows = [("X", 5, 10.0), ("Y", 5, 20.0)]
        for i, (ab, t, w) in enumerate(rows, start=5):
            polr.cell(row=i, column=28, value=ab)
            polr.cell(row=i, column=20, value=t)
            polr.cell(row=i, column=23, value=w)

    path = xlsx_builder(populate)
    wb = fz.Workbook.from_path(str(path), backend="calamine")

    # Evaluate only E5; primes and computes D5 transitively
    assert wb.evaluate_cell("Calculations", 5, 5) == 20.0  # D5=10, E5=20

    # Change criteria and call evaluate_cell again (no evaluate_all)
    wb.set_value("Calculations", 3, 4, "Y")
    assert wb.evaluate_cell("Calculations", 5, 5) == 40.0  # D5=20, E5=40


def test_sumifs_edit_formula_demand_driven(xlsx_builder):
    """Change the SUMIFS formula itself; demand-evaluate dependent only."""

    def populate(wb):
        calc = wb.active
        calc.title = "Calculations"
        calc.cell(row=3, column=4, value="X")  # D3
        # Baseline: include T >= 5
        calc.cell(
            row=5,
            column=4,
            value="=SUMIFS('MONTHLY.POLR'!W:W, 'MONTHLY.POLR'!AB:AB, D3, 'MONTHLY.POLR'!T:T, \">=5\")",
        )
        calc.cell(row=5, column=5, value="=D5*2")

        polr = wb.create_sheet("MONTHLY.POLR")
        rows = [("X", 4, 100.0), ("X", 5, 200.0), ("Y", 5, 300.0), ("X", 7, 400.0)]
        for i, (ab, t, w) in enumerate(rows, start=5):
            polr.cell(row=i, column=28, value=ab)
            polr.cell(row=i, column=20, value=t)
            polr.cell(row=i, column=23, value=w)

    path = xlsx_builder(populate)
    wb = fz.Workbook.from_path(str(path), backend="calamine")

    # Baseline demand: E5=1200 (D5=600)
    assert wb.evaluate_cell("Calculations", 5, 5) == 1200.0

    # Edit D5's formula to tighten the T criterion to ">=6"; do NOT call evaluate_all
    wb.set_formula(
        "Calculations",
        5,
        4,
        "=SUMIFS('MONTHLY.POLR'!W:W, 'MONTHLY.POLR'!AB:AB, D3, 'MONTHLY.POLR'!T:T, \">=6\")",
    )
    # Demand-evaluate E5; should reflect only the row with T=7 → D5=400, E5=800
    assert wb.evaluate_cell("Calculations", 5, 5) == 800.0
