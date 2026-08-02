from tests.dialects.test_dialect import Validator


class TestDAX(Validator):
    dialect = "dax"

    def test_evaluate(self):
        self.validate_transpile("EVALUATE Sales", "SELECT * FROM Sales")
        self.validate_transpile("EVALUATE 'Sales Data'", 'SELECT * FROM "Sales Data"')

    def test_filter(self):
        self.validate_transpile(
            "EVALUATE FILTER(Sales, Sales[Amount] > 100)",
            'SELECT * FROM Sales WHERE Sales."Amount" > 100',
        )
        self.validate_transpile(
            "EVALUATE FILTER('Sales Data', 'Sales Data'[Amount] > 100)",
            'SELECT * FROM "Sales Data" WHERE "Sales Data"."Amount" > 100',
        )
        self.validate_transpile(
            'EVALUATE FILTER(FILTER(Sales, Sales[Amount] > 100), Sales[Region] = "West")',
            'SELECT * FROM Sales WHERE Sales."Amount" > 100 AND Sales."Region" = \'West\'',
        )
        self.validate_transpile(
            "EVALUATE FILTER(Sales, Sales[Amount] > 100 && Sales[Qty] < 5)",
            'SELECT * FROM Sales WHERE Sales."Amount" > 100 AND Sales."Qty" < 5',
        )
        self.validate_transpile(
            "EVALUATE FILTER(Sales, Sales[Amount] > 100 || Sales[Qty] < 5)",
            'SELECT * FROM Sales WHERE Sales."Amount" > 100 OR Sales."Qty" < 5',
        )
        self.validate_transpile(
            "EVALUATE FILTER(Sales, [Total Amount] > 100)",
            'SELECT * FROM Sales WHERE "Total Amount" > 100',
        )
        self.validate_transpile(
            'EVALUATE FILTER(Sales, Sales[Note] = "He said ""hi""")',
            'SELECT * FROM Sales WHERE Sales."Note" = \'He said "hi"\'',
        )
        self.validate_transpile(
            "EVALUATE FILTER(Sales, Sales[Amount] > 100)",
            'SELECT * FROM Sales WHERE Sales."Amount" > 100',
            write_dialect="duckdb",
        )

    def test_order_by(self):
        self.validate_transpile(
            "EVALUATE Sales ORDER BY Sales[Amount] DESC, Sales[Qty]",
            'SELECT * FROM Sales ORDER BY Sales."Amount" DESC, Sales."Qty"',
        )
        self.validate_transpile(
            "EVALUATE FILTER(Sales, Sales[Amount] > 100) ORDER BY Sales[Amount]",
            'SELECT * FROM Sales WHERE Sales."Amount" > 100 ORDER BY Sales."Amount" NULLS FIRST',
            write_dialect="duckdb",
        )
