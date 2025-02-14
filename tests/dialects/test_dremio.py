import pytest
from sqlglot import parse_one, transpile
from sqlglot.dialects import Dremio
from sqlglot.expressions import *


def test_dremio_parsing():
    """Test Dremio-specific parsing."""
    assert parse_one("SELECT QUALIFY ROW_NUMBER() OVER() = 1", dialect="dremio").
        find(Qualify) is not None
    
    assert parse_one("SELECT LISTAGG(name, ',') FROM users", dialect="dremio").
        find(GroupConcat) is not None
    
    assert parse_one("SELECT FLATTEN(arr) FROM table", dialect="dremio").
        find(Explode) is not None
    
    assert parse_one("SELECT REGEXP_EXTRACT(col, 'pattern', 1) FROM table", dialect="dremio").
        find(RegexpExtract) is not None


def test_dremio_tokenizer():
    """Test that Dremio tokenizer correctly identifies keywords."""
    tokens = Dremio.Tokenizer().tokenize("SELECT QUALIFY, LISTAGG, FLATTEN, ILIKE")
    assert any(token.text.upper() == "QUALIFY" for token in tokens)
    assert any(token.text.upper() == "LISTAGG" for token in tokens)
    assert any(token.text.upper() == "FLATTEN" for token in tokens)
    assert any(token.text.upper() == "ILIKE" for token in tokens)


def test_dremio_sql_generation():
    """Test that Dremio SQL is generated correctly."""
    assert transpile("SELECT DATE_ADD(start_date, INTERVAL 5 DAY)", 
                     dialect="dremio")[0] == "SELECT DATE_ADD(start_date, INTERVAL 5 DAY)"

    assert transpile("SELECT QUALIFY ROW_NUMBER() OVER() = 1", 
                     dialect="dremio")[0] == "SELECT QUALIFY ROW_NUMBER() OVER() = 1"

    assert transpile("SELECT REGEXP_EXTRACT(col, 'pattern', 1)", 
                     dialect="dremio")[0] == "SELECT REGEXP_EXTRACT(col, 'pattern', 1)"
