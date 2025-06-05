import pytest
from bnw_scrapie import parse_years, MIN_YEAR, MAX_YEAR

def test_parse_years_valid():
    assert parse_years(['2020', '2025']) == ['2020', '2025']

def test_parse_years_invalid():
    # Out of range and non-integer
    assert parse_years(['abcd', '3000', '2010']) == []

def test_parse_years_mixed():
    years = ['2020', 'abcd', '2011', str(MAX_YEAR+1)]
    assert parse_years(years) == ['2020']
