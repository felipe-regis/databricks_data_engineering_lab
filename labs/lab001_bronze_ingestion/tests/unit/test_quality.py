from labs.lab001_bronze_ingestion.src.data_pipeline.quality import is_valid_customer_id
import pytest

def test_valid_customer_id():
    assert is_valid_customer_id("CUST0001") is True

def test_none_customer_id_is_invalid():
    assert is_valid_customer_id(None) is False

def test_empty_customer_id_is_invalid():
    assert is_valid_customer_id("") is False


@pytest.mark.parametrize("customer_id", [" ", "   ", "\t", "\n", " \t\n "])
def test_whitespace_only_customer_id_is_invalid(customer_id):
    assert is_valid_customer_id(customer_id) is False


@pytest.mark.parametrize("customer_id", [" CUST0001", "CUST0001 ", " CUST0001 "])
def test_customer_id_with_surrounding_whitespace_is_valid(customer_id):
    assert is_valid_customer_id(customer_id) is True


@pytest.mark.parametrize("customer_id", ["0", "123", "abc", "CUST", "CUST-0001"])
def test_non_empty_customer_id_values_are_valid(customer_id):
    assert is_valid_customer_id(customer_id) is True


@pytest.mark.parametrize("customer_id", [0, False, [], {}])
def test_non_string_customer_id_raises_attribute_error(customer_id):
    with pytest.raises(AttributeError):
        is_valid_customer_id(customer_id)

