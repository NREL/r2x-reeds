import pytest

from r2x_reeds.upgrader.helpers import get_function_arguments, validate_string


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        (10, 10),
        (15.0, 15.0),
        ("true", True),
        ("TRUE", True),
        ("false", False),
        ("FALSE", False),
        ("[1,2,3]", [1, 2, 3]),
        ("[1,2,3", "[1,2,3"),
    ],
)
def test_validate_string(value, expected):
    assert validate_string(value) == expected


def test_get_function_arguments():
    def test_function(a, b, c, d=None):
        pass

    argument_input = {"a": 2, "b": 3, "z": 3}
    arguments = get_function_arguments(argument_input=argument_input, function=test_function)

    assert arguments == {"a": 2, "b": 3}
