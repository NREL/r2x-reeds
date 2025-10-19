import ast
import inspect
import io
from collections.abc import Callable
from importlib.resources import files

import polars as pl
from loguru import logger


def validate_string(value):
    """Read cases flag value and convert it to Python type."""
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    if value == "true" or value == "TRUE":
        return True
    if value == "false" or value == "FALSE":
        return False

    try:
        value = ast.literal_eval(value)
    except Exception as e:
        logger.trace("Could not parse {}: {}", value, e)
    finally:
        return value  # noqa: B012


def read_csv(fname: str, package_data: str = "r2x.defaults", **kwargs) -> pl.LazyFrame:
    """Helper function to read csv string data from package data.

    Args:
        fname: Name of the csv file
        package_data: Location of file in package. Default location is r2x.defaults
        **kwargs: Additional keys passed to pandas read_csv function

    Returns
    -------
        A pandas dataframe of the csv requested
    """
    csv_file = files(package_data).joinpath(fname).read_text(encoding="utf-8-sig")
    return pl.LazyFrame(pl.read_csv(io.StringIO(csv_file), **kwargs))


def get_function_arguments(argument_input: dict, function: Callable) -> dict:
    """Get arguments to pass to a function based on its signature.

    This function processes the `argument_input` and returns a dictionary of argument
    values that are valid for the given `function`, using the function's signature
    as a filter. String values are validated, nested dictionaries are flattened,
    and only the valid argument keys (as defined in the function signature) are included.

    Parameters
    ----------
    data_dict : dict
        A dictionary containing potential argument values, which may include
        strings, dictionaries, and other types of data.

    function : str
        The name of the function whose signature is used to filter the arguments.

    Returns
    -------
    dict
        A dictionary of filtered arguments that match the function's signature.
        Only arguments that exist in the function's signature will be included.

    Example
    -------
    >>> def example_function(a, b, c=None):
    >>>     pass
    >>> data = {"a": 1, "b": 2, "c": 3, "extra": 4}
    >>> prepare_function_arguments(data, "example_function")
    {'a': 1, 'b': 2, 'c': 3}
    """
    arguments = {}
    for key, value in argument_input.items():
        if isinstance(value, str):
            value = validate_string(value)
        if isinstance(value, dict):
            arguments.update(value)
        else:
            arguments[key] = value

    return {key: value for key, value in arguments.items() if key in inspect.getfullargspec(function).args}
