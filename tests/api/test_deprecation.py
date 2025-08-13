"""All of maggma.api has been migrated to emmet-api."""

import typing

import pytest


def test_deprecation_warning_utils():
    with pytest.warns(DeprecationWarning, match="The maggma.api module has been deprecated"):
        from maggma.api.utils import merge_queries

        # unfortunately necessary because precommit removes "unused" import statements
        assert isinstance(merge_queries, typing.types.FunctionType)
