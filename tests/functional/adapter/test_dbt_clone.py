import pytest
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseCloneNotPossible


@pytest.mark.skip("Adapter does not support multiple schemas.")
class TestCloneNotPossibleNetezza(BaseCloneNotPossible):
    pass
