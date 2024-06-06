import pytest
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseCloneNotPossible
from dbt.tests.adapter.dbt_clone.test_dbt_clone import (
    TestCloneSameTargetAndState as BaseCloneSameTargetAndState,
)


@pytest.mark.skip("Adapter does not support multiple schemas.")
class TestCloneNotPossibleNetezza(BaseCloneNotPossible):
    pass


@pytest.mark.skip("Adapter does not support multiple schemas.")
class TestCloneSameTargetAndStateNetezza(BaseCloneSameTargetAndState):
    pass
