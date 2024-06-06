from pathlib import Path

import pytest
from dbt.tests.adapter.simple_seed.seeds import seeds__expected_sql
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseTestEmptySeed,
    SeedConfigBase,
    SeedTestBase,
    SeedUniqueDelimiterTestBase,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestBasicSeedTests as BaseBasicSeedTests,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedConfigFullRefreshOff as BaseSeedConfigFullRefreshOff,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedConfigFullRefreshOn as BaseSeedConfigFullRefreshOn,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedCustomSchema as BaseSeedCustomSchema,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedParsing as BaseSeedParsing,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedSpecificFormats as BaseSeedSpecificFormats,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedWithEmptyDelimiter as BaseSeedWithEmptyDelimiter,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedWithUniqueDelimiter as BaseSeedWithUniqueDelimiter,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedWithWrongDelimiter as BaseSeedWithWrongDelimiter,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSimpleSeedEnabledViaConfig as BaseSimpleSeedEnabledViaConfig,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSimpleSeedWithBOM as BaseSimpleSeedWithBOM,
)
from dbt.tests.adapter.simple_seed.test_seed_type_override import (
    BaseSimpleSeedColumnOverride,
)
from dbt.tests.util import copy_file


def _clean_sql(sql: str) -> str:
    # Replace TEXT and TIMESTAMP WITHOUT TIME ZONE types
    sql = sql.replace("TEXT", "VARCHAR(2000)")
    sql = sql.replace("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP")

    # Replace a single insert with multiple values with multiple insert statements
    lines = sql.split("\n")
    insert_index = [lines.index(line) for line in lines if line.startswith("INSERT")][0]
    insert_end = insert_index + 3
    insert_into_lines = lines[insert_index:insert_end]
    insert_into_lines[1] = insert_into_lines[1].replace('"', "")
    new_lines = []
    for line in lines[insert_end:]:
        if line.strip():
            new_lines.extend([*insert_into_lines, line[:-1] + ";"])
    sql = "\n".join(lines[:insert_index] + new_lines)

    return sql


seeds__expected_sql_cleaned = _clean_sql(seeds__expected_sql)


class NetezzaSeedConfigBase(SeedConfigBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"quote_columns": False, "datetimedelim": " "}}

    @pytest.fixture(scope="class")
    def project_cleanup_extra_relations(self):
        return [("table", "seed_expected")]

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql_cleaned)


class NetezzaSeedTestBase(NetezzaSeedConfigBase, SeedTestBase):
    pass


class TestBasicSeedTestsNetezza(NetezzaSeedTestBase, BaseBasicSeedTests):
    pass


class TestSeedConfigFullRefreshOffNetezza(
    NetezzaSeedTestBase, BaseSeedConfigFullRefreshOff
):
    pass


class TestSeedConfigFullRefreshOnNetezza(
    NetezzaSeedTestBase, BaseSeedConfigFullRefreshOn
):
    pass


class TestSeedCustomSchemaNetezza(NetezzaSeedTestBase, BaseSeedCustomSchema):
    pass


class TestSeedParsingNetezza(NetezzaSeedConfigBase, BaseSeedParsing):
    pass


class TestSeedSpecificFormatsNetezza(NetezzaSeedConfigBase, BaseSeedSpecificFormats):
    pass


class NetezzaSeedUniqueDelimiterTest(SeedUniqueDelimiterTestBase):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql_cleaned)

    # Override to specify datetimedelim
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": False, "delimiter": "|", "datetimedelim": " "}
        }


class TestSeedWithEmptyDelimiterNetezza(
    NetezzaSeedUniqueDelimiterTest, BaseSeedWithEmptyDelimiter
):
    pass


class TestSeedWithUniqueDelimiterNetezza(
    NetezzaSeedUniqueDelimiterTest, BaseSeedWithUniqueDelimiter
):
    pass


class TestSeedWithWrongDelimiterNetezza(
    NetezzaSeedUniqueDelimiterTest, BaseSeedWithWrongDelimiter
):
    pass


class TestSimpleSeedEnabledViaConfigNetezza(BaseSimpleSeedEnabledViaConfig):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"boolstyle": "TRUE_FALSE", "datetimedelim": " "}}


class TestSimpleSeedWithBOMNetezza(NetezzaSeedConfigBase, BaseSimpleSeedWithBOM):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql_cleaned)
        copy_file(
            project.test_dir,
            "seed_bom.csv",
            project.project_root / Path("seeds") / "seed_bom.csv",
            "",
        )

    pass


class TestSimpleSeedColumnOverrideNetezza(BaseSimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"boolstyle": "TRUE_FALSE", "datetimedelim": " "}}


class TestEmptySeedNetezza(BaseTestEmptySeed):
    pass
