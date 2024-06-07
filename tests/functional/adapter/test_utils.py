import pytest
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampNaive
from dbt.tests.adapter.utils.test_date_spine import (
    BaseDateSpine,
    models__test_date_spine_sql,
    models__test_date_spine_yml,
)
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_equals import BaseEquals
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesQuote,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_generate_series import BaseGenerateSeries
from dbt.tests.adapter.utils.test_get_intervals_between import BaseGetIntervalsBetween
from dbt.tests.adapter.utils.test_get_powers_of_two import BaseGetPowersOfTwo
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import (
    BaseListagg,
    models__test_listagg_yml,
    seeds__data_listagg_csv,
)
from dbt.tests.adapter.utils.test_null_compare import (
    BaseMixedNullCompare,
    BaseNullCompare,
)
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps
from dbt.tests.adapter.utils.test_validate_sql import BaseValidateSqlMethod
from dbt.tests.util import check_relations_equal, run_dbt


@pytest.mark.skip("any_value not supported by this adapter")
class TestAnyValueNetezza(BaseAnyValue):
    pass


class TestArrayAppendNetezza(BaseArrayAppend):
    pass


class TestArrayConcatNetezza(BaseArrayConcat):
    # Override to skip data type check
    def test_expected_actual(self, project):
        run_dbt(["build"])

        # check contents equal
        check_relations_equal(project.adapter, ["expected", "actual"])


class TestArrayConstructNetezza(BaseArrayConstruct):
    pass


class TestBoolOrNetezza(BaseBoolOr):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"boolstyle": "TRUE_FALSE"}}


class TestCastBoolToTextNetezza(BaseCastBoolToText):
    pass


class TestConcatNetezza(BaseConcat):
    pass


# Netezza does not support datetimes with specified timezone,
# so BaseCurrentTimestampNaive was implemented
class TestCurrentTimestampNetezza(BaseCurrentTimestampNaive):
    pass


class TestCurrentTimestampsNetezza(BaseCurrentTimestamps):
    # Rename current_timestamp to current_timestamp_default
    model_current_timestamp = """
select {{ current_timestamp() }} as current_timestamp_default,
       {{ current_timestamp_in_utc_backcompat() }} as current_timestamp_in_utc_backcompat,
       {{ current_timestamp_backcompat() }} as current_timestamp_backcompat
"""

    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "CURRENT_TIMESTAMP_DEFAULT": "TIMESTAMP",
            "CURRENT_TIMESTAMP_IN_UTC_BACKCOMPAT": "TIMESTAMP",
            "CURRENT_TIMESTAMP_BACKCOMPAT": "TIMESTAMP",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"get_current_timestamp.sql": self.model_current_timestamp}

    # any adapters that don't want to check can set expected schema to None
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return None


class TestDateTruncNetezza(BaseDateTrunc):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"datetimedelim": " "}}


class TestDateAddNetezza(BaseDateAdd):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"datetimedelim": " "}}


class TestDateDiffNetezza(BaseDateDiff):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"datetimedelim": " "}}


class TestDateSpineNetezza(BaseDateSpine):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_spine.yml": models__test_date_spine_yml,
            "test_date_spine.sql": self.interpolate_macro_namespace(
                models__test_date_spine_sql.replace("postgres", "netezza"), "date_spine"
            ),
        }


class TestEscapeSingleQuotesNetezza(BaseEscapeSingleQuotesQuote):
    pass


class TestExceptNetezza(BaseExcept):
    pass


class TestEqualsNetezza(BaseEquals):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"nullvalue": "null"}}


class TestGenerateSeriesNetezza(BaseGenerateSeries):
    pass


class TestGetIntervalsBetweenNetezza(BaseGetIntervalsBetween):
    pass


class TestGetPowersOfTwoNetezza(BaseGetPowersOfTwo):
    pass


class TestHashNetezza(BaseHash):
    pass


class TestIntersectNetezza(BaseIntersect):
    pass


class TestLastDayNetezza(BaseLastDay):
    pass


class TestLengthNetezza(BaseLength):
    pass


class TestListaggNetezza(BaseListagg):
    seeds__data_listagg_output_csv = """group_col,expected,version
3,"g|g|g",pipe_delimiter
3,"g,g,g",no_params
"""
    models__test_listagg_sql = """
with data as (

    select * from {{ ref('data_listagg') }}

),

data_output as (

    select * from {{ ref('data_listagg_output') }}

),

calculate as (

    select
        group_col,
        {{ listagg('string_text', "'|'") }} as actual,
        'pipe_delimiter' as version
    from data
    where group_col = 3
    group by group_col

    union all

    select
        group_col,
        {{ listagg('string_text') }} as actual,
        'no_params' as version
    from data
    where group_col = 3
    group by group_col

)

select
    calculate.actual,
    data_output.expected
from calculate
left join data_output
on calculate.group_col = data_output.group_col
and calculate.version = data_output.version
"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_listagg.csv": seeds__data_listagg_csv,
            "data_listagg_output.csv": self.seeds__data_listagg_output_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                self.models__test_listagg_sql, "listagg"
            ),
        }


class TestListaggErrorOrderByNetezza(BaseListagg):
    models__test_listagg_sql = """
    select {{ listagg('string_text', "'|'", "order by order_col") }},
    from {{ ref('data_listagg') }}
    group by group_col"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                self.models__test_listagg_sql, "listagg"
            ),
        }

    def test_build_assert_equal(self, project):
        with pytest.raises(RuntimeError, match="does not support 'order_by_clause'"):
            super().test_build_assert_equal(project)


class TestListaggErrorLimitNetezza(BaseListagg):
    models__test_listagg_sql = """
    select {{ listagg('string_text', "'|'", "", 2) }},
    from {{ ref('data_listagg') }}
    group by group_col"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                self.models__test_listagg_sql, "listagg"
            ),
        }

    def test_build_assert_equal(self, project):
        with pytest.raises(RuntimeError, match="does not support 'limit_num'"):
            super().test_build_assert_equal(project)


class TestListaggErrorMultiCharacterDelimiterByNetezza(BaseListagg):
    models__test_listagg_sql = """
    select {{ listagg('string_text', "'_|_'") }},
    from {{ ref('data_listagg') }}
    group by group_col"""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                self.models__test_listagg_sql, "listagg"
            ),
        }

    def test_build_assert_equal(self, project):
        with pytest.raises(RuntimeError, match="does not support multiple characters"):
            super().test_build_assert_equal(project)


class TestMixedNullCompareNetezza(BaseMixedNullCompare):
    pass


class TestNullCompareNetezza(BaseNullCompare):
    pass


class TestPositionNetezza(BasePosition):
    pass


class TestReplaceNetezza(BaseReplace):
    pass


class TestRightNetezza(BaseRight):
    pass


class TestSafeCastNetezza(BaseSafeCast):
    pass


class TestSplitPartNetezza(BaseSplitPart):
    pass


class TestStringLiteralNetezza(BaseStringLiteral):
    pass


class TestValidateSqlMethodNetezza(BaseValidateSqlMethod):
    pass
