import os

import pytest
from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    expected_references_catalog,
    no_stats,
)
from dbt.tests.adapter.basic.files import (
    schema_base_yml,
)
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
    BaseGenerateProject,
    models__model_sql,
    models__readme_md,
    models__schema_yml,
    ref_models__docs_md,
    ref_models__ephemeral_copy_sql,
    ref_models__ephemeral_summary_sql,
    ref_models__schema_yml,
    ref_sources__schema_yml,
    run_and_generate,
    verify_catalog,
    write_project_files,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from dbt.tests.util import (
    check_relations_equal,
    relation_from_name,
    run_dbt,
)


@pytest.mark.skip("Fails to rename view to a table due to 'alter table' SQL")
# TODO Implement get_rename_view_sql in dbt-1.7 and enable test
class TestSimpleMaterializationsNetezza(BaseSimpleMaterializations):
    pass


class TestTableMaterializationNetezza(BaseTableMaterialization):
    pass


class TestSingularTestsNetezza(BaseSingularTests):
    pass


class TestSingularTestsEphemeralNetezza(BaseSingularTestsEphemeral):
    pass


class TestEmptyNetezza(BaseEmpty):
    pass


class TestEphemeralNetezza(BaseEphemeral):
    pass


class TestIncrementalNetezza(BaseIncremental):
    incremental_sql = """
    {{ config(materialized="incremental", unique_key="id") }}
    select 
        id, 
        name::varchar(255) as name, 
        some_date 
    from 
        {{ source('raw', 'seed') }}
    """.strip()

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": self.incremental_sql, "schema.yml": schema_base_yml}

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation}", fetch="one"
        )
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["-d", "run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["added", "incremental"])


class TestIncrementalNotSchemaChangeNetezza(BaseIncrementalNotSchemaChange):
    pass


class TestGenericTestsNetezza(BaseGenericTests):
    pass


class NetezzaSnapshotSeedConfig:
    # Override to set varchar widths to allow snapshot to store longer values
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "base": {"+column_types": {"name": "varchar(100)"}},
                    "added": {"+column_types": {"name": "varchar(100)"}},
                }
            }
        }


class TestSnapshotCheckColsNetezza(NetezzaSnapshotSeedConfig, BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampNetezza(NetezzaSnapshotSeedConfig, BaseSnapshotTimestamp):
    pass


class TestCachingNetezza(BaseAdapterMethod):
    def test_adapter_methods(self, project, equal_tables):
        with pytest.raises(RuntimeError, match="does not support"):
            super().test_adapter_methods(project, equal_tables)


class NetezzaGenerateProject(BaseGenerateProject):
    class AnyCharacterVarying:
        def __eq__(self, other):
            return other.startswith("CHARACTER VARYING")

    @pytest.fixture(scope="class", autouse=True)
    def project_cleanup_extra_relations(self):
        return [("view", "second_model"), ("view", "model"), ("table", "seed")]

    @pytest.fixture(scope="class", autouse=True)
    def project_cleanup_use_manifest(self):
        return False

    # Override to remove test schema creation
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        os.environ["DBT_ENV_CUSTOM_ENV_env_key"] = "env_value"
        assets = {"lorem-ipsum.txt": "Lorem ipsum dolor sit amet"}
        write_project_files(project.project_root, "assets", assets)
        run_dbt(["seed"])
        yield
        del os.environ["DBT_ENV_CUSTOM_ENV_env_key"]

    # Override to use the same schema for both schema vars
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "asset-paths": ["assets", "invalid-asset-paths"],
            "vars": {
                "test_schema": unique_schema,
                "alternate_schema": unique_schema,
            },
            "seeds": {"quote_columns": False, "datetimedelim": " "},
        }


class TestDocsGenerateNetezza(NetezzaGenerateProject, BaseDocsGenerate):
    models__second_model_sql = """
    {{
        config(
            materialized='view',
        )
    }}

    select * from {{ ref('seed') }}
    """

    # Override to remove schema declaration in second_model
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "second_model.sql": self.models__second_model_sql,
            "readme.md": models__readme_md,
            "model.sql": models__model_sql,
        }

    # Override to modify casing and types
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user, unique_schema):
        expected = base_expected_catalog(
            project,
            role=profile_user.upper(),
            id_type="INTEGER",
            text_type=self.AnyCharacterVarying(),
            time_type="TIMESTAMP",
            view_type="VIEW",
            table_type="TABLE",
            model_stats=no_stats(),
            case=str.upper,
            case_columns=True,
        )
        expected["nodes"]["model.test.second_model"]["metadata"]["schema"] = (
            unique_schema
        )
        return expected


class TestDocsGenReferencesNetezza(NetezzaGenerateProject, BaseDocsGenReferences):
    # Override to remove invalid 'order by' for view
    ref_models__view_summary_sql = """
    {{
    config(
        materialized = "view"
    )
    }}

    select first_name, ct from {{ref('ephemeral_summary')}}
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": ref_models__schema_yml,
            "sources.yml": ref_sources__schema_yml,
            "view_summary.sql": self.ref_models__view_summary_sql,
            "ephemeral_summary.sql": ref_models__ephemeral_summary_sql,
            "ephemeral_copy.sql": ref_models__ephemeral_copy_sql,
            "docs.md": ref_models__docs_md,
        }

    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        expected = expected_references_catalog(
            project,
            role=profile_user.upper(),
            id_type="INTEGER",
            text_type=self.AnyCharacterVarying(),
            time_type="TIMESTAMP",
            bigint_type="BIGINT",
            view_type="VIEW",
            table_type="TABLE",
            model_stats=no_stats(),
            case=str.upper,
            case_columns=True,
        )

        # Uppercase columns keys and names because expected_references_catalog does not use
        # col_case consistently like base_expected_catalog
        for category in ["nodes", "sources"]:
            for relation in expected[category]:
                columns = {}
                for column_name in expected[category][relation]["columns"]:
                    column = expected[category][relation]["columns"][column_name]
                    column["name"] = column["name"].upper()
                    columns[column_name.upper()] = column
                expected[category][relation]["columns"] = columns
        return expected

    def test_references(self, project, expected_catalog):
        start_time = run_and_generate(project)
        verify_catalog(project, expected_catalog, start_time)


class TestValidateConnectionNetezza(BaseValidateConnection):
    pass
