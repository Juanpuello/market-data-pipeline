import os
from typing import Any, List, Optional

import great_expectations as gx
from great_expectations.core import ExpectationSuite, ValidationDefinition
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.datasource.fluent.interfaces import Datasource
from great_expectations.datasource.fluent.sql_datasource import _SQLAsset
from pydantic import BaseModel

from config.logging_config import get_logger

logger = get_logger(__name__)


def is_running_in_github_actions():
    return os.getenv("GITHUB_ACTIONS") == "true"


class BatchDefinitionConfig(BaseModel):
    type: str = "whole_table"
    date_column: Optional[str] = None
    date_grain: Optional[str] = None


class GXFacade:
    def __init__(
        self,
        data_source_name: str,
        table_name: str,
        expectations: List[Any],
        batch_config: BatchDefinitionConfig,
    ):
        self.context = gx.get_context(mode="file")
        self.data_source_name = data_source_name
        self.data_table_name = table_name
        self.batch_config = batch_config.dict()
        self.expectations = expectations

    def _setup_data_source(self) -> Datasource:
        return self.context.data_sources.get(name=self.data_source_name)

    def _setup_data_asset(self, data_source: Datasource) -> _SQLAsset:
        try:
            return data_source.get_asset(name=f"{self.data_table_name}_asset")
        except LookupError:
            return data_source.add_table_asset(
                table_name=self.data_table_name, name=f"{self.data_table_name}_asset"
            )

    def _setup_batch_definition(self, data_asset: _SQLAsset) -> BatchDefinition:
        batch_config = BatchDefinitionConfig(**self.batch_config)
        batch_type = batch_config.type
        if batch_type == "partitioned":
            batch_date_column = batch_config.date_column
            batch_grain = batch_config.date_grain
            if not batch_date_column or not batch_grain:
                raise ValueError(
                    "date_column and date_grain must be specified for partitioned batch_definition."
                )
            partitioned_batch_name = (
                f"{self.data_table_name}_{batch_date_column}_{batch_grain}"
                "_partitioned_batch"
            )
            try:
                return data_asset.get_batch_definition(name=partitioned_batch_name)
            except LookupError:
                if batch_grain == "daily":
                    return data_asset.add_batch_definition_daily(
                        name=partitioned_batch_name,
                        column=batch_date_column,
                    )
                elif batch_grain == "monthly":
                    return data_asset.add_batch_definition_monthly(
                        name=partitioned_batch_name,
                        column=batch_date_column,
                    )
                elif batch_grain == "yearly":
                    return data_asset.add_batch_definition_yearly(
                        name=partitioned_batch_name,
                        column=batch_date_column,
                    )
        full_batch_name = f"{self.data_table_name}_full_batch"
        try:
            return data_asset.get_batch_definition(name=full_batch_name)
        except LookupError:
            return data_asset.add_batch_definition_whole_table(name=full_batch_name)

    def _setup_expectation_suite(self) -> ExpectationSuite:
        name = f"{self.data_source_name}_{self.data_table_name}_expectation_suite"
        try:
            return self.context.suites.get(name=name)
        except gx.exceptions.DataContextError:
            return self.context.suites.add(gx.ExpectationSuite(name=name))

    def _add_expectations(self, suite: ExpectationSuite) -> None:
        for expectation in self.expectations:
            try:
                suite.delete_expectation(expectation)
                logger.info(f"Replacing existing expectation: {expectation}")
            except KeyError:
                logger.info(f"Expectation not found: {expectation}")
            finally:
                suite.add_expectation(expectation)
        suite.save()

    def _setup_validation_definition(
        self, batch_definition: BatchDefinition, expectation_suite: ExpectationSuite
    ) -> ValidationDefinition:
        validation_name = (
            f"{self.data_source_name}_{self.data_table_name}_"
            f"{batch_definition.name}_validation_definition"
        )
        try:
            return self.context.validation_definitions.get(name=validation_name)
        except gx.exceptions.DataContextError:
            return self.context.validation_definitions.add(
                gx.ValidationDefinition(
                    data=batch_definition,
                    suite=expectation_suite,
                    name=validation_name,
                )
            )

    def build(self):
        data_source: Datasource = self._setup_data_source()
        data_asset: _SQLAsset = self._setup_data_asset(data_source)
        batch_definition: BatchDefinition = self._setup_batch_definition(data_asset)
        expectation_suite: ExpectationSuite = self._setup_expectation_suite()
        self._add_expectations(expectation_suite)
        validation_definition: ValidationDefinition = self._setup_validation_definition(
            batch_definition, expectation_suite
        )
        return validation_definition


if __name__ == "__main__":
    preset_expectation_1 = gx.expectations.ExpectColumnMaxToBeBetween(
        column="clean_data_id", min_value=1, max_value=6, severity="warning"
    )
    preset_expectation_2 = gx.expectations.ExpectColumnMaxToBeBetween(
        column="raw_data_id", min_value=1, max_value=6, severity="warning"
    )
    batch_config_test = BatchDefinitionConfig(
        type="whole_table",
    )

    gx_facade = GXFacade(
        data_source_name="market_data_sqlite",
        table_name="clean_data",
        expectations=[preset_expectation_1, preset_expectation_2],
        batch_config=batch_config_test,
    )
    validation_definition_test = gx_facade.build()
    validation_results = validation_definition_test.run()
    print(validation_results)
