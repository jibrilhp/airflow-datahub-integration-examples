from __future__ import annotations

import enum
import attr
from airflow.models import BaseOperator, BaseOperatorLink, Variable
from airflow.models.xcom import XCom
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.decorators import apply_defaults
# from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.configuration import conf
from sql_metadata import Parser
from datahub_provider.entities import Dataset
from typing import TYPE_CHECKING, Any, Iterable, Sequence
from jinja2 import Environment

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.utils.context import Context

BIGQUERY_JOB_DETAILS_LINK_FMT = "https://console.cloud.google.com/bigquery?j={job_id}"


class BigQueryUIColors(enum.Enum):
    """Hex colors for BigQuery operators"""

    CHECK = "#C0D7FF"
    QUERY = "#A1BBFF"
    TABLE = "#81A0FF"
    DATASET = "#5F86FF"


class IfExistAction(enum.Enum):
    """Action to take if the resource exist"""

    IGNORE = "ignore"
    LOG = "log"
    FAIL = "fail"
    SKIP = "skip"


class BigQueryConsoleLink(BaseOperatorLink):
    """Helper class for constructing BigQuery link."""

    name = "BigQuery Console"

    def get_link(
            self,
            operator: BaseOperator,
            *,
            ti_key: TaskInstanceKey,
    ):
        job_id = XCom.get_value(key="job_id", ti_key=ti_key)
        return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id) if job_id else ""


@attr.s(auto_attribs=True)
class BigQueryConsoleIndexableLink(BaseOperatorLink):
    """Helper class for constructing BigQuery link."""

    index: int = attr.ib()

    @property
    def name(self) -> str:
        return f"BigQuery Console #{self.index + 1}"

    def get_link(
            self,
            operator: BaseOperator,
            *,
            ti_key: TaskInstanceKey,
    ):
        job_ids = XCom.get_value(key="job_id", ti_key=ti_key)
        if not job_ids:
            return None
        if len(job_ids) < self.index:
            return None
        job_id = job_ids[self.index]
        return BIGQUERY_JOB_DETAILS_LINK_FMT.format(job_id=job_id)


class BigQueryExecuteQueryDatahubOperator(BigQueryExecuteQueryOperator):
    template_fields: Sequence[str] = (
        "sql",
        "destination_dataset_table",
        "labels",
        "query_params",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = BigQueryUIColors.QUERY.value

    @property
    def operator_extra_links(self):
        """Return operator extra links"""
        if isinstance(self.sql, str):
            return (BigQueryConsoleLink(),)
        return (BigQueryConsoleIndexableLink(i) for i, _ in enumerate(self.sql))

    def __dummy(self, *args, **kwargs):
        return ''

    def __parse_inlets_bigquery(self, inlet_query: str, params: str, context=None, **kwargs):

        env_variable = Variable.get("environment_name").replace('development', 'DEV').replace('production', 'PROD')

        try:
            with open(conf.get('core', 'dags_folder') + '/' + inlet_query, 'r') as f:
                read_text = f.read()
                clean_query = ''
                for y in read_text.split('\n'):
                    if y.find('--') == -1:
                        clean_query += y + '\n'
                if context is None:
                    contexts = {'params': params, 'task_instance': {'xcom_pull': self.__dummy},
                                'macros': {'timedelta': self.__dummy}, 'logical_date': {'strftime': self.__dummy},
                                'ti': {'xcom_pull': self.__dummy}
                                }
                else:
                    contexts = {'params': params, 'task_instance': context['task_instance']}

                clean_query_rendered = Environment().from_string(
                    clean_query).render(contexts)
                clean_query_rendered = clean_query_rendered.replace("-", "_")

                try:
                    parser = Parser(clean_query_rendered)
                except Exception as e:
                    raise ValueError('cannot parse the query ' + inlet_query)
        except IOError:
            try:
                parser = Parser(inlet_query)
            except Exception as e:
                raise ValueError('cannot parse the query ' + inlet_query)

        result = []
        if parser is not None:
            for x in parser.tables:
                if '.' in x:
                    if len(x.split('.')) == 3:
                        result.append(x.split('.')[0].replace('_', '-') + '.' + (x.split('.')[1]) + '.' + (x.split('.')[2]))
                    else:
                        result.append(x)

        results = []
        for n in result:
            results.append(Dataset("bigquery", n, env_variable))
        return results

    def __parse_outlet_bigquery(self, outlet_table: str):
        env_variable = Variable.get("environment_name").replace('development', 'DEV').replace('production', 'PROD')
        return [Dataset("bigquery", outlet_table, env_variable)]

    @apply_defaults
    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = None
        # Lineage
        self.inlets = self.__parse_inlets_bigquery(self.sql, self.params, None)
        self.outlets = self.__parse_outlet_bigquery(self.destination_dataset_table)

    def execute(self, context: Context):
        if self.hook is None:
            self.log.info("Executing: %s", self.sql)
            self.hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                use_legacy_sql=self.use_legacy_sql,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )
        if isinstance(self.sql, str):
            job_id: str | list[str] = self.hook.run_query(
                sql=self.sql,
                destination_dataset_table=self.destination_dataset_table,
                write_disposition=self.write_disposition,
                allow_large_results=self.allow_large_results,
                flatten_results=self.flatten_results,
                udf_config=self.udf_config,
                maximum_billing_tier=self.maximum_billing_tier,
                maximum_bytes_billed=self.maximum_bytes_billed,
                create_disposition=self.create_disposition,
                query_params=self.query_params,
                labels=self.labels,
                schema_update_options=self.schema_update_options,
                priority=self.priority,
                time_partitioning=self.time_partitioning,
                api_resource_configs=self.api_resource_configs,
                cluster_fields=self.cluster_fields,
                encryption_configuration=self.encryption_configuration,
            )

            self.inlets = self.__parse_inlets_bigquery(self.sql, self.params, context)
            self.outlets = self.__parse_outlet_bigquery(self.destination_dataset_table)

        elif isinstance(self.sql, Iterable):
            job_id = [
                self.hook.run_query(
                    sql=s,
                    destination_dataset_table=self.destination_dataset_table,
                    write_disposition=self.write_disposition,
                    allow_large_results=self.allow_large_results,
                    flatten_results=self.flatten_results,
                    udf_config=self.udf_config,
                    maximum_billing_tier=self.maximum_billing_tier,
                    maximum_bytes_billed=self.maximum_bytes_billed,
                    create_disposition=self.create_disposition,
                    query_params=self.query_params,
                    labels=self.labels,
                    schema_update_options=self.schema_update_options,
                    priority=self.priority,
                    time_partitioning=self.time_partitioning,
                    api_resource_configs=self.api_resource_configs,
                    cluster_fields=self.cluster_fields,
                    encryption_configuration=self.encryption_configuration,
                )
                for s in self.sql
            ]
        else:
            raise ValueError(f"argument 'sql' of type {type(str)} is neither a string nor an iterable")

        context["task_instance"].xcom_push(key="job_id", value=job_id)

    def on_kill(self) -> None:
        super().on_kill()
        if self.hook is not None:
            self.log.info("Cancelling running query")
            self.hook.cancel_job(self.hook.running_job_id)
