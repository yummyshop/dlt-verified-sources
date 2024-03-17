"""Fetches Shopify Orders and Products."""

from typing import Any, Dict, Iterator, Optional, Iterable

import dlt

from dlt.sources import DltResource
from dlt.common.typing import TDataItem, TAnyDateTime
from dlt.common.time import ensure_pendulum_datetime
from dlt.common import pendulum
from dlt.common import jsonpath as jp

from .settings import (
    DEFAULT_API_VERSION,
    FIRST_DAY_OF_MILLENNIUM,
    DEFAULT_ITEMS_PER_PAGE,
    DEFAULT_PARTNER_API_VERSION,
)
from .helpers import ShopifyPartnerApi, ShopifyAdminApi, ShopifySchemaBuilder
from .gql_queries import bulk_query, simple_query
import requests
import json
from datetime import datetime
from time import sleep


DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

def check_status(operation_id: str, shopify_admin_client: ShopifyAdminApi, sleep_time=10, timeout=36000) -> str:
    status_jsonpath = "$.data.currentBulkOperation"
    start = datetime.now().timestamp()

    while datetime.now().timestamp() < (start + timeout):
        status_response = shopify_admin_client.get_operation_status()
        status = next(
            iter(jp.find_values(status_jsonpath, input=status_response.json()))
        )
        if status["id"] != operation_id:
            raise Exception(
                "The current job was not triggered by the process, "
                "check if other service is using the Bulk API"
            )
        if status["url"]:
            return status["url"]
        if status["status"] == "FAILED":
            raise Exception(f"Job failed: {status['errorCode']}")
        sleep(sleep_time)
    raise Exception("Job Timeout")

def parse_response_bulk(json_resp, shopify_admin_api_client: ShopifyAdminApi) -> Iterable[dict]:
    """Parse the response and return an iterator of result rows."""
    operation_id_jsonpath = "$.data.bulkOperationRunQuery.bulkOperation.id"
    error_jsonpath = "$.data.bulkOperationRunQuery.userErrors"
    errors = next(iter(jp.find_values(error_jsonpath, json_resp)), None)
    if errors:
        raise Exception(json.dumps(errors))
    operation_id = next(iter(jp.find_values(operation_id_jsonpath, json_resp)))

    url = check_status(operation_id, shopify_admin_api_client)

    output = requests.get(url, stream=True, timeout=30)
    for line in output.iter_lines():
        print(line)


@dlt.source(name="shopify")
def shopify_source(
    private_app_password: str = dlt.secrets.value,
    api_version: str = DEFAULT_API_VERSION,
    shop_name: str = dlt.config.value,
    start_date: TAnyDateTime = FIRST_DAY_OF_MILLENNIUM,
    end_date: Optional[TAnyDateTime] = None,
    created_at_min: TAnyDateTime = FIRST_DAY_OF_MILLENNIUM,
    items_per_page: int = DEFAULT_ITEMS_PER_PAGE,
) -> Iterable[DltResource]:
    """
    The source for the Shopify pipeline. Available resources are products, orders, and customers.

    `start_time` argument can be used on its own or together with `end_time`. When both are provided
    data is limited to items updated in that time range.
    The range is "half-open", meaning elements equal and newer than `start_time` and elements older than `end_time` are included.
    All resources opt-in to use Airflow scheduler if run as Airflow task

    Args:
        private_app_password: The app password to the app on your shop.
        api_version: The API version to use (e.g. 2023-01).
        shop_url: The URL of your shop (e.g. https://my-shop.myshopify.com).
        items_per_page: The max number of items to fetch per page. Defaults to 250.
        start_date: Items updated on or after this date are imported. Defaults to 2000-01-01.
            If end date is not provided, this is used as the initial value for incremental loading and after the initial run, only new data will be retrieved.
            Accepts any `date`/`datetime` object or a date/datetime string in ISO 8601 format.
        end_time: The end time of the range for which to load data.
            Should be used together with `start_date` to limit the data to items updated in that time range.
            If end time is not provided, the incremental loading will be enabled and after initial run, only new data will be retrieved
        created_at_min: The minimum creation date of items to import. Items created on or after this date are loaded. Defaults to 2000-01-01.
        order_status: The order status to filter by. Can be 'open', 'closed', 'cancelled', or 'any'. Defaults to 'any'.

    Returns:
        Iterable[DltResource]: A list of DltResource objects representing the data resources.
    """
    from graphql import GraphQLObjectType, GraphQLScalarType, GraphQLList, GraphQLNonNull, GraphQLEnumType, build_client_schema
    start_date_obj = ensure_pendulum_datetime(start_date)
    admin_client = ShopifyAdminApi(private_app_password, shop_name, api_version)
    #from .gql_queries import schema_query, queries_query
    #res = admin_client.run_graphql_query(schema_query)
    #data = res["data"]
    ignore_fields = [
        "image",
        "metafield",
        "metafields",
        "metafieldconnection",
        "privateMetafield",
        "privateMetafields",
    ]
    import json
    with open('shopify_schema.json') as f:
        data = json.load(f)
    schema = build_client_schema(data)

    def is_queryable(object_type) -> bool:
        query_type = schema.query_type
        for query_field in query_type.fields.values():
            if query_field.type == object_type:
                return True
        return False


    def construct_fields_query(field_type, level: int = 8, indent: str = "", field_name: Optional[str] = None, visited_types: Optional[set] = None, args = None) -> str:
        if visited_types is None:
            visited_types = set()

        if isinstance(field_type, (GraphQLNonNull, GraphQLList)):
            # Unpack object
            field_type = field_type.of_type
            return construct_fields_query(field_type, level, indent, field_name, visited_types, args)
        elif isinstance(field_type, GraphQLObjectType):
            # Reached maximum allowed depth
            if level == 0:
                return ""

            # If object was already visited along the path, skip it
            # We do not want infinite recursion
            if field_type.name in visited_types:
                return ""
            visited_types.add(field_type.name)

            # If we can query this object using other queries only specify the id field
            if is_queryable(field_type) and field_name not in ["node"]:
                visited_types.remove(field_type.name)
                return f"{indent}{field_name} {{ id }}\n"

            field_queries = []
            for field_name_internal, field in field_type.fields.items():
                if field.deprecation_reason:
                    continue
                if field_name_internal in ignore_fields:
                    continue
                if field_name_internal in ["nodes", "pageInfo", "cursor"]:
                    continue

                field_queries.append(construct_fields_query(field.type, level - 1, indent + " ", field_name_internal, visited_types, field.args))

            # Do not repeat types within the same recursion path
            # But allow same type in different paths
            visited_types.remove(field_type.name)

            # If some fields were found
            if "".join(field_queries):
                if field_type.name.endswith("Connection") and field_name:
                    # Connection types require either first or last argument
                    # There is no good number to use as it depends on how many items are there.
                    field_queries.insert(0, f"{indent}{field_name}(first: 10) {{\n")
                    field_queries.append(f"{indent}}}\n")
                elif args:
                    # In case of any arguments we return an empty results
                    return ""
                elif field_name:
                    field_queries.insert(0, f"{indent}{field_name} {{\n")
                    field_queries.append(f"{indent}}}\n")
                return "".join(field_queries)
        elif isinstance(field_type, (GraphQLScalarType, GraphQLEnumType)):
            return f"{indent}{field_name}\n"
        return ""

    def build_query(query_name, start_date: Optional[pendulum.DateTime] = None) -> str:
        query_type = schema.query_type
        query_field = query_type.fields.get(query_name)

        fields_query = construct_fields_query(query_field.type)

        filters = []
        if start_date:
            filters.append(f'query: "updated_at:>{start_date.strftime(DATETIME_FORMAT)}"')

        query = simple_query
        query = query.replace('__query_name__', query_name)
        query = query.replace('__selected_fields__', fields_query)
        query = query.replace('__filters__', ','.join(filters))
        return query

    def build_bulk_query(query: str) -> str:
        base_query = bulk_query
        return base_query.replace('__full_query__', query)

    final_query = build_query('orders', start_date=start_date_obj)
    final_bulk_query = build_bulk_query(final_query)
    with open('example_shopify_query.graphql', 'w') as f:
        f.write(final_query)
    #res = admin_client.run_graphql_query(final_query)
    #parse_response_bulk(res, admin_client)
    return ()
    # end_date_obj = ensure_pendulum_datetime(end_date) if end_date else None
    # created_at_min_obj = ensure_pendulum_datetime(created_at_min)

    # return schema_builder.get_resources()


@dlt.resource
def shopify_partner_query(
    query: str,
    data_items_path: jp.TJsonPath,
    pagination_cursor_path: jp.TJsonPath,
    pagination_variable_name: str = "after",
    variables: Optional[Dict[str, Any]] = None,
    access_token: str = dlt.secrets.value,
    organization_id: str = dlt.config.value,
    api_version: str = DEFAULT_PARTNER_API_VERSION,
) -> Iterable[TDataItem]:
    """
    Resource for getting paginated results from the Shopify Partner GraphQL API.

    This resource will run the given GraphQL query and extract a list of data items from the result.
    It will then run the query again with a pagination cursor to get the next page of results.

    Example:
        query = '''query Transactions($after: String) {
            transactions(after: $after, first: 100) {
                edges {
                    cursor
                    node {
                        id
                    }
                }
            }
        }'''

        partner_query_pages(
            query,
            data_items_path="data.transactions.edges[*].node",
            pagination_cursor_path="data.transactions.edges[-1].cursor",
            pagination_variable_name="after",
        )

    Args:
        query: The GraphQL query to run.
        data_items_path: The JSONPath to the data items in the query result. Should resolve to array items.
        pagination_cursor_path: The JSONPath to the pagination cursor in the query result, will be piped to the next query via variables.
        pagination_variable_name: The name of the variable to pass the pagination cursor to.
        variables: Mapping of extra variables used in the query.
        access_token: The Partner API Client access token, created in the Partner Dashboard.
        organization_id: Your Organization ID, found in the Partner Dashboard.
        api_version: The API version to use (e.g. 2024-01). Use `unstable` for the latest version.
    Returns:
        Iterable[TDataItem]: A generator of the query results.
    """
    client = ShopifyPartnerApi(
        access_token=access_token,
        organization_id=organization_id,
        api_version=api_version,
    )

    yield from client.get_graphql_pages(
        query,
        data_items_path=data_items_path,
        pagination_cursor_path=pagination_cursor_path,
        pagination_variable_name=pagination_variable_name,
        variables=variables,
    )
