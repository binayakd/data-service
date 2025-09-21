import polars as pl
import subprocess
from pathlib import Path
from jinja2 import Template

# -------- CONFIG --------
DATA_INVENTORY_CSV = "resources/data_inventory.csv"

TYPES_SCHEMA_DIR = "generated/schema/types"
QUERY_SCHEMA_FILE = "generated/schema/query.graphql"
QUERY_FUNCTION_FILE = "generated/query.py"

FEACHER_TEMPLATE_FILE = "scripts/templates/fetcher_template.jinja"
QUERY_TEMPLATE_FILE = "scripts/templates/query_template.jinja"

# mapping from inventory types to GraphQL scalar
GRAPHQL_TYPE_MAP = {
    "DATE": "String",
    "DECIMAL": "Float",
    "INTEGER": "Int"
}

def generate_type_sdl_from_inventory(data_id: str, data_id_inventory_df: pl.DataFrame, type_name: str) -> str:
    # type fields
    fields = []
    for row in data_id_inventory_df.iter_rows(named=True):
        gql_type = GRAPHQL_TYPE_MAP.get(row["data_type"])
        nullable = row["nullable"]
        fields.append(f'  {row["column_name"]}: {gql_type}{"!" if not nullable else ""}')

    type_def_sdl = f"type {type_name} {{\n" + "\n".join(fields) + "\n}"

    sdl_path = f"{TYPES_SCHEMA_DIR}/{data_id}.graphql"
    path = Path(sdl_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    Path(sdl_path).write_text(type_def_sdl)

    return path.as_posix()

def generate_data_id_graphql_dto(data_id: str, data_id_type_schema_path: str):
    # generate type code
    graphql_dto_path = f"generated/providers/{data_id}/dto.py"
    run_strawberry_codegen(data_id_type_schema_path, graphql_dto_path)


def generate_query_fields(query_fields: list[str], type_name: str):
    # add all the queries for each data id
    query_fields.append(f"  get{type_name}ByVersionId(versionId: String!): [{type_name}!]!")


def write_query_schema(query_fields):
    path = Path(QUERY_SCHEMA_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    query_def = "type Query {\n" + "\n".join(query_fields) + "\n}"
    path.write_text(query_def)

    return path.as_posix()


def run_strawberry_codegen(sdl_path: str, output_file: str):
    """Call Strawberry CLI programmatically to generate Python types."""
    subprocess.run(
        ["strawberry", "schema-codegen", sdl_path, "--output", output_file],
        check=True
    )
    print(f"Python types generated in {output_file}")


def generate_fetcher(data_id: str, type_name: str, data_id_inventory_df: pl.DataFrame):
    columns = data_id_inventory_df["column_name"].to_list()

    data = {
        "data_id": data_id,
        "type_name": type_name,
        "columns": columns
    }

    # read template
    template_text = Path(FEACHER_TEMPLATE_FILE).read_text()
    template = Template(template_text)

    # render template
    rendered = template.render(data=data)
    path = Path(f"generated/providers/{data_id}/fetcher.py")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered)


def generate_query(data_id_list: list[str]):
    template_text = Path(QUERY_TEMPLATE_FILE).read_text()
    template = Template(template_text)
    rendered = template.render(data_id_list=data_id_list)
    path = Path(QUERY_FUNCTION_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered)


def main():
    # read data inventory
    df = pl.read_csv(DATA_INVENTORY_CSV)

    # get unique set of data_ids
    data_id_list = df["data_id"].unique().to_list()

    query_fields = []

    # iterate over data ids
    for data_id in data_id_list:
        type_name = "".join([part.capitalize() for part in data_id.split("_")])
        data_id_inventory_df = df.filter(pl.col("data_id") == data_id)

        # generate the type schema
        data_id_type_schema_path = generate_type_sdl_from_inventory(data_id, data_id_inventory_df, type_name)

        # generate graphql dto file
        generate_data_id_graphql_dto(data_id, data_id_type_schema_path)

        # generate fetchers
        generate_fetcher(data_id, type_name, data_id_inventory_df)

        # add the queries for this data id
        generate_query_fields(query_fields, type_name)


    # generate code for query
    generate_query(data_id_list)


if __name__ == "__main__":
    main()