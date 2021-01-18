from typing import Iterable, Mapping, List


def columns_to_rows(columns: Mapping[str, list]) -> List[dict]:
    row_count = len(list(columns.values())[0])
    rows = [{} for _ in range(row_count)]
    for col_name in columns.keys():
        for j, val in enumerate(columns[col_name]):
            rows[j][col_name] = val

    return rows


def rows_to_columns(rows: Iterable[dict]):
    columns = {}
    for i, row in enumerate(list(rows)):
        for col, val in row.items():
            if i == 0:
                columns[col] = []
            columns[col].append(val)

    return columns
