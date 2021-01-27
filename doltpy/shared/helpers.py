from collections import defaultdict
from typing import Iterable, List, Mapping


def columns_to_rows(columns: Mapping[str, list]) -> List[dict]:
    row_count = len(list(columns.values())[0])
    rows: List[dict] = [{} for _ in range(row_count)]
    for col_name in columns.keys():
        for j, val in enumerate(columns[col_name]):
            rows[j][col_name] = val

    return rows


def rows_to_columns(rows: Iterable[dict]) -> Mapping[str, list]:
    columns: Mapping[str, list] = defaultdict(list)
    for i, row in enumerate(list(rows)):
        for col, val in row.items():
            columns[col].append(val)

    return columns
