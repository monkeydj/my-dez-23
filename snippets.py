import pandas as pd

"""Various useful codes from community.
Caveats: with minor refactorings
"""


def find_fixing_columns(df: pd.DataFrame) -> dict:
    """Find problematic columns after inspecting df.dtypes
    by Valentine Zaretsky
    ref: https://datatalks-club.slack.com/archives/C01FABYF2RG/p1676302863137519?thread_ts=1675869990.574139&cid=C01FABYF2RG
    """

    mixed = []
    int_na = []

    for col in df.columns:
        weird = (df[[col]].applymap(type) != df[[col]].iloc[0].apply(type)).any(axis=1)
        if len(df[weird]) > 0:
            mixed.append(col)

    for col in [x for x in df.dtypes.keys() if df.dtypes[x] == 'float64']:
        if all(x.is_integer() for x in df[df[col].isna() == False][col]):
            int_na.append(col)

    return {'mixed_types': mixed, 'int_na': int_na}
