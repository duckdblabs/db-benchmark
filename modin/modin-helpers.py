import os


# Set up HDK backend
os.environ["MODIN_ENGINE"] = "native"
os.environ["MODIN_STORAGE_FORMAT"] = "hdk"
os.environ["MODIN_EXPERIMENTAL"] = "True"


def init_modin_on_hdk(pd):
    """Modin on HDK warmup before benchmarking for calcite"""
    from modin.experimental.sql import query

    data = {"a": [1, 2, 3]}
    df = pd.DataFrame(data)
    query("SELECT * FROM df", df=df)
