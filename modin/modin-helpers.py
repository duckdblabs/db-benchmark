import os

# Run configuration
os.environ["MODIN_CPUS"] = "40"

do_execute=True
# os.environ['MODIN_HDK_FRAGMENT_SIZE'] = "32000000"
solution_txt = (
    f"modin_{os.environ.get('MODIN_HDK_FRAGMENT_SIZE', 'nfs')}_" +
    ("exec" if do_execute else "noexec")
)

# Set up HDK backend
os.environ["MODIN_ENGINE"] = "native"
os.environ["MODIN_STORAGE_FORMAT"] = "hdk"
os.environ["MODIN_EXPERIMENTAL"] = "True"

import pyhdk
pyhdk.init()
# pyhdk.init(enable_non_lazy_data_import=True)


def init_modin_on_hdk(pd):
    from modin.experimental.sql import query

    # Calcite initialization
    data = {"a": [1, 2, 3]}
    df = pd.DataFrame(data)
    query("SELECT * FROM df", df=df)


def execute(df, *, trigger_hdk_import: bool = False):
    if trigger_hdk_import:
        trigger_import(df)
    else:
        if do_execute:
            df._query_compiler._modin_frame._execute()
    return df


def trigger_import(df):
    """
    Trigger import execution for DataFrame obtained by HDK engine.
    Parameters
    ----------
    df : DataFrame
        DataFrame for trigger import.
    """
    modin_frame = df._query_compiler._modin_frame
    if hasattr(modin_frame, "force_import"):
        modin_frame.force_import()
        return
