#!/usr/bin/env python3

print("# rollfun-pandas.py", flush=True)

import os
import gc
import sys
import timeit
import pandas as pd
import numpy as np ## q8 q9

exec(open("./_helpers/helpers.py").read())

ver = pd.__version__
git = pd.__git_version__
task = "rollfun"
solution = "pandas"
fun = ".rolling"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ['SRC_DATANAME']
src_grp = os.path.join("data", data_name+".csv")
print("loading dataset %s" % data_name, flush=True)

na_flag = int(data_name.split("_")[3])
if na_flag > 0:
  print("skip due to na_flag>0: #171", flush=True, file=sys.stderr)
  exit(0) # not yet implemented #171

x = pd.read_csv(src_grp)

print(len(x.index), flush=True)

# window size
w = int(len(x.index)/1e3)
wsmall = int(len(x.index)/1e4)
wbig = int(len(x.index)/1e2)

task_init = timeit.default_timer()
print("rolling...", flush=True)

question = "mean" # q1
gc.collect()
t_start = timeit.default_timer()
ans = x['v1'].rolling(w).mean()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x['v1'].rolling(w).mean()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "window small" # q2
gc.collect()
t_start = timeit.default_timer()
ans = x['v1'].rolling(wsmall).mean()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x['v1'].rolling(wsmall).mean()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "window big" # q3
gc.collect()
t_start = timeit.default_timer()
ans = x['v1'].rolling(wbig).mean()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x['v1'].rolling(wbig).mean()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "min" # q4
gc.collect()
t_start = timeit.default_timer()
ans = x['v1'].rolling(w).min()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x['v1'].rolling(w).min()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "median" # q5
gc.collect()
t_start = timeit.default_timer()
ans = x['v1'].rolling(w).median()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = x['v1'].rolling(w).median()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

question = "multiroll" # q6
gc.collect()
t_start = timeit.default_timer()
ans = pd.concat([x[['v1','v2']].rolling(w-50).mean().reset_index(drop=True), x[['v1','v2']].rolling(w+50).mean().reset_index(drop=True)], axis=1).set_axis(['v1_small', 'v1_big', 'v2_small', 'v2_big'], axis=1)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1_small'].sum(), ans['v1_big'].sum(), ans['v2_small'].sum(), ans['v2_big'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = pd.concat([x[['v1','v2']].rolling(w-50).mean().reset_index(drop=True), x[['v1','v2']].rolling(w+50).mean().reset_index(drop=True)], axis=1).set_axis(['v1_small', 'v1_big', 'v2_small', 'v2_big'], axis=1)
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [ans['v1_small'].sum(), ans['v1_big'].sum(), ans['v2_small'].sum(), ans['v2_big'].sum()]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans

#question = "weighted" # q7 ## not yet implemented, tried with apply but seems to need to pass whole df to lamba which will be very slow
#gc.collect()
#ans = x['v1'].rolling(w).mean()
#t_start = timeit.default_timer()
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = ans.sum()
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x['v1'].rolling(w).mean()
#t_start = timeit.default_timer()
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = ans.sum()
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#del ans

question = "uneven dense" # q8
## we do not include below pre/post-processing in query timing because pandas has rich feature support related to unevenly spaced time series
## it just doesn't seem to support the most basic and most generic integer based index. and we do want to stick most generic approach for portability
y = x.set_axis(pd.to_timedelta(x['id2'], unit='s'))[['v1']]
ws = f'{w}s'
gc.collect()
t_start = timeit.default_timer()
ans = y.rolling(ws).mean()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
ans.iloc[:w-1] = np.nan
ans.reset_index(drop=True, inplace=True)
ans = ans['v1']
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = y.rolling(ws).mean()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
ans.iloc[:w-1] = np.nan
ans.reset_index(drop=True, inplace=True)
ans = ans['v1']
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans, y

question = "uneven sparse" # q9
## we do not include below pre/post-processing in query timing because pandas has rich feature support related to unevenly spaced time series
## it just doesn't seem to support the most basic and most generic integer based index. and we do want to stick most generic approach for portability
y = x.set_axis(pd.to_timedelta(x['id3'], unit='s'))[['v1']]
ws = f'{w}s'
gc.collect()
t_start = timeit.default_timer()
ans = y.rolling(ws).mean()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
ans.iloc[:w-1] = np.nan
ans.reset_index(drop=True, inplace=True)
ans = ans['v1']
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = y.rolling(ws).mean()
print(ans.shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
ans.iloc[:w-1] = np.nan
ans.reset_index(drop=True, inplace=True)
ans = ans['v1']
t_start = timeit.default_timer()
chk = ans.sum()
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=1, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
print(ans.head(3), flush=True)
print(ans.tail(3), flush=True)
del ans, y

#question = "regression" # q10 ## not implemeneted
#gc.collect()
#t_start = timeit.default_timer()
#ans = x['v1'].rolling(w).mean()
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = ans.sum()
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#del ans
#gc.collect()
#t_start = timeit.default_timer()
#ans = x['v1'].rolling(w).mean()
#print(ans.shape, flush=True)
#t = timeit.default_timer() - t_start
#m = memory_usage()
#t_start = timeit.default_timer()
#chk = ans.sum()
#chkt = timeit.default_timer() - t_start
#write_log(task=task, data=data_name, in_rows=x.shape[0], question=question, out_rows=ans.shape[0], out_cols=ans.shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk)
#print(ans.head(3), flush=True)
#print(ans.tail(3), flush=True)
#del ans

print("rolling finished, took %0.fs" % (timeit.default_timer()-task_init), flush=True)

exit(0)
