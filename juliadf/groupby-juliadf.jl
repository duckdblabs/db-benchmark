#!/usr/bin/env julia

print("# groupby-juliadf.jl\n"); flush(stdout);

using DataFrames;
using CSV;
using Statistics; # mean function
using Printf;

include("$(pwd())/_helpers/helpers.jl");

pkgmeta = getpkgmeta("DataFrames");
ver = pkgmeta["version"];
git = pkgmeta["git-tree-sha1"];
task = "groupby";
solution = "juliadf";
fun = "by";
cache = true;
on_disk = false;
machine_type = ENV["MACHINE_TYPE"]
isondisk(indata) = parse(Float64, split(indata, "_")[2])>=10^10 || (parse(Float64, split(indata, "_")[2]) >= 1^9 && machine_type == "c6id.4xlarge")

data_name = ENV["SRC_DATANAME"];
src_grp = string("data/", data_name, ".csv");
println(string("loading dataset ", data_name)); flush(stdout);

# Types are provided explicitly only to reduce memory use when parsing
x = CSV.read(src_grp,
             DataFrame,
             types=[String7, String7, String15, Int32, Int32, Int32, Int32, Int32, Float64],
             ntasks=1,
             pool=true);
in_rows = size(x, 1);
println(in_rows); flush(stdout);

on_disk = isondisk(data_name)

task_init = time();
print("grouping...\n"); flush(stdout);

question = "sum v1 by id1"; # q1
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, :id1), :v1 => sum∘skipmissing => :v1); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = sum(ANS.v1);
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, :id1), :v1 => sum∘skipmissing => :v1); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = sum(ANS.v1);
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "sum v1 by id1:id2"; # q2
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, [:id1, :id2]), :v1 => sum∘skipmissing => :v1); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = sum(ANS.v1);
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, [:id1, :id2]), :v1 => sum∘skipmissing => :v1); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = sum(ANS.v1);
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "sum v1 mean v3 by id3"; # q3
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, :id3), :v1 => sum∘skipmissing => :v1, :v3 => mean∘skipmissing => :v3); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v3)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, :id3), :v1 => sum∘skipmissing => :v1, :v3 => mean∘skipmissing => :v3); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v3)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "mean v1:v3 by id4"; # q4
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, :id4), :v1 => mean∘skipmissing => :v1, :v2 => mean∘skipmissing => :v2, :v3 => mean∘skipmissing => :v3); println(size(ANS)); flush(stdout));
m = memory_usage();
t_start = time_ns();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2), sum(ANS.v3)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, :id4), :v1 => mean∘skipmissing => :v1, :v2 => mean∘skipmissing => :v2, :v3 => mean∘skipmissing => :v3); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2), sum(ANS.v3)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "sum v1:v3 by id6"; # q5
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, :id6), :v1 => sum∘skipmissing => :v1, :v2 => sum∘skipmissing => :v2, :v3 => sum∘skipmissing => :v3); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2), sum(ANS.v3)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, :id6), :v1 => sum∘skipmissing => :v1, :v2 => sum∘skipmissing => :v2, :v3 => sum∘skipmissing => :v3); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v1), sum(ANS.v2), sum(ANS.v3)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "median v3 sd v3 by id4 id5"; # q6
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, [:id4, :id5]), :v3 => median∘skipmissing => :median_v3, :v3 => std∘skipmissing => :sd_v3); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.median_v3), sum(ANS.sd_v3)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, [:id4, :id5]), :v3 => median∘skipmissing => :median_v3, :v3 => std∘skipmissing => :sd_v3); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.median_v3), sum(ANS.sd_v3)];;
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "max v1 - min v2 by id3"; # q7
q7_fun(v1, v2) = maximum(skipmissing(v1))-minimum(skipmissing(v2))
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, :id3), [:v1, :v2] => q7_fun => :range_v1_v2); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = sum(ANS.range_v1_v2);
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, :id3), [:v1, :v2] => q7_fun => :range_v1_v2); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = sum(ANS.range_v1_v2);
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "largest two v3 by id6"; # q8
q8_fun(x) = partialsort!(x, 1:min(2, length(x)), rev=true)
GC.gc(false);
t = @elapsed (ANS = combine(groupby(dropmissing(x, :v3), :id6), :v3 => q8_fun => :largest2_v3); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = sum(ANS.largest2_v3);
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = combine(groupby(dropmissing(x, :v3), :id6), :v3 => q8_fun => :largest2_v3); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = sum(ANS.largest2_v3);
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "regression v1 v2 by id2 id4"; # q9
function cor2(x, y)
    nm = @. !ismissing(x) & !ismissing(y)
    return count(nm) < 2 ? NaN : cor(view(x, nm), view(y, nm))^2
end
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, [:id2, :id4]), [:v1, :v2] => cor2 => :r2); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = sum(skipmissing(ANS.r2));
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, [:id2, :id4]), [:v1, :v2] => cor2 => :r2); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = sum(skipmissing(ANS.r2));
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

question = "sum v3 count by id1:id6"; # q10
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, [:id1, :id2, :id3, :id4, :id5, :id6]), :v3 => sum∘skipmissing => :v3, :v3 => length => :count); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v3), sum(ANS.count)];
write_log(1, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
ANS = 0;
GC.gc(false);
t = @elapsed (ANS = combine(groupby(x, [:id1, :id2, :id3, :id4, :id5, :id6]), :v3 => sum∘skipmissing => :v3, :v3 => length => :count); println(size(ANS)); flush(stdout));
m = memory_usage();
chkt = @elapsed chk = [sum(ANS.v3), sum(ANS.count)];
write_log(2, task, data_name, in_rows, question, size(ANS, 1), size(ANS, 2), solution, ver, git, fun, t, m, cache, make_chk(chk), chkt, on_disk, machine_type);
println(first(ANS, 3));
println(last(ANS, 3));
ANS = 0;

print(@sprintf "grouping finished, took %.0fs\n" (time()-task_init)); flush(stdout);

exit();
