$DO_REPORT && echo "# Rendering report"
$DO_REPORT && Rscript -e 'rmarkdown::render("./_report/index.Rmd", output_dir="public")' > ./out/rmarkdown_index.out 2>&1 && echo "# Benchmark index report produced"
$DO_REPORT && Rscript -e 'rmarkdown::render("./_report/history.Rmd", output_dir="public")' > ./out/rmarkdown_history.out 2>&1 && echo "# Benchmark history report produced"
$DO_REPORT && Rscript -e 'rmarkdown::render("./_report/tech.Rmd", output_dir="public")' > ./out/rmarkdown_tech.out 2>&1 && echo "# Benchmark tech report produced"