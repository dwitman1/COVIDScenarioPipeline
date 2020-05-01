
# XXX: this branch was in a transitory state before packrat, so these packages need to be installed to run this script:
# install.packages("tdigest")
#
# TODO: update dependencies

PROBS = c(0.01, 0.025, seq(0.05, 0.95, by = 0.05), 0.975, 0.99)

suppressMessages({
    library(optparse, quietly=TRUE)
    library(tidyverse, quietly=TRUE)
    library(tdigest)
    library(scales)
    library(parallel)
    library(doParallel)
    library(data.table)
})

##List of specified options
option_list <- list(
    make_option("--name_filter", type="character", default="", help="filename filter, usually deaths"),
    make_option("--nfiles", type="numeric", default=NA, help="number of files to load, default is all"),
    make_option("--ncores", type="numeric", default=parallel::detectCores(), help="number of cores to use in data load, default =6"),
    make_option(c("--outfile","-o"), type="character", default=NULL, help="file to saver output"),
    make_option("--start_date", type="character", default="2020-01-01", help="earliest date to include"),
    make_option("--end_date",  type="character", default="2022-01-01", help="latest date to include")
)

opt_parser <- OptionParser(option_list = option_list, usage="%prog [options] [one or more scenarios]")

arguments <- parse_args(opt_parser, positional_arguments=c(1,Inf))#, args=c("mid-west-coast-AZ-NV_UKFixed_30_40", "--nfiles=1", "--outfile=test.out"))
opt <- arguments$options
scenarios <- arguments$args

if (is.null(opt$outfile)) {
    stop("outfile must be specified")
}

cl = makeCluster(opt$ncores)
doParallel::registerDoParallel(cl)
suppressMessages(geodata <- readr::read_csv("data/geodata.csv"))

opt$start_date <- as.Date(opt$start_date)
opt$end_date <- as.Date(opt$end_date)

post_proc <- function(x, geodata, opt) {
    x %>%
        group_by(geoid) %>%
        mutate(cum_infections=cumsum(incidI)) %>%
        mutate(cum_death=cumsum(incidD)) %>%
        ungroup() %>%
        filter(time >= opt$start_date & time <= opt$end_date) %>%
        rename(infections=incidI, death=incidD, hosp=incidH)
}

res_geoid <- data.table::rbindlist(purrr::pmap(data.frame(scenario=scenarios), function(scenario) { 
    report.generation::load_hosp_sims_filtered(scenario,
                                               name_filter=opt$name_filter,
                                               num_files=opt$nfiles,
                                               post_process=post_proc,
                                               geodata=geodata,
                                               opt=opt) 
}))

q <- function(col) {
  # if col is empty, tquantile fails; in that case, return what quantile() would (all 0's)
  tryCatch(tquantile(tdigest(col), PROBS), error = function(e) { quantile(col, PROBS) })
}

res_split <- split(res_geoid, res_geoid$geoid)
to_save_geo <- foreach(by_geoid=res_split, .combine=rbind, .packages="data.table") %dopar% {
  stopifnot(is.data.table(by_geoid))
  by_geoid[, .(quantile=scales::percent(PROBS),
               hosp_curr=q(hosp_curr),
               cum_death=q(cum_death),
               death=q(death),
               infections=q(infections),
               cum_infections=q(cum_infections),
               hosp=q(hosp)), by=list(time, geoid)]
}

data.table::fwrite(to_save_geo, file=opt$outfile)

stopCluster(cl)