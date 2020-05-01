
# library(devtools)
# devtools::install_github("hadley/dplyr")
# install.packages("tdigest")

PROBS = c(0.01, 0.025, seq(0.05, 0.95, by = 0.05), 0.975, 0.99)

suppressMessages({
    library(optparse, quietly=TRUE)
    library(tidyverse, quietly=TRUE)
    library(tdigest)
    library(scales)
    library(parallel)
})

##List of specified options
option_list <- list(
    make_option("--name_filter", type="character", default="", help="filename filter, usually deaths"),
    make_option("--nfiles", type="numeric", default=NA, help="number of files to load, default is all"),
    make_option("--ncores", type="numeric", default=detectCores(), help="number of cores to use in data load, default =6"),
    make_option(c("--outfile","-o"), type="character", default=NULL, help="file to saver output"),
    make_option("--start_date", type="character", default="2020-01-01", help="earliest date to include"),
    make_option("--end_date",  type="character", default="2022-01-01", help="latest date to include")
)

opt_parser <- OptionParser(option_list = option_list, usage="%prog [options] [one or more scenarios]")

arguments <- parse_args(opt_parser, positional_arguments=c(1,Inf))
opt <- arguments$options
scenarios <- arguments$args

if (is.null(opt$outfile)) {
    stop("outfile must be specified")
}

doParallel::registerDoParallel(opt$ncores)
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

res_geoid <- dplyr::bind_rows(purrr::pmap(data.frame(scenario=scenarios), function(scenario) { 
    report.generation::load_hosp_sims_filtered(scenario,
                                               name_filter=opt$name_filter,
                                               num_files=opt$nfiles,
                                               post_process=post_proc,
                                               geodata=geodata,
                                               opt=opt) 
}))

q <- function(col) {
  tryCatch(tquantile(tdigest(col), PROBS), error = function(e) { quantile(col, PROBS) })
}

to_save_geo <- res_geoid %>%
    group_by(time, geoid) %>%
    summarise(
        quantile=scales::percent(PROBS),
        hosp_curr=q(hosp_curr),
        cum_death=q(cum_death),
        death=q(death),
        infections=q(infections),
        cum_infections=q(cum_infections),
        hosp=q(hosp))

write_csv(to_save_geo, path=opt$outfile)
