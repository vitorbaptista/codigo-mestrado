library(data.table)
library(plyr)
library(stringr)

convert_results <- function(result) {
  coords <- rbindlist(
    lapply(result, function (res) {
      res$legislators[1:2,]
    })
  )
  
  clean_coords = ddply(coords, .(id), function (rows) {
    name = substr(rows[1, "name"], 1, nchar(rows[1, "name"]) - nchar(rows[1, "party"]) - 3)
    data.frame(name = name,
               party = rows[1, "party"],
               state = rows[1, "state"],
               before = rows[1, "coord1D"],
               before.sd = rows[1, "se1D"],
               after = rows[2, "coord1D"],
               after.sd = rows[2, "se1D"],
               diff = diff(rows[, "coord1D"]))
  })

  clean_coords[!is.na(clean_coords$diff),]
}

setwd("~/Projetos/Mestrado/theRealPipeline/results/")

file_regexp = "^([0-9]+)-([0-9]+)-([0-9]+)_([0-9]+)_([0-9]+)\\.rds$"
clean_coords = data.frame()

for (path in list.files(".", pattern = file_regexp)) {
  path_parts = str_match(path, file_regexp)
  aux = convert_results(readRDS(path))
  if (nrow(aux) != 0) {
    aux$legislature = path_parts[1, 2]
    aux$trials = path_parts[1, 3]
    aux$start_vote_index = path_parts[1, 4]
    aux$mid_vote_index = path_parts[1, 5]
    aux$end_vote_index = path_parts[1, 6]
    clean_coords = rbind(clean_coords, aux)
  }
}