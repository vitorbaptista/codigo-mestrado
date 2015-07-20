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

TRIALS = 10
file_regexp = paste0("^([0-9]+)-(", TRIALS, ")-([0-9]+)_([0-9]+)_([0-9]+)\\.rds$")
clean_coords = data.frame()

for (path in list.files("results/", pattern = file_regexp)) {
  path_parts = str_match(path, file_regexp)
  path_parts = as.numeric(path_parts[1, 2:ncol(path_parts)])
  aux = convert_results(readRDS(paste0("results/", path)))
  if (nrow(aux) != 0) {
    aux$legislature = path_parts[1]
    aux$trials = path_parts[2]
    aux$start_vote_id = path_parts[3]
    aux$mid_vote_id = path_parts[4]
    aux$end_vote_id = path_parts[5]
    clean_coords = rbind(clean_coords, aux)
  }
}

changed_coalitions = read.csv("parties_and_coalitions_changes.csv")
changed_coalitions$rollcall_date = as.POSIXct(changed_coalitions$rollcall_date)
clean_coords$changed_coalition = "N"
for (legislature in unique(clean_coords$legislature)) {
  votacoes = read.csv(paste0(legislature, "-votacoes.csv"))
  votacoes$data = as.POSIXct(votacoes$data)

  legislature_start_date = as.Date(format(min(votacoes$data), "%Y-02-01"))
  legislature_end_date = as.Date(paste0(year(legislature_start_date) + 4, "-01-31"))
  changed_coalitions_in_legislature = changed_coalitions[between(as.Date(changed_coalitions$rollcall_date),
                                                                 legislature_start_date,
                                                                 legislature_end_date),,drop=FALSE]

  six_months = 6*30
  clean_coords[clean_coords$legislature == legislature,] = t(apply(clean_coords[clean_coords$legislature == legislature,], 1, function (row) {
    mid_vote = votacoes[votacoes$id == row[["mid_vote_id"]],]
    end_vote = votacoes[votacoes$id == row[["end_vote_id"]],]

    changed_coalitions_in_period = changed_coalitions_in_legislature[changed_coalitions_in_legislature$id == row[["id"]] &
                                                                     between(as.Date(changed_coalitions_in_legislature$rollcall_date),
                                                                             as.Date(mid_vote$data),
                                                                             as.Date(end_vote$data) + six_months),,drop=FALSE]

    row["changed_coalition"] = ifelse(nrow(changed_coalitions_in_period) == 0, "N", "S")
    row
  }))
}

write.csv(clean_coords, file = "data.csv",
          row.names = FALSE, quote = FALSE, na = "")
