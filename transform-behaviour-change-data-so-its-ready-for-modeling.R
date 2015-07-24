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

  clean_coords
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

closest_to = function(element, element_list) {
  if (length(element_list) > 0) {
    closest_index = which.min(abs(element_list - element))
    element_list[[closest_index]]
  } else {
    NA
  }
}

six_months = 6*30
changed_coalitions = read.csv("parties_and_coalitions_changes.csv")
changed_coalitions$rollcall_date = as.POSIXct(changed_coalitions$rollcall_date)
clean_coords$changed_coalition = "N"
clean_coords$start_vote_date = NA
clean_coords$mid_vote_date = NA
clean_coords$end_vote_date = NA
clean_coords$coalition_change_closest_to_start_vote = NA
clean_coords$coalition_change_closest_to_mid_vote = NA
clean_coords$coalition_change_closest_to_end_vote = NA
for (legislature in unique(clean_coords$legislature)) {
  votacoes = read.csv(paste0(legislature, "-votacoes.csv"))
  votacoes$data = as.POSIXct(votacoes$data)

  legislature_start_date = as.Date(format(min(votacoes$data), "%Y-02-01"))
  legislature_end_date = as.Date(paste0(year(legislature_start_date) + 4, "-01-31"))
  changed_coalitions_in_legislature = changed_coalitions[between(as.Date(changed_coalitions$rollcall_date),
                                                                 legislature_start_date,
                                                                 legislature_end_date),,drop=FALSE]

  for (row_number in which(clean_coords$legislature == legislature)) {
    row = clean_coords[row_number,]
    start_vote = votacoes[votacoes$id == row[["start_vote_id"]],]
    mid_vote = votacoes[votacoes$id == row[["mid_vote_id"]],]
    end_vote = votacoes[votacoes$id == row[["end_vote_id"]],]

    changed_coalitions_in_period = changed_coalitions_in_legislature[changed_coalitions_in_legislature$id == row[["id"]] &
                                                                     between(as.Date(changed_coalitions_in_legislature$rollcall_date),
                                                                             as.Date(end_vote$data) - six_months/2,
                                                                             as.Date(end_vote$data) + six_months/2),,drop=FALSE]

    my_changed_coalitions_in_legislature = changed_coalitions_in_legislature[changed_coalitions_in_legislature$id == row[["id"]],]
    row["coalition_change_closest_to_start_vote"] = closest_to(start_vote$data, my_changed_coalitions_in_legislature$rollcall_date)
    row["coalition_change_closest_to_mid_vote"] = closest_to(mid_vote$data, my_changed_coalitions_in_legislature$rollcall_date)
    row["coalition_change_closest_to_end_vote"] = closest_to(end_vote$data, my_changed_coalitions_in_legislature$rollcall_date)
    row["changed_coalition"] = ifelse(nrow(changed_coalitions_in_period) == 0, "N", "S")
    row["start_vote_date"] = start_vote$data
    row["mid_vote_date"] = mid_vote$data
    row["end_vote_date"] = end_vote$data
    clean_coords[row_number,] = row
  }
}
clean_coords$start_vote_date = as.POSIXct(clean_coords$start_vote_date, origin="1970-01-01")
clean_coords$mid_vote_date = as.POSIXct(clean_coords$mid_vote_date, origin="1970-01-01")
clean_coords$end_vote_date = as.POSIXct(clean_coords$end_vote_date, origin="1970-01-01")
clean_coords$coalition_change_closest_to_start_vote = as.POSIXct(clean_coords$coalition_change_closest_to_start_vote, origin="1970-01-01")
clean_coords$coalition_change_closest_to_mid_vote = as.POSIXct(clean_coords$coalition_change_closest_to_mid_vote, origin="1970-01-01")
clean_coords$coalition_change_closest_to_end_vote = as.POSIXct(clean_coords$coalition_change_closest_to_end_vote, origin="1970-01-01")

write.csv(clean_coords, file = "data.csv",
          row.names = FALSE, quote = FALSE, na = "")
