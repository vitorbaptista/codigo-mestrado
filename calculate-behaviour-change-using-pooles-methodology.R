pkgs = c("oc", "wnominate", "pscl", "foreach", "doParallel", "data.table", "doBy")
for (pkg in pkgs) {
  if (!(pkg %in% .packages(all.available = TRUE))) {
    install.packages(pkg)
  }
  library(pkg, character.only = TRUE)
}

TRIALS = 1

minorityVoteShare <- function(votes) {
  countVotesTypes <- table(votes)
  if (length(countVotesTypes) == 2) {
    return(min(countVotesTypes, na.rm=TRUE) / sum(countVotesTypes))
  } else {
    return(0)
  }
}

splitLegislatorOnVote <- function (legislator, vote_metadata, votes) {
  splittedVotes <- splitVotes(votes[votes$id == legislator$id,], vote_metadata)
  votes <- votes[-which(votes$id %in% legislator$id),]
  return(rbind(splittedVotes, votes))
}

splitVotes <- function (votes, vote_metadata) {
  # Votes == [NA, 0, 1, 1, 0, NA, 1, 0]
  # Size == 4
  # Result == [[NA,  0,  1,  1, NA, NA, NA, NA],
  #            [NA, NA, NA, NA,  0, NA,  1, 0]]

  metadata_columns <- 1:4  # id, name, party, state
  metadata <- votes[1, metadata_columns]
  splittenVotes <- doTheThing(votes[-metadata_columns], vote_metadata)
  
  resultMetadata <- metadata[rep(1, length(splittenVotes)),]
  name <- metadata[1, 'name']
  party <- metadata[1, 'party']
  names <- lapply(1:nrow(resultMetadata), function (i) { paste(name, party, i) })
  resultMetadata$name <- names
  
  splittenVotes <- do.call(rbind, splittenVotes)
  return(cbind(resultMetadata, splittenVotes))
}

doTheThing <- function(votes, vote_metadata) {
  column_index <- which(colnames(votes) == vote_metadata$id)
  if (length(column_index) == 0) {
    stop(paste("Couldn't find vote with ID ", vote_metadata$id))
  }
  
  mask <- rep(NA, length(votes))
  mask[1:column_index] <- 0

  not_in_legislature_code = 9
  votes_before = votes
  votes_after = votes
  votes_before[is.na(mask)] = not_in_legislature_code
  votes_after[!is.na(mask)] = not_in_legislature_code
  
  return(list(votes_before, votes_after))
  return(c(list(votes + mask), list(votes + inverse_mask)))
}

calculateOC <- function(votes, votes_metadata, baseline_party) {
  polarity <- getPolarityName(votes, baseline_party)
  
  legis.names <- paste(votes$id, votes$name, sep="-")
  legis.data <- votes[c('id', 'name', 'party', 'state')]
  vote.names <- votes_metadata$id
  vote.data <- votes_metadata
  votes <- votes[, -c(1:ncol(legis.data))]
  rownames(votes) <- legis.names
  
  vote.data <- vote.data[vote.names %in% colnames(votes),]
  vote.names <- colnames(votes)
  metadata <- list(
    legis.names = legis.names,
    legis.data = legis.data,
    vote.names = vote.names,
    vote.data = vote.data
  )
  
  votesRollCall <- rollcall(votes,
                            legis.names=metadata$legis.names, legis.data=metadata$legis.data,
                            vote.names=metadata$vote.names, vote.data=metadata$vote.data)
  
  return(wnominate(votesRollCall, dims = 1, polarity = "73540-José Genoíno",
                   trials = TRIALS))
}

getPolarityName <- function(votes, party) {
  votes_by_party <- votes[votes$party == party,]
  polarity_index <- which.max(apply(votes_by_party[, -c(1:4)], 1, sum, na.rm = TRUE))
  return(votes_by_party[polarity_index,][[1, "name"]])
}

runAnalysis <- function (votes, votes_metadata, baseline_party, changed_coalitions) {
  # TODO: Definir uma data inicial melhor pra considerar a mudança de coalizão
  median_index <- floor(nrow(votes_metadata) / 2)
  start_rollcall <- votes_metadata[order(votes_metadata$data),][floor(median_index/2),]
  end_rollcall <- votes_metadata[order(votes_metadata$data),][median_index + floor(median_index/2),]
  changed_coalitions_in_period <- changed_coalitions[between(changed_coalitions$rollcall_date,
                                                             as.POSIXct("2011-01-01"), #start_rollcall$data,
                                                             as.POSIXct("2015-02-01") #end_rollcall$data
  ),]
  
  todosOC <- calculateOC(votes, votes_metadata, baseline_party)
  todosOC$legislators$coord1D <- scale(todosOC$legislators$coord1D)
  
  legislators <- todosOC$legislators[which(todosOC$legislators$party != baseline_party),
                                     c("id", "name", "party", "state")]
  legislatorsIds <- unique(legislators$id)
  
  amauriTeixeira <- todosOC$legislators[todosOC$legislators$name == "José Genoíno",]
  luizCouto <- todosOC$legislators[todosOC$legislators$name == "Luiz Couto",]
  
  result <- foreach(i = 1:length(legislatorsIds), .combine = rbind) %do% {
    legislatorId <- legislatorsIds[i]
    legislator <- legislators[legislators$id == legislatorId,][1,]
    coord1D <- todosOC$legislators[todosOC$legislators$id == legislator$id, 'coord1D']
    legislator$legislature <- legislature
    legislator$start_date <- start_rollcall$data
    legislator$end_date <- end_rollcall$data
    legislator$start_rollcall_id <- start_rollcall$id
    legislator$end_rollcall_id <- end_rollcall$id
    legislator$sd <- sd(coord1D, na.rm=T)
    legislator$min <- min(coord1D, na.rm=T)
    legislator$max <- max(coord1D, na.rm=T)
    legislator$diff <- coord1D[[2]] - coord1D[[1]]
    legislator$coord1DAmauriTeixeira <- amauriTeixeira$coord1D
    legislator$coord1DLuizCouto <- luizCouto$coord1D
    
    this_legislator_changed_coalitions_dates <- changed_coalitions[changed_coalitions$id == legislator$id,]$rollcall_date - votes_metadata[order(votes_metadata$data),][median_index,]$data
    if (length(this_legislator_changed_coalitions_dates) > 0) {
      blah <- as.numeric(
        this_legislator_changed_coalitions_dates[
          which.min(
            abs(this_legislator_changed_coalitions_dates)
          )
          ],
        units = "days"
      )
    } else {
      blah <- NA
    }
    legislator$changed_coalition_date_diff <- blah
    
    cbind(legislator, data.frame(t(coord1D)))
  }
  result <- result[order(result$sd),]
  result$changed_coalition <- result$id %in% changed_coalitions_in_period$id
  result <- as.data.frame(lapply(result, unlist))
  return(result)
}

baseline_party <- 'PT'
legislature <- 50
csvFile <- paste(legislature, ".csv", sep="")
metadataCsvFile <- paste(legislature, "-votacoes.csv", sep="")
numberColumns <- length(scan(csvFile, sep=",", what="character", nlines=1))
votes <- read.csv(csvFile, header = TRUE,
                  colClasses = c("numeric", rep("character", 3), rep("numeric", numberColumns - 4)),
                  check.names = FALSE)
votes_metadata <- read.csv(metadataCsvFile, header = TRUE,
                           colClasses = c(rep("numeric", 3), "POSIXct", rep("character", 2)),
                           check.names = FALSE)
changed_coalitions <- read.csv("parties_and_coalitions_changes.csv",
                               header = TRUE,
                               check.names = FALSE)
changed_coalitions$rollcall_date <- as.POSIXct(changed_coalitions$rollcall_date)

# Remover votações com percentual de votes da minoria menor que lop
lop <- 0.025
votes <- votes[, c(rep(TRUE, 4), apply(votes[, -c(1:4)], 2, minorityVoteShare) > lop)]
votes_metadata <- votes_metadata[votes_metadata$id %in% colnames(votes)[5:ncol(votes)],]

cl <- makeCluster(detectCores() - 2)
registerDoParallel(cl)

votes_metadata <- votes_metadata[order(votes_metadata$data),]
legislators <- votes[-which(votes$name == "José Genoíno"),
                     c("id", "name", "party", "state")]

start_dates <- seq(as.POSIXct("1995-02-01"), as.POSIXct("1998-02-01"), by = "months")
secs_in_365_days <- 365*24*60*60 # one year
halved_secs_in_365_days <- floor(secs_in_365_days / 2)

closest_vote <- function(votes_metadata, date) {
  # Returns latest rollcall on first day with rollcalls after "date", or
  # earliest rollcall on first day with rollcalls before "date"
  votes_dates_diffs <- abs(trunc(votes_metadata$data, "days") - date)
  candidates_indexes <- which(votes_dates_diffs == min(votes_dates_diffs))
  candidates <- votes_metadata[candidates_indexes,,drop=FALSE]
  if (candidates[1, "data"] <= date) {
    candidates[which.min(candidates$data),]
  } else {
    candidates[which.max(candidates$data),]
  }
}

closest_vote_after <- function(votes_metadata, date) {
  # Returns latest rollcall on first day with rollcalls after "date"
  # (including the date itself)
  votes_dates_diffs <- trunc(votes_metadata$data, "days") - date
  non_negative_votes_dates_diffs <- votes_dates_diffs[votes_dates_diffs >= 0]
  if (length(non_negative_votes_dates_diffs) > 0) {
    candidates_indexes <- which(votes_dates_diffs == min(non_negative_votes_dates_diffs))
    candidates <- votes_metadata[candidates_indexes,,drop=FALSE]
    candidates[which.max(candidates$data),]
  }
}

closest_vote_before <- function(votes_metadata, date) {
  # Returns earliest rollcall on first day with rollcalls before "date"
  # (including the date itself)
  votes_dates_diffs <- trunc(votes_metadata$data, "days") - date
  non_positive_votes_dates_diffs <- votes_dates_diffs[votes_dates_diffs <= 0]
  if (length(non_positive_votes_dates_diffs) > 0) {
    candidates_indexes <- which(votes_dates_diffs == max(non_positive_votes_dates_diffs))
    candidates <- votes_metadata[candidates_indexes,,drop=FALSE]
    candidates[which.min(candidates$data),]
  }
}

already_calculated_run_ids = c()
foreach(start_date = start_dates, .errorhandling = "remove") %do% {
  end_date = start_date + secs_in_365_days
  mid_date = start_date + halved_secs_in_365_days

  start_vote = closest_vote_after(votes_metadata, start_date)
  end_vote = closest_vote_before(votes_metadata, end_date)
  mid_vote = closest_vote(votes_metadata, mid_date)

  start_vote_index = which(votes_metadata$id == start_vote$id)
  mid_vote_index = which(votes_metadata$id == mid_vote$id)
  end_vote_index = which(votes_metadata$id == end_vote$id)

  run_id = paste(start_vote$id, end_vote$id, mid_vote$id)
  if (run_id %in% already_calculated_run_ids) {
    stop("Skipping loop already calculated.");
  } else {
    already_calculated_run_ids = c(already_calculated_run_ids, run_id)
  }
  print(paste("Calculating", start_date, "until", end_date, "split on", mid_date, "..."))
  start_time = proc.time()

  result <- foreach(i = seq(1, nrow(legislators), 1),
                    .inorder = FALSE,
                    .errorhandling = "remove",
                    .packages = c("wnominate", "foreach", "data.table")) %do% {
    set.seed(1)
    legislator <- legislators[i,]
    # As 4 primeiras colunas são metadados dos parlamentares, e as outras são
    # as votações. Por isso adicionamos esse offset
    columns <- c(1:4, (start_vote_index+4):(end_vote_index+4))
    votes_split <- splitLegislatorOnVote(votes[votes$id == legislator$id, columns],
                                         mid_vote,
                                         votes)
    calculateOC(votes_split, votes_metadata, baseline_party)
  }

  print("# Processing time:")
  print(proc.time() - start_time)

  saveRDS(result, file = paste(legislature, "-", TRIALS, "-", start_vote$id, "_", end_vote$id, ".rds", sep=""))

  print("# Writing time:")
  print(proc.time() - start_time)
}
