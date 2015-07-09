pkgs = c("wnominate", "pscl", "foreach", "doParallel", "data.table", "doBy")
for (pkg in pkgs) {
  if (!(pkg %in% .packages(all.available = TRUE))) {
    install.packages(pkg)
  }
  library(pkg, character.only = TRUE)
}

TRIALS = 15

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
  inverse_mask <- rep(NA, length(votes))
  mask[1:column_index] <- 0
  inverse_mask[is.na(mask)] <- 0
  
  return(c(list(votes + mask), list(votes + inverse_mask)))
}

calculateOC <- function(votes, votes_metadata, baseline_party) {
  polarity <- getPolarityName(votes, baseline_party)
  
  legis.names <- votes$name
  legis.data <- votes[c('id', 'name', 'party', 'state')]
  vote.names <- votes_metadata$id
  vote.data <- votes_metadata
  votes <- votes[, -c(1:ncol(legis.data))]
  rownames(votes) <- legis.names # paste(legis.names, legis.data$party)
  
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
  
  return(wnominate(votesRollCall, dims = 1, polarity = "Amauri Teixeira",
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
  
  amauriTeixeira <- todosOC$legislators[todosOC$legislators$name == "Amauri Teixeira",]
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

setwd("~/Projetos/Mestrado/theRealPipeline")
baseline_party <- 'PT'
legislature <- 54
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

cl <- makeCluster(detectCores())
registerDoParallel(cl)

votes_metadata <- votes_metadata[order(votes_metadata$data),]
legislators <- votes[-which(votes$name == "Amauri Teixeira"),
                     c("id", "name", "party", "state")]

for (vote_index in seq(100 + 4, ncol(votes), 10)) {
  print(paste("Calculating 1~", vote_index, "...", sep=""))

  vote_id = (colnames(votes)[[floor((vote_index - 4) / 2)]])
  vote_metadata <- votes_metadata[votes_metadata$id == vote_id,]

  result <- foreach(i = seq(1, nrow(legislators), 1),
                    .inorder = FALSE,
                    .packages = c('wnominate', 'foreach', 'data.table')) %dopar% {
    legislator <- legislators[i,]
    votes_split <- splitLegislatorOnVote(votes[votes$id == legislator$id, 1:vote_index],
                                         vote_metadata,
                                         votes)
    calculateOC(votes_split, votes_metadata, baseline_party)
  }
 
  saveRDS(result, file = paste(legislature, "-1_", vote_index, ".rds", sep=""))
}
