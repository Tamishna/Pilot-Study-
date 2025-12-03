
# Elite Population Research 

# What this script does (high level):
# 1) Reads a seed list with Wikidata Q-IDs (wikiid) and names.
# 2) Crawls ancestry upward (parents P22/P25) and augments with children.
# 3) Builds a directed ancestry graph (child → parent) and saves CSVs + RData.
# 4) Extracts “horizontal” ties (spouses, siblings) two ways:
#    - Inferred from parent edges (co-parents → spouses; shared parents → siblings).
#    - Direct Wikidata claims (P26 spouse, P3373 sibling).
# 5) Exports horizontal edges to CSVs and summarizes counts per person.
# 6) Produces two interactive HTML visualizations using visNetwork:
#    - Ancestry around a chosen seed (Jeff Bezos).
#    - Horizontal relationships around the same seed.

#### 0. Setup & Packages ####
# Purpose: bootstrap the working directory, install/load packages.

# Print the machine's node name 
Sys.info()['nodename']

#Sets a working folder and loads packages.

work_dir <- "/Users/Tamishna/Desktop/Elite Population Research/Pilot Study with new data/Data"
setwd(work_dir)
getwd()  # sanity check

# Packages
pkgs <- c(
  "readxl",
  "writexl",
  "tidyverse",
  "WikidataR",
  "stringr",
  "igraph"
)

lapply(pkgs[!(pkgs %in% installed.packages())], install.packages)
lapply(pkgs, library, character.only = TRUE)

# Clean environment (after libraries are loaded)
rm(list = ls())



#### 1. Load and prepare data ####
# wikiid (Q-ID), Person_name

seed_file <- "/Users/Tamishna/Desktop/Elite Population Research/Pilot Study with new data/Data/Sample of US Elite Population for Tamishna.xlsx"

# Choose the right reader based on extension
#Read the excel file
if (grepl("\\.xlsx?$", seed_file, ignore.case = TRUE)) {
  df_raw <- readxl::read_excel(seed_file)
} else {
  df_raw <- readr::read_csv(seed_file, show_col_types = FALSE)
}

# Normalize column names and filter valid Q-IDs
df <- df_raw %>%
  dplyr::rename(
    id_wikidata = wikiid,
    name        = Person_name
  ) %>%
  dplyr::mutate(
    id_fam = NA_character_  # not available in the new file
  ) %>%
  dplyr::filter(!is.na(id_wikidata), stringr::str_starts(id_wikidata, "Q")) %>%
  dplyr::distinct(id_wikidata, .keep_all = TRUE)

cat("Seeds in file:", nrow(df), "\n")



##### 2. Crawl parents (upward) and children (downwards) #####
# P22 (father) and P25 (mother). Cached per Q-ID locally to reduce API hits

dir.create("data_prepared", showWarnings = FALSE, recursive = TRUE)
dir.create("cache/parents", showWarnings = FALSE, recursive = TRUE)

MAX_DEPTH   <- 20        # safety cap on generations
SEED_LIMIT  <- 6    # set to a small number for pilot run; NA = all
BASE_SLEEP  <- 0.25      # polite delay
RETRY_TRIES <- 3
BACKOFF     <- 1.7

# Build seed vector of Q-IDs
seeds <- df %>%
  dplyr::filter(!is.na(id_wikidata)) %>%
  dplyr::distinct(id_wikidata) %>%
  dplyr::pull(id_wikidata)

if (!is.na(SEED_LIMIT)) {
  seeds <- head(seeds, SEED_LIMIT)
}
cat("Seed count:", length(seeds), "\n")

global_start <- Sys.time()

# Helpers: caching + safe query
parent_cache_file <- function(qid) file.path("cache/parents", paste0(qid, ".rds"))

safe_query <- function(sparql, tries = RETRY_TRIES, sleep = BASE_SLEEP, backoff = BACKOFF) {
  attempt <- 1
  repeat {
    out <- tryCatch(
      { WikidataR::query_wikidata(sparql, format = "tibble") },
      error = function(e) e
    )
    if (!inherits(out, "error")) return(out)
    if (attempt >= tries) stop(out)
    Sys.sleep(sleep)
    sleep <- sleep * backoff
    attempt <- attempt + 1
  }
}

#### 2a. Add children (downward) ####
dir.create("cache/children", showWarnings = FALSE, recursive = TRUE)
child_cache_file <- function(qid) file.path("cache/children", paste0(qid, ".rds"))

wd_get_children <- function(parent_qid, use_cache = TRUE) {
  cf <- child_cache_file(parent_qid)
  if (use_cache && file.exists(cf)) return(readRDS(cf))
  
  sparql <- paste0(
    "SELECT ?child ?childLabel ?side (YEAR(?child_birth) AS ?child_birth_year) WHERE { ",
    "  { ?child wdt:P22 wd:", parent_qid, " . BIND(\"has_father\" AS ?side) } UNION ",
    "  { ?child wdt:P25 wd:", parent_qid, " . BIND(\"has_mother\" AS ?side) } ",
    "  OPTIONAL { ?child wdt:P569 ?child_birth . } ",
    "  SERVICE wikibase:label { bd:serviceParam wikibase:language 'de,en'. } ",
    "}"
  )
  
  Sys.sleep(BASE_SLEEP)
  res <- safe_query(sparql)
  
  # Coerce to a tibble and enforce column presence + types
  res <- tibble::as_tibble(res)
  needed <- c("child","childLabel","side","child_birth_year")
  for (nm in needed) if (!nm %in% names(res)) res[[nm]] <- NA_character_
  
  # This makes types predictable (strings; year can be NA_character_)
  res <- dplyr::mutate(res, dplyr::across(dplyr::everything(), as.character))
  res <- dplyr::select(res, dplyr::all_of(needed))
  
  saveRDS(res, cf)
  res
}


# Build child->parent edges for a set of parents
children_edges_for_parents <- function(parents_qids) {
  edge_buf <- list(); node_buf <- list()
  for (pq in parents_qids) {
    ch <- wd_get_children(pq)
    if (!is.data.frame(ch) || !all(c("child","childLabel","side") %in% names(ch))) next
    if (nrow(ch) == 0) next
    
    ch_ok <- ch %>% dplyr::filter(!is.na(.data$child), stringr::str_starts(.data$child, "Q"))
    
    if (nrow(ch_ok)) {
      edge_buf[[length(edge_buf)+1]] <- ch %>%
        dplyr::filter(!is.na(.data$child), stringr::str_starts(.data$child, "Q")) %>%
        dplyr::transmute(
          from      = .data$child,
          to        = pq,
          type      = .data$side,
          fromLabel = .data$childLabel,
          toLabel   = NA_character_,  
          value     = 1L,             

        )
      
      
      node_buf[[length(node_buf)+1]] <- ch_ok %>%
        dplyr::transmute(node = .data$child, nodeLabel = .data$childLabel)
    }
  }
  
  edges_down <- if (length(edge_buf)) dplyr::bind_rows(edge_buf) else
    tibble::tibble(from=character(), to=character(), type=character(),
                   fromLabel=character(), toLabel=character())
  nodes_add <- if (length(node_buf)) dplyr::bind_rows(node_buf) else
    tibble::tibble(node=character(), nodeLabel=character())
  
  list(edges = dplyr::distinct(edges_down),
       nodes = dplyr::distinct(nodes_add))
}

# Merge existing graph with newly discovered children
augment_graph_with_children <- function(edges, nodes) {
  parent_set <- unique(c(edges$to, nodes$node))
  down <- children_edges_for_parents(parent_set)
  nodes_aug <- dplyr::bind_rows(nodes, down$nodes) %>%
    dplyr::group_by(node) %>%
    dplyr::summarise(
      nodeLabel = dplyr::coalesce(dplyr::first(na.omit(nodeLabel)), NA_character_),
      .groups = "drop"
    )
  edges_aug <- dplyr::bind_rows(edges, down$edges) %>%
    dplyr::distinct(from, to, type, .keep_all = TRUE)
  list(edges = edges_aug, nodes = nodes_aug)
}

#### 2b. Direct horizontal fetchers (P26 spouse, P3373 sibling) ####
dir.create("cache/spouse_p26",    showWarnings = FALSE, recursive = TRUE)
dir.create("cache/sibling_p3373", showWarnings = FALSE, recursive = TRUE)

spouse_cache_file  <- function(qid) file.path("cache/spouse_p26",    paste0(qid, ".rds"))
sibling_cache_file <- function(qid) file.path("cache/sibling_p3373", paste0(qid, ".rds"))

#P26 are direct spouses of a person
wd_get_spouses_p26 <- function(qid, use_cache = TRUE) {
  cf <- spouse_cache_file(qid)
  if (use_cache && file.exists(cf)) return(readRDS(cf))
  sparql <- paste0(
    "SELECT ?sp ?spLabel WHERE { ",
    "  wd:", qid, " wdt:P26 ?sp . ",
    "  SERVICE wikibase:label { bd:serviceParam wikibase:language 'de,en'. } ",
    "}"
  )
  Sys.sleep(BASE_SLEEP)
  res <- safe_query(sparql)
  res <- res %>% dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  saveRDS(res, cf)
  res
}

#p3373 direct siblings of a person
wd_get_siblings_p3373 <- function(qid, use_cache = TRUE) {
  cf <- sibling_cache_file(qid)
  if (use_cache && file.exists(cf)) return(readRDS(cf))
  sparql <- paste0(
    "SELECT ?sib ?sibLabel WHERE { ",
    "  wd:", qid, " wdt:P3373 ?sib . ",
    "  SERVICE wikibase:label { bd:serviceParam wikibase:language 'de,en'. } ",
    "}"
  )
  Sys.sleep(BASE_SLEEP)
  res <- safe_query(sparql)
  res <- res %>% dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  saveRDS(res, cf)
  res
}

#### Horizontal Relationship helpers ####
library(dplyr); library(purrr); library(tidyr); library(readr); library(fs)
dir_create("generated")

# 1) spouses from co-parents
spouse_edges_from_coparents <- function(edges) {
  fathers <- edges %>% dplyr::filter(type == "has_father") %>% dplyr::transmute(child = from, father = to)
  mothers <- edges %>% dplyr::filter(type == "has_mother") %>% dplyr::transmute(child = from, mother = to)
  cop <- fathers %>%
    dplyr::inner_join(mothers, by = "child") %>%
    dplyr::transmute(a = pmin(father, mother), b = pmax(father, mother)) %>%
    dplyr::filter(!is.na(a), !is.na(b), a != b) %>%
    dplyr::distinct()
  cop %>% dplyr::transmute(from = a, to = b, type = "spouse")
}

# 2) siblings from shared parents
sibling_edges_from_parents <- function(edges) {
  parent_children <- dplyr::bind_rows(
    edges %>% dplyr::filter(type == "has_father") %>% dplyr::transmute(parent = to, child = from, side = "paternal"),
    edges %>% dplyr::filter(type == "has_mother") %>% dplyr::transmute(parent = to, child = from, side = "maternal")
  )
  if (nrow(parent_children) == 0) {
    return(tibble::tibble(from = character(), to = character(), type = character()))
  }
  sib_pairs <- parent_children %>%
    dplyr::group_by(parent) %>%
    dplyr::summarise(children = list(sort(unique(child))), .groups = "drop") %>%
    dplyr::mutate(pairs = purrr::map(children, ~{
      kids <- .x
      if (length(kids) < 2) return(tibble::tibble(a = character(), b = character()))
      as_tibble(t(combn(kids, 2))) |> dplyr::rename(a = V1, b = V2)
    })) %>%
    dplyr::select(-children) %>%
    tidyr::unnest(pairs)
  if (nrow(sib_pairs) == 0) {
    return(tibble::tibble(from = character(), to = character(), type = character()))
  }
  detail <- sib_pairs %>%
    dplyr::left_join(parent_children %>% dplyr::select(parent, child, side),
                     by = c("parent" = "parent", "a" = "child")) %>% dplyr::rename(side_a = side) %>%
    dplyr::left_join(parent_children %>% dplyr::select(parent, child, side),
                     by = c("parent" = "parent", "b" = "child")) %>% dplyr::rename(side_b = side) %>%
    dplyr::transmute(a, b, share = paste(sort(c(side_a, side_b)), collapse = "+")) %>%
    dplyr::distinct() %>%
    dplyr::count(a, b, share, name = "n_parents") %>%
    dplyr::mutate(sib_type = dplyr::case_when(
      n_parents >= 2 ~ "full",
      share %in% "maternal+maternal" ~ "maternal_half",
      share %in% "paternal+paternal" ~ "paternal_half",
      TRUE ~ "any"
    ))
  detail %>%
    dplyr::transmute(from = pmin(a, b), to = pmax(a, b), type = paste0("sibling_", sib_type)) %>%
    dplyr::distinct()
}

# 3) combined  inferred horizontal edges
build_horizontal_edges <- function(edges) {
  dplyr::bind_rows(
    spouse_edges_from_coparents(edges),
    sibling_edges_from_parents(edges)
  ) %>% dplyr::distinct()
}

# 4) lookups
get_spouses <- function(qid, edges, nodes) {
  spouses <- spouse_edges_from_coparents(edges)
  tibble::tibble(spouse_id = unique(c(spouses$to[spouses$from == qid], spouses$from[spouses$to == qid]))) %>%
    dplyr::filter(!is.na(spouse_id)) %>%
    dplyr::left_join(nodes %>% dplyr::transmute(qid = node, name = dplyr::coalesce(nodeLabel, node)), by = c("spouse_id" = "qid"))
}

get_siblings <- function(qid, edges, nodes, type = c("any","full","maternal_half","paternal_half")) {
  type <- match.arg(type)
  sib_edges <- sibling_edges_from_parents(edges)
  wanted_types <- if (type == "any") unique(sib_edges$type) else paste0("sibling_", type)
  sib_edges %>%
    dplyr::filter(type %in% wanted_types) %>%
    dplyr::filter(from == qid | to == qid) %>%
    dplyr::transmute(sibling_id = if_else(from == qid, to, from), sib_edge_type = type) %>%
    dplyr::distinct() %>%
    dplyr::left_join(nodes %>% dplyr::transmute(qid = node, name = dplyr::coalesce(nodeLabel, node)), by = c("sibling_id" = "qid"))
}

# 5) sample (optional)
sample_horizontal_relatedness <- function(edges, nodes, graph, seed_qid = NULL, n_each = 10) {
  horiz <- build_horizontal_edges(edges)
  if (!is.null(seed_qid)) {
    comp <- igraph::components(graph)$membership; names(comp) <- igraph::V(graph)$name
    seed_comp <- comp[[seed_qid]]
    in_comp <- names(comp)[comp == seed_comp]
    horiz <- horiz %>% dplyr::filter(from %in% in_comp | to %in% in_comp)
  }
  decorate <- function(df) {
    df %>%
      dplyr::left_join(nodes %>% dplyr::transmute(qid = node, from_label = dplyr::coalesce(nodeLabel, node)), by = c("from" = "qid")) %>%
      dplyr::left_join(nodes %>% dplyr::transmute(qid = node, to_label   = dplyr::coalesce(nodeLabel, node)), by = c("to"   = "qid")) %>%
      dplyr::select(type, from, from_label, to, to_label)
  }
  list(
    spouses  = decorate(horiz %>% dplyr::filter(type == "spouse")            %>% dplyr::slice_head(n = n_each)),
    siblings = decorate(horiz %>% dplyr::filter(startsWith(type, "sibling")) %>% dplyr::slice_head(n = n_each))
  )
}

# 6) export Horizontal relationshops to CSVs and return them in a list
export_horizontal_relationships <- function(edges, nodes, out_dir = "generated",
                                            refresh_direct = FALSE) {
  fs::dir_create(out_dir)

  #Inferred from ancestery edges
  spouses_inf   <- spouse_edges_from_coparents(edges)
  siblings_inf  <- sibling_edges_from_parents(edges)
 
  #Direct Wikidata claims 
  qids <- nodes$node
  spouses_p26   <- spouse_edges_from_p26(qids,  use_cache = !refresh_direct)
  siblings_p33  <- sibling_edges_from_p3373(qids, use_cache = !refresh_direct)
  
  horiz_all <- dplyr::bind_rows(spouses_inf, siblings_inf, spouses_p26, siblings_p33) %>% dplyr::distinct()
  
  readr::write_csv(spouses_inf,  file.path(out_dir, "spouse_edges_inferred.csv"))
  readr::write_csv(siblings_inf, file.path(out_dir, "sibling_edges_inferred.csv"))
  readr::write_csv(spouses_p26,  file.path(out_dir, "spouse_edges_p26.csv"))
  readr::write_csv(siblings_p33, file.path(out_dir, "sibling_edges_p3373.csv"))
  readr::write_csv(horiz_all,    file.path(out_dir, "horizontal_edges_all_sources.csv"))
  
  #Person-level counts of horizontal ties
  counts <- horiz_all %>%
    tidyr::pivot_longer(cols = c(from, to), values_to = "qid") %>%
    dplyr::mutate(rel_bucket = dplyr::case_when(
      type %in% c("spouse", "spouse_p26") ~ "spouse",
      grepl("^sibling", type)             ~ "sibling",
      TRUE                                ~ "other"
    )) %>%
    dplyr::count(qid, rel_bucket, name = "n") %>%
    tidyr::pivot_wider(names_from = rel_bucket, values_from = n, values_fill = 0) %>%
    dplyr::left_join(nodes %>% dplyr::transmute(qid = node, name = dplyr::coalesce(nodeLabel, node)),
                     by = "qid") %>%
    dplyr::relocate(qid, name)
  
  readr::write_csv(counts, file.path(out_dir, "horizontal_counts_per_person.csv"))
  
  cat("Horizontal edges — inferred spouses:", nrow(spouses_inf),
      "| inferred siblings:", nrow(siblings_inf), "\n")
  cat("Horizontal edges — P26 spouses:", nrow(spouses_p26),
      "| P3373 siblings:", nrow(siblings_p33), "\n")
  cat("Combined unique horizontal edges:", nrow(horiz_all), "\n")
  
  invisible(list(
    spouses_inferred   = spouses_inf,
    siblings_inferred  = siblings_inf,
    spouses_p26        = spouses_p26,
    siblings_p3373     = siblings_p33,
    edges_all          = horiz_all,
    counts             = counts
  ))
}

# Direct P26 / P3373 edges across all current nodes
spouse_edges_from_p26 <- function(qids, use_cache = TRUE) {
  out <- purrr::map(qids, function(q) {
    r <- wd_get_spouses_p26(q, use_cache = use_cache)
    if (nrow(r) == 0) return(tibble::tibble(from=character(), to=character()))
    tibble::tibble(a = q, b = r$sp)
  }) %>% dplyr::bind_rows()
  if (nrow(out) == 0) return(tibble::tibble(from=character(), to=character(), type=character()))
  out %>%
    dplyr::transmute(from = pmin(a,b), to = pmax(a,b), type = "spouse_p26") %>%
    dplyr::distinct()
}

sibling_edges_from_p3373 <- function(qids, use_cache = TRUE) {
  out <- purrr::map(qids, function(q) {
    r <- wd_get_siblings_p3373(q, use_cache = use_cache)
    if (nrow(r) == 0) return(tibble::tibble(from=character(), to=character()))
    tibble::tibble(a = q, b = r$sib)
  }) %>% dplyr::bind_rows()
  if (nrow(out) == 0) return(tibble::tibble(from=character(), to=character(), type=character()))
  out %>%
    dplyr::transmute(from = pmin(a,b), to = pmax(a,b), type = "sibling_p3373") %>%
    dplyr::distinct()
}

# Parents fetcher (P22/P25)
wd_get_parents <- function(qid, use_cache = TRUE) {
  cf <- parent_cache_file(qid)
  if (use_cache && file.exists(cf)) { return(readRDS(cf)) }
  sparql <- paste0(
    "SELECT ?item ?itemLabel ?father ?fatherLabel ?mother ?motherLabel ",
    "(YEAR(?item_birth) as ?item_birth_year) (YEAR(?item_death) as ?item_death_year) ",
    "(YEAR(?father_birth) as ?father_birth_year) (YEAR(?father_death) as ?father_death_year) ",
    "(YEAR(?mother_birth) as ?mother_birth_year) (YEAR(?mother_death) as ?mother_death_year) ",
    "WHERE { ",
    "  BIND(wd:", qid, " AS ?item) ",
    "  OPTIONAL { ?item wdt:P569 ?item_birth . } ",
    "  OPTIONAL { ?item wdt:P570 ?item_death . } ",
    "  OPTIONAL { ?item wdt:P22 ?father . ",
    "             OPTIONAL { ?father wdt:P569 ?father_birth . } ",
    "             OPTIONAL { ?father wdt:P570 ?father_death . } ",
    "  } ",
    "  OPTIONAL { ?item wdt:P25 ?mother . ",
    "             OPTIONAL { ?mother wdt:P569 ?mother_birth . } ",
    "             OPTIONAL { ?mother wdt:P570 ?mother_death . } ",
    "  } ",
    "  SERVICE wikibase:label { bd:serviceParam wikibase:language 'de,en'. } ",
    "}"
  )
  Sys.sleep(BASE_SLEEP)
  res <- safe_query(sparql)
  res <- res %>% dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  if (nrow(res) == 0) {
    res <- tibble::tibble(
      item = qid, itemLabel = NA_character_,
      father = NA_character_, fatherLabel = NA_character_,
      mother = NA_character_, motherLabel = NA_character_,
      item_birth_year = NA_character_, item_death_year = NA_character_,
      father_birth_year = NA_character_, father_death_year = NA_character_,
      mother_birth_year = NA_character_, mother_death_year = NA_character_
    )
  }
  saveRDS(res, cf)
  res
}

#### 2c. Ancestor traversal (BFS over parents) ####
ancestors_for_seed <- function(seed_qid, max_depth = MAX_DEPTH) {
  visited <- new.env(hash = TRUE, parent = emptyenv())
  labels  <- list()
  set_label <- function(qid, lbl) { if (isTRUE(nchar(lbl) > 0)) labels[[qid]] <<- lbl }
  edge_buf <- list()
  frontier <- data.frame(qid = seed_qid, depth = 0, stringsAsFactors = FALSE)
  visited[[seed_qid]] <- TRUE
  total_edges <- 0L
  
  while (nrow(frontier) > 0) {
    this_depth <- frontier$depth[1]
    if (this_depth >= max_depth) break
    next_frontier <- list()
    
    for (i in seq_len(nrow(frontier))) {
      child_qid <- frontier$qid[i]
      d         <- frontier$depth[i]
      info <- wd_get_parents(child_qid)
      set_label(child_qid, info$itemLabel[1])
      
      if (!is.na(info$father[1]) && stringr::str_starts(info$father[1], "Q")) {
        father_qid <- info$father[1]
        set_label(father_qid, info$fatherLabel[1])
        edge_buf[[length(edge_buf) + 1]] <- tibble::tibble(
          from = child_qid, to = father_qid, type = "has_father",
          fromLabel = info$itemLabel[1], toLabel = info$fatherLabel[1]
        )
        total_edges <- total_edges + 1L
        if (is.null(visited[[father_qid]])) {
          visited[[father_qid]] <- TRUE
          next_frontier[[length(next_frontier) + 1]] <- data.frame(
            qid = father_qid, depth = d + 1, stringsAsFactors = FALSE
          )
        }
      }
      
      if (!is.na(info$mother[1]) && stringr::str_starts(info$mother[1], "Q")) {
        mother_qid <- info$mother[1]
        set_label(mother_qid, info$motherLabel[1])
        edge_buf[[length(edge_buf) + 1]] <- tibble::tibble(
          from = child_qid, to = mother_qid, type = "has_mother",
          fromLabel = info$itemLabel[1], toLabel = info$motherLabel[1]
        )
        total_edges <- total_edges + 1L
        if (is.null(visited[[mother_qid]])) {
          visited[[mother_qid]] <- TRUE
          next_frontier[[length(next_frontier) + 1]] <- data.frame(
            qid = mother_qid, depth = d + 1, stringsAsFactors = FALSE
          )
        }
      }
    }
    
    if (length(next_frontier) > 0) {
      frontier <- do.call(rbind, next_frontier)
    } else {
      frontier <- frontier[0, , drop = FALSE]
    }
    
    cat(sprintf("  depth %d → next frontier: %d nodes | edges so far: %d\n",
                this_depth + 1, nrow(frontier), total_edges))
  }
  
  edges <- if (length(edge_buf) > 0) dplyr::bind_rows(edge_buf) else
    tibble::tibble(from = character(), to = character(),
                   type = character(), fromLabel = character(), toLabel = character())
  edges <- unique(edges)
  
  node_ids <- unique(c(edges$from, edges$to))
  `%||%` <- function(x, y) if (is.null(x) || length(x) == 0L || is.na(x)) y else x
  node_labels <- vapply(node_ids, function(q) labels[[q]] %||% NA_character_, FUN.VALUE = character(1))
  
  nodes <- tibble::tibble(
    node = node_ids,
    nodeLabel = node_labels
  )
  
  list(edges = edges, nodes = nodes)
}

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0L || is.na(x)) y else x

#### 2d. Merge seeds, dedupe, augment with children ####
all_edges <- list()
all_nodes <- list()

cat(sprintf("Running parent-only ancestry on %d seed(s)\n", length(seeds)))

for (si in seq_along(seeds)) {
  seed <- seeds[[si]]
  cat(sprintf("[%d/%d] seed = %s\n", si, length(seeds), seed))
  res <- tryCatch(
    { ancestors_for_seed(seed, max_depth = MAX_DEPTH) },
    error = function(e) {
      message("  !! Error on seed ", seed, ": ", conditionMessage(e))
      return(NULL)
    }
  )
  if (!is.null(res)) {
    all_edges[[length(all_edges) + 1]] <- res$edges
    res$nodes$is_seed <- res$nodes$node %in% seed
    all_nodes[[length(all_nodes) + 1]] <- res$nodes
  }
}

edges <- if (length(all_edges) > 0) dplyr::bind_rows(all_edges) else
  tibble::tibble(from = character(), to = character(),
                 type = character(), fromLabel = character(), toLabel = character())
nodes <- if (length(all_nodes) > 0) dplyr::bind_rows(all_nodes) else
  tibble::tibble(node = character(), nodeLabel = character(), is_seed = logical())

# Deduplicate
edges <- unique(edges)
nodes <- nodes %>%
  dplyr::group_by(node) %>%
  dplyr::summarize(
    nodeLabel = dplyr::coalesce(dplyr::first(na.omit(nodeLabel)), NA_character_),
    is_seed = any(is_seed, na.rm = TRUE),
    .groups = "drop"
  )

# Use neutral group label 
GROUP_LABEL <- "seed list"
nodes <- nodes %>%
  dplyr::mutate(
    group = ifelse(is_seed | node %in% df$id_wikidata, GROUP_LABEL, "ancestor"),
    id = dplyr::row_number() - 1L,
    nodeLabel2 = paste(nodeLabel, id)
  )

# Minimal weights
edges$value <- 1L

# Remove duplicate edges by (from, to, type)
edges <- edges %>% dplyr::distinct(from, to, type, .keep_all = TRUE)

#### 3. Horizontal relationships (spouse/sibling) ####
cat("----\n")
cat(sprintf("Before augmentation → Nodes: %s | Edges: %s\n", nrow(nodes), nrow(edges)))

# Add child edges (downward) to enrich horizontal inference
aug <- augment_graph_with_children(edges, nodes)
edges <- aug$edges

nodes <- nodes %>%
  dplyr::full_join(aug$nodes, by = "node", suffix = c("", ".new")) %>%
  dplyr::mutate(nodeLabel = dplyr::coalesce(nodeLabel, nodeLabel.new)) %>%
  dplyr::select(-nodeLabel.new)

edges <- edges %>%
  # parent labels
  dplyr::left_join(
    nodes %>% dplyr::transmute(to = node, toLabel_fill = dplyr::coalesce(nodeLabel, node)),
    by = "to"
  ) %>%
  # child labels
  dplyr::left_join(
    nodes %>% dplyr::transmute(from = node, fromLabel_fill = dplyr::coalesce(nodeLabel, node)),
    by = "from"
  ) %>%
  dplyr::mutate(
    toLabel   = dplyr::coalesce(toLabel, toLabel_fill, to),       
    fromLabel = dplyr::coalesce(fromLabel, fromLabel_fill, from), 
    value     = dplyr::coalesce(value, 1L)                        
  ) %>%
  dplyr::select(-toLabel_fill, -fromLabel_fill)

cat(sprintf("After augmentation → Nodes: %s | Edges: %s\n", nrow(nodes), nrow(edges)))

# Quick CSVs
readr::write_csv(nodes, "data_prepared/anc_nodes.csv")
readr::write_csv(edges, "data_prepared/anc_edges.csv")

# Graph
graph <- igraph::graph_from_data_frame(edges, directed = TRUE, vertices = nodes)
cat(sprintf("Final graph → %s nodes | %s edges\n", igraph::vcount(graph), igraph::ecount(graph)))


save(graph, nodes, edges, file = "data_prepared/seedlist_ancestors_only.RData")
cat("Saved:\n  - data_prepared/seedlist_ancestors_only.RData\n",
    "  - data_prepared/anc_nodes.csv\n",
    "  - data_prepared/anc_edges.csv\n")

#Export horoizontal relationships to CSVs
hr <- export_horizontal_relationships(
  edges, nodes,
  out_dir = "generated",
  refresh_direct = FALSE   # set TRUE once to ignore caches, then back to FALSE
)
cat("Files in generated/: ", paste(list.files("generated"), collapse = ", "), "\n")


# Deduplicate spouse edges: it was showing the same couple for inferred and direct wikidata spouse edges, now it will appear once

hr_all_raw <- hr$edges_all

# 1) Collapse spouse/spouse_p26 per unordered pair
spouse_collapsed <- hr_all_raw %>%
  dplyr::filter(type %in% c("spouse", "spouse_p26")) %>%
  dplyr::mutate(
    pair_from = pmin(from, to),
    pair_to   = pmax(from, to)
  ) %>%
  dplyr::group_by(pair_from, pair_to) %>%
  dplyr::summarise(
    # New unified type
    type = "spouse",
    # Optional: remember what was seen (spouse; spouse_p26; or both)
    source_types = paste(sort(unique(type)), collapse = ";"),
    .groups = "drop"
  )

# 2) Keep all non-spouse edges as they are
others <- hr_all_raw %>%
  dplyr::filter(!type %in% c("spouse", "spouse_p26"))

# 3) Combine back into a cleaned horizontal edge table
hr_edges_nodup <- dplyr::bind_rows(
  others,
  spouse_collapsed %>%
    dplyr::transmute(
      from = pair_from,
      to   = pair_to,
      type,
      source_types
    )
)

# Optional: ensure no exact from–to–type duplicates remain
hr_edges_nodup <- hr_edges_nodup %>%
  dplyr::distinct(from, to, type, .keep_all = TRUE)

# Replace the old table with the cleaned one for everything below
hr$edges_all <- hr_edges_nodup

cat("After collapsing spouse/spouse_p26 → unique spouse edges:",
    nrow(hr_edges_nodup), "rows\n")

###NEW 
#### 4. Compute generation + depth for ALL seeds ####

library(dplyr)
library(igraph)
library(purrr)
library(tidyr)

# All seeds
all_seeds <- df$id_wikidata

cat("Computing generation/depth for", length(all_seeds), "seeds...\n")

gen_table_list <- list()

for (seed in all_seeds) {
  if (!seed %in% V(graph)$name) {
    cat("Skipping seed not in graph:", seed, "\n")
    next
  }
  
  vids <- igraph::subcomponent(graph, v = seed, mode = "out")
  g_sub <- induced_subgraph(graph, vids)
  
  dmat <- igraph::distances(g_sub, v = seed, mode = "out")[1, ]
  dmat <- ifelse(is.infinite(dmat), NA, dmat)
  
  gen_table_list[[seed]] <- tibble(
    qid = names(dmat),
    !!paste0("gen_from_", seed) := dmat
  )
}

gen_all <- reduce(gen_table_list, full_join, by = "qid")

# Depth-from-roots computation (global, independent of seed)
roots <- which(igraph::degree(graph, mode = "in") == 0)

dist_list <- lapply(roots, function(r) igraph::distances(graph, v = r, mode = "out")[1, ])
dist_matrix <- do.call(rbind, dist_list)

depth_vec <- apply(dist_matrix, 2, function(x) {
  if (all(is.infinite(x))) NA_integer_ else min(x[is.finite(x)])
})

gen_all$depth_from_roots <- depth_vec[match(gen_all$qid, names(depth_vec))]

# Save
readr::write_csv(gen_all, "generated/nodes_generation_all_seeds.csv")
cat("✔ Saved generation/depth for ALL seeds → generated/nodes_generation_all_seeds.csv\n")


#### 5. Master relationship CSV (vertical + horizontal) ####

library(dplyr)
library(tidyr)
library(readr)

# Load seed-generation table
gen_all <- readr::read_csv("generated/nodes_generation_all_seeds.csv",
                           show_col_types = FALSE)

# Names fallback table
name_map <- nodes %>%
  transmute(qid = node,
            name = dplyr::coalesce(nodeLabel, node))

# Add names from horizontal counts 
if (file.exists("generated/horizontal_counts_per_person.csv")) {
  horiz_counts <- readr::read_csv("generated/horizontal_counts_per_person.csv",
                                  show_col_types = FALSE)
  name_map2 <- horiz_counts %>% select(qid, name)
  name_map <- bind_rows(name_map, name_map2) %>%
    group_by(qid) %>% summarise(name = first(na.omit(name)), .groups = "drop")
}

# Vertical edges (ancestry)
vert_edges <- edges %>%
  filter(type %in% c("has_father", "has_mother")) %>%
  transmute(
    src_qid       = from,
    dst_qid       = to,
    relation_type = type,
    source_file   = "anc_edges"
  )

# Horizontal edges
horiz_edges <- hr$edges_all %>%
  transmute(
    src_qid       = from,
    dst_qid       = to,
    relation_type = type,
    source_file   = "horizontal_edges_all_sources"
  )

master_edges <- bind_rows(vert_edges, horiz_edges) %>%
  distinct()

# Attach names
master_with_names <- master_edges %>%
  left_join(name_map %>% rename(src_qid = qid, src_name = name),
            by = "src_qid") %>%
  left_join(name_map %>% rename(dst_qid = qid, dst_name = name),
            by = "dst_qid")

# Attach all-seed generation info
gen_long <- gen_all %>%
  pivot_longer(
    starts_with("gen_from_"),
    names_to = "seed_id",
    values_to = "generation_from_seed"
  )

master_full <- master_with_names %>%
  left_join(gen_long %>% rename(src_qid = qid,
                                src_generation_from_seed = generation_from_seed),
            by = "src_qid") %>%
  left_join(gen_long %>% rename(dst_qid = qid,
                                dst_generation_from_seed = generation_from_seed),
            by = c("dst_qid", "seed_id")) %>%
  # depth-from-roots
  left_join(gen_all %>% select(qid, depth_from_roots) %>% 
              rename(src_qid = qid, src_depth_from_roots = depth_from_roots),
            by = "src_qid") %>%
  left_join(gen_all %>% select(qid, depth_from_roots) %>% 
              rename(dst_qid = qid, dst_depth_from_roots = depth_from_roots),
            by = "dst_qid")

readr::write_csv(master_full, "generated/master_relationships_full.csv")
cat("✔ Wrote multi-seed master CSV to: generated/master_relationships_full.csv\n")

# Quick sanity check in R:
print(master_full %>% count(relation_type))


###Master CSV Visualization:
#### 6. Visualizations: vertical + horizontal for all seeds ####

library(dplyr)
library(igraph)
library(visNetwork)
library(htmlwidgets)
library(fs)

# 6.1 Build a combined edge list 

# Vertical edges (parents)
edges_vert_viz <- edges %>%
  filter(type %in% c("has_father", "has_mother")) %>%
  transmute(
    from = from,
    to   = to,
    relation_type = type
  )

# Horizontal edges (spouses + siblings)
edges_horiz_viz <- hr$edges_all %>%
  transmute(
    from = from,
    to   = to,
    relation_type = type
  )

# Combine, drop bad edges, deduplicate
edges_all_viz <- bind_rows(edges_vert_viz, edges_horiz_viz) %>%
  filter(!is.na(from), !is.na(to)) %>%             # <— key fix
  distinct()

cat("Viz edges (clean):", nrow(edges_all_viz), "\n")

# 6.2 Build node table for visualization 

all_ids <- sort(unique(c(edges_all_viz$from, edges_all_viz$to)))

nodes_all <- tibble(id = all_ids) %>%
  # bring in labels/groups from the ancestry nodes table where available
  left_join(
    nodes %>%
      transmute(id = node,
                label = dplyr::coalesce(nodeLabel, node),
                group_raw = group),
    by = "id"
  ) %>%
  mutate(
    label   = coalesce(label, id),
    is_seed = id %in% df$id_wikidata,
    group   = case_when(
      is_seed ~ "seed",
      TRUE    ~ "person"
    )
  )

cat("Viz nodes:", nrow(nodes_all), "\n")

# 6.3 Build the master igraph object 

g_all <- graph_from_data_frame(
  d = edges_all_viz,
  directed = TRUE,
  vertices = nodes_all
)

cat("Master viz graph:",
    vcount(g_all), "nodes |",
    ecount(g_all), "edges\n")

# 6.4 Helper: pick a seed by QID or by label 

pick_seed <- function(id_or_label) {
  # If it looks like a QID, use it directly
  if (startsWith(id_or_label, "Q")) {
    if (!id_or_label %in% nodes_all$id) {
      stop("QID not found in nodes_all: ", id_or_label)
    }
    return(id_or_label)
  }
  
  # Otherwise look up by label (exact match)
  idx <- which(nodes_all$label == id_or_label)
  if (length(idx) == 0) stop("No exact label match for: ", id_or_label)
  if (length(idx) > 1) warning("Multiple label matches; taking the first.")
  nodes_all$id[idx[1]]
}

# 6.5 Helper: create a visNetwork graph for one seed 

# mode = "component": full connected component of that seed
# mode = "ego":       only nodes within 'order' steps of the seed

make_seed_vis <- function(seed_input,
                          mode = c("component", "ego"),
                          order = 3,
                          out_dir = "graphs/master_seeds") {
  
  mode <- match.arg(mode)
  dir_create(out_dir)
  
  seed_id <- pick_seed(seed_input)
  
  # Use an undirected view for connectivity (vertical + horizontal)
  g_u <- as.undirected(g_all, mode = "collapse", edge.attr.comb = "first")
  
  if (mode == "component") {
    comp  <- components(g_u)$membership
    seed_comp <- comp[match(seed_id, names(comp))]
    keep_ids <- names(comp)[comp == seed_comp]
    g_sub <- induced_subgraph(g_all, vids = keep_ids)
  } else {
    ego_nodes <- ego(g_u, order = order, nodes = seed_id)[[1]]
    g_sub <- induced_subgraph(g_all, vids = ego_nodes)
  }
  
  vdf <- as_data_frame(g_sub, what = "vertices")
  edf <- as_data_frame(g_sub, what = "edges")
  
  # Nodes for visNetwork
  nodes_vis <- data.frame(
    id    = vdf$name,
    label = vdf$label,
    group = vdf$group,
    title = paste0(
      "<b>", vdf$label, "</b>",
      "<br/>Q-ID: ", vdf$name,
      "<br/>Seed: ", ifelse(vdf$is_seed, "yes", "no")
    ),
    stringsAsFactors = FALSE
  )
  
  # Edges for visNetwork
  edges_vis <- data.frame(
    from   = edf$from,
    to     = edf$to,
    title  = edf$relation_type,
    arrows = ifelse(edf$relation_type %in% c("has_father", "has_mother"),
                    "to", ""),
    color  = dplyr::case_when(
      edf$relation_type %in% c("has_father", "has_mother") ~ "#2c7fb8",  # vertical
      edf$relation_type %in% c("spouse", "spouse_p26")     ~ "#1b9e77",  # spouses
      grepl("^sibling", edf$relation_type)                 ~ "#7570b3",  # siblings
      TRUE                                                 ~ "#aaaaaa"
    ),
    width  = dplyr::case_when(
      edf$relation_type %in% c("spouse", "spouse_p26") ~ 3,
      grepl("^sibling", edf$relation_type)            ~ 2.5,
      TRUE                                            ~ 1.5
    ),
    smooth = TRUE,
    stringsAsFactors = FALSE
  )
  
  vis <- visNetwork(nodes_vis, edges_vis, width = "100%", height = "720px") %>%
    visOptions(
      highlightNearest = TRUE,
      nodesIdSelection = list(enabled = TRUE, useLabels = TRUE)
    ) %>%
    visGroups(groupname = "seed",
              color = list(background = "#00b3b3", border = "#008080")) %>%
    visGroups(groupname = "person",
              color = list(background = "#f0f0f0", border = "#bdbdbd")) %>%
    visLegend(addEdges = data.frame(
      label = c("Parent", "Spouse", "Sibling"),
      color = c("#2c7fb8", "#1b9e77", "#7570b3")
    )) %>%
    visPhysics(stabilization = FALSE)
  
  seed_label <- nodes_all$label[match(seed_id, nodes_all$id)]
  fname <- sprintf("%s/%s_%s.html",
                   out_dir,
                   if (mode == "component") "component" else paste0("ego", order),
                   gsub("\\s+", "_", seed_label, perl = TRUE))
  
  saveWidget(vis, file = fname, selfcontained = TRUE)
  cat("Saved visualization for seed", seed_label, "→", fname, "\n")
  
  invisible(fname)
}

#### 6.6 Global visualization: ALL seeds + ALL relationships ####

library(visNetwork)
library(htmlwidgets)
library(fs)
library(dplyr)
library(igraph)

dir_create("graphs/master_seeds")

# Get vertex and edge data from the global graph g_all
vdf_all <- igraph::as_data_frame(g_all, what = "vertices")
edf_all <- igraph::as_data_frame(g_all, what = "edges")

View(vdf_all)

# Nodes for visNetwork (ALL people)
nodes_vis_all <- data.frame(
  id    = vdf_all$name,
  label = vdf_all$label,
  group = vdf_all$group,
  title = paste0(
    "<b>", vdf_all$label, "</b>",
    "<br/>Q-ID: ", vdf_all$name,
    "<br/>Seed: ", ifelse(vdf_all$is_seed, "yes", "no")
  ),
  stringsAsFactors = FALSE
)

# Edges for visNetwork (vertical + horizontal)
edges_vis_all <- data.frame(
  from   = edf_all$from,
  to     = edf_all$to,
  title  = edf_all$relation_type,
  arrows = ifelse(edf_all$relation_type %in% c("has_father", "has_mother"),
                  "to", ""),
  color  = dplyr::case_when(
    edf_all$relation_type %in% c("has_father", "has_mother") ~ "#2c7fb8",  # parents
    edf_all$relation_type %in% c("spouse", "spouse_p26")     ~ "#1b9e77",  # spouses
    grepl("^sibling", edf_all$relation_type)                 ~ "#7570b3",  # siblings
    TRUE                                                     ~ "#aaaaaa"
  ),
  width  = dplyr::case_when(
    edf_all$relation_type %in% c("spouse", "spouse_p26") ~ 3,
    grepl("^sibling", edf_all$relation_type)            ~ 2.5,
    TRUE                                                ~ 1.5
  ),
  smooth = TRUE,
  stringsAsFactors = FALSE
)

# Build the big interactive graph
vis_all <- visNetwork(nodes_vis_all, edges_vis_all,
                      width = "100%", height = "800px") %>%
  visOptions(
    highlightNearest = TRUE,
    nodesIdSelection = list(enabled = TRUE, useLabels = TRUE)
  ) %>%
  visGroups(
    groupname = "seed",
    color = list(background = "#00b3b3", border = "#008080")
  ) %>%
  visGroups(
    groupname = "person",
    color = list(background = "#f0f0f0", border = "#bdbdbd")
  ) %>%
  visLegend(
    addEdges = data.frame(
      label = c("Parent", "Spouse", "Sibling"),
      color = c("#2c7fb8", "#1b9e77", "#7570b3")
    )
  ) %>%
  visPhysics(
    solver = "forceAtlas2Based",
    stabilization = FALSE
  )

out_file <- "graphs/master_seeds/all_seeds_full.html"
saveWidget(vis_all, file = out_file, selfcontained = TRUE)
cat("Saved global master graph →", out_file, "\n")

# pick_seed <- function(id_or_label) {
#   if (startsWith(id_or_label, "Q")) {
#     return(id_or_label)
#   } else {
#     idx <- match(id_or_label, V(graph)$nodeLabel)
#     if (is.na(idx)) stop("No exact nodeLabel match found for: ", id_or_label)
#     return(V(graph)$name[idx])
#   }
# }
# 
# # Subgraph for a seed (descendants/ancestors reachable via edges)
# subgraph_for_seed <- function(seed_id) {
#   vids <- igraph::subcomponent(graph, v = seed_id, mode = "out")
#   igraph::induced_subgraph(graph, vids = vids)
# }
# 
# # Distances/generation depth from the seed within a subgraph
# depth_from_seed <- function(g, seed_id) {
#   d <- igraph::distances(g, v = seed_id, mode = "out")
#   dv <- as.numeric(d[1, ])
#   names(dv) <- igraph::V(g)$name
#   dv
#}
# #### 4. Seed subgraph & generation depth ####
# SEED_INPUT <- "Jeff Bezos"   
# seed_qid <- pick_seed(SEED_INPUT)
# cat("Chosen seed:", SEED_INPUT, "→ Q-ID:", seed_qid, "\n")
# 
# g_sub <- subgraph_for_seed(seed_qid)
# cat("Subgraph size:", igraph::vcount(g_sub), "nodes |", igraph::ecount(g_sub), "edges\n")
# 
# samp <- sample_horizontal_relatedness(edges, nodes, graph, seed_qid = seed_qid, n_each = 10)
# cat("\n--- SPOUSES (sample) ---\n"); print(samp$spouses)
# cat("\n--- SIBLINGS (sample) ---\n"); print(samp$siblings)
# 
# cat("\n--- Spouses for seed ---\n"); print(get_spouses(seed_qid, edges, nodes))
# cat("\n--- Siblings (any) for seed ---\n"); print(get_siblings(seed_qid, edges, nodes, type = "any"))
# 
# depth_vec <- depth_from_seed(g_sub, seed_qid)
# igraph::V(g_sub)$generation <- depth_vec[igraph::V(g_sub)$name]
# igraph::V(g_sub)$generation[!is.finite(igraph::V(g_sub)$generation)] <- NA
# 
# # 4a. Export generation depth info
# library(fs); fs::dir_create("generated")
# 
# g_sub_full <- g_sub
# all_qids <- igraph::V(g_sub_full)$name
# generation_seed <- tibble::tibble(
#   qid = all_qids,
#   generation_from_seed = {
#     dv <- depth_vec[qid]
#     ifelse(is.finite(dv), as.integer(dv), NA_integer_)
#   }
# )
# 
# nodes_export <- igraph::as_data_frame(g_sub_full, what = "vertices") %>%
#   tibble::as_tibble() %>%
#   dplyr::rename(qid = name) %>%
#   dplyr::select(qid, dplyr::any_of(c("label", "title")))
# 
# nodes_with_generation <- nodes_export %>%
#   dplyr::left_join(generation_seed, by = "qid")
# 
# generation_summary <- nodes_with_generation %>%
#   dplyr::group_by(generation_from_seed) %>%
#   dplyr::summarise(n_nodes = dplyr::n(), .groups = "drop") %>%
#   dplyr::arrange(generation_from_seed)
# 
# readr::write_csv(nodes_with_generation, "generated/nodes_generation_from_seed.csv")
# readr::write_csv(generation_summary,   "generated/generation_from_seed_summary.csv")
# cat("✔ Exported generation_from_seed data to /generated folder\n")
# 
# # 4b. True ancestry depth (distance from roots)
# g_ig <- if (inherits(g_sub_full, "igraph")) g_sub_full else tidygraph::as.igraph(g_sub_full)
# roots <- which(igraph::degree(g_ig, mode = "in") == 0)
# dist_list <- lapply(roots, function(r) { igraph::distances(g_ig, v = r, to = igraph::V(g_ig), mode = "out") })
# dist_mat <- do.call(rbind, dist_list)
# depth_from_roots <- apply(dist_mat, 2, function(x) { if (all(is.infinite(x))) NA_integer_ else as.integer(min(x[is.finite(x)])) })
# names(depth_from_roots) <- colnames(dist_mat)
# 
# nodes_with_generations <- nodes_with_generation %>%
#   dplyr::mutate(depth_from_roots = depth_from_roots[qid])
# 
# readr::write_csv(nodes_with_generations, "generated/nodes_generation_with_ancestry_depth.csv")
# cat("✔ Exported nodes_generation_with_ancestry_depth.csv\n")



# #### 5. Optional: cap super-deep tails for plot ####
# MAX_GEN_SHOW <- 12
# keep <- which(is.na(igraph::V(g_sub)$generation) | igraph::V(g_sub)$generation <= MAX_GEN_SHOW)
# g_sub <- igraph::induced_subgraph(g_sub, vids = keep)
# cat("Plotted subgraph (capped at", MAX_GEN_SHOW, "generations):",
#     igraph::vcount(g_sub), "nodes |", igraph::ecount(g_sub), "edges\n")
# cat("Depth range (shown):",
#     min(igraph::V(g_sub)$generation, na.rm = TRUE), "to",
#     max(igraph::V(g_sub)$generation, na.rm = TRUE), "\n")



# #### 5. Visualizations Static ancestory plot (ggraph) ####
# dir.create("graphs", showWarnings = FALSE)
# 
# viz_pkgs <- c("ggraph", "ggplot2", "tidygraph", "visNetwork", "htmlwidgets")
# lapply(viz_pkgs[!(viz_pkgs %in% installed.packages())], install.packages)
# lapply(viz_pkgs, library, character.only = TRUE)
# 
# # 5a. Static hierarchy plot
# p <- ggraph(g_sub, layout = "sugiyama") +
#   geom_edge_link(aes(edge_colour = type), alpha = 0.6) +
#   geom_node_point(aes(color = group, size = ifelse(group == GROUP_LABEL, 4, 2))) +
#   geom_node_text(
#     aes(label = ifelse(group == GROUP_LABEL | generation <= 2, nodeLabel, "")),
#     repel = TRUE, size = 3
#   ) +
#   scale_edge_colour_manual(values = c(has_father = "#2c7fb8", has_mother = "#f768a1")) +
#   scale_size_identity() +
#   labs(
#     title = paste0("Ancestor tree (parents only) — seed: ",
#                    igraph::V(g_sub)$nodeLabel[igraph::V(g_sub)$name == seed_qid]),
#     subtitle = "Edges: child → parent (blue = father, pink = mother)"
#   ) +
#   theme_minimal()
# 
# print(p)
# ggsave(filename = file.path("graphs", paste0("ancestors_", seed_qid, ".png")),
#        plot = p, width = 10, height = 7, dpi = 300)
# cat("Saved static plot → graphs/", paste0("ancestors_", seed_qid, ".png"), "\n", sep = "")
# 
# # 5b. Interactive ancestery (visNetwork)
# library(visNetwork); library(htmlwidgets)
# 
# nodes_df <- data.frame(
#   id    = igraph::V(g_sub)$name,
#   label = ifelse(is.na(igraph::V(g_sub)$nodeLabel) | igraph::V(g_sub)$nodeLabel=="",
#                  igraph::V(g_sub)$name, igraph::V(g_sub)$nodeLabel),
#   group = igraph::V(g_sub)$group,
#   level = as.integer(ifelse(is.finite(igraph::V(g_sub)$generation),
#                             igraph::V(g_sub)$generation, 0)),
#   title = paste0(
#     "<b>", igraph::V(g_sub)$nodeLabel, "</b>",
#     "<br/>Q-ID: ", igraph::V(g_sub)$name,
#     "<br/>Generation: ", igraph::V(g_sub)$generation
#   ),
#   stringsAsFactors = FALSE
# )
# 
# e <- igraph::as_data_frame(g_sub, what = "edges")
# edges_df <- data.frame(
#   from   = e$from,
#   to     = e$to,
#   arrows = "to",
#   color  = ifelse(e$type == "has_father", "#2c7fb8", "#f768a1"),
#   title  = e$type,
#   smooth = FALSE,
#   stringsAsFactors = FALSE
# )
# 
# vis <- visNetwork(nodes_df, edges_df, width = "100%", height = "720px") %>%
#   visHierarchicalLayout(direction = "UD",
#                         levelSeparation = 120,
#                         nodeSpacing = 180,
#                         treeSpacing = 180,
#                         sortMethod = "directed") %>%
#   visGroups(groupname = GROUP_LABEL,
#             color = list(background = "#00b3b3", border = "#008080")) %>%
#   visGroups(groupname = "ancestor",
#             color = list(background = "#f0f0f0", border = "#bdbdbd")) %>%
#   visOptions(highlightNearest = TRUE,
#              nodesIdSelection = list(enabled = TRUE, useLabels = TRUE)) %>%
#   visLegend() %>%
#   visEdges(width = 2) %>%
#   visPhysics(stabilization = FALSE)
# 
# vis
# 
# seed_label <- nodes_df$label[match(seed_qid, nodes_df$id)]
# out_html <- sprintf("graphs/ancestor_%s.html",
#                     gsub("\\s+", "_", seed_label, perl = TRUE))
# saveWidget(vis, file = out_html, selfcontained = TRUE)
# 
# browseURL(out_html)
# 
# #5c Visualizations, Interactive Horizontal relationships (visNetwork)
# 
# # 0) Pull combined horizontal edges from the export result
# edges_h <- hr$edges_all
# 
# # 1) Clean edges: drop NAs and non-Q IDs; de-duplicate
# edges_h <- edges_h %>%
#   dplyr::filter(!is.na(from), !is.na(to)) %>%
#   dplyr::filter(stringr::str_starts(from, "Q"), stringr::str_starts(to, "Q")) %>%
#   dplyr::select(from, to, type) %>%
#   dplyr::distinct()
# 
# # 2) Build a vertex table that covers *all* endpoints in edges_h
# v_all <- sort(unique(c(edges_h$from, edges_h$to)))
# 
# vdf <- tibble::tibble(name = v_all) %>%
#   dplyr::left_join(
#     nodes %>%
#       dplyr::transmute(name = node,
#                        label = dplyr::coalesce(nodeLabel, node),
#                        group = group),
#     by = "name"
#   ) %>%
#   dplyr::mutate(
#     label = dplyr::coalesce(label, name),
#     group = dplyr::coalesce(group, "ancestor")
#   )
# 
# # 3) (Optional) sanity check: to check if anything is missing
# missing_ids <- setdiff(unique(c(edges_h$from, edges_h$to)), vdf$name)
# if (length(missing_ids)) {
#   cat("!! Missing in vdf (should be 0):", length(missing_ids), "\n")
#   print(head(missing_ids, 10))
# }
# 
# # 4) Build undirected igraph safely
# hgraph <- igraph::graph_from_data_frame(edges_h, directed = FALSE, vertices = vdf)
# 
# # 5) Focus on the seed’s connected component
# seed_qid_h <- pick_seed(SEED_INPUT)
# comp_h   <- igraph::components(hgraph)$membership
# seed_cc  <- comp_h[[seed_qid_h]]
# keep_ids <- names(comp_h)[comp_h == seed_cc]
# hsub     <- igraph::induced_subgraph(hgraph, vids = keep_ids)
# 
# # 6) visNetwork nodes/edges
# hnodes <- data.frame(
#   id    = igraph::V(hsub)$name,
#   label = ifelse(is.na(igraph::V(hsub)$label) | igraph::V(hsub)$label=="",
#                  igraph::V(hsub)$name, igraph::V(hsub)$label),
#   group = igraph::V(hsub)$group,
#   stringsAsFactors = FALSE
# )
# 
# he <- igraph::as_data_frame(hsub, what = "edges")
# hedges <- data.frame(
#   from  = he$from,
#   to    = he$to,
#   title = he$type,
#   color = dplyr::case_when(
#     he$type %in% c("spouse", "spouse_p26") ~ "#1b9e77",
#     grepl("^sibling", he$type)             ~ "#7570b3",
#     TRUE                                   ~ "#aaaaaa"
#   ),
#   width  = dplyr::case_when(
#     he$type %in% c("spouse", "spouse_p26") ~ 3,
#     TRUE                                   ~ 2
#   ),
#   smooth = TRUE,
#   stringsAsFactors = FALSE
# )
# 
# # 7) Render + save
# vis_h <- visNetwork(hnodes, hedges, width = "100%", height = "720px") %>%
#   visOptions(highlightNearest = TRUE,
#              nodesIdSelection = list(enabled = TRUE, useLabels = TRUE)) %>%
#   visLegend(addEdges = data.frame(
#     label = c("Spouse (inferred/P26)", "Sibling (any)"),
#     color = c("#1b9e77", "#7570b3")
#   )) %>%
#   visGroups(groupname = GROUP_LABEL,
#             color = list(background = "#00b3b3", border = "#008080")) %>%
#   visGroups(groupname = "ancestor",
#             color = list(background = "#f0f0f0", border = "#bdbdbd")) %>%
#   visPhysics(stabilization = FALSE)
# 
# vis_h
# 
# dir.create("graphs", showWarnings = FALSE)
# seed_label_h <- hnodes$label[match(seed_qid_h, hnodes$id)]
# out_html_h <- sprintf("graphs/horizontal_%s.html",
#                       gsub("\\s+", "_", seed_label_h, perl = TRUE))
# htmlwidgets::saveWidget(vis_h, file = out_html_h, selfcontained = TRUE)
# browseURL(out_html_h)
# cat("Saved horizontal relationship graph → ", out_html_h, "\n", sep = "")
# 
# ### The End ###

#### 7. Extended kinship: uncles/aunts + cousins for each seed ####

library(dplyr)
library(purrr)
library(tidyr)
library(readr)

# Vertical parent–child edges: child -> parent
vert_edges_core <- edges %>%
  filter(type %in% c("has_father", "has_mother")) %>%
  transmute(
    child       = from,
    parent      = to,
    parent_type = type    
  )

# All sibling edges (inferred + P3373)
sib_edges_core <- hr$edges_all %>%
  filter(grepl("^sibling", type)) %>%
  transmute(
    from,
    to,
    sibling_type = type
  )

#name lookup
name_map <- nodes %>%
  transmute(
    qid  = node,
    name = coalesce(nodeLabel, node)
  )

get_parents <- function(qid) {
  vert_edges_core %>%
    filter(child == qid) %>%
    transmute(
      parent_qid  = parent,
      parent_role = if_else(parent_type == "has_father", "father", "mother")
    ) %>%
    distinct()
}

get_children <- function(qid) {
  vert_edges_core %>%
    filter(parent == qid) %>%
    transmute(
      child_qid   = child,
      parent_qid  = parent,
      parent_role = if_else(parent_type == "has_father", "father", "mother")
    ) %>%
    distinct()
}

get_siblings_any <- function(qid) {
  sib_long <- bind_rows(
    sib_edges_core %>% transmute(ego = from, alter = to, sibling_type),
    sib_edges_core %>% transmute(ego = to,   alter = from, sibling_type)
  )
  
  sib_long %>%
    filter(ego == qid, !is.na(alter)) %>%
    transmute(
      ego_qid     = ego,
      sibling_qid = alter,
      sibling_type
    ) %>%
    distinct()
}

inspect_seed_family <- function(seed_qid) {
  seed_name <- name_map$name[match(seed_qid, name_map$qid)]
  
## 7.1) Siblings + children of seed
  
  sibs <- get_siblings_any(seed_qid) %>%
    left_join(name_map, by = c("sibling_qid" = "qid")) %>%
    rename(sibling_name = name) %>%
    mutate(
      seed_qid  = seed_qid,
      seed_name = seed_name,
      .before   = 1
    )
  
  kids <- get_children(seed_qid) %>%
    left_join(name_map, by = c("child_qid" = "qid")) %>%
    rename(child_name = name) %>%
    mutate(
      seed_qid  = seed_qid,
      seed_name = seed_name,
      .before   = 1
    )
  
  ## 7.2) Seed’s siblings’ children (nieces/nephews of seed) 
  
  nieces_nephews <- tibble()
  if (nrow(sibs) > 0) {
    nieces_nephews <- sibs %>%
      select(seed_qid, seed_name, sibling_qid, sibling_name) %>%
      left_join(
        vert_edges_core %>%
          transmute(
            parent_qid = parent,
            child_qid  = child,
            parent_type
          ),
        by = c("sibling_qid" = "parent_qid")
      ) %>%
      filter(!is.na(child_qid)) %>%
      left_join(name_map, by = c("child_qid" = "qid")) %>%
      rename(niece_nephew_name = name) %>%
      mutate(
        relationship = case_when(
          parent_type == "has_father" ~ "niece_nephew_through_paternal_sibling",
          parent_type == "has_mother" ~ "niece_nephew_through_maternal_sibling",
          TRUE                        ~ "niece_nephew"
        )
      ) %>%
      select(seed_qid, seed_name,
             sibling_qid, sibling_name,
             child_qid, niece_nephew_name,
             relationship)
  }
  
  ## 7.3) Seed’s children & their uncles/aunts (seed’s siblings)
  
  uncles_of_kids <- tibble()
  if (nrow(sibs) > 0 && nrow(kids) > 0) {
    uncles_of_kids <- kids %>%
      select(seed_qid, seed_name,
             child_qid, child_name, parent_role) %>%
      tidyr::crossing(
        sibs %>% select(sibling_qid, sibling_name)
      ) %>%
      mutate(
        relationship = case_when(
          parent_role == "father" ~ "paternal_uncle_aunt_of_child",
          parent_role == "mother" ~ "maternal_uncle_aunt_of_child",
          TRUE                    ~ "uncle_aunt_of_child"
        )
      )
  }
  
  ## 7.4) Seed’s parents, their siblings (uncles/aunts) & their children 
  
  parents <- get_parents(seed_qid) %>%
    left_join(name_map, by = c("parent_qid" = "qid")) %>%
    rename(parent_name = name)
  
  ua <- tibble()
  if (nrow(parents) > 0) {
    ua_list <- lapply(seq_len(nrow(parents)), function(i) {
      p_qid  <- parents$parent_qid[i]
      p_role <- parents$parent_role[i]
      
      sib_p <- get_siblings_any(p_qid)
      if (nrow(sib_p) == 0) return(NULL)
      
      sib_p %>%
        transmute(
          seed_qid       = seed_qid,
          parent_qid     = p_qid,
          parent_role    = p_role,
          uncle_aunt_qid = sibling_qid,
          sibling_type   = sibling_type
        )
    })
    
    ua_list <- Filter(Negate(is.null), ua_list)
    if (length(ua_list) > 0) {
      ua <- bind_rows(ua_list) %>%
        left_join(name_map, by = c("uncle_aunt_qid" = "qid")) %>%
        rename(uncle_aunt_name = name) %>%
        left_join(name_map, by = c("parent_qid" = "qid")) %>%
        rename(parent_name = name) %>%
        mutate(
          seed_name = seed_name,
          relationship = case_when(
            parent_role == "father" ~ "paternal_uncle_aunt",
            parent_role == "mother" ~ "maternal_uncle_aunt",
            TRUE                    ~ "uncle_aunt"
          ),
          .before = 1
        )
    }
  }
  
  cousins <- tibble()
  if (nrow(ua) > 0) {
    cousins <- ua %>%
      select(seed_qid, seed_name,
             parent_qid, parent_name, parent_role,
             uncle_aunt_qid, uncle_aunt_name, sibling_type) %>%
      left_join(
        vert_edges_core %>%
          transmute(
            uncle_aunt_qid = parent,
            cousin_qid     = child,
            parent_type
          ),
        by = "uncle_aunt_qid"
      ) %>%
      filter(!is.na(cousin_qid)) %>%
      left_join(name_map, by = c("cousin_qid" = "qid")) %>%
      rename(cousin_name = name) %>%
      mutate(
        relationship = case_when(
          parent_role == "father" ~ "paternal_first_cousin",
          parent_role == "mother" ~ "maternal_first_cousin",
          TRUE                    ~ "first_cousin"
        )
      )
  }
  
  list(
    siblings       = sibs,
    children       = kids,
    nieces_nephews = nieces_nephews,
    uncles_of_kids = uncles_of_kids,
    uncles_aunts   = ua,
    cousins        = cousins
  )
}

# Use the limited seed set, not all df$id_wikidata
all_seeds <- seeds
cat("Computing local family for", length(all_seeds), "seeds...\n")

kin_list <- lapply(all_seeds, inspect_seed_family)

# Combine across seeds for each relationship type
siblings_all       <- bind_rows(lapply(kin_list, `[[`, "siblings"))
children_all       <- bind_rows(lapply(kin_list, `[[`, "children"))
nieces_nephews_all <- bind_rows(lapply(kin_list, `[[`, "nieces_nephews"))
uncles_of_kids_all <- bind_rows(lapply(kin_list, `[[`, "uncles_of_kids"))
uncles_aunts_all   <- bind_rows(lapply(kin_list, `[[`, "uncles_aunts"))
cousins_all        <- bind_rows(lapply(kin_list, `[[`, "cousins"))

dir.create("generated/extended_kin", showWarnings = FALSE, recursive = TRUE)

write_csv(siblings_all,       "generated/extended_kin/siblings_all_seeds.csv")
write_csv(children_all,       "generated/extended_kin/children_all_seeds.csv")
write_csv(nieces_nephews_all, "generated/extended_kin/nieces_nephews_all_seeds.csv")
write_csv(uncles_of_kids_all, "generated/extended_kin/uncles_of_kids_all_seeds.csv")
write_csv(uncles_aunts_all,   "generated/extended_kin/uncles_aunts_all_seeds.csv")
write_csv(cousins_all,        "generated/extended_kin/cousins_all_seeds.csv")

cat("✔ Saved extended kinship CSVs for all seeds in SEED_LIMIT to generated/extended_kin/\n")


##End of timer
global_end <- Sys.time()

total_time <- as.numeric(global_end - global_start, units = "secs")
avg_time   <- total_time / length(seeds)

cat("\n===== OVERALL SEED PIPELINE TIMING =====\n")
cat("Seeds used in ancestry (SEED_LIMIT):", length(seeds), "\n")
cat("Total time (ancestors + spouses/siblings + kinship):",
    round(total_time, 2), "seconds\n")
cat("Average time per seed:",
    round(avg_time, 3), "seconds\n")
cat("Estimated time for 200 seeds:",
    round(avg_time * 200, 1), "seconds (",
    round(avg_time * 200 / 60, 2), "minutes )\n")
        

