library(tidyverse)
library(visNetwork)
library(sparklyr)
library(reshape2)
library(leaflet)
library(leaflet.extras)

Sys.setenv(SPARK_HOME = '/usr/local/Cellar/apache-spark/2.2.1/libexec')
sc <- spark_connect(master = "local")

# https://github.com/longhowlam/BitsAndPieces/blob/master/FPGrowth_sparklyr.R

#' =============== READ DATA =================== #
df <- read_csv('wiltshirepolice.csv')
summary(df)


#' =============== PROFILING =================== #
null_test <- df %>% select(`Crime ID`, `Last outcome category`)
test_1 <- null_test %>% filter(is.na(`Crime ID`) & !is.na(`Last outcome category`)) %>%
  nrow()
test_2 <- null_test %>% filter(!is.na(`Crime ID`) & is.na(`Last outcome category`)) %>%
  nrow()


df %>% count(`Reported by`, `Falls within`)

df_clean <- df %>% select(-c(`Crime ID`, `Reported by`, 
                             `Falls within`, `LSOA code`, Context)) %>%
  filter(grepl('Swindon', `LSOA name`) | grepl('Wiltshire', `LSOA name`)) %>%
  replace_na(list(`Last outcome category` = "Unresolved")) %>%
  mutate(Location = trimws(str_replace(Location, 'On or near', ""))) %>%
  rowid_to_column("id")


df %>% dplyr::filter(Month == '2015-03' | Month == '2015-04') %>%
  filter(`Crime type`== 'Anti-social behaviour') %>%
  select(Longitude, Latitude) %>% 
  group_by(Longitude, Latitude) %>% 
  count() %>%
  leaflet() %>%
  addTiles() %>%
  addWebGLHeatmap(lng=~Longitude, lat=~Latitude, intensity = ~n, size=1000, opacity = 0.6)

df %>% dplyr::filter(Month == '2015-03')  %>%
  select(Longitude, Latitude, `Crime type`) %>% 
  leaflet() %>%
  addTiles() %>%
  addMarkers(
  clusterOptions = markerClusterOptions(),
  popup=~`Crime type`
)

df %>% filter(is.na(`Last outcome category`))


df_assoc <- df_clean %>% select(id,
                                  Location, 
                                `LSOA name`, 
                                `Crime type`, 
                                `Last outcome category`) %>%
  melt(id.vars = 'id') %>%
  select(id, value)



df_assoc_tbl <- sparklyr::sdf_copy_to(sc, df_assoc, overwrite = T)

df_assoc_collect <- df_assoc_tbl %>% 
  group_by(id) %>%
  summarise(
    items = collect_list(value)
  )

uid = sparklyr:::random_string("fpgrowth_")
jobj = invoke_new(sc, "org.apache.spark.ml.fpm.FPGrowth", uid) 

FPGmodel <- jobj %>% 
  invoke("setItemsCol", "items") %>%
  invoke("setMinConfidence", 0.03) %>%
  invoke("setMinSupport", 0.01)  %>%
  invoke("fit", spark_dataframe(df_assoc_test))

ml_fpgrowth_extract_rules = function(FPGmodel, nLHS = 2, nRHS = 1)
{
  rules = FPGmodel %>% invoke("associationRules")
  sdf_register(rules, "rules")
  
  exprs1 <- lapply(
    0:(nLHS - 1), 
    function(i) paste("CAST(antecedent[", i, "] AS string) AS LHSitem", i, sep="")
  )
  exprs2 <- lapply(
    0:(nRHS - 1), 
    function(i) paste("CAST(consequent[", i, "] AS string) AS RHSitem", i, sep="")
  )
  
  splittedLHS = rules %>% invoke("selectExpr", exprs1) 
  splittedRHS = rules %>% invoke("selectExpr", exprs2) 
  p1 = sdf_register(splittedLHS, "tmp1")
  p2 = sdf_register(splittedRHS, "tmp2")
  
  ## collecting output rules to R should be OK and not flooding R
  bind_cols(
    sdf_bind_cols(p1, p2) %>% collect(),
    rules %>% collect() %>% select(confidence)
  )
}

rules <- ml_fpgrowth_extract_rules(FPGmodel)

plot_rules = function(rules, LHS = "LHSitem0", RHS = "RHSitem0", cf = 0.5)
{
  rules = rules %>% filter(confidence > cf)
  nds = unique(
    c(
      rules[,LHS][[1]],
      rules[,RHS][[1]]
    )
  )
  
  nodes = data.frame(id = nds, label = nds, title = nds) %>% arrange(id)
  
  edges = data.frame(
    from =  rules[,LHS][[1]],
    to = rules[,RHS][[1]]
  )
  visNetwork(nodes, edges, main = "Swindon Crime Associations", size=1) %>%
    visOptions(highlightNearest = TRUE, nodesIdSelection = TRUE) %>%
    visEdges(arrows = 'from') %>%
    visPhysics(
      solver = "barnesHut", 
      forceAtlas2Based = list(gravitationalConstant = -20, maxVelocity = 1)
    )
}

spark_disconnect(sc)
