---
title: "Swindon and Wiltshire Police Association Rules"
author: "Alistair Rogers"
date: "4/5/2018"
output: 
  html_document:
    self_contained: false
    keep_md: true
---






## Introduction

Data about crimes reported by a particular police force/constabulary is published here: <https://data.police.uk/data/>

As a Swindoner born and bred, I have noticed the amount of weirdness and crime that goes on in my area and I am interested in if there are any associations.

In this little project, I have used all of the Wiltshire Police Crime data from March 2015 to February 2018. Knowing Swindon, this may require a Spark job. (Yes that's right, I did just insult my hometown with Big Data, I am hilarious.)

### Reading the data
data.police.gov.uk provide a different file for all of the crimes that happened in a partiuclar month. Also these are all contained in different folders. e.g. 2015-03, 2017-01 and so on.
I will need all of these in one file so I will run a quick bash command to append these together.


```bash
rm wiltshirepolice.csv
touch wiltshirepolice.csv
FILES="~Documents/Police/*/*.csv"
OUTPUT="wiltshirepolice.csv"
i=0
for filename in $FILES; do
if [ "$filename"  != "$OUTPUT" ] ;      # Avoid recursion
 then
   if [[ $i -eq 0 ]] ; then
      head -1  $filename >   $OUTPUT # Copy header if it is the first file
   fi
   tail -n +2  $filename >>  $OUTPUT # Append from the 2nd line each file
   i=$(( $i + 1 ))                        # Increase the counter
 fi
done
```

Lets have a look at the structure of the data...


```r
df <- read_csv('wiltshirepolice.csv')
```


```r
str(df)
```

```
## Classes 'tbl_df', 'tbl' and 'data.frame':	174085 obs. of  12 variables:
##  $ Crime ID             : chr  NA "461678767ecffc20aeca77074a608604b0317183b048bfee8318818511aa0b99" "db25706e6d89316edf5fad4937432a77c11f75ca4c50d1d7936e13b41367b152" NA ...
##  $ Month                : chr  "2015-03" "2015-03" "2015-03" "2015-03" ...
##  $ Reported by          : chr  "Wiltshire Police" "Wiltshire Police" "Wiltshire Police" "Wiltshire Police" ...
##  $ Falls within         : chr  "Wiltshire Police" "Wiltshire Police" "Wiltshire Police" "Wiltshire Police" ...
##  $ Longitude            : num  -2.36 -1.94 -2.25 -1.71 -1.71 ...
##  $ Latitude             : num  51.4 51.7 51.3 51.6 51.6 ...
##  $ Location             : chr  "On or near Railway Street" "On or near Langet" "On or near Rudge Hill" "On or near Downs View" ...
##  $ LSOA code            : chr  "E01014371" "E01022223" "E01029022" "E01015519" ...
##  $ LSOA name            : chr  "Bath and North East Somerset 007B" "Cotswold 010B" "Mendip 001A" "Swindon 001A" ...
##  $ Crime type           : chr  "Anti-social behaviour" "Vehicle crime" "Burglary" "Anti-social behaviour" ...
##  $ Last outcome category: chr  NA "Investigation complete; no suspect identified" "Status update unavailable" NA ...
##  $ Context              : chr  NA NA NA NA ...
##  - attr(*, "spec")=List of 2
##   ..$ cols   :List of 12
##   .. ..$ Crime ID             : list()
##   .. .. ..- attr(*, "class")= chr  "collector_character" "collector"
##   .. ..$ Month                : list()
##   .. .. ..- attr(*, "class")= chr  "collector_character" "collector"
##   .. ..$ Reported by          : list()
##   .. .. ..- attr(*, "class")= chr  "collector_character" "collector"
##   .. ..$ Falls within         : list()
##   .. .. ..- attr(*, "class")= chr  "collector_character" "collector"
##   .. ..$ Longitude            : list()
##   .. .. ..- attr(*, "class")= chr  "collector_double" "collector"
##   .. ..$ Latitude             : list()
##   .. .. ..- attr(*, "class")= chr  "collector_double" "collector"
##   .. ..$ Location             : list()
##   .. .. ..- attr(*, "class")= chr  "collector_character" "collector"
##   .. ..$ LSOA code            : list()
##   .. .. ..- attr(*, "class")= chr  "collector_character" "collector"
##   .. ..$ LSOA name            : list()
##   .. .. ..- attr(*, "class")= chr  "collector_character" "collector"
##   .. ..$ Crime type           : list()
##   .. .. ..- attr(*, "class")= chr  "collector_character" "collector"
##   .. ..$ Last outcome category: list()
##   .. .. ..- attr(*, "class")= chr  "collector_character" "collector"
##   .. ..$ Context              : list()
##   .. .. ..- attr(*, "class")= chr  "collector_character" "collector"
##   ..$ default: list()
##   .. ..- attr(*, "class")= chr  "collector_guess" "collector"
##   ..- attr(*, "class")= chr "col_spec"
```

As we can see, we have 174085 recorded crimes with their approximate location (inc. Lat, Long, Street, Lower Layer Super Output Area). We also have the crime type and the last outcome category of the particular crime. 

### Data Cleaning

On initial inspection, it seems that cases/incidents that do not have a Crime ID also do not have a Last Outcome Category. Lets check this:


```r
null_test <- df %>% dplyr::select('Crime ID', 'Last outcome category')
# Check number of rows where Crime ID is NULL and Last outcome category is not NULL
test_1 <- null_test %>% dplyr::filter(is.na('Crime ID') & !is.na('Last outcome category')) %>%
  nrow()
# Check number of rows where Crime ID is not NULL and Last outcome category is NULL
test_2 <- null_test %>% dplyr::filter(!is.na('Crime ID') & is.na('Last outcome category')) %>%
  nrow()
print(c(test_1, test_2))
```

```
## [1] 0 0
```
I am only going to keep Last outcome category for this purpose as I don't really require an ID.

Also, all the crimes should have been reported by Wiltshire Police as well as falling under the jurisidiction of Wiltshire Police.. If this is the case, then we can drop it.


```r
colnames(df)
```

```
##  [1] "Crime ID"              "Month"                
##  [3] "Reported by"           "Falls within"         
##  [5] "Longitude"             "Latitude"             
##  [7] "Location"              "LSOA code"            
##  [9] "LSOA name"             "Crime type"           
## [11] "Last outcome category" "Context"
```


```r
df %>% count(`Reported by`)
```

```
## # A tibble: 1 x 2
##   `Reported by`         n
##   <chr>             <int>
## 1 Wiltshire Police 174085
```

On a related note, I am only interested in the data for Wiltshire (inc. Swindon). Therefore I want to remove any LSOAs that aren't Wiltshire or Swindon. Let's see if any exist.


```r
df %>% filter(!grepl('Swindon', `LSOA name`) & !grepl('Wiltshire', `LSOA name`)) %>%
  count()
```

```
## # A tibble: 1 x 1
##       n
##   <int>
## 1   277
```

Now the column Context appears to be entirely full of NAs. Lets check this.


```r
df %>% filter(!is.na(Context)) %>% 
  nrow()
```

```
## [1] 0
```

Now lets look at the most common elements in the Last Outcome Category
It seems that we have a large amount of NAs, but why?


```r
df %>% select(`Last outcome category`) %>%
  table(useNA = 'always') %>%
  sort(decreasing = T) %>%
  head(10)
```

```
## .
## Investigation complete; no suspect identified 
##                                         53778 
##                                          <NA> 
##                                         51603 
##                   Unable to prosecute suspect 
##                                         35047 
##                     Status update unavailable 
##                                          7539 
##                      Court result unavailable 
##                                          6320 
##                           Under investigation 
##                                          6098 
##                      Offender given a caution 
##                                          3119 
##                              Local resolution 
##                                          2048 
##                        Awaiting court outcome 
##                                          1921 
##    Action to be taken by another organisation 
##                                          1315
```

On first glance, it appears to be Anti Social Behaviour cases that all have NAs


```r
df %>% filter(is.na(`Last outcome category`)) %>% 
  select(-c(`LSOA code`, `Crime ID`,  `Reported by`, `Falls within`, Context, Month, Longitude, Latitude)) %>%
  head(10)
```

```
## # A tibble: 10 x 4
##    Location                  `LSOA name`    `Crime type`  `Last outcome c…
##    <chr>                     <chr>          <chr>         <chr>           
##  1 On or near Railway Street Bath and Nort… Anti-social … <NA>            
##  2 On or near Downs View     Swindon 001A   Anti-social … <NA>            
##  3 On or near Queens Avenue  Swindon 001A   Anti-social … <NA>            
##  4 On or near Supermarket    Swindon 001A   Anti-social … <NA>            
##  5 On or near The Elms       Swindon 001B   Anti-social … <NA>            
##  6 On or near Supermarket    Swindon 001B   Anti-social … <NA>            
##  7 On or near Swindon Street Swindon 001B   Anti-social … <NA>            
##  8 On or near Henley Drive   Swindon 001C   Anti-social … <NA>            
##  9 On or near Lechlade Road  Swindon 001C   Anti-social … <NA>            
## 10 On or near Vicarage Lane  Swindon 001E   Anti-social … <NA>
```

To confirm this, let's look deeper:


```r
df %>% filter(is.na(`Last outcome category`)) %>% 
  select(`Crime type`) %>%
  table()
```

```
## .
## Anti-social behaviour 
##                 51603
```

Well this is very worrying that all of the Anti Social Behaviour cases are null for their outcome category.

Unfortunately, there could be a number of outcomes associated with Anti Social Behaviour, I can't just assume that they are all unresolved. So, unfortunately, I will have to exclude anti-social behaviour from my association rules.


```r
df %>% filter(`Crime type` == 'Anti-social behaviour') %>% 
  select(`Last outcome category`) %>%
  table()
```

```
## < table of extent 0 >
```

Lets finish the data cleaning


```r
df_clean <- df %>% select(-c(`Crime ID`, `Reported by`, 
                             `Falls within`, `LSOA code`, Context)) %>%
  filter(grepl('Swindon', `LSOA name`) | grepl('Wiltshire', `LSOA name`)) %>%
  filter(`Crime type` != 'Anti-social behaviour') %>%
  mutate(Location = trimws(str_replace(Location, 'On or near', ""))) %>%
  rowid_to_column("id")
```

### Some Visualisation
Visualisation removed from the markdown, will get to adding a html version


### Feature Engineering and Introducing Spark.
Well, I say Feature Engineering. In order to use association rules in sparklyr, I need to have my data in the right format.

I will need to convert my wide form data into a long format, then collect each element by id into a list.

I am looking for associations between the Location of the crime, the outcome and the crime type.


```r
df_assoc <- df_clean %>% select(id,
                                  `LSOA name`,
                                Location,
                                `Crime type`, 
                                `Last outcome category`) %>%
  melt(id.vars = 'id') %>%
  select(id, value)

head(df_assoc)
```

```
##   id        value
## 1  1 Swindon 001A
## 2  2 Swindon 001A
## 3  3 Swindon 001A
## 4  4 Swindon 001A
## 5  5 Swindon 001A
## 6  6 Swindon 001A
```

Now lets set up our spark instance and copy our data into the environment:


```r
#' Initialise a local instance of Spark (production version would use a cluster)
Sys.setenv(SPARK_HOME = '/usr/local/Cellar/apache-spark/2.2.1/libexec')
sc <- sparklyr::spark_connect(master='local')

#' Copy Data to Environment
df_assoc_tbl <- sparklyr::sdf_copy_to(sc, df_assoc, overwrite = T)
```


In order to create the 'basket' commonly used in Association Rule mining, we will need to collect each element by id into a list.


```r
df_assoc_collect <- df_assoc_tbl %>% 
  group_by(id) %>%
  summarise(
    items = collect_list(value)
  )
```

We will be using the FPGrowth algorithm which is a better version of apriori for large datasets.

There used to be no method for this in sparklyr so one would have to invoke it from Scala. But now there is!


Now lets build our association rule model, specifying a minimum confidence of 0.7 and a minimum support of 0.01


```r
model <- sparklyr::ml_fpgrowth(df_assoc_collect, min_support = 0.01, min_confidence = 0.7)
rules <- sparklyr::ml_association_rules(model)
as.data.frame(rules)
```

```
##      antecedent                                    consequent confidence
## 1      Burglary Investigation complete; no suspect identified  0.8084049
## 2 Vehicle crime Investigation complete; no suspect identified  0.8626025
## 3 Bicycle theft Investigation complete; no suspect identified  0.8328088
```

Well, what are we saying here?
If your Bike, Car or Home Possessions get stolen in Swindon or Wiltshire, give up. The case will be closed with no suspect identified...


```r
sparklyr::spark_disconnect(sc)
```
