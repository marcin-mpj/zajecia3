#install.packages("DBI")
#install.packages("RSQLite")
#install.packages("RPostgres")
#install.packages("tidyverse")

library(DBI)
library(RSQLite)
library(rstudioapi)
library(RPostgres)

#zajecia 3
konta<-read.csv("konta.csv")
View(konta)
lengthOfFile("konta.csv")

lengthOfFile<- function(filepath,systemLinuxUnix=FALSE){
  #if(.Platform$OS.type == "unix" )
  if ( systemLinuxUnix){
    l <- try(system(paste("wc -l",filepath),intern=TRUE))
    l<-strsplit(l,split=" ")
    l<-as.numeric(l[[1]])
    l
  }
  else{
    l<-length(count.fields(filepath))
    l
  }
}

srednia<- function(filepath,columnname,size,sep=",",header=TRUE){
  fileConnection <- file(description = filepath, open = "r")
  suma<-0
  counter<-0
  data<-read.table(fileConnection, nrows = size, header = header, fill = TRUE, sep=sep)
  columnsNames<-names(data)
  repeat{
    print(suma/counter)
    if(nrow(data)==0){
      close(fileConnection)
      break
    }
    
    data<- na.omit(data)
    counter = counter + nrow(data)
    suma<-suma + sum(data[[columnname]])
    data<-read.table(fileConnection, nrows = size,col.names = columnsNames , fill = TRUE, sep=sep)
    
  }
  suma/counter
}

wynik <- srednia("konta.csv","saldo",10000)
mean(konta[['saldo']])

readToBase<-function(filepath,dbpath,tablename,size,sep=",",header = TRUE, delete = TRUE){
  ap<-!delete
  ov<-delete
  fileConnection <- file(description = filepath, open = "r")
  dbConn<-dbConnect(SQLite(), dbpath)
  data<-read.table(fileConnection, nrows = size, header = header, fill = TRUE, sep=sep)
  columnsNames<-names(data)
  dbWriteTable(conn = dbConn , name = tablename, data, append = ap, overwrite = ov )
  repeat{
    if(nrow(data)==0){
      close(fileConnection)
      dbDisconnect(dbConn)
      break
    }
    data<-read.table(fileConnection, nrows = size,col.names = columnsNames , fill = TRUE, sep=sep)
    dbWriteTable(conn = dbConn, name =tablename, data, append =TRUE, overwrite = FALSE)
  } 
}


dbp<-"konta.sqlite"
con<-dbConnect(SQLite(),dbp)
readToBase("konta.csv",dbp,"tableName", 10000)

dbGetQuery(con,"SELECT count(*) FROM tableName")
lengthOfFile("konta.csv")


connectMe<-function(typ=Postgres(),dbname = "serjqxpr",host = "rogue.db.elephantsql.com",user ="serjqxpr"){
  con<-dbConnect(
    typ,
    dbname = dbname, 
    host = host, 
    user = user,
    password = askForPassword("baza - podaj haslo"))
}



readToBase2<-function(filepath,dbConn,tablename,size,sep=",",header = TRUE, delete = TRUE){
  ap<-!delete
  ov<-delete
  fileConnection <- file(description = filepath, open = "r")
  data<-read.table(fileConnection, nrows = size, header = header, fill = TRUE, sep=sep)
  columnsNames<-names(data)
  dbWriteTable(conn = dbConn , name = tablename, data, append = ap, overwrite = ov )
  repeat{
    if(nrow(data)==0){
      close(fileConnection)
      dbDisconnect(dbConn)
      break
    }
    data<-read.table(fileConnection, nrows = size,col.names = columnsNames , fill = TRUE, sep=sep)
    dbWriteTable(conn = dbConn, name =tablename, data, append =TRUE, overwrite = FALSE)
  } 
}

con<-connectMe()
dbGetInfo(con)
dbListTables(con)
dbDisconnect(con)

pom<-1
readToBase2("pjatk_su.csv",con,"pjatk_su", 10000)
query <- "SELECT * FROM konta where occupation = 'NAUCZYCIEL' order by saldo desc limit 10"
pom <- dbGetQuery(con,query)

library(tidyverse)
suicadesTable<-tbl(con,"pjatk_su")
tableR <- suicadesTable %>% select(everything())%>% collect()

object.size(tableR)
object.size(suicadesTable)
tableR %>% select(everything())

  