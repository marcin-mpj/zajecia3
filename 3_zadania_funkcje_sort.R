
library(DBI)
library(RSQLite)
library(rstudioapi)
library(RPostgres)
library(dplyr)

#Dane
konta<-read.csv("konta.csv")

#Zadanie 1
rankAccount <- function(dataFrame,colName,groupName,valueSort,num){
    dfFiltered <- filter(dataFrame, dataFrame[colName] == groupName)
    dfSorted <- arrange(dfFiltered, desc(dfFiltered[valueSort]))
    num2 = nrow(dfSorted)
    num <- min(num, num2)
    dfCut<-dfSorted[1:num,]
    return(dfCut)
}

zadanie1<-rankAccount(konta, 'occupation','NAUCZYCIEL','saldo',10)

#Zadanie 2

rankAccountBigDatatoChunk <- function(filepath , size, colName, groupName, valueSort, num, header=TRUE, sep=","){

  fileConnection <- file(description = filepath,open = "r")
  data<-read.table(fileConnection , nrows=size, header = header, fill=TRUE, sep=sep)
  dataAdjust <- rankAccount(data,colName,groupName,valueSort,num)
  columnsNames<-names(data)
  
  repeat{
        if ( nrow(data)==0){
            close(fileConnection)
            break 
        }
    
    data<-read.table(fileConnection,nrows=size,col.names=columnsNames,fill=TRUE,sep=sep)
    
    if ( nrow(data)>0){
      datapom <- bind_rows(dataAdjust,data)
      dataAdjust <- rankAccount(datapom,colName,groupName,valueSort,num)
      #print(dataAdjust)
    }
  }
  return(dataAdjust)
}

zadanie2 <- rankAccountBigDatatoChunk("konta.csv", 1000 ,'occupation','NAUCZYCIEL','saldo',2)

#Zadanie 3 - odpowiedź - da się <-query.

connectMe<-function(typ=Postgres(),dbname = "serjqxpr",host = "rogue.db.elephantsql.com",user ="serjqxpr"){
  con<-dbConnect(
    typ,
    dbname = dbname, 
    host = host, 
    user = user,
    password = askForPassword("baza - podaj haslo"))
}

con<-connectMe()
dbGetInfo(con)
dbListTables(con)

query <- "SELECT * FROM konta where occupation = 'NAUCZYCIEL' order by saldo desc limit 10"
zadanie3 <- dbGetQuery(con,query)
dbDisconnect(con)

