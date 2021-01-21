import partitioning_paper as Partioning
import rangequery_paper as RangeQuery
import datetime


host = ["postgres1.c1lmbamulyk4.us-east-2.rds.amazonaws.com", "postgres2.c1lmbamulyk4.us-east-2.rds.amazonaws.com",
            "postgres3.c1lmbamulyk4.us-east-2.rds.amazonaws.com", "postgres4.c1lmbamulyk4.us-east-2.rds.amazonaws.com",
            "postgres5.c1lmbamulyk4.us-east-2.rds.amazonaws.com"]
dbname = ["dds1", "dds2","dds3","dds4","dds5"]

print("Creating Databases in all the servers")
Partioning.createDB()

print("Getting connection from all the servers")
con = Partioning.getOpenConnection()
conAWS = []
for i in range(5):
    conAWS.append(Partioning.getOpenConnectionAWS(host[i], dbname[i]))

print("Creating and Loading the ratings table")
Partioning.loadRatings('ratings','data/ij.dat',con)

st = datetime.datetime.now()
print("Doing Range Partitioning")
Partioning.rangePartition('ratings',5,con, conAWS)
en = datetime.datetime.now()
print("Time taken for range partitioning ", en-st)

st = datetime.datetime.now()
#print("Performing Fast Range Query")
RangeQuery.FastRangeQuery('aa','az',10,con, conAWS)
en = datetime.datetime.now()

print(en-st)

st = datetime.datetime.now()
print("Performing Range Query")
RangeQuery.RangeQuery('aa','az',10,con, conAWS)
en = datetime.datetime.now()

print(en-st)

print("Deleting all Tables")
#Partioning.deleteTables('all',con, conAWS)