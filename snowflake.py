#!/usr/bin/env python
from classes.DBBuilder import DBBuilder


# Gets the version
snowflake = DBBuilder()
deleteStatement= ""
try:
   insertStatement = snowflake.insertProcedure("TESTBEGIN",["TEST2"],["NAME"])
   #selectStatement = snowflake.SelectStatement("TESTBEGIN",['ID','NAME'],'ID','1','LIKE','MSSQLLocal')
    #updateStatement = snowflake.updateStatement("TESTBEGIN",['ID','NAME'],[2,'test'],'ID','1')
    #deleteStatement = snowflake.deleteStatement("TESTBEGIN",'ID','1','LIKE')

    #fetch result
finally:
   # print(deleteStatement)
    #print(updateStatement)
    print(insertStatement)
    #print(selectStatement)
   # print(selectStatement)
