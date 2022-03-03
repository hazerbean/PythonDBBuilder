#!/usr/bin/env python
from classes.DBBuilderBlock import DBBuilder


# Gets the version
snowflake = DBBuilder()

try:
   #insertStatement = snowflake.insertProcedure("TESTBEGIN",["TEST2"],["NAME"])
   #selectStatement = snowflake.SelectProcedure("TESTBEGIN",['ID','NAME'],'ID','1','',''
   updateStatement = snowflake.updateProcedure("TESTBEGIN",['ID','NAME'],[2,'test'],'ID',1)
    #deleteStatement = snowflake.deleteStatement("TESTBEGIN",'ID','1','LIKE')
    #fetch result
finally:
    #print(deleteStatement)
   #print(insertStatement)
   print(updateStatement)
   # print(selectStatement)
    #print(selectStatement)
