#!/usr/bin/env python
from classes.DBBuilderBlock import DBBuilder


# Gets the version
snowflake = DBBuilder()
deleteStatement= ""
try:
   #insertStatement = snowflake.insertProcedure("TESTBEGIN",["TEST2"],["NAME"])
   selectStatement = snowflake.SelectProcedure("TESTBEGIN",['ID','NAME'],'ID','1','','')
    #updateStatement = snowflake.updateStatement("TESTBEGIN",['ID','NAME'],[2,'test'],'ID','1')
    #deleteStatement = snowflake.deleteStatement("TESTBEGIN",'ID','1','LIKE')

    #fetch result
finally:
    #print(deleteStatement)
    #print(updateStatement)
    #print(insertStatement)
    print(selectStatement)
    #print(selectStatement)
