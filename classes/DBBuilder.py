import snowflake.connector
import pyodbc 
from classes.StatementBuilder import StatementBuilder
import pandas as pd


class DBBuilder:
    userN='Haz'
    passwordN='Erichobbs8'
    accountN='bk63659.west-europe.azure'
    warehouseN='COMPUTE_WH'
    databaseN='DEMO'
    schemaN='public'
    localSqlDB = '.\SQLEXPRESS'
    StatementBuilderIntial = StatementBuilder()
    
    def InitialiseConnection(self, engineUsed="snowflake"):
        engine = None
        if engineUsed == "snowflake":
            engine = snowflake.connector.connect(
                user=self.userN,
                password=self.passwordN,
                account=self.accountN,
                warehouse=self.warehouseN,
                database=self.databaseN,
                schema=self.schemaN)
            
        elif engineUsed=="MSSQLLocal":
            engine = pyodbc.connect('Driver={SQL Server};'
                                    'Server=['+self.localSqlDB+'];'
                                    'Database=['+self.databaseN+'];'
                                    'Trusted_Connection=yes;')
        else:
            engine = snowflake.connector.connect(
                user=self.userN,
                password=self.passwordN,
                account=self.accountN,
                warehouse=self.warehouseN,
                database=self.databaseN,
                schema=self.schemaN)
        return engine

    def insertStatement(self,Table,Tablevalues,Columns =[],engineUsed=""):
        cs = self.InitialiseConnection(engineUsed)
        try:
            statement = "Insert INTO "+Table+" "
            statement += self.StatementBuilderIntial.BuildInsertStatement(Tablevalues,Columns)
            cs.cursor().execute_async(statement)
            cs.cursor().close()
            cs.close()
    
            return "worked"
        except Exception as e:
            cs.close()
            return "failed" + str(e)
    def insertProcedure(self,Table,Tablevalues,Columns= [],engineUsed=""):
        cs = self.InitialiseConnection(engineUsed)
        try:
            StringInsertValue = self.StatementBuilderIntial.constructProcedureValues(Tablevalues)
            if len(ColumnsList) != 0:
                StringInsertColumns = self.StatementBuilderIntial.constructProcedureColumns(Columns)
                Statement= "call INSERTTABLECOLUMNS('"+Table+"',"+ StringInsertColumns +","+ StringInsertValue +")"
            else:
                Statement= "call INSERTTABLEVALUES('"+Table+"',"+ StringInsertValue +")"            
            cs.cursor().execute(Statement);
            cs.cursor().close()
            cs.close()
    
            return "worked"
        except Exception as e:
            cs.close()
            return "failed" + str(e)
        
    def SelectStatement(self,Table,ColumnsList =[],whereColumn="",whereValue="",WhereOperator="",engineUsed=""):
        cs = self.InitialiseConnection(engineUsed)
        statement=" "
        try:
            if len(ColumnsList) != 0:
                statement += "select "+ ', '.join(ColumnsList) + " from " + Table
            else:
                statement += "select * from " + Table
            if whereColumn != "" and whereValue!="":
               statement += self.StatementBuilderIntial.BuildWhere(whereColumn,whereValue,WhereOperator)
            DataTable = cs.cursor().execute_async(statement)
            if engineUsed=="MSSQLLocal":
                ReturnedResult =  pd.DataFrame(DataTable)
            else:
                ReturnedResult = DataTable.fetchall()
            DataTable.close()
            cs.close()
            self.InitialiseConnection.close()
            return ReturnedResult
        except Exception as e:
            cs.close()
            return "failed " + str(e)
        
     def SelectProcedure(self,Table,ColumnsList =[],whereColumn="",whereValue="",WhereOperator="",engineUsed=""):
        cs = self.InitialiseConnection(engineUsed)
        statement=" "
        
        StringWhereValue = ""
        StringWhereColumn = ""
        StringWhereOperator = ""
        try:
            if whereColumn != "" and whereValue!="":
                StringWhereValue = whereValue
                StringWhereColumn = whereColumn
                if WhereOperator !="":
                    StringWhereOperator = WhereOperator
                else:
                    StringWhereOperator = "="
            if len(ColumnsList) != 0:
                StringSelectColumns = self.StatementBuilderIntial.constructProcedureColumns(ColumnsList)
                Statement= "call SELECTTABLECOLUMNS('"+Table+"',"+ StringSelectColumns +",'"+StringWhereColumn+"',"+ StringWhereValue +",'"+WhereOperator+"')"
            else:
                Statement= "call SELECTTABLECOLUMNS('"+Table+"','','"+StringWhereColumn+"',"+ StringWhereValue +",'"+WhereOperator+"')"

            DataTable = cs.cursor().execute(Statement);
            cs.cursor().close()
            cs.close()

            if engineUsed=="MSSQLLocal":
                ReturnedResult =  pd.DataFrame(DataTable)
            else:
                ReturnedResult = DataTable.fetchall()
            DataTable.close()
            cs.close()
            self.InitialiseConnection.close()
            return ReturnedResult
        except Exception as e:
            cs.close()
            return "failed " + str(e)

    def updateStatement(self,Table,updateColumnsList,updateValues,whereColumn,whereValue,WhereOperator="",engineUsed="snowflake"):
        cs = self.InitialiseConnection(engineUsed)
        try:
            statement="update " + Table +" SET "
            statement += self.StatementBuilderIntial.BuildUpdateStatement(updateColumnsList,updateValues)
            statement += self.StatementBuilderIntial.BuildWhere(whereColumn,whereValue,WhereOperator)
            cs.cursor().execute(statement)
            cs.cursor().close()
            cs.close()
            return "worked"
        except Exception as e:
            cs.close()
            return "failed " + str(e)

    def deleteStatement(self,Table,whereColumn="",whereValue="",WhereOperator="",engineUsed="snowflake"):
        cs = self.InitialiseConnection(engineUsed)
        try:
            statement="DELETE FROM " + Table +" "
            statement += self.StatementBuilderIntial.BuildWhere(whereColumn,whereValue,WhereOperator)
            cs.cursor().execute(statement)
            cs.cursor().close()
            cs.close()
            return "worked"
        except Exception as e:
            cs.close()
            return "failed " + str(e)
   
