class StatementBuilder:

    def BuildInsertStatement(self,Tablevalues,ColumnsList = []):
        statement = ""
        if len(ColumnsList) != 0:
            statement += "("+ ','.join(str(v) for v in ColumnsList)+")"
        statement += " values ("
        statement += ','.join(str(v) for v in Tablevalues)
        statement +=")"
        return statement

    def BuildUpdateStatement(self,ColumnsList,Tablevalues):
        statement= ""
        i=0
        while i < len(ColumnsList):
            if (i) >= (len(ColumnsList)-1):
                statement+= ColumnsList[i] + "="+ self.checkEqualType(Tablevalues[i])+""
            else:
                statement+= ColumnsList[i] + "="+ self.checkEqualType(Tablevalues[i])+","
            i += 1
        return statement
    
    def BuildWhere(self,whereColumn,whereValue,WhereOperator=""):
        statement = " where " +whereColumn+ " "
        if WhereOperator !="":
            statement += WhereOperator+ " " + self.checkEqualType(whereValue)  
        else:
            statement += "= " + self.checkEqualType(whereValue)
        return statement

    def checkEqualType(self, where):
        if isinstance(where,(str,bool)):
            return "'"+ where+"'"
        elif isinstance(where,(int,float)):
          return str(where)
