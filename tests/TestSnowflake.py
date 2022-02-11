import unittest
import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from classes.DBBuilder import DBBuilder
from classes.StatementBuilder import StatementBuilder

class testSnowflake(unittest.TestCase):
    snowflake = DBBuilder()
        
    def testInsert(self):
        insertStatementOnly = self.snowflake.insertStatement("TESTBEGIN",["'TEST'"])
        self.assertTrue(insertStatementOnly != None)
        self.assertEqual(insertStatementOnly,"worked")
        
    def testInsertCol(self):
        insertStatementcol = self.snowflake.insertStatement("TESTBEGIN",["'TEST'"],['NAME'])
        self.assertTrue(insertStatementcol != None)
        self.assertEqual(insertStatementcol,"worked")

    def testSelectAll(self):
        selectStatement = self.snowflake.SelectStatement("TESTBEGIN")
        self.assertTrue(selectStatement != None)

    def testSelectSome(self):
        selectStatement = self.snowflake.SelectStatement("TESTBEGIN",['ID','NAME'],'ID','1','LIKE')
        self.assertTrue(selectStatement != None)

    def testUpdate(self):
        updateStatement = self.snowflake.updateStatement("TESTBEGIN",['NAME'],['testy'],'ID','1')
        self.assertTrue(updateStatement != None)
        self.assertEqual(updateStatement,"worked")

    def testDelete(self):
        deleteStatement = self.snowflake.deleteStatement("TESTBEGIN",'ID','1','LIKE')
        self.assertTrue(deleteStatement != None)
        self.assertEqual(deleteStatement,"worked")

if __name__ == '__main__':
    unittest.main()
    
