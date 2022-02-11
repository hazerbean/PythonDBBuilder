import unittest
import os, sys
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from classes.DBBuilder import DBBuilder
from classes.StatementBuilder import StatementBuilder

class TestMSSql(unittest.TestCase):
    snowflake = DBBuilder()
        
    def testInsert(self):
        insertStatement = self.snowflake.insertStatement("TESTBEGIN",[1,"'TEST'"],'MSSQLLocal')
        self.assertTrue(insertStatement != None)
        self.assertEqual(insertStatement,"worked")
        
    def testInsertCol(self):
        insertStatement = self.snowflake.insertStatement("TESTBEGIN",[1,"'TEST'"],['ID','NAME'],'MSSQLLocal')
        self.assertTrue(insertStatement != None)
        self.assertEqual(insertStatement,"worked")

    def testSelectAll(self):
        selectStatement = self.snowflake.SelectStatement("TESTBEGIN",'MSSQLLocal')
        self.assertTrue(selectStatement != None)

    def testSelectSome(self):
        selectStatement = self.snowflake.SelectStatement("TESTBEGIN",['ID','NAME'],'ID','1','LIKE','MSSQLLocal')
        self.assertTrue(selectStatement != None)

    def testUpdate(self):
        updateStatement = self.snowflake.updateStatement("TESTBEGIN",['ID','NAME'],[2,'test'],'ID','1','MSSQLLocal')
        self.assertTrue(updateStatement != None)
        self.assertEqual(updateStatement,"worked")

    def testDelete(self):
        deleteStatement self.snowflake.deleteStatement("TESTBEGIN",'ID','1','LIKE','MSSQLLocal')
        self.assertTrue(deleteStatement != None)
        self.assertEqual(deleteStatement,"worked")

if __name__ == '__main__':
    unittest.main()
    

