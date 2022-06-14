import unittest
import time
import gc
from xbxbxb import xbxbxb
#from os.path import exists
import os
class test(unittest.TestCase):
    def test_oneinsert(self):
        x=xbxbxb()
        tdb={}
        tdb["DB"]={}
        tdb["DB"]["Table1"]={}
        tdb["DB"]["Table1"]["Col1"]="str"
        tdb["DB"]["Table1"]["Col2"]="int"
        tdb["DB"]["Table1"]["Col3"]="float"
        tdb["DB"]["Table1"]["Col4"]="bool"
        tdb["DB"]["Table1"]["Col5"]=["str","key"]
        x.createdatabase(tdb)
        tdata={}
        tdata["insert"]={}
        tdata["insert"]["Table1"]={}
        tdata["insert"]["Table1"]["Col1"]="Test String"
        x.executestruct("DB",tdata)
        assert len(x.data["DB"])==1, "Database Mismatch"
        assert len(x.data["DB"]["Table1"])==1, "Datatable Mismatch"
        h=list(x.data["DB"]["Table1"])[0] # get the 1st hash
        assert x.data["DB"]["Table1"][h]["Col1"]=="Test String", "Datacolumn Mismatch {}".format(x.data["DB"]["Table1"][h])

    def test_multiinsert(self):
        x=xbxbxb()
        tdb={}
        tdb["DB"]={}
        tdb["DB"]["Table1"]={}
        tdb["DB"]["Table1"]["Col1"]="str"
        tdb["DB"]["Table1"]["Col2"]="int"
        tdb["DB"]["Table1"]["Col3"]="float"
        tdb["DB"]["Table1"]["Col4"]="bool"
        tdb["DB"]["Table1"]["Col5"]=["str","key"]
        x.createdatabase(tdb)
        tdata={}
        tdata["insert"]=[]
        tdict={}
        tdict["Table1"]={}
        tdict["Table1"]["Col1"]="Test String1"
        tdict["Table1"]["Col2"]="Test String1"
        tdata["insert"].append(tdict)
        tdict={}
        tdict["Table1"]={}
        tdict["Table1"]["Col1"]="Test String2"
        tdata["insert"].append(tdict)
        x.executestruct("DB",tdata)
        assert len(x.data["DB"])==1, "Database Mismatch"
        assert len(x.data["DB"]["Table1"])==2, "Datatable Mismatch"
        h=list(x.data["DB"]["Table1"])[0] # get the 1st hash
        assert x.data["DB"]["Table1"][h]["Col1"]=="Test String1", "Datacolumn Mismatch {}".format(x.data["DB"]["Table1"][h])
        h=list(x.data["DB"]["Table1"])[1] # get the 2nd hash
        assert x.data["DB"]["Table1"][h]["Col1"]=="Test String2", "Datacolumn Mismatch {}".format(x.data["DB"]["Table1"][h])

    def test_select(self):
        x=xbxbxb()
        tdb={}
        tdb["DB"]={}
        tdb["DB"]["Table1"]={}
        tdb["DB"]["Table1"]["Col1"]="str"
        tdb["DB"]["Table1"]["Col2"]="int"
        tdb["DB"]["Table1"]["Col3"]="float"
        tdb["DB"]["Table1"]["Col4"]="bool"
        tdb["DB"]["Table1"]["Col5"]=["str","key"]
        x.createdatabase(tdb)
        tdata={}
        tdata["insert"]=[]
        tdict={}
        tdict["Table1"]={}
        tdict["Table1"]["Col1"]="Test String1"
        tdict["Table1"]["Col2"]="Test String1"
        tdata["insert"].append(tdict)
        tdict={}
        tdict["Table1"]={}
        tdict["Table1"]["Col1"]="Test String2"
        tdata["insert"].append(tdict)
        x.executestruct("DB",tdata)
        assert len(x.data["DB"])==1, "Database Mismatch"
        assert len(x.data["DB"]["Table1"])==2, "Datatable Mismatch"

        tdata={}
        tdata["select"]={}
        tdata["select"]["columns"]=["*"]
        tdata["select"]["from"]={"Table1": "tab"}
        tdata["select"]["where"]='tab["Col1"]=="Test String3"'
        res=x.executestruct("DB",tdata)
        assert res=={}, "Negative Test Mismatch {}".format(res)
        tdata["select"]={}
        tdata["select"]["columns"]=["*"]
        tdata["select"]["from"]={"Table1": "tab"}
        tdata["select"]["where"]='tab["Col1"]=="Test String1"'
        res=x.executestruct("DB",tdata)
        assert len(res)==1, "Select Results Mismatch {}".format(res)
        tdata["select"]={}
        tdata["select"]["columns"]=["*"]
        tdata["select"]["from"]={"Table1": "tab"}
        tdata["select"]["where"]='tab["Col1"]=="Test String2"'
        res=x.executestruct("DB",tdata)
        assert len(res)==1, "Select Results Mismatch {}".format(res)
        tdata["select"]={}
        tdata["select"]["columns"]=["*"]
        tdata["select"]["from"]={"Table1": "tab"}
        tdata["select"]["where"]='tab["Col3"]==None'
        res=x.executestruct("DB",tdata)
        assert len(res)==2, "Select Results Mismatch {}".format(res)


    def test_update(self):
        x=xbxbxb()
        tdb={}
        tdb["DB"]={}
        tdb["DB"]["Table1"]={}
        tdb["DB"]["Table1"]["Col1"]="str"
        tdb["DB"]["Table1"]["Col2"]="int"
        tdb["DB"]["Table1"]["Col3"]="float"
        tdb["DB"]["Table1"]["Col4"]="bool"
        tdb["DB"]["Table1"]["Col5"]=["str","key"]
        x.createdatabase(tdb)
        tdata={}
        tdata["insert"]=[]
        tdict={}
        tdict["Table1"]={}
        tdict["Table1"]["Col1"]="Test String1"
        tdict["Table1"]["Col2"]="Test String1"
        tdata["insert"].append(tdict)
        tdict={}
        tdict["Table1"]={}
        tdict["Table1"]["Col1"]="Test String2"
        tdata["insert"].append(tdict)
        x.executestruct("DB",tdata)
        assert len(x.data["DB"])==1, "Database Mismatch"
        assert len(x.data["DB"]["Table1"])==2, "Datatable Mismatch"

        tdata={}
        tdata["update"]={}
        tdata["update"]["columns"]=["Col1"]
        tdata["update"]["values"]=["Test String3"]
        tdata["update"]["from"]={"Table1": "tab"}
        tdata["update"]["where"]='tab["Col1"]=="Test String1"'
        res=x.executestruct("DB",tdata)
        assert len(res)==1, "Update Results Mismatch {}".format(res)

        tdata={}
        tdata["select"]={}
        tdata["select"]["columns"]=["*"]
        tdata["select"]["from"]={"Table1": "tab"}
        tdata["select"]["where"]='tab["Col1"]=="Test String3"'
        res=x.executestruct("DB",tdata)
        assert len(res)==1, "Update Test Mismatch {}".format(res)


    def test_delete(self):
        x=xbxbxb()
        tdb={}
        tdb["DB"]={}
        tdb["DB"]["Table1"]={}
        tdb["DB"]["Table1"]["Col1"]="str"
        tdb["DB"]["Table1"]["Col2"]="int"
        tdb["DB"]["Table1"]["Col3"]="float"
        tdb["DB"]["Table1"]["Col4"]="bool"
        tdb["DB"]["Table1"]["Col5"]=["str","key"]
        x.createdatabase(tdb)
        tdata={}
        tdata["insert"]=[]
        tdict={}
        tdict["Table1"]={}
        tdict["Table1"]["Col1"]="Test String1"
        tdict["Table1"]["Col2"]="Test String1"
        tdata["insert"].append(tdict)
        tdict={}
        tdict["Table1"]={}
        tdict["Table1"]["Col1"]="Test String2"
        tdata["insert"].append(tdict)
        x.executestruct("DB",tdata)
        assert len(x.data["DB"])==1, "Database Mismatch"
        assert len(x.data["DB"]["Table1"])==2, "Datatable Mismatch"

        tdata={}
        tdata["delete"]={}
        tdata["delete"]["from"]={"Table1": "tab"}
        tdata["delete"]["where"]='tab["Col1"]=="Test String1"'
        res=x.executestruct("DB",tdata)
        assert len(res)==1, "Update Results Mismatch {}".format(res)
        assert len(x.data["DB"]["Table1"])==1, "Datatable Mismatch"

        tdata={}
        tdata["select"]={}
        tdata["select"]["columns"]=["*"]
        tdata["select"]["from"]={"Table1": "tab"}
        tdata["select"]["where"]='tab["Col1"]=="Test String1"'
        res=x.executestruct("DB",tdata)
        assert len(res)==0, "Update Test Mismatch {}".format(res)
        tdata={}
        tdata["select"]={}
        tdata["select"]["columns"]=["*"]
        tdata["select"]["from"]={"Table1": "tab"}
        tdata["select"]["where"]='tab["Col1"]=="Test String2"'
        res=x.executestruct("DB",tdata)
        assert len(res)==1, "Update Test Mismatch {}".format(res)


    def test_createtables(self):
        x=xbxbxb()
        tdb={}
        tdb["DB"]={}
        tdb["DB"]["Table1"]={}
        tdb["DB"]["Table1"]["Col1"]="str"
        tdb["DB"]["Table1"]["Col2"]="int"
        tdb["DB"]["Table1"]["Col3"]="float"
        tdb["DB"]["Table1"]["Col4"]="bool"
        tdb["DB"]["Table1"]["Col5"]=["str","key"]
        x.createdatabase(tdb)
        tdata={}
        tdata["Table2"]={}
        tdata["Table2"]["col1"]="str"
        tdata["Table3"]={}
        tdata["Table3"]["col1"]="str"
        x.createtables("DB",tdata)
        assert x.DBschema["DB"]["Table2"]["col1"]=="STR", f"Createtables mismatch Table2 {x.DBschema}"
        assert x.DBschema["DB"]["Table3"]["col1"]=="STR", f"Createtables mismatch Table3 {x.DBschema}"

    def test_scriptcreatedb(self):
        x=xbxbxb()
        s="CREATE DATABASE TESTDB;"
        x.execute("",s)
        assert x.DBschema["TESTDB"]=={},"Assert blank database exists in schema"

    def test_scriptcreatetable(self):
        x=xbxbxb()
        s="CREATE DATABASE TESTDB;"
        x.execute("",s)
        assert x.DBschema["TESTDB"]=={},"Assert blank database exists in schema"
        s="""CREATE TABLE Table1 (col1 str, col2 str,col3 str,
                    col4 str,
                    col5 int);
        """
        x.execute("TESTDB",s)
        assert x.DBschema["TESTDB"]["Table1"]["col1"]=="STR", f"Createtables script mismatch Table1 {x.DBschema}"

    def test_scriptInsertintotable1(self):
        x=xbxbxb()
        s="CREATE DATABASE TESTDB;"
        x.execute("",s)
        assert x.DBschema["TESTDB"]=={},"Assert blank database exists in schema"
        s="""CREATE TABLE Table1 (col1 str, col2 int,col3 float,
                    col4 bool,
                    col5 int);
        """
        x.execute("TESTDB",s)
        assert x.DBschema["TESTDB"]["Table1"]["col1"]=="STR", f"Createtables script mismatch Table1 {x.DBschema}"
        s="""INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String', 56, 12.8765, True);"""
        x.execute("TESTDB",s)
        assert len(x.data["TESTDB"])==1, "Database Mismatch"
        assert len(x.data["TESTDB"]["Table1"])==1, "Datatable Mismatch"
        h=list(x.data["TESTDB"]["Table1"])[0] # get the 1st hash
        assert x.data["TESTDB"]["Table1"][h]["col1"]=="Test String", "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col2"]==56, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col3"]==12.8765, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col4"]==True, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col5"]==None, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])

    def test_scriptInsertintotable1multi(self):
        x=xbxbxb()
        s="CREATE DATABASE TESTDB;"
        x.execute("",s)
        assert x.DBschema["TESTDB"]=={},"Assert blank database exists in schema"
        s="""CREATE TABLE Table1 (col1 str, col2 int,col3 float,
                    col4 bool,
                    col5 int);
        """
        x.execute("TESTDB",s)
        assert x.DBschema["TESTDB"]["Table1"]["col1"]=="STR", f"Createtables script mismatch Table1 {x.DBschema}"
        s="""INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String', 56, 12.8765, True);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String1', 24, 24.12345, False);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String2', 12, 56.3456, True);"""
        x.execute("TESTDB",s)
        assert len(x.data["TESTDB"])==1, "Database Mismatch"
        assert len(x.data["TESTDB"]["Table1"])==3, "Datatable Mismatch"
        h=list(x.data["TESTDB"]["Table1"])[0] # get the 1st hash
        assert x.data["TESTDB"]["Table1"][h]["col1"]=="Test String", "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col2"]==56, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col3"]==12.8765, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col4"]==True, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col5"]==None, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        h=list(x.data["TESTDB"]["Table1"])[1] # get the 2nd hash
        assert x.data["TESTDB"]["Table1"][h]["col1"]=="Test String1", "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col2"]==24, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col3"]==24.12345, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col4"]==False, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col5"]==None, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        h=list(x.data["TESTDB"]["Table1"])[2] # get the 3rd hash
        assert x.data["TESTDB"]["Table1"][h]["col1"]=="Test String2", "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col2"]==12, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col3"]==56.3456, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col4"]==True, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert x.data["TESTDB"]["Table1"][h]["col5"]==None, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])

    def test_scriptselect(self):
        x=xbxbxb()
        s="CREATE DATABASE TESTDB;"
        x.execute("",s)
        assert x.DBschema["TESTDB"]=={},"Assert blank database exists in schema"
        s="""CREATE TABLE Table1 (col1 str, col2 int,col3 float,
                    col4 bool,
                    col5 int);
        """
        x.execute("TESTDB",s)
        assert x.DBschema["TESTDB"]["Table1"]["col1"]=="STR", f"Createtables script mismatch Table1 {x.DBschema}"
        s="""INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String', 56, 12.8765, True);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String1', 24, 24.12345, False);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String2', 12, 56.3456, True);"""
        x.execute("TESTDB",s)
        assert len(x.data["TESTDB"])==1, "Database Mismatch"
        assert len(x.data["TESTDB"]["Table1"])==3, "Datatable Mismatch"
        s="""SELECT col1, col2,col3, col4 from Table1 as t1 where t1['col4']==True;"""
        res=x.execute("TESTDB",s)
        h=list(res)[0] # get the 1st hash
        assert res[h]["col1"]=="Test String", "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col2"]==56, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col3"]==12.8765, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col4"]==True, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert 'col5' not in res[h], "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        h=list(res)[1] # get the 2nd hash
        assert res[h]["col1"]=="Test String2", "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col2"]==12, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col3"]==56.3456, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col4"]==True, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert 'col5' not in res[h], "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        s="""SELECT col1, col2,col3, col4 from Table1 as t1 where t1['col4']==False;"""
        res=x.execute("TESTDB",s)
        h=list(res)[0] # get the 1st hash
        assert res[h]["col1"]=="Test String1", "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col2"]==24, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col3"]==24.12345, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col4"]==False, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert 'col5' not in res[h], "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])

    def test_scriptupdate(self):
        x=xbxbxb()
        s="CREATE DATABASE TESTDB;"
        x.execute("",s)
        assert x.DBschema["TESTDB"]=={},"Assert blank database exists in schema"
        s="""CREATE TABLE Table1 (col1 str, col2 int,col3 float,
                    col4 bool,
                    col5 int);
        """
        x.execute("TESTDB",s)
        assert x.DBschema["TESTDB"]["Table1"]["col1"]=="STR", f"Createtables script mismatch Table1 {x.DBschema}"
        s="""INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String', 56, 12.8765, True);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String1', 24, 24.12345, False);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String2', 12, 56.3456, True);"""
        x.execute("TESTDB",s)
        assert len(x.data["TESTDB"])==1, "Database Mismatch"
        assert len(x.data["TESTDB"]["Table1"])==3, "Datatable Mismatch"
        s="""UPDATE Table1 as T1 set col1='Updated Test String' where T1['col2']==56;"""
        res=x.execute("TESTDB",s)
        s="SELECT * FROM Table1 as T1 WHERE T1['col2']==56;"
        res=x.execute("TESTDB",s)
        h=list(res)[0] # get the 1st hash
        assert res[h]["col1"]=="Updated Test String", "Datacolumn Mismatch "+x.data["TESTDB"]["Table1"][h]

    def test_scriptdelete(self):
        x=xbxbxb()
        s="CREATE DATABASE TESTDB;"
        x.execute("",s)
        assert x.DBschema["TESTDB"]=={},"Assert blank database exists in schema"
        s="""CREATE TABLE Table1 (col1 str, col2 int,col3 float,
                    col4 bool,
                    col5 int);
        """
        x.execute("TESTDB",s)
        assert x.DBschema["TESTDB"]["Table1"]["col1"]=="STR", f"Createtables script mismatch Table1 {x.DBschema}"
        s="""INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String', 56, 12.8765, True);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String1', 24, 24.12345, False);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String2', 12, 56.3456, True);"""
        x.execute("TESTDB",s)
        assert len(x.data["TESTDB"])==1, "Database Mismatch"
        assert len(x.data["TESTDB"]["Table1"])==3, "Datatable Mismatch"

        s="SELECT * FROM Table1 as T1 WHERE T1['col2']==56;"
        res=x.execute("TESTDB",s)
        assert len(res)==1, "Datatable Mismatch"

        s="""DELETE FROM Table1 as T1 where T1['col2']==56;"""
        res=x.execute("TESTDB",s)

        s="SELECT * FROM Table1 as T1 WHERE T1['col2']==56;"
        res=x.execute("TESTDB",s)
        assert len(res)==0, "Datatable Mismatch"

    def test_verifywritedata(self):
        x=xbxbxb(filename="testdb.txt")
        s="CREATE DATABASE TESTDB;"
        x.execute("",s)
        assert x.DBschema["TESTDB"]=={},"Assert blank database exists in schema"
        s="""CREATE TABLE Table1 (col1 str, col2 int,col3 float,
                    col4 bool,
                    col5 int);
        """
        x.execute("TESTDB",s)
        assert x.DBschema["TESTDB"]["Table1"]["col1"]=="STR", f"Createtables script mismatch Table1 {x.DBschema}"
        s="""INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String', 56, 12.8765, True);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String1', 24, 24.12345, False);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String2', 12, 56.3456, True);"""
        x.execute("TESTDB",s)
        x=None
        gc.collect()

        assert os.path.exists("testdb.txt"), "Error database not saved"
        os.remove("testdb.txt")

    def test_verifyreaddata(self):
        x=xbxbxb(filename="testdb1.txt")
        s="CREATE DATABASE TESTDB;"
        x.execute("",s)
        assert x.DBschema["TESTDB"]=={},"Assert blank database exists in schema"
        s="""CREATE TABLE Table1 (col1 str, col2 int,col3 float,
                    col4 bool,
                    col5 int);
        """
        x.execute("TESTDB",s)
        assert x.DBschema["TESTDB"]["Table1"]["col1"]=="STR", f"Createtables script mismatch Table1 {x.DBschema}"
        s="""INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String', 56, 12.8765, True);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String1', 24, 24.12345, False);
                INSERT INTO Table1 (col1, col2, col3, col4) VALUES ('Test String2', 12, 56.3456, True);"""
        x.execute("TESTDB",s)
        x=None
        gc.collect()

        assert os.path.exists("testdb1.txt"), "Error database not saved"

        x=xbxbxb(filename="testdb1.txt")

        s="""SELECT col1, col2,col3, col4 from Table1 as t1 where t1['col4']==True;"""
        res=x.execute("TESTDB",s)
        h=list(res)[0] # get the 1st hash
        assert res[h]["col1"]=="Test String", "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col2"]==56, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col3"]==12.8765, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col4"]==True, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert 'col5' not in res[h], "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        h=list(res)[1] # get the 2nd hash
        assert res[h]["col1"]=="Test String2", "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col2"]==12, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col3"]==56.3456, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col4"]==True, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert 'col5' not in res[h], "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        s="""SELECT col1, col2,col3, col4 from Table1 as t1 where t1['col4']==False;"""
        res=x.execute("TESTDB",s)
        h=list(res)[0] # get the 1st hash
        assert res[h]["col1"]=="Test String1", "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col2"]==24, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col3"]==24.12345, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert res[h]["col4"]==False, "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])
        assert 'col5' not in res[h], "Datacolumn Mismatch {}".format(x.data["TESTDB"]["Table1"][h])

        x=None
        gc.collect()

        os.remove("testdb1.txt")
