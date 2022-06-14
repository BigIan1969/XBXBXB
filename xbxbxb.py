from random import randrange
import pyparsing as pp
import hashlib
import json
import re
import os
import zlib

class xbxbxb():

    def __init__(self, **KWARGS):
        self.DBschema={}
        self.data={}
        self.commands={}
        self.commands["CREATE DATABASE"]=re.compile("create database\s+(.*?)?;",flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        self.commands["CREATE TABLE"]=re.compile("create table\s+(.*?)\s*\((.*?)\)\s*?;",flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        self.commands["INSERT"]=re.compile("insert\s+into\s+(.*?)\s+\((.*?)\)\s+values\s+\((.*?)\)?;",flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        self.commands["SELECT"]=re.compile("select\s+(.*?)\s+from\s+(.*?)\s+where\s+(.*?)\s*?;",flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        self.commands["UPDATE"]=re.compile("update\s+(.*?)\s+set\s+(.*?)\s*where\s(.*?)\s*;",flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        self.commands["DELETE"]=re.compile("delete from\s+(.*?)\s*where\s(.*?)\s*;",flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if 'readhook' in KWARGS and 'writehook' in KWARGS:
            self.readhook=KWARGS['readhook']
            self.writehook=KWARGS['writehook']
        elif 'readhook' in KWARGS or 'writehook' in KWARGS:
            self.writehook=None
            self.readhook=None
        else:
            self.writehook=self.compressbytes
            self.readhook=self.decompressbytes
        if 'filename' in KWARGS:
            self.filename=KWARGS['filename']
            self.readdata()
        else:
            self.filename=None
        pass

    def __del__(self):
        self.writedata()
        pass

    def execute(self, _database, _script):
        if not isinstance(_database,str):
            raise SystemExit("Error execute Database only accepts type string")
        if not isinstance(_script,str):
            raise SystemExit("Error execute Script only accepts type string")
        if not _database in self.data and _database:
            self.data[_database]={}
        while _script!="":
            found=False
            for command, reg in self.commands.items():
                matchobj=reg.match(_script)
                if matchobj:
                    try:
                        Print("----")
                        print(0,matchobj.group())
                        print(1,matchobj.group(1))
                        print(2,matchobj.group(2))
                        print(3,matchobj.group(3))
                    except:
                        pass
                    if command=="CREATE DATABASE":
                        td={}
                        td[matchobj.group(1)]={}
                        self.createdatabase(td)
                    elif command=="CREATE TABLE":
                        td={}
                        td[matchobj.group(1)]={}
                        for col in matchobj.group(2).split(","):
                            colname, coltype= col.lstrip().split(" ")
                            td[matchobj.group(1)][colname]=coltype
                        self.createtables(_database,td)
                    elif command=="INSERT":
                        td={}
                        td["insert"]={}
                        targetcols=matchobj.group(2).split(",")
                        values1 = pp.commaSeparatedList.parseString(matchobj.group(3)).asList()
                        values=[]
                        for v in values1:
                            if self.is_float(v):
                                values.append(float(v))
                            elif self.is_int(v):
                                values.append(int(v))
                            elif self.is_bool(v):
                                if v=="True":
                                    values.append(True)
                                elif v=="False":
                                    values.append(False)
                                else:
                                    values.append(bool(v))
                            else:
                                values.append(v.strip("'").strip('"'))
                        #values=re.split(r",(?=')",matchobj.group(3))
                        td["insert"][matchobj.group(1)]=dict(zip(targetcols,values))
                        self.executestruct(_database,td)
                    elif command=="SELECT":
                        td={}
                        td["select"]={}
                        td["select"]["columns"]=matchobj.group(1).split(",")
                        dfrom=matchobj.group(2).split(",")
                        td["select"]["from"]={}
                        for f in dfrom:
                            x=f.split(" ") # this is a cludge I need to come back to and do properly

                            if x==None:
                                td["select"]["from"][f]=f
                            elif len(x)>2:
                                td["select"]["from"][x[0]]=x[2]
                            else:
                                td["select"]["from"][x[0]]=x[0]
                        td["select"]["where"]=matchobj.group(3)
                        return self.executestruct(_database,td)
                    elif command=="UPDATE":
                        td={}
                        td["update"]={}
                        colsvals=matchobj.group(2)
                        tables=matchobj.group(1)
                        td["update"]["where"]=matchobj.group(3)
                        td["update"]["columns"]=[]
                        td["update"]["values"]=[]
                        td["update"]["from"]={}

                        for m in re.finditer("(?:,|^)\s*([^\s=]+)\s*=\s*('[^']*'|\S+)", colsvals,flags=re.IGNORECASE | re.MULTILINE | re.DOTALL):
                            td["update"]["columns"].append(m.group(1))
                            td["update"]["values"].append(m.group(2).strip("'"))

                        for m in re.finditer("(?:,|^)\s*([^\s=]+)\s*as\s*('[^']*'|\S+)", tables,flags=re.IGNORECASE | re.MULTILINE | re.DOTALL):
                            td["update"]["from"][m.group(1)]=m.group(2)
                        return self.executestruct(_database,td)
                    elif command=="DELETE":
                        td={}
                        td["delete"]={}
                        tables=matchobj.group(1)
                        td["delete"]["where"]=matchobj.group(2)
                        td["delete"]["from"]={}
                        for m in re.finditer("(?:,|^)\s*([^\s=]+)\s*as\s*('[^']*'|\S+)", tables,flags=re.IGNORECASE | re.MULTILINE | re.DOTALL):
                            td["delete"]["from"][m.group(1)]=m.group(2)
                        return self.executestruct(_database,td)
                    else:
                        assert False, f"Unrecognised command: {_script}"
                    _script=_script[matchobj.end():]
                    _script=_script.strip()

                    found=True
                    break
            if not found:
                break

    def writedata(self):
        if self.filename!=None:
            wrapper={}
            wrapper["SCHEME"]=self.DBschema
            wrapper["DATA"]=self.data
            if self.writehook!=None:
                print(wrapper)
                wrapper=self.writehook(bytes(str(json.dumps(wrapper)).encode()))
                print(wrapper)
                wrapper=bytes(wrapper)
            else:
                wrapper=json.dumps(wrapper)
            with open(self.filename, 'wb') as out:
                out.write(wrapper)

    def readdata(self):
        if self.filename:
            if os.path.exists(self.filename):
                with open(self.filename, 'rb') as json_file:
                    if self.readhook!=None:
                        d=json_file.read()
                        d=self.readhook(d)
                        wrapper=json.loads(d)
                        self.DBschema=wrapper["SCHEME"]
                        self.data=wrapper["DATA"]
                    else:
                        wrapper=json.load(json_file)
                        self.DBschema=wrapper["SCHEME"]
                        self.data=wrapper["DATA"]

    def compressbytes(_dummy, _bytes):
        return zlib.compress(_bytes, level=6)

    def decompressbytes(_dummy, _bytes):
        return zlib.decompress(_bytes)

    def createdatabase(self, _database):
        if not isinstance(_database,dict):
            raise SystemExit("Error createdatabase only accepts type dict")
        for k,v in _database.items():
            self.DBschema[k]=self.parsetables(v)

    def createtables(self, _database, _tables):
        if not isinstance(_database,str):
            raise SystemExit("Error createtables database only accepts type string")
        if not isinstance(_tables,dict):
            raise SystemExit("Error createtables tables only accepts type dict")
        for k, v in self.parsetables(_tables).items():
            if not k in self.DBschema[_database]:
                self.DBschema[_database][k]=v
            else:
                raise SystemExit(f"Error createtables cannor create '{k}' as it already exists in database {_database}")
    def parsetables(self, _tables):
        tables={}
        if not isinstance(_tables,dict):
            raise SystemExit("Error parsetables only accepts type dict")
        for k,v in _tables.items():
            tables[k]=self.parsetable(v)
        return(tables)

    def parsetable(self, _table):
        columns={}
        columns["__XBXBXB"]="str"
        columns["__KEYS"]=["__XBXBXB"]

        if not isinstance(_table,dict):
            raise SystemExit("Error: parsetable only accepts type dict")
        for k,v in _table.items():
            columns[k]=self.parsecolumn(k, v, columns["__KEYS"])
        return(columns)

    def parsecolumn(self, _columnname, _column, _keys):
        column=""
        if not isinstance(_column,list) and not isinstance(_column, str):
            raise SystemExit("Error: parsecolumn only accepts type list or string\n"+_column)
        if isinstance(_column, str):
            if _column.upper() in ["STR","INT","FLOAT","BOOL"]:
              return(_column.upper())
        else:
            for v in _column:
                if v.upper() in ["STR","INT","FLOAT","BOOL"]:
                    column=v.upper()
                else:
                    if v.upper() in ["KEY"]:
                        _keys.append(_columnname.upper())

        if not column:
            raise SystemExit("Error Column must have a data type")
        return(column)
    def executestruct(self, _database, _commandstructure):
        if not isinstance(_commandstructure,dict):
            raise SystemExit("Error execute only accepts type dict")
        if not _database in self.data:
            self.data[_database]={}
        for k,v in _commandstructure.items():
            if k.upper()=="INSERT":
                self.insert(_database, v)
            elif k.upper()=="SELECT":
                return(self.select(_database, v))
            elif k.upper()=="UPDATE":
                return(self.update(_database, v))
            elif k.upper()=="DELETE":
                return(self.delete(_database, v))

    def insert(self, _database, _table):

        if not isinstance(_table,list) and not isinstance(_table, dict):
            raise SystemExit("Error: insert only accepts type list or dict\n"+_table)
        if isinstance(_table, dict):
            self.insertintotable(_database, _table)
        else:
            for v in _table:
                if not isinstance(v,dict):
                    raise SystemExit("Error: insert only accepts a dict or a list of dicts"+_table)
                self.insertintotable(_database, v)

    def insertintotable(self, _database, _values):
        if not isinstance(_values,dict):
            raise SystemExit("Error insertintotable only accepts type dict")
        for table, columns in _values.items():
            if not table in self.data[_database]:
                self.data[_database][table]={}
            columns["__XBXBXBTEMP"]=randrange(999999999)
            hashval=hashlib.sha512(str(columns).encode()).hexdigest()[:16]
            self.data[_database][table][hashval]={}
            del columns["__XBXBXBTEMP"]
            for col, coltype in self.DBschema[_database][table].items():
                if col!="__KEYS":
                    self.data[_database][table][hashval][col]=None
            for column, value in columns.items():
                if not column.strip() in self.DBschema[_database][table]:
                    raise SystemExit(f"Error: column {column} not in table {table} of database {_database}")
                self.data[_database][table][hashval][column.strip()]=value
            self.data[_database][table][hashval]["__XBXBXB"]=hashval

    def select(self, _database, _criteria):

        if not isinstance(_criteria,dict):
            raise SystemExit("Error: select only accepts type dict\n"+_criteria)
        alias = list(_criteria["from"].values())[0]
        table =list(_criteria["from"].keys())[0]

        #td={}
        #td[alias] = self.data[_database][table]
        tdict= self.data[_database][table]

        #locals().update(td)

        processstr=f"[h for h, {alias} in tdict.items() if {_criteria['where']}]"
        hashes = eval(processstr)
        output={}
        for h in hashes:
            output[h]={}
            if _criteria["columns"]==["*"]:
                #all columns
                for k,v in tdict[h].items():
                    output[h][k]=v
            else:
                for k in _criteria["columns"]:
                    k=k.strip()
                    output[h][k]=tdict[h][k]
        return(output)

    def update(self, _database, _criteria):

        if not isinstance(_criteria,dict):
            raise SystemExit("Error: select only accepts type dict\n"+_criteria)
        alias = list(_criteria["from"].values())[0]
        table =list(_criteria["from"].keys())[0]

        #td={}
        #td[alias] = self.data[_database][table]
        tdict= self.data[_database][table]

        #locals().update(td)

        processstr=f"[h for h, {alias} in tdict.items() if {_criteria['where']}]"
        hashes = eval(processstr)
        output=[]
        for h in hashes:
            output.append(h)
            iter=0
            for k in _criteria["columns"]:
                tdict[h][k]=_criteria["values"][iter]
                iter+=1
        return(output)

    def delete(self, _database, _criteria):

        if not isinstance(_criteria,dict):
            raise SystemExit("Error: select only accepts type dict\n"+_criteria)
        alias = list(_criteria["from"].values())[0]
        table =list(_criteria["from"].keys())[0]

        #td={}
        #td[alias] = self.data[_database][table]
        tdict= self.data[_database][table]

        #locals().update(td)

        processstr=f"[h for h, {alias} in tdict.items() if {_criteria['where']}]"
        hashes = eval(processstr)
        output=[]
        for h in hashes:
            output.append(h)
            iter=0
            del(tdict[h])

        return(output)

    def is_float(self, element):
        if not "." in element:
            return False
        try:
            float(element)
            return True
        except ValueError:
            return False
    def is_int(self, element):
        if "." in element:
            return False
        try:
            int(element)
            return True
        except ValueError:
            return False

    def is_bool(self, element):
        if element not in ["True","False"]:
            return False
        try:
            bool(element)
            return True
        except ValueError:
            return False
