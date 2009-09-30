using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Virtual.Attributes;
using Org.Reddragonit.Dbpro.Connections;
using System.Reflection;
using Org.Reddragonit.Dbpro.Structure.Mapping;

namespace Org.Reddragonit.Dbpro.Virtual
{
    public class VirtualTableConnection
    {
        private struct ExtractedVirtualField
        {
            private Type _referencingTable;
            public Type ReferencingTable
            {
                get { return _referencingTable; }
            }

            private string _fieldName;
            public string FieldName
            {
                get { return _fieldName; }
            }

            private string _classFieldName;
            public string ClassFieldName
            {
                get { return _classFieldName; }
            }

            public ExtractedVirtualField(VirtualField field, string classFieldName)
            {
                _referencingTable = field.ReferencingTable;
                _fieldName = field.FieldName;
                _classFieldName = classFieldName;
            }
        }
        
        private struct TablePath{
        	private string _endTable;
        	public string EndTable{
        		get{return _endTable;}
        	}
        	
        	private string _path;
        	public string Path{
        		get{return _path;}
        	}
        	
        	public TablePath(string endTable,string path)
        	{
        		_endTable=endTable;
        		_path=path;
        	}
        }

        private List<ExtractedVirtualField> ExtractFieldFromType(Type type)
        {
            List<ExtractedVirtualField> ret = new List<ExtractedVirtualField>();
            foreach (PropertyInfo pi in type.GetProperties())
            {
                if (pi.GetCustomAttributes(typeof(VirtualField), true).Length > 0)
                {
                    ret.Add(new ExtractedVirtualField((VirtualField)pi.GetCustomAttributes(typeof(VirtualField),true)[0],pi.Name));
                }
            }
            return ret;
        }

        public List<object> SelectVirtualTable(Type type)
        {
            if (type.GetCustomAttributes(typeof(VirtualTableAttribute), true).Length == 0)
                throw new Exception("Unable to execute a Virtual Table Query from a class that does not have a VirtualTableAttribute attached to it.");
            List<object> ret = new List<object>();
            Type mainTable = VirtualTableAttribute.GetMainTableTypeForVirtualTable(type);
            Connection conn = ConnectionPoolManager.GetConnection(mainTable).getConnection();
            TableMap mainMap = ClassMapper.GetTableMap(mainTable);
            string originalQuery = conn.queryBuilder.SelectAll(mainTable);
            string fieldString = "";
            List<ExtractedVirtualField> fields = ExtractFieldFromType(type);
            List<ExtractedVirtualField> fieldsUsed = new List<ExtractedVirtualField>();
            List<TablePath> paths = new List<TablePath>();
            foreach (TableMap.FieldNamePair fnp in mainMap.FieldNamePairs){
            	if (mainMap[fnp] is ExternalFieldMap){
            		ExternalFieldMap efm = (ExternalFieldMap)mainMap[fnp];
	            	TableMap extMap = ClassMapper.GetTableMap(efm.Type);
	            	bool found=false;
	            	foreach (TablePath tp in paths){
	            		if (tp.EndTable==extMap.Name){
	            			found=true;
	            			break;
	            		}
	            	}
	            	if (!found){
	            		string innerJoin = " INNER JOIN ";
	            		if (efm.IsArray){
	            			innerJoin+=conn.Pool.CorrectName(mainMap.Name+"_"+extMap.Name)+" ON ";
	            			foreach (InternalFieldMap ifm in mainMap.PrimaryKeys)
	            				innerJoin+=" virtualTable."+conn.Pool.CorrectName(ifm.FieldName)+" = "+conn.Pool.CorrectName(mainMap.Name+"_"+extMap.Name)+"."+conn.Pool.CorrectName("PARENT_"+ifm.FieldName)+" AND ";
	            			innerJoin=innerJoin.Substring(0,innerJoin.Length-5);
	            			innerJoin+=" INNER JOIN "+conn.Pool.CorrectName(extMap.Name)+" ON ";
	            			foreach (InternalFieldMap ifm in extMap.PrimaryKeys)
	            				innerJoin+=" "+conn.Pool.CorrectName(mainMap.Name+"_"+extMap.Name)+"."+conn.Pool.CorrectName("CHILD_"+ifm.FieldName)+" = "+conn.Pool.CorrectName(extMap.Name)+"."+conn.Pool.CorrectName(ifm.FieldName)+" AND ";
	            			innerJoin=innerJoin.Substring(0,innerJoin.Length-5);
	            		}else{
	            			innerJoin+=conn.Pool.CorrectName(extMap.Name)+" ON ";
	            			foreach (InternalFieldMap ifm in extMap.PrimaryKeys)
	            				innerJoin+=" virtualTable."+conn.Pool.CorrectName(efm.AddOnName+"_"+ifm.FieldName)+" = "+conn.Pool.CorrectName(extMap.Name)+"."+conn.Pool.CorrectName(ifm.FieldName)+" AND ";
	            			innerJoin=innerJoin.Substring(0,innerJoin.Length-5);
	            		}	            		
	            		paths.Add(new TablePath(extMap.Name,innerJoin));
	            	}
            	}
            }
            RecurExtractPaths(ref paths);
            string appendedJoins = "";
            foreach (ExtractedVirtualField field in fields)
            {
            	if (field.ReferencingTable.FullName == mainTable.FullName){
                    fieldString += ", virtualTable." + mainMap.GetTableFieldName(field.FieldName) + " AS " + field.ClassFieldName;
                    fieldsUsed.Add(field);
            	}
            	else{
            		TableMap extMap = ClassMapper.GetTableMap(field.ReferencingTable);
            		foreach (TablePath tp in paths){
            			if (tp.EndTable==extMap.Name){
            				if (!appendedJoins.Contains(tp.Path)){
            					appendedJoins+=" "+tp.Path;
            					break;
            				}
            			}
            		}
            		if (!appendedJoins.Contains(conn.Pool.CorrectName(extMap.Name)))
            			throw new Exception("Unable to tie relation from "+mainMap.Name+" to "+extMap.Name);
            		fieldString+=", "+conn.Pool.CorrectName(extMap.Name)+"."+conn.Pool.CorrectName(extMap.GetTableFieldName(field.FieldName))+" AS "+field.ClassFieldName;
            	}
            }
            fieldString = "SELECT " + fieldString.Substring(1) + " FROM (" + originalQuery+") virtualTable "+appendedJoins;
            conn.ExecuteQuery(fieldString);
            while (conn.Read()){
            	object obj = type.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
            	for (int x=0;x<conn.FieldCount;x++){
            		PropertyInfo pi = type.GetProperty(fields[x].ClassFieldName);
            		if (conn.IsDBNull(x))
            			pi.SetValue(obj,null,new object[0]);
            		else
            			pi.SetValue(obj,conn.GetValue(x),new object[0]);
            	}
            	ret.Add(obj);
            }
            conn.CloseConnection();
            return ret;
        }
        
        private void RecurExtractPaths(ref List<TablePath> paths){
        	
        }

    }
}
