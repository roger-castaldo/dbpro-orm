/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 23/10/2008
 * Time: 11:45 AM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */

using Org.Reddragonit.Dbpro.Connections.Parameters;
using System;
using System.Collections.Generic;
using System.Data;
using Org.Reddragonit.Dbpro.Structure;
using ExtractedFieldMap = Org.Reddragonit.Dbpro.Connections.ExtractedFieldMap;
using ExtractedTableMap = Org.Reddragonit.Dbpro.Connections.ExtractedTableMap;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Connections
{
	/// <summary>
	/// Description of QueryBuilder.
	/// </summary>
	internal class QueryBuilder
	{
		
		private ConnectionPool _pool;
		protected ConnectionPool pool{
			get{return _pool;}
		}
		
		private Connection _conn;
		protected Connection conn{
			get{return _conn;}
		}
		
		public QueryBuilder(ConnectionPool pool,Connection conn)
		{
			_pool=pool;
			_conn=conn;
		}
		
		public virtual string CreateParameterName(string parameter)
		{
			return "@"+parameter;
		}
		
		#region VersionTableNaming
		internal string VersionTableInsertTriggerName(string table)
		{
			return table+"_VERSION_INSERT";
		}
		
		internal string VersionTableUpdateTriggerName(string table)
		{
			return table+"_VERSION_UPDATE";
		}
		
		internal string VersionTableName(string table)
		{
			return table+"_VERSION";
		}
		
		internal string VersionFieldName(string table)
		{
			return table+"_VERSION_ID";
		}
		
		internal string RemoveVersionName(string table)
		{
			return table.Substring(0,table.Length-8);
		}
		#endregion
		
		#region abstracts
		#region Triggers
		protected virtual string SelectTriggersString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SelectGeneratorsString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		
		protected virtual string CreateGeneratorString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string DropGeneratorString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string GetGeneratorValueString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SetGeneratorValueString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		#endregion
		
		#region TableStructure
		protected virtual string SelectTableNamesString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SelectTableFieldsString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SelectForeignKeysString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SelectCurrentIdentities{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string CreateIdentityString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string DropNotNullString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string DropPrimaryKeyString{
			get{
				return "ALTER TABLE {0} DROP PRIMARY KEY({1})";
			}
		}
		
		protected virtual string DropForeignKeyString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string SetIdentityFieldValueString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}
		
		protected virtual string DropIdentityFieldString{
			get{
				throw new Exception("Method Not Implemented.");
			}
		}

        protected virtual string DropTableIndexString
        {
            get
            {
                throw new Exception("Method Not Implemented");
            }
        }

        protected virtual string CreateTableIndexString
        {
            get
            {
                throw new Exception("Method Not Implemented");
            }
        }

        protected virtual string SelectProceduresString
        {
            get
            {
                throw new Exception("Method Not Implemented");
            }
        }

        protected virtual string CreateProcedureString
        {
            get
            {
                throw new Exception("Method Not Implemented");
            }
        }

        protected virtual string UpdateProcedureString
        {
            get
            {
                throw new Exception("Method Not Implemented");
            }
        }

        protected virtual string DropProcedureString
        {
            get
            {
                throw new Exception("Method Not Implemented");
            }
        }

        protected virtual string SelectViewsString
        {
            get
            {
                throw new Exception("Method Not Implemented");
            }
        }

        protected virtual string CreateViewString
        {
            get
            {
                return "CREATE VIEW {0} AS {1}";
            }
        }

        protected virtual string DropViewString
        {
            get { return "DROP VIEW {0}"; }
        }
		#endregion
		#endregion
		
		#region virtual
		
		#region Selecting
		internal virtual string SelectWithConditions
		{
			get{return "SELECT {0} FROM {1} WHERE {2}";}
		}
		
		protected virtual string SelectWithoutConditions
		{
			get{return "SELECT {0} FROM {1}";}
		}
		
		protected virtual string SelectMaxWithConditions
		{
			get{return "SELECT MAX({0}) FROM {1} WHERE {2}";}
		}
		
		protected virtual string SelectMaxWithoutConditions
		{
			get{return "SELECT MAX({0}) FROM {1}";}
		}

        protected virtual string SelectMinWithConditions
        {
            get { return "SELECT MIN({0}) FROM {1} WHERE {2}"; }
        }

        protected virtual string SelectMinWithoutConditions
        {
            get { return "SELECT Min({0}) FROM {1}"; }
        }
		
		public virtual string OrderBy
		{
			get{return "{0} ORDER BY {1}";}
		}

		protected virtual string SelectWithPagingIncludeOffset
		{
			get { return "{0} LIMIT {1},{2}"; }
		}
		
		protected virtual string SelectCountString{
			get{return "SELECT COUNT(*) FROM({0}) tbl";}
		}
		#endregion
		
		#region Insert
		protected virtual string InsertString
		{
			get{return "INSERT INTO {0}({1}) VALUES ({2})";}
		}
		#endregion
		
		#region Delete
		protected virtual string DeleteWithConditions
		{
			get{return "DELETE FROM {0} WHERE {1}";}
		}
		
		protected virtual string DeleteWithoutConditions
		{
			get{return "DELETE FROM {0}";}
		}
		#endregion
		
		#region Update
		protected virtual string UpdateWithConditions
		{
			get{return "UPDATE {0} SET {1} WHERE {2}";}
		}
		
		protected virtual string UpdateWithoutConditions
		{
			get{return "UPDATE {0} SET {1}";}
		}
		#endregion
		
		#region AlterFields
		protected virtual string AlterFieldTypeString
		{
			get{return "ALTER TABLE {0} ALTER COLUMN {1} TYPE {2}";}
		}
		
		protected virtual string CreatePrimaryKeyString
		{
			get{return "ALTER TABLE {0} ADD PRIMARY KEY({1})";}
		}
		
		protected virtual string CreateNullConstraintString
		{
			get{return "ALTER TABLE {0} ADD CONSTRAINT nn_{1} {1} NOT NULL";}
		}
		
		protected virtual string CreateForeignKeyString
		{
			get{return "ALTER TABLE {0} ADD Foreign KEY ({1}) REFERENCES {2}({3}) ON UPDATE {4} ON DELETE {5}";}
		}
		
		protected virtual string CreateColumnString
		{
			get{return "ALTER TABLE {0} ADD {1} {2}";}
		}
		
		protected virtual string DropColumnString
		{
			get{return "ALTER TABLE {0} DROP {1}";}
		}
		#endregion
		
		#region Tables
		protected virtual string DropTableString
		{
			get{return "DROP TABLE {0}";}
		}
		
		protected virtual string DropTriggerString
		{
			get{return "DROP TRIGGER {0}";}
		}
		
		protected virtual string CreateTableString{
			get{return "CREATE TABLE {0} ( {1} )";}
		}
		
		protected virtual string CreateTriggerString{
			get{return "CREATE TRIGGER {0} {1} {2}";}
		}
		#endregion
		
		#endregion
		
		#region Metadata
		internal string SelectTableNames()
		{
			return SelectTableNamesString;
		}
		
		internal virtual string SelectIdentities()
		{
			return SelectCurrentIdentities;
		}
		
		internal string SelectTableFields(string tableName)
		{
			return String.Format(SelectTableFieldsString,tableName);
		}
		
		internal string SelectForeignKeys(string tableName)
		{
			return String.Format(SelectForeignKeysString,tableName);
		}

        internal virtual List<Index> ExtractTableIndexes(string tableName, Connection conn)
        {
            throw new Exception("Method Not Implemented");
        }

        internal virtual string SelectProcedures()
        {
            return SelectProceduresString;
        }

        internal virtual string SelectViews()
        {
            return SelectViewsString;
        }

        internal string CreateProcedure(StoredProcedure procedure)
        {
            return string.Format(CreateProcedureString, new object[]{procedure.ProcedureName,procedure.ParameterLines,procedure.ReturnLine,procedure.DeclareLines,procedure.Code});
        }

        internal string UpdateProcedure(StoredProcedure procedure)
        {
            return string.Format(UpdateProcedureString, new object[] { procedure.ProcedureName, procedure.ParameterLines, procedure.ReturnLine, procedure.DeclareLines, procedure.Code });
        }

        internal string DropProcedure(string procedureName)
        {
            return string.Format(DropProcedureString, procedureName);
        }

        internal virtual string CreateView(View view)
        {
            return string.Format(CreateViewString, view.Name,  view.Query);
        }

        internal virtual string DropView(string view)
        {
            return string.Format(DropViewString, view);
        }
		#endregion
		
		#region TableAlterations
		internal string DropColumn(string table, string field)
		{
			return string.Format(DropColumnString,table,field);
		}

        internal string DropTableIndex(string table, string indexName)
        {
            return string.Format(DropTableIndexString, table, indexName);
        }

        internal virtual string CreateTableIndex(string table, string[] fields, string indexName, bool unique,bool ascending)
        {
            string sfields = "";
            foreach (string str in fields)
                sfields += str + ",";
            sfields = sfields.Substring(0, sfields.Length - 1);
            return string.Format(CreateTableIndexString, new object[]{
                table,
                sfields,
                indexName,
                (unique ? "UNIQUE" : ""),
                (ascending ? "ASC" : "DESC")
            });
        }
		
		internal string DropTable(string table)
		{
			return string.Format(DropTableString,table);
		}
		
		internal string DropTrigger(string trigger)
		{
			return string.Format(DropTriggerString,trigger);
		}
		
		internal string DropIdentityField(IdentityField field)
		{
			return string.Format(DropIdentityFieldString,field.TableName,field.FieldName,field.FieldType,field.CurValue);
		}
		
		internal string CreateIdentityField(IdentityField field)
		{
			return string.Format(CreateIdentityString,field.TableName,field.FieldName,field.FieldType,field.CurValue);
		}
		
		internal string CreateTrigger(Trigger trigger)
		{
			return string.Format(CreateTriggerString,trigger.Name,trigger.Conditions,trigger.Code);
		}
		
		internal string SelectTriggers(){
			return SelectTriggersString;
		}
		
		internal string SelectGenerators(){
			return SelectGeneratorsString;
		}
		
		internal string CreateGenerator(string name){
			return string.Format(CreateGeneratorString,name);
		}
		
		internal string DropGenerator(string name){
			return string.Format(DropGeneratorString,name);
		}
		
		internal string GetGeneratorValue(string name){
			return string.Format(GetGeneratorValueString,name);
		}
		
		internal string SetGeneratorValue(string name,long value){
			return string.Format(SetGeneratorValueString,name,value.ToString());
		}
		
		internal string SetIdentityFieldValue(IdentityField field)
		{
			return string.Format(SetIdentityFieldValueString,field.TableName,field.FieldName,field.FieldType,field.CurValue);
		}
		#endregion
		
		#region TableAltering
		internal string CreateColumn(string table,ExtractedFieldMap field)
		{
			return string.Format(CreateColumnString,table,field.FieldName,field.FullFieldType);
		}
		
		internal virtual string AlterFieldType(string table, ExtractedFieldMap field,ExtractedFieldMap oldFieldInfo)
		{
			return string.Format(AlterFieldTypeString,table,field.FieldName,field.FullFieldType);
		}
		
		internal virtual string DropPrimaryKey(PrimaryKey key)
		{
			string fields="";
			foreach (string str in key.Fields)
				fields+=str+",";
			return String.Format(DropPrimaryKeyString,key.Name,fields.Substring(0,fields.Length-1));
		}
		
		internal string CreatePrimaryKey(PrimaryKey key)
		{
			string ret="";
			foreach(string str in key.Fields)
			{
				ret+=str+",";
			}
			return string.Format(CreatePrimaryKeyString,key.Name,ret.Substring(0,ret.Length-1));
		}
		
		internal string CreateNullConstraint(string table,ExtractedFieldMap field)
		{
			return string.Format(CreateNullConstraintString,table,field.FieldName,field.FullFieldType);
		}
		
		internal virtual string DropNullConstraint(string table,ExtractedFieldMap field)
		{
			return String.Format(DropNotNullString,table,field.FieldName,field.FullFieldType);
		}
		
		internal virtual string DropForeignKey(string table,string externalTable,string primaryField,string relatedField)
		{
			return string.Format(DropForeignKeyString,table,externalTable);
		}
		
		internal string CreateForeignKey(ForeignKey key)
		{
			string field="";
			foreach (string str in key.InternalFields)
			{
				field+=str+",";
			}
			field=field.Substring(0,field.Length-1);
			string Foreigns = "";
			foreach (string str in key.ExternalFields)
			{
				Foreigns+=str+",";
			}
			Foreigns=Foreigns.Substring(0,Foreigns.Length-1);
			return string.Format(CreateForeignKeyString,key.InternalTable,field,key.ExternalTable,Foreigns,key.OnUpdate,key.OnDelete);
		}
		
		internal string CreateTable(ExtractedTableMap table)
		{
			string fields = "";
			foreach (ExtractedFieldMap efm in table.Fields)
			{
				fields+="\t"+efm.FieldName+" "+efm.FullFieldType+",\n";
			}
			fields = fields.Substring(0,fields.Length-2);
			return String.Format(CreateTableString,table.TableName,fields);
		}
		#endregion
		
		#region Inserts
        internal string InsertWithIdentity(Table table, out List<IDbDataParameter> insertParameters)
        {
            sTable tbl = _pool.Mapping[table.GetType()];
            insertParameters = new List<IDbDataParameter>();
            try
            {
                string values = "";
                string parameters = "";
                foreach (string prop in tbl.Properties)
                {
                    sTableField[] flds = tbl[prop];
                    if (flds.Length > 0)
                    {
                        PropertyInfo pi = table.GetType().GetProperty(prop, Utility._BINDING_FLAGS);
                        if (pi.GetCustomAttributes(false)[0] is Org.Reddragonit.Dbpro.Structure.Attributes.IForeignField)
                        {
                            Table eTable = (Table)table.GetField(prop);
                            if (eTable == null)
                            {
                                foreach (sTableField fld in flds)
                                {
                                    values += fld.Name + ",";
                                    parameters += "," + CreateParameterName(fld.Name);
                                    insertParameters.Add(conn.CreateParameter(CreateParameterName(fld.Name), null, fld.Type, fld.Length));
                                }
                            }
                            else
                            {
                                foreach (sTableField fld in flds)
                                {
                                    values += fld.Name + ",";
                                    parameters += "," + CreateParameterName(fld.Name);
                                }
                                Type etype = pi.PropertyType;
                                while (true)
                                {
                                    sTable etbl = _pool.Mapping[etype];
                                    foreach (sTableField fld in flds){
                                        foreach (sTableField efld in etbl.Fields)
                                        {
                                            if (fld.ExternalField == efld.Name)
                                            {
                                                object val = LocateFieldValue(eTable, fld, pool);
                                                if (val==null)
                                                    insertParameters.Add(conn.CreateParameter(CreateParameterName(fld.Name), null, fld.Type, fld.Length));
                                                else
                                                    insertParameters.Add(conn.CreateParameter(CreateParameterName(fld.Name), val));
                                                break;
                                            }
                                        }
                                    }
                                    etype = etype.BaseType;
                                    if (etype.Equals(typeof(Table)))
                                        break;
                                }
                            }
                        }else
                        {
                            values += flds[0].Name + ",";
                            parameters += "," + CreateParameterName(prop);
                            if (table.IsFieldNull(prop))
                                insertParameters.Add(conn.CreateParameter(CreateParameterName(prop), null, flds[0].Type, flds[0].Length));
                            else
                            {
                                if (flds[0].Type == FieldType.ENUM)
                                    insertParameters.Add(conn.CreateParameter(CreateParameterName(prop), conn.Pool.GetEnumID(table.GetType().GetProperty(prop, Utility._BINDING_FLAGS).PropertyType, table.GetField(prop).ToString())));
                                else
                                    insertParameters.Add(conn.CreateParameter(CreateParameterName(prop), table.GetField(prop), flds[0].Type, flds[0].Length));
                            }
                        }
                    }
                }
                values = values.Substring(0, values.Length - 1);
                parameters = parameters.Substring(1);
                return string.Format(InsertString, tbl.Name, values, parameters);
            }
            catch (Exception e)
            {
                Logger.LogLine(e.Message);
                return null;
            }
        }

		internal string Insert(Table table,out List<IDbDataParameter> insertParameters,out string select,out List<IDbDataParameter> selectParameters)
		{
            sTable tbl = _pool.Mapping[table.GetType()];
			insertParameters=new List<IDbDataParameter>();
			selectParameters=new List<IDbDataParameter>();
			string whereConditions = "";
			select=null;
			try{
				string values="";
				string parameters="";
                foreach (string prop in tbl.Properties)
                {
                    sTableField[] flds = tbl[prop];
                    if (flds.Length > 0)
                    {
                        PropertyInfo pi = table.GetType().GetProperty(prop, Utility._BINDING_FLAGS);
                        if (pi.GetCustomAttributes(false)[0] is Org.Reddragonit.Dbpro.Structure.Attributes.IForeignField)
                        {
                            Table eTable = (Table)table.GetField(prop);
                            if (eTable == null)
                            {
                                foreach (sTableField fld in flds)
                                {
                                    values += fld.Name + ",";
                                    parameters += "," + CreateParameterName(fld.Name);
                                    insertParameters.Add(conn.CreateParameter(CreateParameterName(fld.Name), null, fld.Type, fld.Length));
                                    whereConditions += " AND " + fld.Name + " IS NULL ";
                                }
                            }
                            else
                            {
                                foreach (sTableField fld in flds)
                                {
                                    values += fld.Name + ",";
                                    parameters += "," + CreateParameterName(fld.Name);
                                    whereConditions += " AND " + fld.Name + " = " + CreateParameterName(fld.Name) + " ";
                                }
                                Type etype = pi.PropertyType;
                                while (true)
                                {
                                    sTable etbl = _pool.Mapping[etype];
                                    foreach (sTableField fld in flds)
                                    {
                                        foreach (sTableField efld in etbl.Fields)
                                        {
                                            if (fld.ExternalField == efld.Name)
                                            {
                                                insertParameters.Add(conn.CreateParameter(CreateParameterName(fld.Name), eTable.GetField(efld.ClassProperty)));
                                                break;
                                            }
                                        }
                                    }
                                    etype = etype.BaseType;
                                    if (etype.Equals(typeof(Table)))
                                        break;
                                }
                            }
                        }
                        else if (!Utility.StringsEqual(prop,tbl.AutoGenProperty))
                        {
                            values += flds[0].Name + ",";
                            parameters += "," + CreateParameterName(prop);
                            whereConditions += " AND "+flds[0].Name+" = " + CreateParameterName(prop) + " ";
                            if (table.IsFieldNull(prop))
                                insertParameters.Add(conn.CreateParameter(CreateParameterName(prop), null, flds[0].Type, flds[0].Length));
                            else
                            {
                                if (flds[0].Type == FieldType.ENUM)
                                    insertParameters.Add(conn.CreateParameter(CreateParameterName(prop), conn.Pool.GetEnumID(table.GetType().GetProperty(prop, Utility._BINDING_FLAGS).PropertyType, table.GetField(prop).ToString())));
                                else
                                    insertParameters.Add(conn.CreateParameter(CreateParameterName(prop), table.GetField(prop), flds[0].Type, flds[0].Length));
                            }
                        }
                    }
                }
				values=values.Substring(0,values.Length-1);
				parameters=parameters.Substring(1);
				whereConditions=whereConditions.Substring(4);
				if (tbl.AutoGenProperty!=null)
				{
                    select = string.Format(SelectMaxWithConditions, tbl[tbl.AutoGenProperty][0].Name, tbl.Name, whereConditions);
                    selectParameters.AddRange(insertParameters);
				}
				return string.Format(InsertString,tbl.Name,values,parameters);
			}catch (Exception e)
			{
				Logger.LogLine(e.Message);
				return null;
			}
		}
		
		internal string Insert(string table,string fields,string parameters)
		{
			return string.Format(InsertString,table,fields,parameters);
		}
		#endregion
		
		#region Deletes
        internal string Delete(Type tableType, SelectParameter[] pars, out List<IDbDataParameter> parameters)
        {
            parameters = new List<IDbDataParameter>();
            try
            {
                string conditions = "";
                sTable tbl = _pool.Mapping[tableType];
                int parCount = 0;
                foreach (SelectParameter eq in pars)
                {
                    conditions += eq.ConstructString(tableType, _conn, this, ref parameters, ref parCount) + " AND ";
                }
                if (conditions.Length > 0)
                    conditions = conditions.Substring(0, conditions.Length - 4).Replace("main_table.", "");
                return string.Format(DeleteWithConditions, tbl.Name, conditions);
            }
            catch (Exception e)
            {
                Logger.LogLine(e.Message);
                return null;
            }
        }

		internal string Delete(Table table,out List<IDbDataParameter> parameters)
		{
            sTable tbl = _pool.Mapping[table.GetType()];
            List<SelectParameter> tmpPars = new List<SelectParameter>();
            List<string> pkeys = new List<string>(tbl.PrimaryKeyFields);
            if (tbl.PrimaryKeyFields.Length == 0)
            {
                foreach (string prop in tbl.Properties)
                {
                    tmpPars.Add(new EqualParameter(prop, table.GetInitialPrimaryValue(prop)));
                }
            }
            else
            {
                foreach (string prop in tbl.Properties)
                {
                    sTableField[] flds = tbl[prop];
                    if (flds.Length > 0)
                    {
                        if (pkeys.Contains(flds[0].Name))
                            tmpPars.Add(new EqualParameter(prop, table.GetInitialPrimaryValue(prop)));
                    }
                }
            }
            return Delete(table.GetType(), tmpPars.ToArray(), out parameters);
		}
		
		internal string Delete(string tableName,string conditions)
		{
			return string.Format(DeleteWithConditions,tableName,conditions);
		}

        internal string DeleteAll(Type tableType){
            return string.Format(DeleteWithoutConditions, _pool.Mapping[tableType].Name);
        }

        internal string DeleteAll(string tableName)
        {
            return string.Format(DeleteWithoutConditions, tableName);
        }
		#endregion
		
		#region Updates
		internal Dictionary<string,List<List<IDbDataParameter>>> UpdateMapArray(Table table,string property,bool ignoreautogen)
		{
			Dictionary<string, List<List<IDbDataParameter>>> ret = new Dictionary<string, List<List<IDbDataParameter>>>();
			try{
                sTable tbl = _pool.Mapping[table.GetType()];
                sTableRelation rel = tbl.GetRelationForProperty(property).Value;
				Table[] values = (Table[])table.GetField(property);
				if (values!=null)
				{
                    sTable iTable = _pool.Mapping[table.GetType(), property];
					string delString = "DELETE FROM " + rel.IntermediateTable + " WHERE ";
                    string insertString = "INSERT INTO " + iTable.Name + "(";
                    string valueString = "VALUES(";
					List<IDbDataParameter> pars = new List<IDbDataParameter>();
                    List<string> pkeys = new List<string>(tbl.PrimaryKeyFields);
                    foreach (sTableField f in tbl.Fields)
                    {
                        if (pkeys.Contains(f.Name))
                        {
                            foreach (sTableField fld in iTable.Fields)
                            {
                                if (Utility.StringsEqual(fld.ExternalField, f.Name))
                                {
                                    delString += fld.Name + " = " + CreateParameterName(fld.Name) + " AND ";
                                    pars.Add(conn.CreateParameter(conn.CreateParameterName(fld.Name), LocateFieldValue(table, f, _pool)));
                                    insertString += f.Name + ",";
                                    valueString += CreateParameterName(f.Name) + ",";
                                    break;
                                }
                            }
                        }
                    }
					ret.Add(delString.Substring(0, delString.Length - 4),new List<List<IDbDataParameter>>(new List<IDbDataParameter>[]{new List<IDbDataParameter>(pars.ToArray())}));
                    sTable relTable = _pool.Mapping[table.GetType().GetProperty(property, Utility._BINDING_FLAGS).PropertyType.GetElementType()];
                    foreach (string pkey in relTable.PrimaryKeyFields)
                    {
                        foreach (sTableField fld in iTable.Fields)
                        {
                            if (Utility.StringsEqual(fld.ExternalField, pkey))
                            {
                                insertString += fld.Name + ",";
                                valueString += CreateParameterName(fld.Name) + ",";
                                break;
                            }
                        }
                    }
                    insertString = insertString.Substring(0, delString.Length - 1) + (ignoreautogen ? "," + pool.CorrectName("INDEX") : "") + ") " + valueString.Substring(0, valueString.Length - 1) + (ignoreautogen ? "," + CreateParameterName("index") : "") + ")";
                    ret.Add(insertString, new List<List<IDbDataParameter>>());
                    int index = 0;
                    pkeys.Clear();
                    pkeys.AddRange(relTable.PrimaryKeyFields);
					foreach (Table t in values)
					{
                        foreach (sTableField fld in relTable.Fields)
                        {
                            if (pkeys.Contains(fld.Name))
                            {
                                foreach (sTableField f in iTable.Fields){
                                    if (Utility.StringsEqual(f.ExternalField, fld.Name))
                                    {
                                        for (int x = 0; x < pars.Count; x++)
                                        {
                                            if (pars[x].ParameterName == CreateParameterName(f.Name))
                                            {
                                                pars.RemoveAt(x);
                                                break;
                                            }
                                        }
                                        object val = LocateFieldValue(t, fld, pool);
                                        if (val == null)
                                            pars.Add(conn.CreateParameter(CreateParameterName(f.Name), null, f.Type, f.Length));
                                        else
                                            pars.Add(conn.CreateParameter(CreateParameterName(f.Name), val));
                                        break;
                                    }
                                }
                            }
                        }
                        if (ignoreautogen)
                        {
                            for (int x = 0; x < pars.Count; x++)
                            {
                                if (pars[x].ParameterName == CreateParameterName("index"))
                                {
                                    pars.RemoveAt(x);
                                    break;
                                }
                            }
                            pars.Add(conn.CreateParameter(CreateParameterName("index"), index, FieldType.INTEGER, 4));
                        }
                        ret[insertString].Add(new List<IDbDataParameter>(pars.ToArray()));
                        index++;
					}
				}
			}catch (Exception e)
			{
				Logger.LogLine(e.Message);
				return null;
			}
			return ret;
		}

        internal static object LocateFieldValue(Table table,sTableField fld,ConnectionPool pool)
        {
            if (fld.ExternalField == null)
                return table.GetField(fld.ClassProperty);
            else
            {
                Table val = (Table)table.GetField(fld.ClassProperty);
                if (val == null)
                    return null;
                else
                {
                    foreach (sTableField field in pool.Mapping[val.GetType()].Fields)
                    {
                        if (field.Name == fld.ExternalField)
                        {
                            return LocateFieldValue(val, field, pool);
                        }
                    }
                }
            }
            return null;
        }

        internal string Update(Type tableType, Dictionary<string, object> updateFields, SelectParameter[] parameters, out List<IDbDataParameter> queryParameters)
        {
            queryParameters = new List<IDbDataParameter>();
            sTable table = _pool.Mapping[tableType];
            try
            {
                string fields = "";
                string conditions = "";
                bool addedAutogenCorrection = false;
                List<string> pkeys = new List<string>(table.PrimaryKeyFields);
                foreach (string prop in updateFields.Keys)
                {
                    sTableField[] flds = table[prop];
                    if (flds.Length > 0)
                    {
                        if (pkeys.Contains(flds[0].Name) && !Utility.StringsEqual(table.AutoGenProperty, prop) && !addedAutogenCorrection && pkeys.Count > 0)
                        {
                            if (table.AutoGenProperty != null)
                            {
                                fields += table.AutoGenProperty + " = (SELECT (CASE WHEN MAX(" + table.AutoGenProperty + ") IS NULL THEN 0 ELSE MAX(" + table.AutoGenProperty + ") END)+1 FROM " + table.Name + " WHERE ";
                                foreach (sTableField fld in table.Fields)
                                {
                                    if (pkeys.Contains(fld.Name) && !Utility.StringsEqual(table.AutoGenProperty, fld.Name))
                                    {
                                        object val = updateFields[fld.ClassProperty];
                                        if (val == null)
                                            fields += fld.Name + " IS NULL AND ";
                                        else
                                        {
                                            fields += fld.Name + " = " + CreateParameterName(fld.Name);
                                            queryParameters.Add(conn.CreateParameter(CreateParameterName(fld.Name), val));
                                        }
                                    }
                                }
                                fields = fields.Substring(0, fields.Length - 4);
                                fields += "), ";
                                addedAutogenCorrection = true;
                            }
                        }
                        if (flds[0].ExternalField == null)
                        {
                            sTable relTable = _pool.Mapping[tableType.GetProperty(flds[0].ClassProperty, Utility._BINDING_FLAGS).PropertyType];
                            if (updateFields[flds[0].ClassProperty] == null)
                            {
                                foreach (sTableField fld in flds)
                                {
                                    fields += fld.Name + " = " + CreateParameterName(fld.Name);
                                    queryParameters.Add(conn.CreateParameter(CreateParameterName(fld.Name), null, fld.Type, fld.Length));
                                }
                            }
                            else
                            {
                                Table relatedTable = (Table)updateFields[flds[0].ClassProperty];
                                foreach (sTableField fld in flds)
                                {
                                    foreach (sTableField f in relTable.Fields)
                                    {
                                        if (fld.ExternalField == f.Name)
                                        {
                                            object val = LocateFieldValue(relatedTable, f, pool);
                                            fields += fld.Name + " = " + CreateParameterName(fld.Name);
                                            if (val == null)
                                                queryParameters.Add(conn.CreateParameter(CreateParameterName(fld.Name), null, fld.Type, fld.Length));
                                            else
                                                queryParameters.Add(conn.CreateParameter(CreateParameterName(fld.Name), val));
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            fields += flds[0].Name + " = " + CreateParameterName(flds[0].Name) + ", ";
                            if (updateFields[flds[0].ClassProperty] == null)
                                queryParameters.Add(conn.CreateParameter(CreateParameterName(flds[0].Name), null,flds[0].Type,flds[0].Length));
                            else
                            {
                                if (flds[0].Type==FieldType.ENUM)
                                    queryParameters.Add(conn.CreateParameter(CreateParameterName(flds[0].Name), pool.GetEnumID(updateFields[flds[0].ClassProperty].GetType(), updateFields[flds[0].ClassProperty].ToString())));
                                else
                                    queryParameters.Add(conn.CreateParameter(CreateParameterName(flds[0].Name), updateFields[flds[0].ClassProperty], flds[0].Type, flds[0].Length));
                            }
                        }
                    }
                }
                int parCount = 0;
                foreach (SelectParameter eq in parameters)
                {
                    conditions += eq.ConstructString(tableType, _conn, this, ref queryParameters, ref parCount) + " AND ";
                }
                if (fields.Length == 0)
                    return "";
                fields = fields.Substring(0, fields.Length - 2);
                if (conditions.Length > 0)
                {
                    return String.Format(UpdateWithConditions, table.Name, fields, conditions.Substring(0, conditions.Length - 4).Replace("main_table.", ""));
                }
                else
                    return String.Format(UpdateWithoutConditions, table.Name, fields);
            }
            catch (Exception e)
            {
                Logger.LogLine(e.Message);
            }
            return null;
        }

        internal string Update(Table table, out List<IDbDataParameter> queryParameters)
        {
            queryParameters = new List<IDbDataParameter>();
            if ((table.ChangedFields == null) || (table.ChangedFields.Count == 0))
                return "";
            sTable tbl = _pool.Mapping[table.GetType()];
            List<string> changedFields = table.ChangedFields;
            if (changedFields == null)
                changedFields = new List<string>(tbl.Properties);
            if (changedFields.Count == 0)
                return "";
            Dictionary<string, object> updateFields = new Dictionary<string, object>();
            List<SelectParameter> parameters = new List<SelectParameter>();
            List<string> pkeys = new List<string>(tbl.PrimaryKeyProperties);
            foreach (string prop in tbl.Properties)
            {
                if (changedFields.Contains(prop))
                    updateFields.Add(prop, table.GetField(prop));
                if (pkeys.Count == 0 || pkeys.Contains(prop))
                    parameters.Add(new EqualParameter(prop, table.GetInitialPrimaryValue(prop)));
            }
            return Update(table.GetType(), updateFields, parameters.ToArray(), out queryParameters);
        }
		#endregion
		
		#region Selects
		private string GetSubqueryTable(sTable tbl,Type type,ref int count)
		{
            if (!_pool.Mapping.IsMappableType(type.BaseType))
                return tbl.Name;
            sTable parentTable = _pool.Mapping[type.BaseType];
			string fields = "";
            int origCount = count;
            string tables = tbl.Name + " table_" + origCount.ToString() + ", ";
            count++;
            tables += GetSubqueryTable(parentTable, type.BaseType, ref count);
            tables+=" table_" + ((int)(count + 1)).ToString();
			string where ="";
            foreach (string prop in tbl.Properties)
            {
                foreach (sTableField fld in tbl[prop])
                    fields += "table_" + origCount.ToString() + "." + fld.Name + ",";
            }
            foreach (string key in parentTable.PrimaryKeyFields)
            {
                if (!fields.Contains("table_" + origCount.ToString() + "." + key + ","))
                    fields += "table_" + origCount.ToString() + "." + key + ",";
                where += " table_" + origCount.ToString() + "." + key + " = table_" + (count + 1).ToString() + "." + key + " AND";
            }
			count++;
            Type btype = type.BaseType;
            while (_pool.Mapping.IsMappableType(btype))
            {
                sTable t = _pool.Mapping[btype];
                foreach (string prop in tbl.Properties)
                {
                    foreach (sTableField fld in t[prop])
                    {
                        if (!fields.Contains("table_" + count.ToString() + "." + fld.Name + ","))
                            fields += "table_" + origCount.ToString() + "." + fld.Name + ",";
                    }
                }
                btype = btype.BaseType;
            }
			fields=fields.Substring(0,fields.Length-1);
			if (where.EndsWith(" AND"))
				where = where.Substring(0,where.Length-4);
			return String.Format("("+SelectWithConditions+")",fields,tables,where);
		}
		
		private bool ObtainFieldTableWhereList(out string fields,out string tables,out string joins,out string where,System.Type type)
		{
			fields="";
			tables="";
			joins="";
			where="";
			int count=0;
			try{
				sTable tbl = _pool.Mapping[type];
				joins=GetSubqueryTable(tbl,type,ref count)+" main_table";
                foreach (string prop in tbl.Properties)
                {
                    foreach (sTableField fld in tbl[prop])
                        fields += ",main_table." + fld.Name;
                }
                Type btype = type.BaseType;
                while (_pool.Mapping.IsMappableType(btype))
                {
                    sTable t = _pool.Mapping[btype];
                    foreach (string prop in tbl.Properties)
                    {
                        foreach (sTableField fld in t[prop])
                        {
                            if (!fields.Contains(",main_table." + fld.Name))
                                fields += ",main_table." + fld.Name;
                        }
                    }
                    btype = btype.BaseType;
                }
				fields=fields.Substring(1);
				if (where.Length>0)
				{
					where = where.Substring(0,where.Length-4);
				}
			}catch (Exception e)
			{
				return false;
			}
			return true;
		}

        internal void AppendJoinsForParameter(List<string> fields,ref string joins,Type baseType)
        {
            for(int x=0;x<fields.Count;x++)
            {
                string field = fields[x];
                sTable map = _pool.Mapping[baseType];
                Type curType = baseType;
                string alias = "main_table";
                bool parentIsNullable = false;
                if (field.Contains("."))
                {
                    while (field.Contains("."))
                    {
                        sTableRelation rel = map.GetRelationForProperty(field.Substring(0, field.IndexOf("."))).Value;
                        PropertyInfo pi = curType.GetProperty(field.Substring(0, field.IndexOf(".")), Utility._BINDING_FLAGS);
                        sTable relMap = _pool.Mapping[(rel.IntermediateTable != null ? pi.PropertyType.GetElementType() : pi.PropertyType)];
                        string className = pi.Name;
                        string innerJoin = " INNER JOIN ";
                        if (rel.Nullable)
                        {
                            parentIsNullable = true;
                            innerJoin = " LEFT JOIN ";
                        }
                        else if (parentIsNullable)
                            innerJoin = " LEFT JOIN ";
                        string tbl = _conn.queryBuilder.SelectAll((rel.IntermediateTable!=null ? pi.PropertyType.GetElementType() : pi.PropertyType), null);
                        if (rel.IntermediateTable!=null)
                        {
                            sTable iMap = _pool.Mapping[curType, pi.Name];
                            innerJoin += iMap.Name + " " + alias + "_intermediate_" + className + " ON ";
                            List<string> pkeys = new List<string>(map.PrimaryKeyFields);
                            foreach (sTableField fld in iMap.Fields)
                            {
                                if (pkeys.Contains(fld.ExternalField))
                                    innerJoin += " " + alias + "." + fld.ExternalField + " = " + alias + "_intermediate_" + className + "." + fld.Name + " AND ";
                            }
                            innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                            if (!joins.Contains(innerJoin))
                                joins += innerJoin;
                            innerJoin = " " + (rel.Nullable || parentIsNullable ? "LEFT" : "INNER") + " JOIN (" + tbl + ") " + alias + "_" + className + " ON ";
                            pkeys.Clear();
                            pkeys.AddRange(relMap.PrimaryKeyFields);
                            foreach (sTableField fld in iMap.Fields)
                            {
                                if (pkeys.Contains(fld.ExternalField))
                                    innerJoin += " " + alias + "_intermediate_" + className + "." + fld.Name + " = " + alias + "_" + className + "." + fld.Name + " AND ";
                            }
                            innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                        }
                        else
                        {
                            innerJoin += "(" + tbl + ") " + alias + "_" + className + " ON ";
                            List<string> pkeys = new List<string>(relMap.PrimaryKeyFields);
                            foreach (sTableField fld in map[pi.Name])
                            {
                                if (pkeys.Contains(fld.ExternalField))
                                    innerJoin += " " + alias + "." + fld.Name + " = " + alias + "_" + className + "." + fld.ExternalField + " AND ";
                            }
                            innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                        }
                        alias += "_" + field.Substring(0, field.IndexOf("."));
                        field = field.Substring(field.IndexOf(".") + 1);
                        if (!joins.Contains(innerJoin))
                            joins += innerJoin;
                        map = relMap;
                        curType = (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType);
                    }

                }
            }
        }
		
		internal string SelectAll(System.Type type,string[] OrderByFields)
		{
			List<IDbDataParameter> pars = new List<IDbDataParameter>();
			return Select(type,new SelectParameter[0],out pars,OrderByFields);
		}
		
		internal string SelectMax(System.Type type,string maxField,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters)
		{
			if (parameters==null)
				return SelectMax(type,maxField,new SelectParameter[0],out queryParameters);
			else
				return SelectMax(type,maxField,parameters.ToArray(),out queryParameters);
		}
		
		internal string SelectMax(System.Type type,string maxField,SelectParameter[] parameters,out List<IDbDataParameter> queryParameters)
		{
            sTable map = _pool.Mapping[type];
			string fields="";
			string tables="";
			string joins="";
			string where="";
			bool startAnd=false;
			queryParameters = new List<IDbDataParameter>();
			if (ObtainFieldTableWhereList(out fields,out tables, out joins,out where, type))
			{
                AppendJoinsForParameter(new List<string>(new string[] { maxField }), ref joins, type);
                string alias = "main_table";
                if (maxField.Contains("."))
                {
                    sTable curMap = map;
                    Type curType = type;
                    while (maxField.Contains("."))
                    {
                        PropertyInfo pi = curType.GetProperty(maxField.Substring(0, maxField.IndexOf(".")), Utility._BINDING_FLAGS);
                        curType = (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType);
                        curMap = _pool.Mapping[curType];
                        alias += "_" + maxField.Substring(0, maxField.IndexOf("."));
                        maxField = maxField.Substring(maxField.IndexOf(".") + 1);
                    }
                    fields = alias + "." + curMap[maxField][0].Name;
                }
                else
                {
                    sTableField[] flds = map[maxField];
                    if (flds.Length > 0)
                        fields = alias + "." + flds[0].Name;
                    else
                        fields = maxField;
                }
				if ((parameters!=null)&&(parameters.Length>0))
				{
					startAnd=(where.Length>0);
					string appended="";
					int parCount=0;
					foreach (SelectParameter par in parameters)
					{
                        AppendJoinsForParameter(par.Fields, ref joins, type);
						appended+="("+par.ConstructString(type,conn,this,ref queryParameters,ref parCount)+") AND ";
					}
					appended=appended.Substring(0,appended.Length-4);
					if (!startAnd)
						where = "("+appended+")";
					else
						where+=" AND ("+appended+")";
				}
				if (where.Length>0)
					return String.Format(SelectMaxWithConditions ,fields,joins+tables,where);
				else
					return String.Format(SelectMaxWithoutConditions,fields,joins+tables);
			}else
				return null;
		}

        internal string SelectMin(System.Type type, string maxField, List<SelectParameter> parameters, out List<IDbDataParameter> queryParameters)
        {
            if (parameters == null)
                return SelectMin(type, maxField, new SelectParameter[0], out queryParameters);
            else
                return SelectMin(type, maxField, parameters.ToArray(), out queryParameters);
        }

        internal string SelectMin(System.Type type, string maxField, SelectParameter[] parameters, out List<IDbDataParameter> queryParameters)
        {
            sTable map = _pool.Mapping[type];
            string fields = "";
            string tables = "";
            string joins = "";
            string where = "";
            bool startAnd = false;
            queryParameters = new List<IDbDataParameter>();
            if (ObtainFieldTableWhereList(out fields, out tables, out joins, out where, type))
            {
                AppendJoinsForParameter(new List<string>(new string[] { maxField }), ref joins, type);
                string alias = "main_table";
                if (maxField.Contains("."))
                {
                    sTable curMap = map;
                    Type curType = type;
                    while (maxField.Contains("."))
                    {
                        PropertyInfo pi = curType.GetProperty(maxField.Substring(0, maxField.IndexOf(".")), Utility._BINDING_FLAGS);
                        curType = (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType);
                        curMap = _pool.Mapping[curType];
                        alias += "_" + maxField.Substring(0, maxField.IndexOf("."));
                        maxField = maxField.Substring(maxField.IndexOf(".") + 1);
                    }
                    fields = alias + "." + curMap[maxField][0].Name;
                }
                else
                {
                    sTableField[] flds = map[maxField];
                    if (flds.Length > 0)
                        fields = alias + "." + flds[0].Name;
                    else
                        fields = maxField;
                }
                if ((parameters != null) && (parameters.Length > 0))
                {
                    startAnd = (where.Length > 0);
                    string appended = "";
                    int parCount = 0;
                    foreach (SelectParameter par in parameters)
                    {
                        AppendJoinsForParameter(par.Fields, ref joins, type);
                        appended += "(" + par.ConstructString(type, conn, this, ref queryParameters, ref parCount) + ") AND ";
                    }
                    appended = appended.Substring(0, appended.Length - 4);
                    if (!startAnd)
                        where = "(" + appended + ")";
                    else
                        where += " AND (" + appended + ")";
                }
                if (where.Length > 0)
                    return String.Format(SelectMinWithConditions, fields, joins + tables, where);
                else
                    return String.Format(SelectMinWithoutConditions, fields, joins + tables);
            }
            else
                return null;
        }
		
		internal string Select(System.Type type,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters,string[] OrderByFields)
		{
			if (parameters==null)
				return Select(type,new SelectParameter[0],out queryParameters,OrderByFields);
			else
				return Select(type,parameters.ToArray(),out queryParameters,OrderByFields);
		}

        internal string Select(System.Type type, SelectParameter[] parameters, out List<IDbDataParameter> queryParameters, string[] OrderByFields)
		{
            sTable tbl = _pool.Mapping[type];
			string fields="";
			string tables="";
			string joins="";
			string where="";
			bool startAnd=false;
			queryParameters=new List<IDbDataParameter>();
            string order = "";
            if ((OrderByFields != null) && (OrderByFields.Length > 0))
            {
                foreach (string str in OrderByFields)
                {
                    foreach (sTableField stf in tbl[str])
                        order += stf.Name + ",";
                }
                if (order.Length>0)
                    order = order.Substring(0, order.Length - 1);
            }
			if (ObtainFieldTableWhereList(out fields,out tables, out joins,out where,type))
			{
				if ((parameters!=null)&&(parameters.Length>0))
				{
					startAnd=(where.Length>0);
					string appended="";
					int parCount=0;
					foreach (SelectParameter par in parameters)
					{
                        AppendJoinsForParameter(par.Fields, ref joins, type);
                        appended+="("+par.ConstructString(type,conn,this,ref queryParameters,ref parCount)+") AND ";
					}
					appended=appended.Substring(0,appended.Length-4);
					if (!startAnd)
						where = "("+appended+")";
					else
						where+=" AND ("+appended+")";
				}
                if (order.Length > 0)
                {
                    if (where.Length > 0)
                        return String.Format(OrderBy,String.Format(SelectWithConditions, fields, joins + tables, where),order);
                    else
                        return String.Format(OrderBy,String.Format(SelectWithoutConditions, fields, joins + tables),order);
                }
                else
                {
                    if (where.Length > 0)
                        return String.Format(SelectWithConditions, fields, joins + tables, where);
                    else
                        return String.Format(SelectWithoutConditions, fields, joins + tables);
                }
			}else
				return null;
		}
		
		internal string SelectCount(System.Type type,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters)
		{
			if (parameters==null)
				return SelectCount(type,new SelectParameter[0],out queryParameters);
			else
				return SelectCount(type,parameters.ToArray(),out queryParameters);
		}
		
		internal string SelectCount(System.Type type,SelectParameter[] parameters,out List<IDbDataParameter> queryParameters)
		{
			string query=Select(type,parameters,out queryParameters,null);
			return String.Format(SelectCountString,query);
		}

        internal string SelectPaged(System.Type type, List<SelectParameter> parameters, out List<IDbDataParameter> queryParameters, ulong? start, ulong? recordCount)
        {
            return SelectPaged(type, parameters, out queryParameters, start, recordCount, null);
        }
		
		internal string SelectPaged(System.Type type,List<SelectParameter> parameters,out List<IDbDataParameter> queryParameters,ulong? start,ulong? recordCount,string[] OrderByFields)
		{
			if (parameters==null)
				return SelectPaged(type,new SelectParameter[0],out queryParameters,start,recordCount,OrderByFields);
			else
				return SelectPaged(type,parameters.ToArray(),out queryParameters,start,recordCount,OrderByFields);
		}

        internal virtual string SelectPaged(System.Type type, SelectParameter[] parameters, out List<IDbDataParameter> queryParameters, ulong? start, ulong? recordCount, string[] OrderByFields)
        {
            string query = Select(type, parameters, out queryParameters, OrderByFields);
            if (queryParameters == null)
                queryParameters = new List<IDbDataParameter>();
            if (!start.HasValue)
                start = 0;
            if (!recordCount.HasValue)
                recordCount = 0;
            queryParameters.Add(conn.CreateParameter(CreateParameterName("startIndex"), (long)start.Value));
            queryParameters.Add(conn.CreateParameter(CreateParameterName("rowCount"), (long)recordCount.Value));
            return String.Format(SelectWithPagingIncludeOffset, query, CreateParameterName("startIndex"), CreateParameterName("rowCount"));
        }

        internal string SelectPaged(string baseQuery, sTable mainMap, ref List<IDbDataParameter> queryParameters, ulong? start, ulong? recordCount)
        {
            return SelectPaged(baseQuery, mainMap, ref queryParameters, start, recordCount, null);
        }

        internal virtual string SelectPaged(string baseQuery,sTable mainMap, ref List<IDbDataParameter> queryParameters, ulong? start, ulong? recordCount, string[] OrderByFields)
        {
            if (!start.HasValue)
                start = 0;
            if (!recordCount.HasValue)
                recordCount = 0;
            queryParameters.Add(conn.CreateParameter(CreateParameterName("startIndex"), (long)start.Value));
            queryParameters.Add(conn.CreateParameter(CreateParameterName("rowCount"), (long)recordCount.Value));
            return String.Format(SelectWithPagingIncludeOffset, baseQuery, CreateParameterName("startIndex"), CreateParameterName("rowCount"));
        }
		#endregion
	}
}
