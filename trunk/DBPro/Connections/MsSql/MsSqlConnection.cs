using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using Org.Reddragonit.Dbpro.Connections;
using FieldType = Org.Reddragonit.Dbpro.Structure.Attributes.FieldType;
using VersionTypes = Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes;
using System.Reflection;
using System.Xml;
using System.IO;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System.Text.RegularExpressions;
using Org.Reddragonit.Dbpro.Virtual;
using Org.Reddragonit.Dbpro.Connections.Parameters;

namespace Org.Reddragonit.Dbpro.Connections.MsSql
{
	class MsSqlConnection : Connection
	{
        internal const string _ASSEMBLY_NAME = "System.Data";
        internal const string _CONNECTION_TYPE_NAME = "System.Data.SqlClient.SqlConnection";
        private const string _COMMAND_TYPE_NAME = "System.Data.SqlClient.SqlCommand";

        internal override string ConcatenationCharacter
        {
            get
            {
                return "+";
            }
        }
		
		internal override string DefaultTableString {
			get {
				return "sysobjects";
			}
		}
		
		internal override bool UsesIdentities {
			get { return true; }
		}

		public MsSqlConnection(ConnectionPool pool, string ConnectionString,bool Readonly,bool exclusiveLock)
			: base(pool, ConnectionString,Readonly,exclusiveLock)
		{ }

		internal override IDbTransaction EstablishExclusiveTransaction()
        {
            return conn.BeginTransaction(IsolationLevel.Serializable);
        }

		internal override void GetAddAutogen(ExtractedTableMap map, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers,out List<StoredProcedure> procedures)
		{
            Type t = Pool.Mapping[map.TableName];
            PropertyInfo pi = Pool.Mapping[map.TableName, map.PrimaryKeys[0].FieldName];
            bool imediate = t == null;
            if (imediate)
                t = Pool.Mapping.GetTypeForIntermediateTable(map.TableName, out pi);
			identities=new List<IdentityField>();
			generators=null;
			triggers = new List<Trigger>();
            procedures = new List<StoredProcedure>();
			ExtractedFieldMap field = map.PrimaryKeys[0];
			if ((map.PrimaryKeys.Count>1)&&(!field.AutoGen))
			{
				foreach (ExtractedFieldMap efm in map.PrimaryKeys)
				{
					if (efm.AutoGen)
					{
						field=efm;
						break;
					}
				}
			}
			string fields = "";
			string primarys="";
			foreach (ExtractedFieldMap efm in map.Fields)
			{
				if (!efm.AutoGen)
					fields+=","+efm.FieldName;
				if (efm.PrimaryKey)
					primarys+="tbl."+efm.FieldName+" = ins."+efm.FieldName+" AND ";
			}
			primarys = primarys.Substring(0,primarys.Length-4);
			if (field.Type.ToUpper().Contains("DATE")||field.Type.ToUpper().Contains("TIME"))
			{
				triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t,pi,this) : Pool.Translator.GetInsertTriggerName(t,this)),
				                         "ON "+map.TableName+" INSTEAD OF INSERT\n",
				                         "AS "+
				                         "BEGIN SET NOCOUNT ON;\n"+
                                         "INSERT INTO " + map.TableName + "(" + field.FieldName + fields + ") SELECT GETDATE()" + fields.Replace(",", ",tbl.") + " from INSERTED tbl;\n" +
				                         "END"));
			}else if (field.Type.ToUpper().Contains("INT")){
				if (map.PrimaryKeys.Count==1)
					identities.Add(new IdentityField(map.TableName,field.FieldName,field.FullFieldType,"1"));
				else
				{
					string code = "AS \n";
					string declares="";
					string sets="";
					string queryFields="";
                    string valueSets = "";
					foreach (ExtractedFieldMap efm in map.Fields)
					{
                        if (efm.ComputedCode == null)
                        {
                            declares += "DECLARE " + queryBuilder.CreateParameterName(efm.FieldName) + " " + efm.FullFieldType+ ";\n";
                            if (!efm.AutoGen)
                            {
                                sets += " SET " + queryBuilder.CreateParameterName(efm.FieldName) + " = (SELECT " + efm.FieldName + " FROM INSERTED);\n";
                                valueSets += "," + queryBuilder.CreateParameterName(efm.FieldName);
                                if (efm.PrimaryKey)
                                    queryFields += " AND " + efm.FieldName + " = " + queryBuilder.CreateParameterName(efm.FieldName);
                            }
                        }
					}
					code+=declares;
					code+=sets;
					code+="SET "+queryBuilder.CreateParameterName(field.FieldName)+" = (SELECT MAX("+field.FieldName+") FROM "+map.TableName+" WHERE ";
					code+=queryFields.Substring(4)+");\n";
					code+="IF ("+queryBuilder.CreateParameterName(field.FieldName)+" is NULL)\n";
					code+="\tSET "+queryBuilder.CreateParameterName(field.FieldName)+" = -1;\n";
					code+="INSERT INTO "+map.TableName+"("+field.FieldName+fields+") VALUES("+queryBuilder.CreateParameterName(field.FieldName)+"+1"+valueSets+");\n";
                    triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t, pi, this) : Pool.Translator.GetInsertTriggerName(t, this)), "ON " + map.TableName + " INSTEAD OF INSERT\n", code));
				}
            }
            else if (field.Type.ToUpper().Contains("VARCHAR")){
                XmlDocument doc = new XmlDocument();
                doc.LoadXml(new StreamReader(Assembly.GetAssembly(typeof(MsSqlConnection)).GetManifestResourceStream("Org.Reddragonit.Dbpro.Connections.MsSql.StringIDProcedures.xml")).ReadToEnd());
                foreach (XmlElement proc in doc.GetElementsByTagName("Procedure"))
                    procedures.Add(new StoredProcedure(proc.ChildNodes[0].InnerText,
                        proc.ChildNodes[1].InnerText,
                        proc.ChildNodes[2].InnerText,
                        proc.ChildNodes[3].InnerText,
                        proc.ChildNodes[4].InnerText));
                string code = "AS \n";
                code += string.Format(@"DECLARE @IDVAL VARCHAR(38),
                @cnt BIGINT;
                SET @cnt = 1;
                WHILE (@cnt>0)
                BEGIN
                	SET @IDVAL = (SELECT [dbo].[Org_Reddragonit_Dbpro_Connections_MsSql_GeneateUniqueID] (
                		   CEILING(RAND( (DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()) )*((DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()))*(Year(GetDate())*Month(GetDate())*Day(GetDate())*RAND()))
                		  ,CEILING(RAND( (DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()) )*((DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()))*(Year(GetDate())*Month(GetDate())*Day(GetDate())*RAND()))
                		  ,CEILING(RAND( (DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()) )*((DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()))*(Year(GetDate())*Month(GetDate())*Day(GetDate())*RAND()))
                		  ,CEILING(RAND( (DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()) )*((DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()))*(Year(GetDate())*Month(GetDate())*Day(GetDate())*RAND()))
                		  ,CEILING(RAND( (DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()) )*((DATEPART(mm, GETDATE()) * 100000 )+ (DATEPART(ss, GETDATE()) * 1000 )+ DATEPART(ms, GETDATE()))*(Year(GetDate())*Month(GetDate())*Day(GetDate())*RAND()))));
                	SET @cnt = (SELECT COUNT(*) FROM {0} WHERE {1} = @IDVAL);
                END
                INSERT INTO {0}({1}{2}) SELECT @IDVAL{3} from INSERTED tbl;",
                new object[]{map.TableName,field.FieldName, fields, fields.Replace(",", ",tbl.")});
                triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t, pi, this) : Pool.Translator.GetInsertTriggerName(t, this)),
                                         "ON " + map.TableName + " INSTEAD OF INSERT\n",
                                         code));
            }else
                throw new Exception("Unable to create autogenerator for non date or digit type.");
		}
		
		internal override void GetDropAutogenStrings(ExtractedTableMap map, out List<IdentityField> identities, out List<Generator> generators, out List<Trigger> triggers)
		{
            Type t = Pool.Mapping[map.TableName];
            PropertyInfo pi = Pool.Mapping[map.TableName, map.PrimaryKeys[0].FieldName];
            bool imediate = t == null;
            if (imediate)
                t = Pool.Mapping.GetTypeForIntermediateTable(map.TableName, out pi);
			identities=new List<IdentityField>();
			generators=null;
			triggers = new List<Trigger>();
			ExtractedFieldMap field=map.PrimaryKeys[0];
			if ((map.PrimaryKeys.Count>1)&&(!field.AutoGen))
			{
				foreach (ExtractedFieldMap efm in map.PrimaryKeys)
				{
					if (efm.AutoGen)
					{
						field=efm;
						break;
					}
				}
			}
            if (field.Type.ToUpper().Contains("DATE") || field.Type.ToUpper().Contains("TIME") || (map.PrimaryKeys.Count > 0) || (field.Type.ToUpper().Contains("VARCHAR")))
			{
                triggers.Add(new Trigger((imediate ? Pool.Translator.GetInsertIntermediateTriggerName(t, pi, this) : Pool.Translator.GetInsertTriggerName(t, this)), "", ""));
			}
			else if (field.Type.ToUpper().Contains("INT"))
			{
				identities.Add(new IdentityField(map.TableName,field.FieldName,field.FullFieldType,""));
			}
			else
			{
				throw new Exception("Unable to create autogenerator for non date or digit type.");
			}
		}

        protected override IDbCommand EstablishCommand()
        {
            return (IDbCommand)Utility.LocateType(_COMMAND_TYPE_NAME).GetConstructor(new Type[] { typeof(string), Utility.LocateType(_CONNECTION_TYPE_NAME) }).Invoke(new object[] { "", conn });
        }

        protected override IDbConnection EstablishConnection()
        {
            return (IDbConnection)Utility.LocateType(_CONNECTION_TYPE_NAME).GetConstructor(new Type[] { typeof(String) }).Invoke(new object[] { connectionString });
        }
		
		internal override List<Trigger> GetVersionTableTriggers(ExtractedTableMap table,VersionTypes versionType)
		{
            Type t = Pool.Mapping.GetTypeForVersionTable(table.TableName);
			List<Trigger> ret = new List<Trigger>();
			string tmp = "AS \n BEGIN\n";
			tmp+="\tINSERT INTO "+table.TableName+" SELECT null";
			for(int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				tmp+=",tbl."+efm.FieldName;
			}
			tmp+=" FROM INSERTED ins, "+Pool.Mapping[t].Name+" tbl WHERE ";
			for (int x=1;x<table.Fields.Count;x++)
			{
				ExtractedFieldMap efm = table.Fields[x];
				if (efm.PrimaryKey)
				{
					tmp+="tbl."+efm.FieldName+" = ins."+efm.FieldName+" AND ";					
				}
			}
			tmp=tmp.Substring(0,tmp.Length-4)+";";
			tmp+="\nEND\n\n";
			ret.Add(new Trigger(Pool.Translator.GetVersionInsertTriggerName(t,this),
			                    "ON "+Pool.Mapping[t].Name+" AFTER INSERT,UPDATE ",
			                                     tmp));
			return ret;
		}

        internal override List<Trigger> GetDeleteParentTrigger(ExtractedTableMap table, ExtractedTableMap parent)
        {
            List<Trigger> ret = new List<Trigger>();
            string tmp = "AS \n BEGIN \n IF ((SELECT COUNT(*) FROM DELETED)>0) BEGIN DELETE FROM " + parent.TableName + " WHERE ";
            string fields = "(";
            foreach (ExtractedFieldMap efm in parent.PrimaryKeys)
                fields += string.Format("CAST({0} AS VARCHAR(MAX))+'-'+",efm.FieldName);
            fields = fields.Substring(0, fields.Length - 5) + ")";
            tmp += fields + " IN (SELECT " + fields + " FROM DELETED);\nEND\nEND\n\n";
            ret.Add(new Trigger(Pool.Translator.GetDeleteParentTriggerName(Pool.Mapping[table.TableName],this),
                "ON " + table.TableName + " AFTER DELETE",
                tmp));
            return ret;
        }

		internal override string TranslateFieldType(FieldType type, int fieldLength)
		{
			string ret = null;
			switch (type)
			{
				case FieldType.BOOLEAN:
					ret = "BIT";
					break;
				case FieldType.BYTE:
					if ((fieldLength == -1)||(fieldLength>8000))
						ret = "VARBINARY(MAX)";
					else
						ret = "VARBINARY(" + fieldLength.ToString() + ")";
					break;
				case FieldType.CHAR:
					if ((fieldLength == -1)||(fieldLength>8000))
						ret = "VARCHAR(MAX)";
					else
						ret = "VARCHAR(" + fieldLength.ToString() + ")";
					break;
				case FieldType.DATE:
				case FieldType.DATETIME:
				case FieldType.TIME:
					ret = "DATETIME";
					break;
				case FieldType.DECIMAL:
				case FieldType.DOUBLE:
                    ret = "DECIMAL(18,9)";
                    fieldLength = 8;
					break;
				case FieldType.FLOAT:
					ret = "FLOAT";
					break;
				case FieldType.IMAGE:
					ret = "IMAGE";
					break;
				case FieldType.INTEGER:
				case FieldType.ENUM:
					ret = "INT";
					break;
                case FieldType.UNSIGNED_INTEGER:
                    ret = "CHAR(4)";
                    break;
				case FieldType.LONG:
					ret = "BIGINT";
					break;
                case FieldType.UNSIGNED_LONG:
                    ret = "CHAR(8)";
                    break;
				case FieldType.MONEY:
					ret = "MONEY";
					break;
				case FieldType.SHORT:
					ret = "SMALLINT";
					break;
                case FieldType.UNSIGNED_SHORT:
                    ret = "CHAR(2)";
                    break;
				case FieldType.STRING:
					if ((fieldLength == -1)||(fieldLength>8000))
						ret = "VARCHAR(MAX)";
					else
						ret = "VARCHAR(" + fieldLength.ToString() + ")";
					break;
			}
			return ret;
		}

        internal override void DisableAutogens()
        {
            string query = "";
            this.ExecuteQuery(queryBuilder.SelectIdentities());
            while (this.Read())
            {
                query += "SET IDENTITY_INSERT "+this[0].ToString()+" ON;\n";
            }
            this.Close();
            this.ExecuteQuery(queryBuilder.SelectTableNames());
            while (this.Read())
            {
                query += "ALTER TABLE " + this[0].ToString() + " DISABLE TRIGGER all;\n";
            }
            this.Close();
            Logger.LogLine("Disabling of autogens for MSSQL resulting query: " + query);
            foreach (string str in query.Split('\n'))
            {
                if (str.Trim().Length > 0)
                    this.ExecuteNonQuery(str);
            }
        }

        internal override void EnableAndResetAutogens()
        {
            string query = "";
            this.ExecuteQuery(queryBuilder.SelectIdentities());
            while (this.Read())
            {
                query += "SET IDENTITY_INSERT " + this[0].ToString() + " OFF;\n";
            }
            this.ExecuteQuery(queryBuilder.SelectTableNames());
            while (this.Read())
            {
                query += "ALTER TABLE " + this[0].ToString() + " ENABLE TRIGGER all;\n";
            }
            this.Close();
            List<IdentityField> identities = new List<IdentityField>();
            this.ExecuteQuery(this.queryBuilder.SelectIdentities());
            while (this.Read())
            {
                identities.Add(new IdentityField((string)this[0], (string)this[1], (string)this[2], (string)this[3]));
            }
            this.Close();
            foreach (IdentityField id in identities)
            {
                this.ExecuteQuery("SELECT ISNULL(MAX(" + id.FieldName + "),0)+1 FROM " + id.TableName);
                this.Read();
                query += queryBuilder.SetIdentityFieldValue(new IdentityField(id.TableName,id.FieldName,id.FieldType,this[0].ToString()))+"\n";
                this.Close();
            }
            Logger.LogLine("Enabling and resetting of autogens for MSSQL resulting query: " + query);
            foreach (string str in query.Split('\n'))
            {
                if (str.Trim().Length > 0)
                    this.ExecuteNonQuery(str);
            }
        }

        private const string _DEL_TRIGGER_NAME = "TRIG_DEL_{0}";
        private const string _UPDATE_TRIGGER_NAME = "TRIG_UPDATE_{0}";

        internal void MaskComplicatedRelations(int index, ref List<ExtractedTableMap> tables, ref List<Trigger> triggers,Dictionary<string,string> AutoDeleteParentTables)
        {
            ExtractedTableMap map = tables[index];
            foreach (string tblName in map.RelatedTables)
            {
                bool createTriggers = false;
                List<List<ForeignRelationMap>> maps = map.RelatedFieldsForTable(tblName);
                if (maps.Count > 1)
                {
                    foreach (List<ForeignRelationMap> rel in maps)
                    {
                        foreach (ForeignRelationMap frm in rel)
                        {
                            if (map.GetField(frm.InternalField).PrimaryKey && (
                                frm.OnUpdate == ForeignField.UpdateDeleteAction.CASCADE.ToString().Replace("_","")||
                                frm.OnDelete == ForeignField.UpdateDeleteAction.CASCADE.ToString().Replace("_", "")))
                            {
                                createTriggers = true;
                                break;
                            }
                        }
                        if (createTriggers)
                            break;
                    }
                }
                if (!createTriggers&&
                    (maps[0][0].OnUpdate == ForeignField.UpdateDeleteAction.CASCADE.ToString().Replace("_", "")
                        || maps[0][0].OnUpdate == ForeignField.UpdateDeleteAction.CASCADE.ToString().Replace("_", "")))
                {
                    foreach (ExtractedTableMap etm in tables)
                    {
                        if (etm.TableName == tblName)
                        {
                            foreach (string cstr in map.RelatedTables)
                            {
                                if (cstr != tblName && etm.RelatesToTable(cstr,tables))
                                {
                                    createTriggers = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                if (!createTriggers &&
                    (maps[0][0].OnUpdate == ForeignField.UpdateDeleteAction.CASCADE.ToString().Replace("_", "")
                        || maps[0][0].OnUpdate == ForeignField.UpdateDeleteAction.CASCADE.ToString().Replace("_", "")))
                {
                    foreach (Trigger t in triggers)
                    {
                        if (t.Conditions.Contains("INSTEAD OF") 
                            && (t.Conditions.Contains(tblName)||t.Conditions.Contains(map.TableName))
                            && (t.Conditions.Contains("UPDATE")||t.Conditions.Contains("DELETE")))
                        {
                            createTriggers = true;
                            break;
                        }
                    }
                }
                if (createTriggers)
                {
                    for (int x = 0; x < map.ForeignFields.Count; x++)
                    {
                        if (map.ForeignFields[x].ExternalTable == tblName)
                        {
                            map.ForeignFields[x] = new ForeignRelationMap(map.ForeignFields[x].ID, map.ForeignFields[x].InternalField,
                                map.ForeignFields[x].ExternalTable,
                                map.ForeignFields[x].ExternalField,
                                ForeignField.UpdateDeleteAction.NO_ACTION.ToString(),
                                ForeignField.UpdateDeleteAction.NO_ACTION.ToString());
                        }
                    }
                    List<ForeignRelationMap> frms;
                    for (int x = 0; x < maps.Count; x++)
                    {
                        for (int y = x + 1; y < maps.Count; y++)
                        {
                            if (maps[x][0].ExternalTable.Equals(maps[y][0].ExternalTable))
                            {
                                if (maps[x][0].InternalField.CompareTo(maps[y][0].InternalField) > 0)
                                {
                                    frms = maps[y];
                                    maps[y] = maps[x];
                                    maps[x] = frms;
                                }
                            }
                            else if (maps[x][0].ExternalTable.CompareTo(maps[y][0].ExternalTable) > 0)
                            {
                                frms = maps[y];
                                maps[y] = maps[x];
                                maps[x] = frms;
                            }

                        }
                    }
                    string delCode = "AS\nBEGIN\nSET NOCOUNT ON;\n";
                    string fields = "";
                    for(int x=0;x<triggers.Count;x++){
                        Trigger t = triggers[x];
                        if (t.Conditions==string.Format("ON {0} INSTEAD OF DELETE",tblName)){
                            triggers.RemoveAt(x);
                            delCode = t.Code;
                            if (delCode.Contains("DELETE FROM " + tblName+" WHERE "))
                                delCode = delCode.Substring(0, delCode.LastIndexOf("DELETE FROM " + tblName+" WHERE "));
                            break;
                        }
                    }
                    for(int x=0;x<maps.Count;x++)
                    {
                        List<ForeignRelationMap> rel = maps[x];
                        string disableTrigger = "";
                        string enableTrigger = "";
                        if (AutoDeleteParentTables.ContainsKey(map.TableName))
                        {
                            disableTrigger = string.Format("ALTER TABLE {0} DISABLE TRIGGER {0}_DEL;\n", map.TableName);
                            enableTrigger = string.Format("ALTER TABLE {0} ENABLE TRIGGER {0}_DEL;\n", map.TableName);
                        }
                        delCode+=string.Format("IF ((SELECT COUNT(*) FROM DELETED)>0) BEGIN {1} DELETE FROM {0} WHERE (",map.TableName,disableTrigger);
                        fields = "";
                        foreach (ForeignRelationMap frm in rel)
                        {
                            delCode += string.Format("CAST({0} AS VARCHAR(MAX))+'-'+",frm.InternalField);
                            fields += string.Format("CAST({0} AS VARCHAR(MAX))+'-'+", frm.ExternalField);
                        }
                        delCode = delCode.Substring(0,delCode.Length-5);
                        delCode += string.Format(") IN (SELECT ({0}) FROM DELETED); {1} END\n",
                            fields.Substring(0, fields.Length - 5),
                            enableTrigger);
                        if (x == maps.Count - 1)
                        {
                            delCode += string.Format("DELETE FROM {0} WHERE ({1}) IN (SELECT ({1}) FROM DELETED);\nEND\n\n",
                                tblName,
                                fields.Substring(0, fields.Length - 5));
                        }
                    }
                    triggers.Add(new Trigger(string.Format(_DEL_TRIGGER_NAME, tblName),
                        string.Format("ON {0} INSTEAD OF DELETE", tblName),
                        Utility.RemoveDuplicateStrings(delCode,new string []{"COMMIT;","END","BEGIN"})));
                    string updCode = @"AS
BEGIN
    SET NOCOUNT ON;
    ";
                    fields = "";
                    string compares = "";
                    foreach (ExtractedTableMap etm in tables)
                    {
                        if (etm.TableName == tblName)
                        {
                            foreach (ExtractedFieldMap efm in etm.Fields)
                            {
                                if (efm.ComputedCode == null){
                                    updCode+=string.Format("DECLARE @NEW_{0} AS {1};\nDECLARE @OLD_{0} AS {1};\n",
                                        efm.FieldName,
                                        efm.FullFieldType);
                                    fields += string.Format("{0},", efm.FieldName);
                                    if (efm.PrimaryKey)
                                        compares += string.Format("@NEW_{0}<>@OLD_{0} OR ", efm.FieldName);
                                }
                            }
                            fields = fields.Substring(0,fields.Length-1);
                            updCode += string.Format(@"DECLARE InsertCursor CURSOR LOCAL FOR SELECT {0} FROM INSERTED;
DECLARE DeleteCursor CURSOR LOCAL FOR SELECT {0} FROM DELETED;
OPEN InsertCursor;
OPEN DeleteCursor;
FETCH NEXT FROM InsertCursor INTO @NEW_{1};
FETCH NEXT FROM DeleteCursor INTO @OLD_{2};
WHILE @@FETCH_STATUS = 0
BEGIN
IF ({3})
BEGIN
", new object[]{fields,fields.Replace(",",",@NEW_"),fields.Replace(",",",@OLD_"),compares.Substring(0,compares.Length-3)});
                            break;
                        }
                    }
                    for (int x = 0; x < triggers.Count; x++)
                    {
                        Trigger t = triggers[x];
                        if (t.Conditions == string.Format("ON {0} INSTEAD OF UPDATE", tblName))
                        {
                            triggers.RemoveAt(x);
                            updCode = t.Code;
                            if (updCode.Contains("UPDATE " + tblName + " "))
                            {
                                updCode = updCode.Substring(0, updCode.IndexOf("UPDATE " + tblName + " ")).Trim();
                                updCode = updCode.Substring(0, updCode.LastIndexOf("END"));
                            }
                            break;
                        }
                    }
                    foreach (List<ForeignRelationMap> rel in maps)
                    {
                        updCode = updCode.Replace("WHILE @@FETCH_STATUS = 0",
                            string.Format(@"ALTER TABLE {0} NOCHECK CONSTRAINT ALL;
WHILE @@FETCH_STATUS = 0", map.TableName));
                        updCode += string.Format(@"
UPDATE {0} SET ", map.TableName);
                        string where = " WHERE ";
                        foreach (ForeignRelationMap frm in rel)
                        {
                            updCode += string.Format("{0} = @NEW_{1},",
                                frm.InternalField,
                                frm.ExternalField);
                            where+=string.Format("{0} = @OLD_{1} AND ",
                                frm.InternalField,
                                frm.ExternalField);
                        }
                        updCode = updCode.Substring(0, updCode.Length - 1) +
                            where.Substring(0, where.Length - 4);
                        updCode += ";\n";
                    }
                    foreach (ExtractedTableMap etm in tables)
                    {
                        if (etm.TableName == tblName)
                        {
                            updCode += string.Format("END\nUPDATE {0} SET ", tblName);
                            string where = " WHERE ";
                            foreach (ExtractedFieldMap efm in etm.Fields)
                            {
                                if (efm.ComputedCode == null)
                                {
                                    if (!efm.PrimaryKey || etm.PrimaryKeys.Count>1)
                                        updCode += string.Format("{0} = @NEW_{0},",
                                            efm.FieldName);
                                    if (efm.PrimaryKey)
                                        where += string.Format("{0} = @OLD_{0} AND ",
                                            efm.FieldName);
                                }
                            }
                            updCode = updCode.Substring(0,updCode.Length-1)+
                                where.Substring(0,where.Length-4)+string.Format(@";
FETCH NEXT FROM InsertCursor INTO @NEW_{0};
FETCH NEXT FROM DeleteCursor INTO @OLD_{1};
END
CLOSE InsertCursor;
DEALLOCATE InsertCursor;
CLOSE DeleteCursor;
DEALLOCATE DeleteCursor;
", fields.Replace(",",",@NEW_"),fields.Replace(",",",@OLD_"));
                            break;
                        }
                    }
                    Regex reg = new Regex("ALTER TABLE (.+) NOCHECK CONSTRAINT ALL;", RegexOptions.ECMAScript | RegexOptions.Compiled);
                    string checks = "";
                    foreach (Match m in reg.Matches(updCode))
                        checks += string.Format("\nALTER TABLE {0} CHECK CONSTRAINT ALL;", m.Groups[1].Value);
                    triggers.Add(new Trigger(string.Format(_UPDATE_TRIGGER_NAME, tblName),
                        string.Format("ON {0} INSTEAD OF UPDATE", tblName),
                        Utility.RemoveDuplicateStrings(updCode + checks + "\nEND\n\n", new string[] { "COMMIT;", "END", "BEGIN", string.Format("FETCH NEXT FROM InsertCursor INTO @NEW_{0};", fields.Replace(",", ",@NEW_")), string.Format("FETCH NEXT FROM DeleteCursor INTO @OLD_{0};", fields.Replace(",", ",@OLD_")) })));
                    //recurse back through first ones checked to adjust relationships accordingly
                    //once using INSTEAD OF, all relationship cascades MUST be done in the trigger
                    for (int x = 0; x < index; x++)
                    {
                        MaskComplicatedRelations(x, ref tables, ref triggers,AutoDeleteParentTables);
                    }
                }
            }
        }

        #region ClassView
        public override List<Org.Reddragonit.Dbpro.Virtual.IClassView> SelectClassView(Type type, Org.Reddragonit.Dbpro.Connections.Parameters.SelectParameter[] pars, string[] OrderByFields)
        {
            if (OrderByFields != null && OrderByFields.Length > 0)
            {
                ClassViewAttribute cva = Pool[type];
                if (cva == null || !new List<Type>(type.GetInterfaces()).Contains(typeof(IClassView)))
                    throw new Exception("Unable to execute a Class View Query from a class that does not have a ClassViewAttributes attached to it as well as has the interface IClassView.");
                Pool.Updater.InitType(type, this);
                List<IClassView> ret = new List<IClassView>();
                int parCount = 0;
                List<IDbDataParameter> queryParameters = new List<IDbDataParameter>();
                string parString = "";
                string orderByString = "";
                if (pars != null)
                {
                    foreach (SelectParameter par in pars)
                    {
                        foreach (string str in par.Fields)
                        {
                            if (cva.Query.GetOrdinal(str) == -1)
                                throw new Exception("Unable to execute a Class View Query with parameters that are not fields in the Class View");
                        }
                        parString += " AND ( " + par.ConstructClassViewString(cva, Pool, queryBuilder, ref queryParameters, ref parCount) + " ) ";
                    }
                }
                if (OrderByFields != null)
                {
                    foreach (string str in OrderByFields)
                    {
                        if (str.EndsWith(" ASC") || str.EndsWith(" DESC"))
                        {
                            if (cva.Query.GetOrdinal(str.Split(new char[] { ' ' })[0]) == -1)
                                throw new Exception("Unable to execute a Class View Query with Order By Fields that are not fields in the Class View");
                        }
                        else
                        {
                            if (cva.Query.GetOrdinal(str) == -1)
                                throw new Exception("Unable to execute a Class View Query with Order By Fields that are not fields in the Class View");
                        }
                        orderByString += "," + str;
                    }
                }
                this.ExecuteQuery("SELECT * FROM ("+cva.Query.QueryString+") tbl "+ (parString == "" ? "" : " WHERE " + parString.Substring(4)) + (orderByString == "" ? "" : " ORDER BY " + orderByString.Substring(1)), queryParameters.ToArray());
                ViewResultRow vrr = new ViewResultRow(this);
                while (Read())
                {
                    IClassView icv = (IClassView)type.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
                    icv.LoadFromRow(vrr);
                    ret.Add(icv);
                }
                Close();
                return ret;
            }
            return base.SelectClassView(type, pars, OrderByFields);
        }

        public override List<IClassView> SelectPagedClassView(Type type, List<SelectParameter> parameters, ulong? StartIndex, ulong? RowCount, string[] OrderByFields)
        {
            ClassViewAttribute cva = Pool[type];
            if (cva == null || !new List<Type>(type.GetInterfaces()).Contains(typeof(IClassView)))
                throw new Exception("Unable to execute a Class View Query from a class that does not have a ClassViewAttribute attached to it and inherits IClassView.");
            if (OrderByFields == null)
                throw new Exception("Unable to execute a Paged Class View Query without specifying the OrderByFields");
            Pool.Updater.InitType(type, this);
            if (!StartIndex.HasValue)
                StartIndex = 0;
            if (!RowCount.HasValue)
                RowCount = 0;
            List<IClassView> ret = new List<IClassView>();
            int parCount = 0;
            List<IDbDataParameter> queryParameters = new List<IDbDataParameter>();
            string parString = "";
            string orderByString = "";
            if (parameters != null)
            {
                foreach (SelectParameter par in parameters)
                {
                    foreach (string str in par.Fields)
                    {
                        if (cva.Query.GetOrdinal(str) == -1)
                            throw new Exception("Unable to execute a Class View Query with parameters that are not fields in the Class View");
                    }
                    parString += " AND ( " + par.ConstructClassViewString(cva, Pool, queryBuilder, ref queryParameters, ref parCount) + " ) ";
                }
            }
            foreach (string str in OrderByFields)
            {
                if (str.EndsWith(" ASC") || str.EndsWith(" DESC"))
                {
                    if (cva.Query.GetOrdinal(str.Split(new char[] { ' ' })[0]) == -1)
                        throw new Exception("Unable to execute a Class View Query with Order By Fields that are not fields in the Class View");
                }
                else
                {
                    if (cva.Query.GetOrdinal(str) == -1)
                        throw new Exception("Unable to execute a Class View Query with Order By Fields that are not fields in the Class View");
                }
                orderByString += "," + str;
            }
            queryParameters.Add(Pool.CreateParameter(queryBuilder.CreateParameterName("startIndex"), (long)StartIndex.Value));
            queryParameters.Add(Pool.CreateParameter(queryBuilder.CreateParameterName("rowCount"), (long)RowCount.Value));
            this.ExecuteQuery(string.Format(@"SELECT * FROM (SELECT *,ROW_NUMBER() OVER (ORDER BY {3}) RowNum
					 FROM ({0}) internalTbl {4}) cntTbl WHERE RowNum BETWEEN {1} AND {1}+{2}",
                        new object[]{
                            cva.Query.QueryString,
                            queryBuilder.CreateParameterName("startIndex"),
                            queryBuilder.CreateParameterName("rowCount"),
                            orderByString.Substring(1),
                            (parString=="" ? "" : "WHERE "+parString.Substring(4))
                        })
                , queryParameters.ToArray());
            ViewResultRow vrr = new ViewResultRow(this);
            while (Read())
            {
                IClassView icv = (IClassView)type.GetConstructor(Type.EmptyTypes).Invoke(new object[0]);
                icv.LoadFromRow(vrr);
                ret.Add(icv);
            }
            Close();
            return ret;
        }
        #endregion
    }
}
