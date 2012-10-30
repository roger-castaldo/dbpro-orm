using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Connections.PoolComponents
{
    internal class NameTranslator
    {
        private const string _INTERMEDIATE_INDEX_FIELD_NAME = "VALUE_INDEX";
        private const string _INTERMEDIATE_VALUE_FIELD_NAME = "VALUE";
        private const string _ENUM_ID_FIELD_NAME = "ID";
        private const string _ENUM_VALUE_FIELD_NAME = "VALUE";

        private const string _ENUM_TABLE_DESCRIPTION = "ENUM:{0}";
        private const string _TABLE_DESCRIPTION = "TBL:{0}";
        private const string _INTERMEDIATE_TABLE_DESCRIPTION = "ITMD:{0}\t{1}";
        private const string _FIELD_DESCRIPTION = "FLD:{0}.{1}";
        private const string _INTERMEDIATE_FIELD_DESCRIPTION = "ITMD_FLD:{0}.{1}.{2}_{3}";
        private const string _GENERATOR_DESCRIPTION = "GEN:{0}.{1}";
        private const string _TRIGGER_DESCRIPTION = "TRIG:{0}.{1}";
        private const string _VIEW_DESCRIPTION = "VIEW:{0}\t[{1}]";
        private const string _VIEW_FIELD_FORMAT = "{0}:{1}";
        private const string _INDEX_DESCRIPTION = "IND:{0}.{1}";

        private ConnectionPool _pool;
        private Dictionary<string, string> _nameTranslations;
        private List<string> _createDescriptions;

        public void ApplyAllDescriptions(List<ExtractedTableMap> tables, List<Trigger> triggers, List<Generator> generators, List<IdentityField> identities, List<View> views, List<StoredProcedure> procedures, Connection conn)
        {
            Type t;
            PropertyInfo pi;
            for(int x=0;x<_createDescriptions.Count;x++)
            {
                string str = _createDescriptions[x];
                bool remove = false;
                switch (str.Split(':')[0])
                {
                    case "ENUM":
                    case "TBL":
                    case "ITMD":
                        foreach (ExtractedTableMap etm in tables)
                        {
                            if (etm.TableName == _nameTranslations[str])
                            {
                                conn.ExecuteNonQuery(conn.queryBuilder.SetTableDescription(etm.TableName, str));
                                remove = true;
                                break;
                            }
                        }
                        break;
                    case "FLD":
                        foreach (ExtractedTableMap etm in tables)
                        {
                            t = _pool.Mapping[etm.TableName];
                            if (t != null)
                            {
                                if (str.StartsWith(string.Format(_FIELD_DESCRIPTION, t.FullName, "")))
                                {
                                    foreach (ExtractedFieldMap efm in etm.Fields)
                                    {
                                        if (_nameTranslations[str] == efm.FieldName)
                                        {
                                            conn.ExecuteNonQuery(conn.queryBuilder.SetFieldDescription(etm.TableName, efm.FieldName, str));
                                            remove = true;
                                            break;
                                        }
                                    }
                                    if (remove)
                                        break;
                                }
                            }
                        }
                        break;
                    case "ITMD_FLD":
                        foreach (ExtractedTableMap etm in tables)
                        {
                            t = _pool.Mapping.GetTypeForIntermediateTable(etm.TableName, out pi);
                            if (t != null)
                            {
                                if (str.StartsWith(string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, "",""}).Trim('.')))
                                {
                                    foreach (ExtractedFieldMap efm in etm.Fields)
                                    {
                                        if (_nameTranslations[str] == efm.FieldName)
                                        {
                                            conn.ExecuteNonQuery(conn.queryBuilder.SetFieldDescription(etm.TableName, efm.FieldName, str));
                                            remove = true;
                                            break;
                                        }
                                    }
                                    if (remove)
                                        break;
                                }
                            }
                        }
                        break;
                    case "GEN":
                        foreach (Generator gen in generators)
                        {
                            if (gen.Name == _nameTranslations[str])
                            {
                                conn.ExecuteNonQuery(conn.queryBuilder.SetGeneratorDescription(gen.Name, str));
                                remove = true;
                                break;
                            }
                        }
                        break;
                    case "TRIG":
                        foreach (Trigger trig in triggers)
                        {
                            if (trig.Name == _nameTranslations[str])
                            {
                                conn.ExecuteNonQuery(conn.queryBuilder.SetTriggerDescription(trig.Name, str));
                                remove = true;
                                break;
                            }
                        }
                        break;
                    case "VIEW":
                        foreach (View vw in views)
                        {
                            if (vw.Name == _nameTranslations[str])
                            {
                                conn.ExecuteNonQuery(conn.queryBuilder.SetViewDescription(vw.Name, str));
                                remove = true;
                                break;

                            }
                        }
                        break;
                    case "IND":
                        foreach (ExtractedTableMap etm in tables)
                        {
                            foreach (Index ind in etm.Indices)
                            {
                                if (_nameTranslations[str] == ind.Name)
                                {
                                    conn.ExecuteNonQuery(conn.queryBuilder.SetIndexDescription(ind.Name, str));
                                    remove = true;
                                    break;
                                }
                            }
                            if (remove)
                                break;
                        }
                        break;
                }
                if (remove)
                {
                    _createDescriptions.RemoveAt(x);
                    x--;
                }
            }
        }

        public NameTranslator(ConnectionPool pool, Connection conn)
        {
            _pool = pool;
            _nameTranslations = new Dictionary<string, string>();
            conn.ExecuteQuery(conn.queryBuilder.GetAllObjectDescriptions());
            while (conn.Read())
                _nameTranslations.Add(conn[0].ToString().Trim(), conn[1].ToString().Trim());
            conn.Close();
            _createDescriptions = new List<string>();
        }

        private string _ConvertCamelCaseName(string name)
        {
            if (name.ToUpper() == name)
                return name;
            string ret = "";
            foreach (char c in name.ToCharArray())
            {
                if (c.ToString().ToUpper() == c.ToString())
                {
                    ret += "_" + c.ToString().ToLower();
                }
                else
                {
                    ret += c;
                }
            }
            if (ret[0] == '_')
            {
                ret = ret[1].ToString().ToUpper() + ret.Substring(2);
            }
            ret = ret.ToUpper();
            ret = ret.Replace("__", "_");
            return ret;
        }

        private string _ExtractTableName(Type tbl)
        {
            string ret = null;
            if (tbl.GetCustomAttributes(typeof(Org.Reddragonit.Dbpro.Structure.Attributes.Table), false).Length > 0)
                ret = ((Org.Reddragonit.Dbpro.Structure.Attributes.Table)tbl.GetCustomAttributes(typeof(Org.Reddragonit.Dbpro.Structure.Attributes.Table), false)[0]).TableName;
            if (ret == null)
                ret = _ConvertCamelCaseName(tbl.Name);
            return ret;
        }

        private List<string> TableNames
        {
            get
            {
                List<string> ret = new List<string>();
                foreach (string str in _nameTranslations.Keys)
                {
                    if (str.StartsWith(string.Format(_ENUM_TABLE_DESCRIPTION,"")) ||
                        str.StartsWith(string.Format(_TABLE_DESCRIPTION,"")) ||
                        str.StartsWith(string.Format(_INTERMEDIATE_TABLE_DESCRIPTION,"","").Trim('\t')))
                        ret.Add(_nameTranslations[str]);
                }
                return ret;
            }
        }

        private List<string> GetTableFields(Type t)
        {
            List<string> ret = new List<string>();
            foreach (string str in _nameTranslations.Keys)
            {
                if (str.StartsWith(string.Format(_FIELD_DESCRIPTION, t.FullName, "")))
                    ret.Add(_nameTranslations[str]);
            }
            return ret;
        }

        private List<string> GetTableFields(Type t, PropertyInfo pi)
        {
            List<string> ret = new List<string>();
            foreach (string str in _nameTranslations.Keys)
            {
                if (str.StartsWith(string.Format(_INTERMEDIATE_FIELD_DESCRIPTION,new object[]{t.FullName,pi.Name,"",""}).Trim('.')))
                    ret.Add(_nameTranslations[str]);
            }
            return ret;
        }

        private List<string> GeneratorNames
        {
            get
            {
                List<string> ret = new List<string>();
                foreach (string str in _nameTranslations.Keys)
                {
                    if (str.StartsWith(string.Format(_GENERATOR_DESCRIPTION, "", "").Trim('.')))
                        ret.Add(_nameTranslations[str]);
                }
                return ret;
            }
        }

        private List<string> TriggerNames
        {
            get
            {
                List<string> ret = new List<string>();
                foreach (string str in _nameTranslations.Keys)
                {
                    if (str.StartsWith(string.Format(_TRIGGER_DESCRIPTION, "", "").Trim('.')))
                        ret.Add(_nameTranslations[str]);
                }
                return ret;
            }
        }

        private List<string> ViewNames
        {
            get
            {
                List<string> ret = new List<string>();
                foreach (string str in _nameTranslations.Keys)
                {
                    if (str.StartsWith("VIEW:"))
                        ret.Add(_nameTranslations[str]);
                }
                return ret;
            }
        }

        private List<string> GetIndexNames(Type t)
        {
            List<string> ret = new List<string>();
            foreach (string str in _nameTranslations.Keys)
            {
                if (str.StartsWith(string.Format(_INDEX_DESCRIPTION, t.FullName, "")))
                    ret.Add(_nameTranslations[str]);
            }
            return ret;
        }

        internal string GetTableName(Type t,Connection conn)
        {
            string ret = "";
            if (t.IsEnum)
            {
                if (_nameTranslations.ContainsKey(string.Format(_ENUM_TABLE_DESCRIPTION, t.FullName)))
                    ret = _nameTranslations[string.Format(_ENUM_TABLE_DESCRIPTION, t.FullName)];
                else
                {
                    ret = CorrectName("ENUM_" + _ConvertCamelCaseName(t.FullName.Substring(t.FullName.LastIndexOf(".") + 1).Replace("+", "")), TableNames);
                    _nameTranslations.Add(string.Format(_ENUM_TABLE_DESCRIPTION, t.FullName), ret);
                    _createDescriptions.Add(string.Format(_ENUM_TABLE_DESCRIPTION, t.FullName));
                }
            }
            else
            {
                if (_nameTranslations.ContainsKey(string.Format(_TABLE_DESCRIPTION, t.FullName)))
                    ret = _nameTranslations[string.Format(_TABLE_DESCRIPTION, t.FullName)];
                else
                {
                    ret = CorrectName(_ExtractTableName(t), TableNames);
                    _nameTranslations.Add(string.Format(_TABLE_DESCRIPTION, t.FullName), ret);
                    _createDescriptions.Add(string.Format(_TABLE_DESCRIPTION, t.FullName));
                }
            }
            return ret;
        }

        internal string GetVersionTableName(Type t, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_TABLE_DESCRIPTION, t.FullName+"_VERSION")))
                ret = _nameTranslations[string.Format(_TABLE_DESCRIPTION, t.FullName + "_VERSION")];
            else
            {
                ret = CorrectName(_ExtractTableName(t) + "_VERSION", TableNames);
                _nameTranslations.Add(string.Format(_TABLE_DESCRIPTION, t.FullName + "_VERSION"), ret);
                _createDescriptions.Add(string.Format(_TABLE_DESCRIPTION, t.FullName + "_VERSION"));
            }
            return ret;
        }

        internal string GetIntermediateTableName(Type t, PropertyInfo pi, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_INTERMEDIATE_TABLE_DESCRIPTION, t.FullName, pi.Name)))
                ret = _nameTranslations[string.Format(_INTERMEDIATE_TABLE_DESCRIPTION, t.FullName, pi.Name)];
            else
            {
                ret = CorrectName(GetTableName(t, conn) + "_" + _ConvertCamelCaseName(pi.Name),TableNames);
                _nameTranslations.Add(string.Format(_INTERMEDIATE_TABLE_DESCRIPTION,t.FullName,pi.Name),ret);
                _createDescriptions.Add(string.Format(_INTERMEDIATE_TABLE_DESCRIPTION, t.FullName, pi.Name));
            }
            return ret;
        }

        internal string GetFieldName(Type t, PropertyInfo pi, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_FIELD_DESCRIPTION, t.FullName, pi.Name)))
                ret = _nameTranslations[string.Format(_FIELD_DESCRIPTION, t.FullName, pi.Name)];
            else
            {
                string fldName = _ConvertCamelCaseName(pi.Name);
                foreach (object obj in pi.GetCustomAttributes(false))
                {
                    if (obj is IField)
                    {
                        if (((IField)obj).Name != null)
                            fldName = ((IField)obj).Name;
                        else if (obj is Field)
                        {
                            ((Field)obj).InitFieldName(pi);
                            fldName = ((Field)obj).Name;
                        }
                        break;
                    }
                }
                ret = CorrectName(fldName, GetTableFields(t));
                _nameTranslations.Add(string.Format(_FIELD_DESCRIPTION, t.FullName, pi.Name), ret);
                _createDescriptions.Add(string.Format(_FIELD_DESCRIPTION, t.FullName, pi.Name));
            }
            return ret;
        }

        internal string GetFieldName(Type t, PropertyInfo pi, string fieldName, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_FIELD_DESCRIPTION, t.FullName, pi.Name+"."+fieldName)))
                ret = _nameTranslations[string.Format(_FIELD_DESCRIPTION, t.FullName, pi.Name + "." + fieldName)];
            else
            {
                string fldName = _ConvertCamelCaseName(pi.Name)+"_"+fieldName;
                ret = CorrectName(fldName, GetTableFields(t));
                _nameTranslations.Add(string.Format(_FIELD_DESCRIPTION, t.FullName, pi.Name + "." + fieldName), ret);
                _createDescriptions.Add(string.Format(_FIELD_DESCRIPTION, t.FullName, pi.Name + "." + fieldName));
            }
            return ret;
        }

        internal string GetVersionFieldIDName(Type t, Connection conn)
        {
            return CorrectName(GetTableName(t, conn) + "_VERSION_ID",new List<string>());
        }

        internal string GetEnumIDFieldName(Type t, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_FIELD_DESCRIPTION, t.FullName, _ENUM_ID_FIELD_NAME)))
                ret = _nameTranslations[string.Format(_FIELD_DESCRIPTION, t.FullName, _ENUM_ID_FIELD_NAME)];
            else
            {
                ret = CorrectName(_ENUM_ID_FIELD_NAME, GetTableFields(t));
                _nameTranslations.Add(string.Format(_FIELD_DESCRIPTION, t.FullName, _ENUM_ID_FIELD_NAME), ret);
                _createDescriptions.Add(string.Format(_FIELD_DESCRIPTION, t.FullName, _ENUM_ID_FIELD_NAME));
            }
            return ret;
        }

        internal string GetEnumValueFieldName(Type t, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_FIELD_DESCRIPTION, t.FullName, _ENUM_VALUE_FIELD_NAME)))
                ret = _nameTranslations[string.Format(_FIELD_DESCRIPTION, t.FullName, _ENUM_VALUE_FIELD_NAME)];
            else
            {
                ret = CorrectName(_ENUM_VALUE_FIELD_NAME, GetTableFields(t));
                _nameTranslations.Add(string.Format(_FIELD_DESCRIPTION, t.FullName, _ENUM_VALUE_FIELD_NAME), ret);
                _createDescriptions.Add(string.Format(_FIELD_DESCRIPTION, t.FullName, _ENUM_VALUE_FIELD_NAME));
            }
            return ret;
        }

        internal string GetIntermediateFieldName(Type t, PropertyInfo pi,string fieldName, bool isParent, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, (isParent ? "PARENT" : "CHILD"), fieldName })))
                ret = _nameTranslations[string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, (isParent ? "PARENT" : "CHILD"), fieldName })];
            else
            {
                ret = CorrectName((isParent ? "PARENT" : "CHILD") + "_" + fieldName, GetTableFields(t, pi));
                _nameTranslations.Add(string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, (isParent ? "PARENT" : "CHILD"), fieldName }), ret);
                _createDescriptions.Add(string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, (isParent ? "PARENT" : "CHILD"), fieldName }));
            }
            return ret;
        }

        internal string GetIntermediateValueFieldName(Type t, PropertyInfo pi, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, _INTERMEDIATE_VALUE_FIELD_NAME, "" }).Trim('_')))
                ret = _nameTranslations[string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, _INTERMEDIATE_VALUE_FIELD_NAME, "" }).Trim('_')];
            else
            {
                ret = CorrectName(_INTERMEDIATE_VALUE_FIELD_NAME, GetTableFields(t, pi));
                _nameTranslations.Add(string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, _INTERMEDIATE_VALUE_FIELD_NAME, "" }).Trim('_'), ret);
                _createDescriptions.Add(string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, _INTERMEDIATE_VALUE_FIELD_NAME, "" }).Trim('_'));
            }
            return ret;
        }

        internal string GetIntermediateIndexFieldName(Type t, PropertyInfo pi, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, _INTERMEDIATE_INDEX_FIELD_NAME, "" }).Trim('_')))
                ret = _nameTranslations[string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, _INTERMEDIATE_INDEX_FIELD_NAME, "" }).Trim('_')];
            else
            {
                ret = CorrectName(_INTERMEDIATE_INDEX_FIELD_NAME, GetTableFields(t, pi));
                _nameTranslations.Add(string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, _INTERMEDIATE_INDEX_FIELD_NAME, "" }).Trim('_'), ret);
                _createDescriptions.Add(string.Format(_INTERMEDIATE_FIELD_DESCRIPTION, new object[] { t.FullName, pi.Name, _INTERMEDIATE_INDEX_FIELD_NAME, "" }).Trim('_'));
            }
            return ret;
        }

        internal string GetGeneratorName(Type t, PropertyInfo pi, Connection conn)
        {
            string fieldName = "";
            string pName = "";
            if (t.IsEnum && pi == null){
                fieldName = "ID";
                pName = "ID";
            }else{
                fieldName = GetFieldName(t, pi, conn);
                pName = pi.Name;
            }
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_GENERATOR_DESCRIPTION, t.FullName, pName)))
                ret = _nameTranslations[string.Format(_GENERATOR_DESCRIPTION, t.FullName, pName)];
            else
            {
                ret = CorrectName("GEN_" + GetTableName(t, conn) + "_" + fieldName, GeneratorNames);
                _nameTranslations.Add(string.Format(_GENERATOR_DESCRIPTION, t.FullName, pName), ret);
                _createDescriptions.Add(string.Format(_GENERATOR_DESCRIPTION, t.FullName, pName));
            }
            return ret;
        }

        internal string GetIntermediateGeneratorName(Type t, PropertyInfo pi, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_GENERATOR_DESCRIPTION, t.FullName, pi.Name)))
                ret = _nameTranslations[string.Format(_GENERATOR_DESCRIPTION, t.FullName, pi.Name)];
            else
            {
                ret = CorrectName("GEN_" + GetIntermediateTableName(t, pi, conn) + "_" + GetIntermediateIndexFieldName(t, pi, conn), GeneratorNames);
                _nameTranslations.Add(string.Format(_GENERATOR_DESCRIPTION, t.FullName, pi.Name),ret);
                _createDescriptions.Add(string.Format(_GENERATOR_DESCRIPTION, t.FullName, pi.Name));
            }
            return ret;
        }

        internal string GetEnumGeneratorName(Type t, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_GENERATOR_DESCRIPTION, t.FullName, "ID")))
                ret = _nameTranslations[string.Format(_GENERATOR_DESCRIPTION, t.FullName, "ID")];
            else
            {
                ret = CorrectName("GEN_" + GetTableName(t, conn) + "_" + GetEnumIDFieldName(t,conn), GeneratorNames);
                _nameTranslations.Add(string.Format(_GENERATOR_DESCRIPTION, t.FullName, "ID"), ret);
                _createDescriptions.Add(string.Format(_GENERATOR_DESCRIPTION, t.FullName, "ID"));
            }
            return ret;
        }

        internal string GetInsertTriggerName(Type t, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "INSERT")))
                ret = _nameTranslations[string.Format(_TRIGGER_DESCRIPTION, t.FullName, "INSERT")];
            else
            {
                ret = CorrectName(GetTableName(t,conn) + "_GEN",TriggerNames);
                _nameTranslations.Add(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "INSERT"), ret);
                _createDescriptions.Add(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "INSERT"));
            }
            return ret;
        }

        internal string GetVersionInsertTriggerName(Type t, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "INSERT_VERSION")))
                return _nameTranslations[string.Format(_TRIGGER_DESCRIPTION, t.FullName, "INSERT_VERSION")];
            else
            {
                ret = CorrectName(GetTableName(t, conn) + "_VERSION_INSERT", TriggerNames);
                _nameTranslations.Add(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "INSERT_VERSION"), ret);
                _createDescriptions.Add(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "INSERT_VERSION"));
            }
            return ret;
        }

        internal string GetVersionUpdateTriggerName(Type t, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "UPDATE_VERSION")))
                return _nameTranslations[string.Format(_TRIGGER_DESCRIPTION, t.FullName, "UPDATE_VERSION")];
            else
            {
                ret = CorrectName(GetTableName(t, conn) + "_VERSION_UPDATE", TriggerNames);
                _nameTranslations.Add(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "UPDATE_VERSION"), ret);
                _createDescriptions.Add(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "UPDATE_VERSION"));
            }
            return ret;
        }

        internal string GetInsertIntermediateTriggerName(Type t,PropertyInfo pi, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_TRIGGER_DESCRIPTION, t.FullName+"."+pi.Name, "INSERT")))
                ret = _nameTranslations[string.Format(_TRIGGER_DESCRIPTION, t.FullName + "." + pi.Name, "INSERT")];
            else
            {
                ret = CorrectName(GetIntermediateTableName(t,pi, conn) + "_GEN", TriggerNames);
                _nameTranslations.Add(string.Format(_TRIGGER_DESCRIPTION, t.FullName + "." + pi.Name, "INSERT"), ret);
                _createDescriptions.Add(string.Format(_TRIGGER_DESCRIPTION, t.FullName + "." + pi.Name, "INSERT"));
            }
            return ret;
        }

        internal string GetDeleteParentTriggerName(Type t, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "DELETE")))
                ret = _nameTranslations[string.Format(_TRIGGER_DESCRIPTION, t.FullName, "DELETE")];
            else
            {
                ret = CorrectName(GetTableName(t, conn) + "_DEL", TriggerNames);
                _nameTranslations.Add(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "DELETE"), ret);
                _createDescriptions.Add(string.Format(_TRIGGER_DESCRIPTION, t.FullName, "DELETE"));
            }
            return ret;
        }

        internal string GetViewName(Type t, List<string> properties, out Dictionary<string, string> translatedProperties, Connection conn)
        {
            string ret = "";
            translatedProperties = new Dictionary<string, string>();
            string flds = "";
            List<string> tlist = new List<string>();
            foreach (string prop in properties){
                string tmp = CorrectName(_ConvertCamelCaseName(prop),tlist);
                tlist.Add(tmp);
                translatedProperties.Add(prop, tmp);
                flds += string.Format(_VIEW_FIELD_FORMAT, prop, tmp) + ",";
            }
            if (flds.Length > 0)
                flds = flds.Substring(0, flds.Length - 1);
            if (_nameTranslations.ContainsKey(string.Format(_VIEW_DESCRIPTION, t.FullName, flds)))
                ret = _nameTranslations[string.Format(_VIEW_DESCRIPTION, t.FullName, flds)];
            else
            {
                ret = CorrectName(_ConvertCamelCaseName(t.Name), ViewNames);
                _nameTranslations.Add(string.Format(_VIEW_DESCRIPTION, t.FullName, flds), ret);
                _createDescriptions.Add(string.Format(_VIEW_DESCRIPTION, t.FullName, flds));
            }
            return ret;
        }

        internal string GetIndexName(Type t, string indexName, Connection conn)
        {
            string ret = "";
            if (_nameTranslations.ContainsKey(string.Format(_INDEX_DESCRIPTION, t.FullName, indexName)))
                ret = _nameTranslations[string.Format(_INDEX_DESCRIPTION, t.FullName, indexName)];
            else
            {
                ret = CorrectName(indexName,GetIndexNames(t));
                _nameTranslations.Add(string.Format(_INDEX_DESCRIPTION, t.FullName, indexName), ret);
                _createDescriptions.Add(string.Format(_INDEX_DESCRIPTION, t.FullName, indexName));
                return ret;
            }
            return ret;
        }

        private string CorrectName(string currentName, List<string> existingNames)
        {
            string ret = currentName;
            bool reserved = false;
            foreach (string str in _pool.ReservedWords)
            {
                if (Utility.StringsEqualIgnoreCaseWhitespace(str, currentName))
                {
                    reserved = true;
                    break;
                }
            }
            if (reserved)
                ret = "RES_" + ret;
            ret = ShortenName(ret).ToUpper();
            if (existingNames.Contains(ret))
            {
                int _nameCounter = 0;
                while (existingNames.Contains(ret.Substring(0, _pool.MaxFieldNameLength - 1 - (_nameCounter.ToString().Length)) + "_" + _nameCounter.ToString()))
                {
                    _nameCounter++;
                }
                ret = ret.Substring(0, _pool.MaxFieldNameLength - 1 - (_nameCounter.ToString().Length));
                ret += "_" + _nameCounter.ToString();
            }
            return ret.ToUpper();
        }

        private string ShortenName(string name)
        {
            if (name.Length <= _pool.MaxFieldNameLength)
                return name;
            string ret = "";
            if (name.Contains("_"))
            {
                string[] tmp = name.Split('_');
                int len = (int)Math.Floor((double)_pool.MaxFieldNameLength / (double)tmp.Length);
                if (len == 1)
                {
                    if ((tmp[0].Length + (tmp.Length - 2) + tmp[tmp.Length - 1].Length) <= _pool.MaxFieldNameLength)
                    {
                        ret = tmp[0];
                        for (int x = 1; x <= tmp.Length - 1; x++)
                        {
                            ret += "_";
                        }
                        ret += tmp[tmp.Length - 1];
                    }
                    else
                    {
                        len = (int)Math.Floor((decimal)((tmp[0].Length + (tmp.Length - 2) + tmp[tmp.Length - 1].Length) - _pool.MaxFieldNameLength) / (decimal)2);
                        if (tmp[0].Length > len)
                            ret = tmp[0].Substring(0, len);
                        else
                            ret = tmp[0];
                        for (int x = 1; x <= tmp.Length - 1; x++)
                        {
                            ret += "_";
                        }
                        if (tmp[tmp.Length - 1].Length > len)
                            ret += tmp[tmp.Length - 1].Substring(0, len);
                        else
                            ret += tmp[tmp.Length - 1];
                    }
                }
                else
                {
                    foreach (string str in tmp)
                    {
                        if (str.Length != 0)
                        {
                            if (str.Length > len - 1)
                                ret += str.Substring(0, len - 1) + "_";
                            else
                                ret += str + "_";
                        }
                    }
                    ret = ret.Substring(0, ret.Length - 1);
                }
            }
            else
            {
                int diff = name.Length - _pool.MaxFieldNameLength - 1;
                int len = (int)Math.Floor((double)(name.Length - diff) / (double)2);
                ret = name.Substring(0, len) + "_" + name.Substring(name.Length - len);
            }
            return ret;
        }
    }
}
