/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 15/01/2009
 * Time: 4:14 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using Org.Reddragonit.Dbpro.Structure.Attributes;
using System;
using System.Collections.Generic;
using Org.Reddragonit.Dbpro.Connections.ClassSQL;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections
{
	internal struct PrimaryKey{
		
		private string _tableName;
		private List<string> _fields;
		
		public string Name{
			get{return _tableName;}
		}
		
		public List<string> Fields{
			get{return _fields;}
		}
		
		public PrimaryKey(string TableName,List<string> Fields)
		{
			_tableName=TableName;
			_fields=Fields;
		}
		
		public PrimaryKey(ExtractedTableMap table)
		{
			_tableName=table.TableName;
			_fields=new List<string>();
			foreach (ExtractedFieldMap efm in table.PrimaryKeys)
				_fields.Add(efm.FieldName);
		}

        public override bool Equals(object obj)
        {
            return ((PrimaryKey)obj).Name == Name;
        }

        public bool IsForForeignRelation(ForeignKey fk)
        {
            return fk.ExternalTable == Name;
        }

        public bool ContainsForeignFields(ForeignKey fk)
        {
            bool ret = false;
            if (fk.InternalTable == Name)
            {
                foreach (string str in fk.InternalFields)
                {
                    if (Fields.Contains(str))
                    {
                        ret = true;
                        break;
                    }
                }
            }
            return ret;
        }
	}
	
	internal struct IdentityField{
		private string _tableName;
		private string _fieldName;
		private string _fieldType;
		private string _curValue;
		
		public string TableName{
			get{return _tableName;}
		}
		
		public string FieldName{
			get{return _fieldName;}
		}
				
		public string CurValue{
			get{return _curValue;}
		}
		
		public string FieldType{
			get{return _fieldType;}
		}
		
		public IdentityField(string tableName,string fieldName,string fieldType,string curValue)
		{
			_tableName=tableName;
			_fieldName=fieldName;
			_fieldType=fieldType;
			_curValue=curValue;
		}
	}
		
	
	internal struct ForeignKey{
		
		private string _internalTableName;
		private List<string> _internalFields;
		private string _externalTableName;
		private List<string> _externalFields;
		private string _update;
		private string _delete;
		
		public string InternalTable{
			get{return _internalTableName;}
		}
		
		public List<string> InternalFields{
			get{return _internalFields;}
		}
		
		public string ExternalTable{
			get{return _externalTableName;}
		}
		
		public List<string> ExternalFields{
			get{return _externalFields;}
		}
		
		public string OnUpdate{
			get{return _update;}
		}
		
		public string OnDelete{
			get{return _delete;}
		}
		
		public ForeignKey(string InternalTable,List<string> InternalFields,
		                  string ExternalTable,List<string> ExternalFields,
		                 string OnUpdate,string OnDelete)
		{
			_internalTableName=InternalTable;
			_internalFields=InternalFields;
			_externalTableName=ExternalTable;
			_externalFields=ExternalFields;
			_update=OnUpdate.Replace("_"," ");
			_delete=OnDelete.Replace("_"," ");
		}
		
		public ForeignKey(ExtractedTableMap table,string externalTable,string id)
		{
			_internalTableName=table.TableName;
			_externalTableName=externalTable;
			_update="";
			_delete="";
			_internalFields=new List<string>();
			_externalFields=new List<string>();
			foreach(List<ForeignRelationMap> frms in table.RelatedFieldsForTable(externalTable))
			{
                if (frms[0].ID == id)
                {
                    foreach (ForeignRelationMap frm in frms)
                    {
                        _internalFields.Add(frm.InternalField);
                        _externalFields.Add(frm.ExternalField);
                        _update = frm.OnUpdate.Replace("_", " ");
                        _delete = frm.OnDelete.Replace("_", " ");
                    }
                    break;
                }
			}
		}

        public override bool Equals(object obj)
        {
            return Equals((ForeignKey)obj);
        }

        public bool Equals(ForeignKey fk)
        {
            if ((_internalTableName != fk._internalTableName) ||
                (_externalTableName != fk._externalTableName) ||
                (_internalFields.Count!=fk._internalFields.Count) ||
                (_externalFields.Count!=fk._externalFields.Count))
                return false;
            foreach(string str in _internalFields)
            {
                if (!fk._internalFields.Contains(str))
                    return false;
            }
            foreach (string str in _externalFields)
            {
                if (!fk._externalFields.Contains(str))
                    return false;
            }
            return true;
        }
	}
	
	internal struct Generator{
		
		private string _generatorName;
		private long _value;
		
		public string Name{
			get{return _generatorName;}
		}
		
		public long Value{
			get{return _value;}
			set{_value=value;}
		}
		
		public Generator(string name)
		{
			_generatorName=name;
			_value=0;
		}
	}
	
	public struct Trigger{
		
		private string _triggerName;
		private string _triggerCode;
		private string _triggerConditions;
		
		public string Name{
			get{return _triggerName;}
		}
		
		public string Conditions{
			get{return _triggerConditions;}
		}
		
		public string Code{
			get{return _triggerCode;}
		}
		
		public Trigger(string name,string conditions,string code)
		{
			_triggerName=name;
			_triggerCode=code;
			_triggerConditions=conditions;
		}
	}

	public struct View
    {
        private string _name;
        private string _query;

        public string Name
        {
            get { return _name; }
        }

        public string Query
        {
            get { return _query; }
        }

        private Queue<Type> _requiredTypes;
        internal Queue<Type> RequiredTypes
        {
            get { return _requiredTypes; }
        }

        public View(string name, string query)
        {
            _name = name;
            _query=query;
            _requiredTypes = null;
        }

        public View(string name, ClassQuery query)
        {
            _name = name;
            _query = query.QueryString;
            _requiredTypes = query.RequiredTypes;
        }
    }

    public struct StoredProcedure
    {
        private string _procedureName;
        private string _parameterLines;
        private string _returnLine;
        private string _declareLines;
        private string _code;

        public string ProcedureName
        {
            get { return _procedureName; }
        }

        public string ParameterLines
        {
            get { return _parameterLines; }
        }

        public string ReturnLine
        {
            get { return _returnLine; }
        }

        public string DeclareLines
        {
            get { return _declareLines; }
        }

        public string Code
        {
            get { return _code; }
        }

        public StoredProcedure(string procedureName, string parameterLines, string returnLines, string declareLines, string code)
        {
            _procedureName = procedureName.Trim();
            _parameterLines = (parameterLines == null ? null : parameterLines.Trim());
            _returnLine = (returnLines==null ? null : returnLines.Trim());
            _declareLines = (declareLines==null ? null : declareLines.Trim());
            _code = code.Trim();
        }

        public override bool Equals(object obj)
        {
            StoredProcedure proc = (StoredProcedure)obj;
            return Utility.StringsEqualIgnoreWhitespace(proc.DeclareLines, this.DeclareLines)
                    && Utility.StringsEqualIgnoreWhitespace(proc.ReturnLine, this.ReturnLine)
                    && Utility.StringsEqualIgnoreWhitespace(proc.ParameterLines, this.ParameterLines)
                    && Utility.StringsEqualIgnoreWhitespace(proc.Code, this.Code);
        }
    }
	
	internal struct ForeignRelationMap : IComparable
	{
		private string _internalField;
		private string _externalTable;
		private string _externalField;
		private string _onUpdate;
		private string _onDelete;
        private string _id;
		
		public ForeignRelationMap(string id,string internalField,string externalTable,string externalField,string OnUpdate,string OnDelete)
		{
            _id = id;
			_internalField=internalField;
			_externalTable=externalTable;
			_externalField=externalField;
			_onUpdate=OnUpdate.Replace("_"," ");
			_onDelete=OnDelete.Replace("_"," ");
		}
		
		public string InternalField{
			get{return _internalField;}
			set{_internalField=value;}
		}
		
		public string ExternalTable{
			get{return _externalTable;}
			set{_externalTable=value;}
		}
		
		public string ExternalField{
			get{return _externalField;}
			set{_externalField=value;}
		}
		
		public string OnUpdate{
			get{return _onUpdate;}
			set{_onUpdate=value;}
		}
		
		public string OnDelete{
			get{return _onDelete;}
			set{_onDelete=value;}
		}

        public string ID
        {
            get { return _id; }
        }

        #region IComparable Members

        public int CompareTo(object obj)
        {
            return InternalField.CompareTo(((ForeignRelationMap)obj).InternalField);
        }

        #endregion
    }

    internal struct Index
    {
        private string _name;
        public string Name
        {
            get { return _name; }
        }

        private string[] _fields;
        public string[] Fields
        {
            get { return _fields; }
            set { _fields = value; }
        }

        private bool _unique;
        public bool Unique
        {
            get { return _unique; }
        }

        private bool _ascending;
        public bool Ascending
        {
            get { return _ascending; }
        }

        public Index(string name, string[] fields, bool unique,bool ascending)
        {
            _name = name;
            _fields = fields;
            _unique = unique;
            _ascending = ascending;
        }

        public override bool Equals(object obj)
        {
            Index ind = (Index)obj;
            bool ret = Name==ind.Name && ind.Unique==Unique && ind.Ascending==Ascending;
            if (ret)
            {
                if (Fields.Length == ind.Fields.Length)
                {
                    for (int x = 0; x < Fields.Length; x++)
                    {
                        if (Fields[x] != ind.Fields[x])
                        {
                            ret = false;
                            break;
                        }
                    }
                }
                else
                    ret = false;
            }
            return ret;
        }
    }

	internal struct ExtractedTableMap
	{
		private string _tableName;
		private List<ExtractedFieldMap> _fields;
		private List<ForeignRelationMap> _ForeignFields;
        private List<Index> _indices;

		public ExtractedTableMap(string tableName)
		{
            _toString = null;
            _toComputedString = null;
			_tableName=tableName;
			_fields=new List<ExtractedFieldMap>();
			_ForeignFields=new List<ForeignRelationMap>();
            _indices = new List<Index>();
		}

		public string TableName{get{return _tableName;}}
        public List<Index> Indices { get { return _indices; } set { _indices = value; } }
		public List<ExtractedFieldMap> Fields{get{return _fields;}set{_fields=value;}}
		public List<ForeignRelationMap> ForeignFields{get{return _ForeignFields;} set{_ForeignFields=value;}}
		public List<ExtractedFieldMap> PrimaryKeys{
			get{
				List<ExtractedFieldMap> ret = new List<ExtractedFieldMap>();
				foreach (ExtractedFieldMap efm in Fields)
				{
					if (efm.PrimaryKey)
						ret.Add(efm);
				}
				return ret;
			}
		}
		
		public List<string> RelatedTables{
			get{
				List<string> ret = new List<string>();
				foreach (ForeignRelationMap frm in ForeignFields)
				{
					if (!ret.Contains(frm.ExternalTable))
						ret.Add(frm.ExternalTable);
				}
                ret.Sort();
				return ret;
			}
		}
		
		public List<List<ForeignRelationMap>> RelatedFieldsForTable(string tableName){
            List<List<ForeignRelationMap>> ret = new List<List<ForeignRelationMap>>();
            List<string> ids = new List<string>();
            foreach (ForeignRelationMap frm in ForeignFields)
            {
                if (!ids.Contains(frm.ID)&&(frm.ExternalTable==tableName))
                    ids.Add(frm.ID);
            }
            foreach (string str in ids)
            {
                List<ForeignRelationMap> tmp = new List<ForeignRelationMap>();
                foreach (ForeignRelationMap frm in ForeignFields)
                {
                    if (frm.ID == str)
                        tmp.Add(frm);
                }
                ret.Add(tmp);
            }
			return ret;
		}
		
		public List<string> ExternalTablesForField(string FieldName)
		{
			List<string> ret = new List<string>();
			foreach (ForeignRelationMap frm in ForeignFields)
			{
				if (frm.InternalField==FieldName)
					ret.Add(frm.ExternalTable);
			}
			return ret;
		}
		
		public bool RelatesToField(string TableName,string FieldName)
		{
			foreach (ForeignRelationMap frm in ForeignFields)
			{
				if ((frm.ExternalField==FieldName)&&(frm.ExternalTable==TableName))
					return true;
			}
			return false;
		}

        public ExtractedFieldMap GetField(string fieldName)
        {
            foreach (ExtractedFieldMap efm in Fields)
            {
                if (efm.FieldName.ToUpper() == fieldName.ToUpper())
                    return efm;
            }
            return new ExtractedFieldMap();
        }

        public List<ExtractedFieldMap> ComputedFields
        {
            get
            {
                List<ExtractedFieldMap> ret = new List<ExtractedFieldMap>();
                foreach (ExtractedFieldMap efm in Fields)
                {
                    if (efm.ComputedCode != null)
                        ret.Add(efm);
                }
                return ret;
            }
        }

        public List<ExtractedFieldMap> GetComputedFieldsForField(string fieldName)
        {
            List<ExtractedFieldMap> ret = new List<ExtractedFieldMap>();
            foreach (ExtractedFieldMap efm in ComputedFields)
            {
                if (efm.ComputedCode.Contains(fieldName))
                    ret.Add(efm);
            }
            return ret;
        }

        private string _toString;
        public override string ToString()
        {
            if (_toString == null)
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendLine(string.Format("CREATE TABLE {0}(", TableName));
                List<string> tmp = new List<string>();
                foreach (ExtractedFieldMap efm in Fields)
                    tmp.Add(string.Format("{0} {1} {2},", efm.FieldName, efm.FullFieldType, (efm.Nullable ? "NULL" : "NOT NULL")));
                tmp.Sort();
                foreach (string str in tmp)
                    sb.AppendLine(str);
                if (PrimaryKeys.Count > 0)
                {
                    sb.Append("PRIMARY KEY(");
                    foreach (ExtractedFieldMap efm in PrimaryKeys)
                        sb.Append(string.Format("{0},", efm.FieldName));
                    sb.Remove(sb.Length - 1, 1);
                    sb.AppendLine("),");
                }
                foreach (string tbl in RelatedTables)
                {
                    List<ForeignRelationMap> frms;
                    List<List<ForeignRelationMap>> rels = RelatedFieldsForTable(tbl);
                    for (int x = 0; x < rels.Count; x++)
                    {
                        for (int y = x + 1; y < rels.Count; y++)
                        {
                            if (rels[x][0].ExternalTable.Equals(rels[y][0].ExternalTable))
                            {
                                if (rels[x][0].InternalField.CompareTo(rels[y][0].InternalField) > 0)
                                {
                                    frms = rels[y];
                                    rels[y] = rels[x];
                                    rels[x] = frms;
                                }
                            }
                            else if (rels[x][0].ExternalTable.CompareTo(rels[y][0].ExternalTable) > 0)
                            {
                                frms = rels[y];
                                rels[y] = rels[x];
                                rels[x] = frms;
                            }

                        }
                    }
                    foreach (List<ForeignRelationMap> maps in rels)
                    {
                        maps.Sort();
                        sb.Append("FOREIGN KEY(");
                        foreach (ForeignRelationMap frm in maps)
                            sb.Append(frm.InternalField + ",");
                        sb.Remove(sb.Length - 1, 1);
                        sb.Append(string.Format(") RELATES TO {0}(", maps[0].ExternalTable));
                        foreach (ForeignRelationMap frm in maps)
                            sb.Append(frm.ExternalField + ",");
                        sb.Remove(sb.Length - 1, 1);
                        sb.AppendLine(string.Format("ON UPDATE {0} ON DELETE {1},",
                            maps[0].OnUpdate,
                            maps[0].OnDelete));
                    }
                }
                if (sb.ToString().EndsWith(","))
                    sb.Remove(sb.Length - 1, 1);
                sb.Append(");");
                _toString = sb.ToString();
            }
            return _toString;
        }

        private string _toComputedString;
        public string ToComputedString(){
            if (_toComputedString == null)
            {
                StringBuilder sb = new StringBuilder();
                foreach (ExtractedFieldMap efm in Fields)
                {
                    if (efm.ComputedCode != null)
                        sb.AppendLine(string.Format("{0} AS ({1}),",
                            efm.FieldName,
                            efm.ComputedCode));
                }
                _toComputedString = sb.ToString();
            }
            return _toComputedString;
        }

        //used for tracing down relations when overriding the CASCADE conditions
        internal bool RelatesToTable(string cstr,List<ExtractedTableMap> maps)
        {
            if (RelatedTables.Contains(cstr))
                return true;
            else
            {
                foreach (string str in RelatedTables)
                {
                    if (str != TableName)
                    {
                        foreach (ExtractedTableMap etm in maps)
                        {
                            if (etm.TableName == str && etm.RelatesToTable(str, maps))
                                return true;
                        }
                    }
                }
            }
            return false;
        }
    }

	internal struct ExtractedFieldMap
	{
		private string _fieldName;
		private string _type;
		private long _size;
		private bool _primaryKey;
		private bool _nullable;
		private bool _autogen;
        private string _computedCode;
		
		public ExtractedFieldMap(string fieldName,string type, long size, bool primary, bool nullable,bool autogen,string computedCode)
		{
			_fieldName = fieldName;
			_type = type;
			if (type.Contains("CHAR("))
			{
                if (type.Substring(type.IndexOf("CHAR(") + 5).Replace(")", "").ToUpper() == "MAX")
                    _size = -1;
                else
				    _size = long.Parse(type.Substring(type.IndexOf("CHAR(")+5).Replace(")",""));
                _type = _type.Substring(0,_type.IndexOf("("));
			}else if (type.Contains("VARBINARY(")){
                if (type.Substring(type.IndexOf("VARBINARY(")+10).Replace(")","").ToUpper() == "MAX")
                    _size = -1;
                else
                    _size = long.Parse(type.Substring(type.IndexOf("VARBINARY(") + 10).Replace(")", ""));
                _type = _type.Substring(0,_type.IndexOf("("));
			}
			else if (type.Contains("VARYING(")){
                if (type.Substring(type.IndexOf("VARYING(") + 8).Replace(")", "").ToUpper() == "MAX")
                    _size = -1;
                else
                    _size = long.Parse(type.Substring(type.IndexOf("VARYING(") + 8).Replace(")", ""));
                _type = _type.Substring(0,_type.IndexOf("("));
            }else
				_size = size;
			_primaryKey = primary;
			_nullable = nullable;
			_autogen=autogen;
            _computedCode = computedCode;
		}

		public ExtractedFieldMap(string fieldName, string type, long size, bool primary, bool nullable,string computedCode) : this(fieldName,type,size,primary,nullable,false,computedCode)
		{
		}

		public string FieldName { get { return _fieldName; } }
		public string Type { get { return _type; } }
		public long Size { get { return _size; } }
		public bool PrimaryKey { get { return _primaryKey; } }
		public bool Nullable { get { return _nullable; } }
		public bool AutoGen {get {return _autogen;} set{_autogen=value;}}
        public string ComputedCode { get { return _computedCode; } }
		public string FullFieldType{
			get{
				if (Type.ToUpper().Contains("CHAR")||Type.Contains("VARBINARY")||Type.Contains("VARYING")){
                    if (Type.Contains("{0}"))
                        return string.Format(Type,"("+(Size==-1 ? "MAX" : Size.ToString())+")");
					return Type+"("+(Size==-1 ? "MAX" : Size.ToString())+")";
                }
				return Type;
			}
		}
	}
}
