/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 15/01/2009
 * Time: 4:14 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Collections.Generic;

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
		
		public ForeignKey(ExtractedTableMap table,string externalTable)
		{
			_internalTableName=table.TableName;
			_externalTableName=externalTable;
			_update="";
			_delete="";
			_internalFields=new List<string>();
			_externalFields=new List<string>();
			foreach(ForeignRelationMap frm in table.RelatedFieldsForTable(externalTable))
			{
				_internalFields.Add(frm.InternalField);
				_externalFields.Add(frm.ExternalField);
				_update=frm.OnUpdate.Replace("_"," ");
				_delete=frm.OnDelete.Replace("_"," ");
			}
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
	
	internal struct Trigger{
		
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
	
	internal struct ForeignRelationMap
	{
		private string _internalField;
		private string _externalTable;
		private string _externalField;
		private string _onUpdate;
		private string _onDelete;
		
		public ForeignRelationMap(string internalField,string externalTable,string externalField,string OnUpdate,string OnDelete)
		{
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
	}

	internal struct ExtractedTableMap
	{
		private string _tableName;
		private List<ExtractedFieldMap> _fields;
		private List<ForeignRelationMap> _ForeignFields;

		public ExtractedTableMap(string tableName)
		{
			_tableName=tableName;
			_fields=new List<ExtractedFieldMap>();
			_ForeignFields=new List<ForeignRelationMap>();
		}

		public string TableName{get{return _tableName;}}
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
				return ret;
			}
		}
		
		public List<ForeignRelationMap> RelatedFieldsForTable(string tableName){
			List<ForeignRelationMap> ret = new List<ForeignRelationMap>();
			foreach (ForeignRelationMap frm in ForeignFields)
			{
				if (frm.ExternalTable==tableName)
					ret.Add(frm);
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
			foreach (ForeignRelationMap frm in RelatedFieldsForTable(TableName))
			{
				if (frm.ExternalField==FieldName)
					return true;
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
		
		public ExtractedFieldMap(string fieldName,string type, long size, bool primary, bool nullable,bool autogen)
		{
			_fieldName = fieldName;
			_type = type;
			if (type.Contains("CHAR("))
			{
				_size = long.Parse(type.Substring(type.IndexOf("CHAR(")+5).Replace(")",""));
				_type = _type.Replace("("+_size.ToString()+")","");
			}else
				_size = size;
			_primaryKey = primary;
			_nullable = nullable;
			_autogen=autogen;
		}

		public ExtractedFieldMap(string fieldName, string type, long size, bool primary, bool nullable) : this(fieldName,type,size,primary,nullable,false)
		{
		}

		public string FieldName { get { return _fieldName; } }
		public string Type { get { return _type; } }
		public long Size { get { return _size; } }
		public bool PrimaryKey { get { return _primaryKey; } }
		public bool Nullable { get { return _nullable; } }
		public bool AutoGen {get {return _autogen;} set{_autogen=value;}}
		public string FullFieldType{
			get{
				if (Type.ToUpper().Contains("CHAR"))
					return Type+"("+Size.ToString()+")";
				return Type;
			}
		}
	}
}
