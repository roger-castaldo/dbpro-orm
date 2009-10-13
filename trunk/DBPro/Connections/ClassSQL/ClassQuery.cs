/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 08/06/2009
 * Time: 9:24 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using Org.Reddragonit.Dbpro.Exceptions;
using System;
using System.Collections.Generic;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using System.Data;
using Org.Reddragonit.Dbpro.Structure;

namespace Org.Reddragonit.Dbpro.Connections.ClassSQL
{
	/// <summary>
	/// Description of ClassQueryTranslator.
	/// </summary>
	public class ClassQuery : IDataReader
	{
		private static readonly List<string> _conditionOperators =
			new List<string>(new string[]{"=","NOT","IN","LIKE",">","<"});
		
		private string _namespace;
		private QueryTokenizer _tokenizer;
		private string _outputQuery;
		private Dictionary<int, int> _subQueryIndexes;
        private Dictionary<string, Type> _tableFields;
        private Dictionary<int, string> _fieldNames;
        private Dictionary<int, int> _tableFieldCounts;
		private Connection _conn = null;
		
		public ClassQuery(string NameSpace,string query)
		{
			_namespace=NameSpace;
            if (query.StartsWith("(") && query.EndsWith(")"))
                query = query.TrimStart('(').TrimEnd(')');
			_tokenizer=new QueryTokenizer(query);
			_tokenizer.parse();
            _tableFieldCounts = new Dictionary<int, int>();
            _fieldNames = new Dictionary<int, string>();
            _tableFields = new Dictionary<string, Type>();
            Translate();
            System.Diagnostics.Debug.WriteLine("Class Query: " + query + "\nTranslated To:" + _outputQuery);
		}
		
		private void Translate()
		{
            for (int y = 0; y < _tokenizer.Tokens.Count; y++)
            {
                _outputQuery += _tokenizer.Tokens[y].Value + " ";
            }
            _fieldNames = new Dictionary<int, string>();
            _tableFields = new Dictionary<string, Type>();
			Dictionary<string,string> tableTranslations = new Dictionary<string, string>();
			_subQueryIndexes=new Dictionary<int, int>();
			int pos = 0;
			List<IDbDataParameter> parameters;
			EstablishSubQueries(ref pos,_tokenizer.Tokens);
			Dictionary<int, string> subQueryTranlsations = TranslateSubQueries(out parameters);
			foreach (int x in subQueryTranlsations.Keys){
                string orig = "";
                for (int y = x; y < _subQueryIndexes[x]; y++)
                {
                    orig += _tokenizer.Tokens[y].Value + " ";
                }
                _outputQuery = _outputQuery.Replace(orig, subQueryTranlsations[x]);
			}
        }

        #region ConnectionFunctions
        public void Execute()
        {
            _conn.ExecuteQuery(_outputQuery);
        }

        private int TranslateFieldIndex(int i)
        {
            if (_tableFields.Count > 0)
            {
                int ret = i;
                foreach (int index in _tableFieldCounts.Keys)
                {
                    if (index < i)
                        ret += _tableFieldCounts[index];
                }
                return ret;
            }
            return i;
        }
        #endregion

        #region SubQueries
        private void EstablishSubQueries(ref int pos, List<QueryToken> tokens)
		{
			int start = pos;
			int bracketCount = 0;
			for (int x = pos + 1; x < tokens.Count; x++)
			{
				if (
					(tokens[x].Value.ToUpper() == "SELECT") &&
					(x > 0) &&
					(tokens[x - 1].Value == "(")
				)
				{
					pos = x;
					EstablishSubQueries(ref pos, tokens);
					x = pos;
				}
				if (tokens[x].Value == "(")
				{
					bracketCount++;
				}
				if (tokens[x].Value == ")")
				{
					bracketCount--;
					if (bracketCount == -1)
					{
						pos = x;
						break;
					}
				}
			}
			if (pos == start)
				pos = tokens.Count;
			_subQueryIndexes.Add(start, pos - start);
		}
		
		private Dictionary<int, string> TranslateSubQueries(out List<IDbDataParameter> parameters)
		{
			parameters = new List<IDbDataParameter>();
			Dictionary<int, string> ret = new Dictionary<int, string>();
			foreach (int i in _subQueryIndexes.Keys)
			{
				ret.Add(i, TranslateSubQuery(i, ref parameters));
			}
			return ret;
		}

		private string TranslateSubQuery(int i, ref List<IDbDataParameter> parameters)
		{
			List<int> tableDeclarations = new List<int>();
			Dictionary<int, string> fieldAliases = new Dictionary<int, string>();
			Dictionary<string, string> tableAliases = new Dictionary<string, string>();
			List<int> fieldIndexes = new List<int>();
			Dictionary<int, int> conditionIndexes=new Dictionary<int, int>();
			int x = 1;
			int whereIndex=-1;
			while (
				(x < _subQueryIndexes[i]) &&
				(_tokenizer.Tokens[i + x].Value.ToUpper() != "FROM")
			)
			{
				if ((_tokenizer.Tokens[i + x].Type == TokenType.KEY) &&
				    (
				    	(_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "SELECT") ||
				    	(_tokenizer.Tokens[i + x - 1].Value == ",") ||
				    	(_tokenizer.Tokens[i + x - 1].Value == "(") ||
				    	((_tokenizer.Tokens[i + x + 1].Value.ToUpper() == "AS") && (_tokenizer.Tokens[i + x + 1].Type == TokenType.KEYWORD)) ||
				    	(_tokenizer.Tokens[i + x + 1].Type == TokenType.OPERATOR) ||
				    	(_tokenizer.Tokens[i + x - 1].Type == TokenType.OPERATOR) ||
				    	(_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "WHEN") ||
				    	(_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "THEN") ||
				    	(_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "ELSE")
				    ))
				{
					fieldIndexes.Add(i + x);
					if ((_tokenizer.Tokens[i + x + 1].Value.ToUpper() == "AS") && (_tokenizer.Tokens[i + x + 1].Type == TokenType.KEYWORD))
					{
						fieldAliases.Add(i + x, _tokenizer.Tokens[i + x + 2].Value);
					}
				}
				x++;
			}
			if (_tokenizer.Tokens[i + x].Value.ToUpper() == "FROM")
			{
				while (x < _subQueryIndexes[i])
				{
					if (_subQueryIndexes.ContainsKey(i + x))
						x += _subQueryIndexes[i + x] + 2;
					else
					{
						if (((_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "FROM") || (_tokenizer.Tokens[i + x - 1].Value == ",")) && (_tokenizer.Tokens[i + x].Value != "("))
						{
							if ((_tokenizer.Tokens[i + x + 1].Value.ToUpper() != ",") && (_tokenizer.Tokens[i + x + 1].Value != "(") && (_tokenizer.Tokens[i + x + 1].Value != "WHERE"))
							{
								if (_tokenizer.Tokens[i + x + 1].Value.ToUpper() == "AS")
								{
									if (!tableAliases.ContainsKey(_tokenizer.Tokens[i + x + 2].Value))
										tableAliases.Add(_tokenizer.Tokens[i + x + 2].Value, _tokenizer.Tokens[i + x].Value);
								}
								else
								{
									if (!tableAliases.ContainsKey(_tokenizer.Tokens[i + x + 1].Value))
										tableAliases.Add(_tokenizer.Tokens[i + x + 1].Value, _tokenizer.Tokens[i + x].Value);
								}
							}
							tableDeclarations.Add(i + x);
						}
						else if (_tokenizer.Tokens[i + x].Value.ToUpper() == "WHERE")
							break;
					}
					x++;
				}
			}
			if (x < _subQueryIndexes[i] && (_tokenizer.Tokens[x + i].Value.ToUpper() == "WHERE"))
			{
				whereIndex=x;
				while (x < _subQueryIndexes[i])
				{
					if ((_tokenizer.Tokens[x-1].Value=="WHERE")
					    ||(_tokenizer.Tokens[x-1].Value=="(")
					    ||(_tokenizer.Tokens[x-1].Value.ToUpper()=="OR")
					    ||(_tokenizer.Tokens[x-1].Value.ToUpper()=="AND")
					   ){
						if (_tokenizer.Tokens[x].Value!="("){
							int start=x;
							x++;
							while (x < _subQueryIndexes[i])
							{
								if ((_tokenizer.Tokens[x].Value==")")
								    ||(_tokenizer.Tokens[x].Value.ToUpper()=="OR")
								    ||(_tokenizer.Tokens[x].Value.ToUpper()=="AND")){
									break;
								}
								x++;
							}
							conditionIndexes.Add(start,x);
						}
					}
					x++;
				}
			}
			List<int> whereFieldIndexes = new List<int>();
			foreach (int index in conditionIndexes.Keys){
				whereFieldIndexes.AddRange(ExtractFieldsFromCondition(index,conditionIndexes[index]));
			}
			Dictionary<string, List<string>> fieldList = new Dictionary<string, List<string>>();
			string tables = CreateTableQuery(tableDeclarations, fieldIndexes,ref fieldList,whereFieldIndexes);
			string fields = TranslateFields(i,fieldIndexes, tableDeclarations, fieldAliases, tableAliases,fieldList);
			string wheres = TranslateWhereConditions(whereIndex,conditionIndexes,tableAliases,tableDeclarations,fieldList);
			return fields+" FROM " + tables+wheres;
		}
		#endregion
		
		#region ConditionTranslating
		private string TranslateWhereConditions(int whereIndex,Dictionary<int, int> conditionIndexes,Dictionary<string, string> tableAliases,List<int> tableDeclarations,Dictionary<string, List<string>> fieldList){
			string ret = "";
			if (whereIndex>0){
				int lastIndex=whereIndex;
				foreach (int index in conditionIndexes.Keys){
					for (int x=lastIndex;x<index;x++){
						ret+=_tokenizer.Tokens[x].Value+" ";
					}
					List<int> fields = ExtractFieldsFromCondition(index,conditionIndexes[index]);
					string condition="";
					int cnt=index;
					bool started=false;
					while (cnt<conditionIndexes[index]){
						if (_conditionOperators.Contains(_tokenizer.Tokens[cnt].Value.ToUpper())){
							started=true;
							condition+=_tokenizer.Tokens[cnt].Value;
						}else if (started)
							break;
						cnt++;
					}
					bool isTabled=false;
					foreach (int x in fields){
						isTabled=isTabled||IsFieldConditionTable(x,tableAliases,tableDeclarations);
					}
					if (isTabled){
                        //TODO: Need to implement code to handle all variations of using a table value, including if an arrayed parameter of tables is passed in for IN
						throw new Exception("Unable to handle conditions using entire tables at this time.");
					}else{
						for(int x=index;x<conditionIndexes[index];x++){
							if (fields.Contains(x)){
								ret+=TranslateConditionField(x,tableDeclarations,tableAliases,fieldList)[0]+" ";
							}else
								ret+=_tokenizer.Tokens[x].Value+" ";
						}
					}
					lastIndex=conditionIndexes[index];
				}
			}
			return ret;
		}
		
		private bool IsFieldConditionTable(int index,Dictionary<string, string> tableAliases,List<int> tableDeclarations){
			QueryToken field = _tokenizer.Tokens[index];
			string tableName = "";
			string fieldName = _tokenizer.Tokens[index].Value;
			if (field.Value.Contains("."))
			{
				tableName = field.Value.Substring(0, field.Value.IndexOf("."));
				fieldName = fieldName.Substring(tableName.Length + 1);
			}
			else
			{
				if (tableDeclarations.Count == 1)
				{
					tableName = _tokenizer.Tokens[tableDeclarations[0]].Value;
				}
			}
			if (tableAliases.ContainsKey(tableName))
				tableName = tableAliases[tableName];
			Type t = LocateTableType(tableName);
			if (fieldName.EndsWith(".*"))
				return true;
			if (t != null)
			{
				TableMap map = ClassMapper.GetTableMap(t);
				if (fieldName.Contains("."))
				{
					while (fieldName.Contains("."))
					{
						ExternalFieldMap efm = (ExternalFieldMap)map[fieldName.Substring(0, fieldName.IndexOf("."))];
						map = ClassMapper.GetTableMap(efm.Type);
						fieldName = fieldName.Substring(fieldName.IndexOf(".") + 1);
					}
					if (map[fieldName] is ExternalFieldMap)
						return true;
					else
						return false;
				}
				else
				{
					if (map[fieldName] is ExternalFieldMap)
						return true;
					else
						return false;
				}
			}
			return false;
		}
		
		private List<string> TranslateConditionField(int index, List<int> tableDeclarations, Dictionary<string, string> tableAliases,Dictionary<string,List<string>> fieldList)
		{
			List<string> ret = new List<string>();
			QueryToken field = _tokenizer.Tokens[index];
			string tableName = "";
			string fieldName = _tokenizer.Tokens[index].Value;
			if (field.Value.Contains("."))
			{
				tableName = field.Value.Substring(0, field.Value.IndexOf("."));
				fieldName = fieldName.Substring(tableName.Length + 1);
			}
			else
			{
				if (tableDeclarations.Count == 1)
				{
					tableName = _tokenizer.Tokens[tableDeclarations[0]].Value;
				}
			}
			ret.Add(field.Value);
			if (tableAliases.ContainsKey(tableName))
				tableName = tableAliases[tableName];
			Type t = LocateTableType(tableName);
			if (t != null)
			{
				TableMap map = ClassMapper.GetTableMap(t);
				if (fieldName.Contains("."))
				{
					while (fieldName.Contains("."))
					{
						ExternalFieldMap efm = (ExternalFieldMap)map[fieldName.Substring(0, fieldName.IndexOf("."))];
						map = ClassMapper.GetTableMap(efm.Type);
						fieldName = fieldName.Substring(fieldName.IndexOf(".") + 1);
					}
					if (fieldName != "*")
					{
						if (map[fieldName] is ExternalFieldMap)
						{
							ret.RemoveAt(0);
							foreach (string str in fieldList[field.Value])
							{
								ret.Add(field.Value.Replace(".", "_") + "." + str);
							}
						}else
						{
							ret.RemoveAt(0);
							ret.Add(field.Value.Substring(0, field.Value.LastIndexOf(".")).Replace(".", "_") + "." + map.GetTableFieldName(fieldName));
						}
					}
					else
					{
						ret.RemoveAt(0);
						foreach (string str in fieldList[field.Value])
						{
							ret.Add(field.Value.Replace(".", "_") + "." + str);
						}
					}
				}
				else
				{
					if (fieldName == "*")
					{
						ret.RemoveAt(0);
						foreach (string str in fieldList[field.Value])
						{
							ret.Add(field.Value.Replace(".", "_") + "." + str);
						}
					}
					else
					{
						if (map[fieldName] is ExternalFieldMap)
						{
							TableMap extMap = ClassMapper.GetTableMap(((ExternalFieldMap)map[fieldName]).Type);
							ret.RemoveAt(0);
							foreach (string str in fieldList[field.Value])
							{
								ret.Add(field.Value.Replace(".", "_") + "." + str);
							}
						}else{
							ret.RemoveAt(0);
							ret.Add(map.GetTableFieldName(fieldName));
						}
					}
				}
			}
			return ret;
		}
		
		private List<int> ExtractFieldsFromCondition(int start,int end){
			List<int> ret = new List<int>();
			for (int x=start;x<end;x++){
				if (_tokenizer.Tokens[x-1].Value.ToUpper()=="IN")
					break;
				if ((_tokenizer.Tokens[x].Type == TokenType.KEY) &&
				    (
				    	(_tokenizer.Tokens[x - 1].Value.ToUpper() == "CASE") ||
				    	(_tokenizer.Tokens[x - 1].Value.ToUpper() == "CAST") ||
				    	(_tokenizer.Tokens[x - 1].Value == ",") ||
				    	((_tokenizer.Tokens[x - 1].Value == "(") && (_tokenizer.Tokens[x-2].Value.ToUpper()!="IN") )||
				    	(_tokenizer.Tokens[x + 1].Type == TokenType.OPERATOR) ||
				    	(_tokenizer.Tokens[x - 1].Type == TokenType.OPERATOR) ||
				    	(_tokenizer.Tokens[x - 1].Value.ToUpper() == "WHEN") ||
				    	(_tokenizer.Tokens[x - 1].Value.ToUpper() == "THEN") ||
				    	(_tokenizer.Tokens[x - 1].Value.ToUpper() == "ELSE") ||
				    	((x+1<end)&&
				    	 (
				    	 	(_tokenizer.Tokens[x+1].Value.ToUpper()=="NOT")||
				    	 	(_tokenizer.Tokens[x+1].Value.ToUpper()=="IN")||
				    	 	(_tokenizer.Tokens[x+1].Value.ToUpper()=="LIKE")
				    	 )
				    	)
				    ))
				{
					ret.Add(x);
				}
			}
			return ret;
		}
		#endregion

		#region TableTranslating
		private string CreateTableQuery(List<int> tableDeclarations,List<int> fieldIndexes,ref Dictionary<string,List<string>> fieldLists,List<int> whereFieldIndexes)
		{
			string tables = "";
			Dictionary<string, List<string>> joins = new Dictionary<string, List<string>>();
			for (int y = 0; y < tableDeclarations.Count; y++)
			{
				int x = tableDeclarations[y];
				Type t = LocateTableType(_tokenizer.Tokens[x].Value);
				if (t == null)
					throw new CannotLocateTable(_tokenizer.Tokens[x].Value);
				else
				{
					if (_conn == null)
						_conn = ConnectionPoolManager.GetConnection(t).getConnection();
					TableMap map = ClassMapper.GetTableMap(t);
					tables += map.Name + " ";
					string alias = _tokenizer.Tokens[x].Value;
					if ((x + 1 < _tokenizer.Tokens.Count) && (_tokenizer.Tokens[x + 1].Value != ",") && (_tokenizer.Tokens[x + 1].Value.ToUpper() != "WHERE"))
						alias = _tokenizer.Tokens[x + 1].Value;
					List<string> tmpJoins = new List<string>();
					foreach (int index in fieldIndexes)
					{
						if (_tokenizer.Tokens[index].Value.StartsWith(alias))
							tmpJoins = TraceJoins(tmpJoins,  map, _tokenizer.Tokens[index].Value.Substring(alias.Length + 1), alias,ref fieldLists);
					}
					foreach (int index in whereFieldIndexes){
						if (_tokenizer.Tokens[index].Value.StartsWith(alias))
							tmpJoins=TraceJoins(tmpJoins,map,_tokenizer.Tokens[index].Value.Substring(alias.Length+1),alias,ref fieldLists);
					}
					if (alias != _tokenizer.Tokens[x].Value)
					{
						joins.Add(map.Name + " " + alias, tmpJoins);
						tables += alias;
					}
					else
						joins.Add(map.Name + " ", tmpJoins);
					tables += ",";
				}
			}
			foreach (string str in joins.Keys)
			{
				string joinString = "";
				foreach (string s in joins[str])
					joinString += s + " ";
				tables = tables.Replace(str, str + " " + joinString);
			}
			if (tables.EndsWith(","))
				tables = tables.Substring(0, tables.Length - 1);
			return tables;
		}

		private List<string> TraceJoins(List<string> joins, TableMap baseMap, string field, string alias,ref Dictionary<string,List<string>> fieldLists)
		{
			string origField = field;
			string origAlias = alias;
			TableMap map = baseMap;
			if (field.Contains("."))
			{
				while (field.Contains("."))
				{
					ExternalFieldMap efm = (ExternalFieldMap)map[field.Substring(0, field.IndexOf("."))];
					TableMap eMap = ClassMapper.GetTableMap(efm.Type);
					string className = field.Substring(0, field.IndexOf("."));
					string innerJoin = " INNER JOIN ";
					if (efm.Nullable)
						innerJoin = " LEFT JOIN ";
                    string tbl = _conn.queryBuilder.SelectAll(efm.Type);
					if (efm.IsArray)
					{
						innerJoin += _conn.Pool.CorrectName(map.Name + "_" + eMap.Name) + " " + alias + "_intermediate_" + className + " ON ";
						foreach (InternalFieldMap ifm in map.PrimaryKeys)
							innerJoin += " " + alias + "." + _conn.Pool.CorrectName(ifm.FieldName) + " = " + alias + "_intermediate_" + className + "." + _conn.Pool.CorrectName("PARENT_" + ifm.FieldName) + " AND ";
						innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                        if (!joins.Contains(innerJoin))
                            joins.Add(innerJoin);
                        innerJoin = " INNER JOIN (" + tbl + ") " + alias + "_" + className + " ON ";
						foreach (InternalFieldMap ifm in eMap.PrimaryKeys)
							innerJoin += " " + alias + "_intermediate_" + className + "." + _conn.Pool.CorrectName("CHILD_" + ifm.FieldName) + " = " + alias + "_" + className + "." + _conn.Pool.CorrectName(ifm.FieldName) + " AND ";
						innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
					}
					else
					{
                        innerJoin += "(" + tbl + ") " + alias + "_" + className + " ON ";
						foreach (InternalFieldMap ifm in eMap.PrimaryKeys)
							innerJoin += " " + alias + "." + _conn.Pool.CorrectName(efm.AddOnName + "_" + ifm.FieldName) + " = " + alias + "_" + className + "." + _conn.Pool.CorrectName(ifm.FieldName) + " AND ";
						innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
					}
					alias += "_" + field.Substring(0, field.IndexOf("."));
					field = field.Substring(field.IndexOf(".") + 1);
					if (!joins.Contains(innerJoin))
						joins.Add(innerJoin);
                    map = eMap;
				}
				
			}
			if (map[field] is ExternalFieldMap)
			{
				ExternalFieldMap efm = (ExternalFieldMap)baseMap[field];
				TableMap eMap = ClassMapper.GetTableMap(efm.Type);
				string className = field;
				string innerJoin = " INNER JOIN ";
				if (efm.Nullable)
					innerJoin = " LEFT JOIN ";
				string tbl = _conn.queryBuilder.SelectAll(efm.Type);
				List<string> fields = new List<string>();
				string fieldString = tbl.Substring(tbl.IndexOf("SELECT") + "SELECT".Length);
				fieldString = fieldString.Substring(0, fieldString.IndexOf("FROM"));
				foreach (string str in fieldString.Split(','))
				{
					if (str.Length > 0)
						fields.Add(str.Substring(str.LastIndexOf(".") + 1));
				}
				if (!fieldLists.ContainsKey(origAlias+"."+origField))
					fieldLists.Add(origAlias + "." + origField, fields);
				if (efm.IsArray)
				{
					innerJoin += _conn.Pool.CorrectName(map.Name + "_" + eMap.Name) + " " + alias + "_intermediate_" + className + " ON ";
					foreach (InternalFieldMap ifm in baseMap.PrimaryKeys)
						innerJoin += " " + alias + "." + _conn.Pool.CorrectName(ifm.FieldName) + " = " + alias + "_intermediate_" + className + "." + _conn.Pool.CorrectName("PARENT_" + ifm.FieldName) + " AND ";
					innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                    if (!joins.Contains(innerJoin))
                        joins.Add(innerJoin);
                    innerJoin = " INNER JOIN (" + tbl + ") " + alias + "_" + className + " ON ";
					foreach (InternalFieldMap ifm in eMap.PrimaryKeys)
						innerJoin += " " + alias + "_intermediate_" + className + "." + _conn.Pool.CorrectName("CHILD_" + ifm.FieldName) + " = " + alias + "_" + className + "." + _conn.Pool.CorrectName(ifm.FieldName) + " AND ";
					innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
				}
				else
				{
					innerJoin += "(" + tbl + ") " + alias + "_" + className + " ON ";
					foreach (InternalFieldMap ifm in eMap.PrimaryKeys)
						innerJoin += " " + alias + "." + _conn.Pool.CorrectName(efm.AddOnName + "_" + ifm.FieldName) + " = " + alias + "_" + className + "." + _conn.Pool.CorrectName(ifm.FieldName) + " AND ";
					innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
				}
				if (!joins.Contains(innerJoin))
					joins.Add(innerJoin);
			}
			return joins;
		}

		private Type LocateTableType(string tablename)
		{
			Type ret = Utility.LocateType(tablename);
			if (ret == null)
			{
				if ((_namespace != null) && (_namespace.Length > 0))
					ret = Utility.LocateType(_namespace + "." + tablename);
			}
			return ret;
		}
		#endregion

		#region FieldTranslating
		private string TranslateFields(int subqueryIndex,List<int> fieldIndexes,List<int> tableDeclarations, Dictionary<int, string> fieldAliases, Dictionary<string, string> tableAliases,Dictionary<string,List<string>> fieldList)
		{
            int ordinal = 0;
			string ret = "";
			int previousIndex = subqueryIndex;
			foreach (int x in fieldIndexes){
				for (int y = previousIndex; y<x ; y++)
				{
					ret += _tokenizer.Tokens[y].Value + " ";
				}
                if (ret.TrimEnd(' ').EndsWith(","))
                    ordinal++;
				string fieldAlias;
                if (subqueryIndex == 0)
                    ret += TranslateFieldName(ordinal, x, out fieldAlias, tableDeclarations, fieldAliases, tableAliases, fieldList) + " ";
                else
                    ret += TranslateFieldName(-1, x, out fieldAlias, tableDeclarations, fieldAliases, tableAliases, fieldList) + " ";
				if (ret.EndsWith(",  "))
				{
					if (_tokenizer.Tokens[x + 1].Value.ToUpper() == "AS")
						previousIndex = x + 4;
					else if (_tokenizer.Tokens[x + 1].Value == ",")
						previousIndex = x + 2;
					else
					{
						ret = ret.Substring(0, ret.Length - 3) + " ";
						previousIndex = x + 1;
					}
				}
				else
				{
                    if ((fieldAlias != "") && ((_tokenizer.Tokens[x + 1].Value == ",") || (_tokenizer.Tokens[x + 1].Value == "FROM")))
                        ret += " AS " + fieldAlias;
					previousIndex = x + 1;
				}
                if (ret.TrimEnd(' ').EndsWith(","))
                    ordinal++;
			}
			int z = previousIndex;
			while (_tokenizer.Tokens[z].Value.ToUpper() != "FROM")
			{
				ret += _tokenizer.Tokens[z].Value+" ";
				z++;
			}
			return ret;
		}

		private string TranslateFieldName(int ordinal,int index, out string fieldAlias, List<int> tableDeclarations, Dictionary<int, string> fieldAliases, Dictionary<string, string> tableAliases,Dictionary<string,List<string>> fieldList)
		{
			QueryToken field = _tokenizer.Tokens[index];
			string tableName = "";
			string fieldName = _tokenizer.Tokens[index].Value;
			if (field.Value.Contains("."))
			{
				fieldAlias = field.Value.Substring(field.Value.LastIndexOf(".") + 1);
				tableName = field.Value.Substring(0, field.Value.IndexOf("."));
				fieldName = fieldName.Substring(tableName.Length + 1);
			}
			else
			{
				fieldAlias = field.Value;
				if (tableDeclarations.Count == 1)
				{
					tableName = _tokenizer.Tokens[tableDeclarations[0]].Value;
				}
			}
			if (fieldAliases.ContainsKey(index))
				fieldAlias = fieldAliases[index];
			if (tableAliases.ContainsKey(tableName))
				tableName = tableAliases[tableName];
			string ret = field.Value;
            if (ordinal != -1)
            {
                if (_fieldNames.ContainsKey(ordinal))
                    _fieldNames.Remove(ordinal);
                _fieldNames.Add(ordinal, fieldAlias);
            }
			Type t = LocateTableType(tableName);
			if (t != null)
			{
				TableMap map = ClassMapper.GetTableMap(t);
				if (fieldName.Contains("."))
				{
					while (fieldName.Contains("."))
					{
						ExternalFieldMap efm = (ExternalFieldMap)map[fieldName.Substring(0, fieldName.IndexOf("."))];
						map = ClassMapper.GetTableMap(efm.Type);
						fieldName = fieldName.Substring(fieldName.IndexOf(".") + 1);
					}
					if (fieldName != "*")
					{
						if (map[fieldName] is ExternalFieldMap)
						{
                            if (ordinal != -1)
                            {
                                _tableFieldCounts.Add(ordinal, fieldList[field.Value].Count);
                                _tableFields.Add(fieldAlias, ((ExternalFieldMap)map[fieldName]).Type);
                            }
							ret = "";
							foreach (string str in fieldList[field.Value])
							{
								ret += field.Value.Replace(".", "_") + "." + str+" AS "+fieldAlias+"_"+str+", ";
							}
						}else
							ret = field.Value.Substring(0, field.Value.LastIndexOf(".")).Replace(".", "_") + "." + map.GetTableFieldName(fieldName);
					}
					else
					{
						fieldAlias = "";
						ret = field.Value.Substring(0, field.Value.LastIndexOf(".")).Replace(".", "_") + ".*";
					}
				}
				else
				{
					if (fieldName == "*")
					{
						fieldAlias = "";
						ret = "*";
					}
					else
					{
						if (map[fieldName] is ExternalFieldMap)
						{
							TableMap extMap = ClassMapper.GetTableMap(((ExternalFieldMap)map[fieldName]).Type);
							ret = "";
                            if (ordinal != -1)
                            {
                                _tableFieldCounts.Add(ordinal, fieldList[field.Value].Count);
                                _tableFields.Add(fieldAlias, ((ExternalFieldMap)map[fieldName]).Type);
                            }
							foreach (string str in fieldList[field.Value])
							{
								ret += field.Value.Replace(".", "_") + "." + str + " AS " + fieldAlias + "_" + str + ", ";
							}
						}else
							ret = map.GetTableFieldName(fieldName);
					}
				}
			}
			return ret;
		}
		#endregion

        #region IDataReader Members

        public bool Read()
        {
            return _conn.Read();
        }

        public void Close()
        {
            _conn.CloseConnection();
        }

        public int Depth
        {
            get { return _conn.Depth; }
        }

        public DataTable GetSchemaTable()
        {
            return _conn.GetSchemaTable();
        }

        public bool IsClosed
        {
            get { return _conn.IsClosed; }
        }

        public bool NextResult()
        {
            return _conn.NextResult();
        }

        public int RecordsAffected
        {
            get { return _conn.RecordsAffected; }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            _conn.Dispose();
        }

        #endregion

        #region IDataRecord Members

        public int FieldCount
        {
            get { return _fieldNames.Count; }
        }

        public bool GetBoolean(int i)
        {
            return _conn.GetBoolean(TranslateFieldIndex(i));
        }

        public byte GetByte(int i)
        {
            return _conn.GetByte(TranslateFieldIndex(i));
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return _conn.GetBytes(TranslateFieldIndex(i), fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return _conn.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return GetChars(TranslateFieldIndex(i), fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return _conn.GetData(TranslateFieldIndex(i));
        }

        public string GetDataTypeName(int i)
        {
            return _conn.GetDataTypeName(TranslateFieldIndex(i));
        }

        public DateTime GetDateTime(int i)
        {
            return _conn.GetDateTime(TranslateFieldIndex(i));
        }

        public decimal GetDecimal(int i)
        {
            return _conn.GetDecimal(TranslateFieldIndex(i));
        }

        public double GetDouble(int i)
        {
            return _conn.GetDouble(TranslateFieldIndex(i));
        }

        public Type GetFieldType(int i)
        {
            if (_tableFieldCounts.ContainsKey(i))
                return _tableFields[_fieldNames[i]];
            else
                return _conn.GetFieldType(TranslateFieldIndex(i));
        }

        public float GetFloat(int i)
        {
            return _conn.GetFloat(TranslateFieldIndex(i));
        }

        public Guid GetGuid(int i)
        {
            return _conn.GetGuid(TranslateFieldIndex(i));
        }

        public short GetInt16(int i)
        {
            return _conn.GetInt16(TranslateFieldIndex(i));
        }

        public int GetInt32(int i)
        {
            return _conn.GetInt32(TranslateFieldIndex(i));
        }

        public long GetInt64(int i)
        {
            return _conn.GetInt64(TranslateFieldIndex(i));
        }

        public string GetName(int i)
        {
            return _fieldNames[i];
        }

        public int GetOrdinal(string name)
        {
            foreach (int x in _fieldNames.Keys)
            {
                if (_fieldNames[x] == name)
                    return x;
            }
            throw new Exception("No Field at given position.");
        }

        public string GetString(int i)
        {
            return _conn.GetString(TranslateFieldIndex(i));
        }

        public object GetValue(int i)
        {
            if (IsDBNull(i))
                return null;
            if (_tableFieldCounts.ContainsKey(i))
            {
                Table t = (Table)LazyProxy.Instance(_tableFields[_fieldNames[i]].GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]));
                t.SetValues(_conn,_fieldNames[i]);
                t.LoadStatus = LoadStatus.Complete;
                return t;
            }else
                return _conn.GetValue(TranslateFieldIndex(i));
        }

        public int GetValues(object[] values)
        {
            values = new object[_fieldNames.Count];
            for (int x = 0; x < values.Length; x++)
            {
                values[x] = GetValue(x);
            }
            return values.Length;
        }

        public bool IsDBNull(int i)
        {
            return _conn.IsDBNull(TranslateFieldIndex(i));
        }

        public object this[string name]
        {
            get { return GetValue(GetOrdinal(name)); }
        }

        public object this[int i]
        {
            get { return GetValue(i); }
        }

        #endregion
    }
}
