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
using System.Data;
using Org.Reddragonit.Dbpro.Structure;
using System.Collections;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using System.Reflection;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using Table = Org.Reddragonit.Dbpro.Structure.Table;

namespace Org.Reddragonit.Dbpro.Connections.ClassSQL
{
	/// <summary>
	/// Description of ClassQueryTranslator.
	/// </summary>
	public class ClassQuery : IDataReader
	{
		private static readonly List<string> _conditionOperators =
			new List<string>(new string[]{"=","NOT","IN","LIKE",">","<","<=",">=","IS"});
		
		private string _namespace;
		private QueryTokenizer _tokenizer;
		private string _outputQuery;
        internal string QueryString
        {
            get { return _outputQuery; }
        }
		private Dictionary<int, int> _subQueryIndexes;
        private Dictionary<string, Type> _tableFields;
        private Dictionary<int, string> _fieldNames;
        private Dictionary<int, int> _tableFieldCounts;
        private Dictionary<int, Type> _enumFields;
        private bool _connectionPassed;
		private Connection _conn = null;
	
		public ClassQuery(string NameSpace,string query)
		{
            _connectionPassed = false;
            NewQuery(NameSpace, query);
		}

        public ClassQuery(string NameSpace, string query, Connection conn)
        {
            _connectionPassed = true;
            _conn = conn;
            NewQuery(NameSpace, query);
        }

        public void NewQuery(string NameSpace, string query)
        {
            _outputQuery = "";
            _namespace = NameSpace;
            if (query.StartsWith("(") && query.EndsWith(")"))
                query = query.TrimStart('(').TrimEnd(')');
            query = query.Trim();
            _tokenizer = new QueryTokenizer(query);
            _tokenizer.parse();
            _tableFieldCounts = new Dictionary<int, int>();
            _fieldNames = new Dictionary<int, string>();
            _tableFields = new Dictionary<string, Type>();
            _enumFields = new Dictionary<int, Type>();
            _subQueryIndexes = new Dictionary<int, int>();
            if (_conn != null)
            {
                try
                {
                    _conn.Close();
                }
                catch (Exception e)
                {
                }
            }
            Translate();
            _outputQuery = _outputQuery.Trim();
            for (int x = 0; x < _fieldNames.Count; x++)
                _fieldNames[x] = _fieldNames[x].Trim("'\"".ToCharArray());
            Logger.LogLine("Class Query: " + query + "\nTranslated To:" + _outputQuery);
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
			foreach (int x in Utility.SortDictionaryKeys(subQueryTranlsations.Keys)){
                string orig = "";
                for (int y = 0; y < _subQueryIndexes[x]; y++)
                {
                    orig += _tokenizer.Tokens[y+x].Value + " ";
                }
                _outputQuery = _outputQuery.Replace(orig, subQueryTranlsations[x]);
			}
        }

        #region ConnectionFunctions
        public IDbDataParameter CreateParameter(string name, object value)
        {
            if ((value != null) && (value.GetType().IsEnum))
            {
                if ((value.GetType().IsArray) || (value is IEnumerable))
                {
                    ArrayList tmp = new ArrayList();
                    foreach (Enum en in (IEnumerable)value)
                    {
                        tmp.Add(_conn.Pool.GetEnumID(value.GetType(), value.ToString()));
                    }
                    return _conn.CreateParameter(name, tmp);
                }
                else
                    return _conn.CreateParameter(name, _conn.Pool.GetEnumID(value.GetType(), value.ToString()));
            }else if (value==null)
                return _conn.CreateParameter(name, DBNull.Value);
            else
                return _conn.CreateParameter(name, value);
        }

        private List<IDbDataParameter> CorrectParameters(IDbDataParameter[] parameters,ref string outputQuery)
        {
            List<IDbDataParameter> ret = new List<IDbDataParameter>();
            foreach (IDbDataParameter par in parameters)
            {
                System.Diagnostics.Debug.WriteLine(par.Value.GetType().ToString());
                System.Diagnostics.Debug.WriteLine(par.Value.GetType().IsArray);
                if ((par.Value is IEnumerable) && !(par.Value is string))
                {
                    string newPar = "";
                    int cnt = 0;
                    foreach (object obj in (IEnumerable)par.Value)
                    {
                        newPar += par.ParameterName + "_" + cnt.ToString()+", ";
                        if (obj == null)
                            newPar = newPar.Replace(par.ParameterName + "_" + cnt.ToString() + ", ", "NULL, ");
                        else if (obj is Table)
                        {
                            sTable tm = _conn.Pool.Mapping[obj.GetType()];
                            if (tm.PrimaryKeyFields.Length== 1)
                            {
                                ret.Add(_conn.CreateParameter(par.ParameterName + "_" + cnt.ToString(), ((Table)obj).GetField(tm.PrimaryKeyProperties[0])));
                            }
                            else
                                throw new Exception("Unable to handle arrayed parameter with complex primary key.");
                        }
                        else
                            ret.Add(_conn.CreateParameter(par.ParameterName + "_" + cnt.ToString(), obj));
                        cnt++;
                    }
                    outputQuery = outputQuery.Replace("****" + par.ParameterName + "****", newPar.Substring(0,newPar.Length-2));
                }else{
                    if (par.Value is Table)
                    {
                        sTable tm = _conn.Pool.Mapping[par.Value.GetType()];
                        Table t = (Table)par.Value;
                        if (t == null)
                        {
                            foreach (string prop in tm.PrimaryKeyProperties)
                                outputQuery = Utility.StripNullParameter(outputQuery, par.ParameterName + "_" + prop);
                        }
                        else
                        {
                            foreach (string prop in tm.PrimaryKeyProperties)
                                ret.Add(_conn.CreateParameter(par.ParameterName + "_" + prop, t.GetField(prop)));
                        }
                    }
                    else if (Utility.IsParameterNull(par))
                    {
                        outputQuery = Utility.StripNullParameter(outputQuery, par.ParameterName);
                    }else
                        ret.Add(par);
                }
            }
            return ret;
        }

        private List<IDbDataParameter> CorrectParameters(List<IDbDataParameter> parameters,ref string outputQuery)
        {
            return CorrectParameters(parameters.ToArray(),ref outputQuery);
        }

        public void Execute()
        {
            Execute(new IDbDataParameter[0]);
        }

        public void Execute(List<IDbDataParameter> parameters){
            Execute(parameters.ToArray());
        }

        public void Execute(IDbDataParameter[] parameters)
        {
            try
            {
                if ((parameters != null) && (parameters.Length > 0))
                {
                    List<IDbDataParameter> pars = CorrectParameters(parameters, ref _outputQuery);
                    _conn.ExecuteQuery(_outputQuery, pars);
                }
                else
                    _conn.ExecuteQuery(_outputQuery);
            }
            catch (Exception e)
            {
                throw new Exception("An error occured executing translated query: " + _outputQuery, e);
            }
        }

        public void ExecutePaged(Type primaryTable,ulong? start, ulong? recordCount)
        {
            ExecutePaged(primaryTable,new IDbDataParameter[0], start, recordCount);
        }

        public void ExecutePaged(Type primaryTable, List<IDbDataParameter> parameters, ulong? start, ulong? recordCount)
        {
            ExecutePaged(primaryTable,parameters.ToArray(), start, recordCount);
        }

        public void ExecutePaged(Type primaryTable, IDbDataParameter[] parameters, ulong? start, ulong? recordCount)
        {
            string query = "";
            try
            {
                List<IDbDataParameter> pars = new List<IDbDataParameter>();
                if (parameters != null)
                    pars.AddRange(parameters);
                query = _conn.queryBuilder.SelectPaged(_outputQuery, _conn.Pool.Mapping[primaryTable], ref pars, start, recordCount);
                List<IDbDataParameter> p = CorrectParameters(pars, ref query);
                _conn.ExecuteQuery(query, p);
            }
            catch (Exception e)
            {
                throw new Exception("An error occured executing translated query: " + query, e);
            }
        }

        private int TranslateFieldIndex(int i)
        {
            if (_tableFields.Count > 0)
            {
                int ret = i;
                int shift = 0;
                foreach (int index in Utility.SortDictionaryKeys(_tableFieldCounts.Keys))
                {
                    if (index < i)
                    {
                        ret += _tableFieldCounts[index];
                        shift++;
                    }
                }
                return (ret==i ? i : ret-shift);
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
					(
                        (tokens[x - 1].Value == "(")||
                        (tokens[x-1].Value=="UNION")
                    )
				)
				{
					pos = x;
                    int curPos = x-2;
					EstablishSubQueries(ref pos, tokens);
					x = pos;
                    if (tokens[curPos - 1].Value.ToUpper() == "UNION")
                        pos = curPos;
                    if (x == tokens.Count)
                    {
                        break;
                    }
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
                if (tokens.Count == (x + 1) && bracketCount == 0)
                {
                    pos = x+1;
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
			foreach (int i in Utility.SortDictionaryKeys(_subQueryIndexes.Keys))
			{
                if (!ret.ContainsKey(i))
                {
                    ret.Add(i, TranslateSubQuery(i,new Dictionary<string,string>(), ref parameters,ref ret));
                }
			}
			return ret;
		}

		private string TranslateSubQuery(int i,Dictionary<string,string> parentTableAliases, ref List<IDbDataParameter> parameters,ref Dictionary<int,string> translations)
		{
			List<int> tableDeclarations = new List<int>();
			Dictionary<int, string> fieldAliases = new Dictionary<int, string>();
			Dictionary<string, string> tableAliases = new Dictionary<string, string>();
			List<int> fieldIndexes = new List<int>();
            List<int> joinConditionIndexes = new List<int>();
			Dictionary<int, int> conditionIndexes=new Dictionary<int, int>();
			int x = 1;
			int whereIndex=-1;
            int fromIndex = -1;
			while (
				(x < _subQueryIndexes[i]) &&
				(_tokenizer.Tokens[i + x].Value.ToUpper() != "FROM")
			)
			{
                if (_subQueryIndexes.ContainsKey(i + x)){
					x += _subQueryIndexes[i + x];
                    if ((x > _subQueryIndexes[i]) ||
                    (_tokenizer.Tokens[i + x].Value.ToUpper() == "FROM"))
                        break;
                }
				if ((_tokenizer.Tokens[i + x].Type == TokenType.KEY) &&
				    (
				    	(_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "SELECT") ||
				    	(_tokenizer.Tokens[i+x-1].Value.ToUpper()=="DISTINCT") ||
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
                fromIndex = i + x;
                while (x < _subQueryIndexes[i])
                {
                    if (_subQueryIndexes.ContainsKey(i + x))
                        x += _subQueryIndexes[i + x] + 2;
                    if (((_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "FROM") || (_tokenizer.Tokens[i + x - 1].Value == ",") || _tokenizer.Tokens[i + x - 1].Value.ToUpper() == "JOIN") && (_tokenizer.Tokens[i + x].Value != "("))
                    {
                        if (i + x + 1 < _tokenizer.Tokens.Count)
                        {
                            if ((_tokenizer.Tokens[i + x + 1].Value.ToUpper() != ",") && (_tokenizer.Tokens[i + x + 1].Value != "(") && (_tokenizer.Tokens[i + x + 1].Value.ToUpper() != "WHERE") && (_tokenizer.Tokens[i + x + 1].Value.ToUpper() != "GROUP") && (_tokenizer.Tokens[i + x + 1].Value.ToUpper() != "ORDER"))
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
                        }
                        tableDeclarations.Add(i + x);
                    }
                    else if (_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "ON")
                    {
                        while (x < _subQueryIndexes[i])
                        {
                            if (_subQueryIndexes.ContainsKey(i + x))
                                x += _subQueryIndexes[i + x] + 2;
                            if (_tokenizer.Tokens[i + x].Value.ToUpper() == "WHERE" || _tokenizer.Tokens[i + x].Value.ToUpper() == "ORDER" || _tokenizer.Tokens[i + x].Value.ToUpper() == "GROUP" || _tokenizer.Tokens[i + x].Value == "," || _tokenizer.Tokens[i + x].Value.ToUpper() == "INNER" || _tokenizer.Tokens[i + x].Value.ToUpper() == "OUTER" || _tokenizer.Tokens[i + x].Value.ToUpper() == "LEFT" || _tokenizer.Tokens[i + x].Value.ToUpper() == "RIGHT")
                                break;
                            else if (x + i + 1 == _tokenizer.Tokens.Count)
                                break;
                            else if ((_tokenizer.Tokens[i + x].Type == TokenType.KEY) &&
                                (
                                    (_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "ON") ||
                                    (_tokenizer.Tokens[i + x - 1].Value == ",") ||
                                    (_tokenizer.Tokens[i + x - 1].Value == "(") ||
                                    (_tokenizer.Tokens[i + x + 1].Type == TokenType.OPERATOR) ||
                                    (_tokenizer.Tokens[i + x - 1].Type == TokenType.OPERATOR) ||
                                    (_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "WHEN") ||
                                    (_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "THEN") ||
                                    (_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "ELSE") ||
                                    (_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "AND") ||
                                    (_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "OR")
                                ))
                            {
                                joinConditionIndexes.Add(i + x);
                            }
                            x++;
                        }
                        x--;
                    }
                    else if (x >= _subQueryIndexes[i])
                        break;
                    else if (_tokenizer.Tokens[i + x].Value.ToUpper() == "WHERE" || _tokenizer.Tokens[i + x].Value.ToUpper() == "ORDER" || _tokenizer.Tokens[i + x].Value.ToUpper() == "GROUP")
                        break;
                    x++;
                }
			}
            if (x < _subQueryIndexes[i] && (_tokenizer.Tokens[i + x].Value.ToUpper() == "WHERE" || _tokenizer.Tokens[i + x].Value.ToUpper() == "ORDER" || _tokenizer.Tokens[i + x].Value.ToUpper() == "GROUP"))
			{
				whereIndex=x+i;
				while (x < _subQueryIndexes[i])
				{
                    if (_subQueryIndexes.ContainsKey(i + x))
                        x += _subQueryIndexes[i + x] + 2;
                    if ((_tokenizer.Tokens[i + x].Value.ToUpper() == "GROUP") ||
                        (_tokenizer.Tokens[i + x].Value.ToUpper() == "ORDER"))
                        break;
                    if ((_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "WHERE")
                        || (_tokenizer.Tokens[i + x - 1].Value == "(")
                        || (_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "OR")
                        || (_tokenizer.Tokens[i + x - 1].Value.ToUpper() == "AND")
                        || ((_tokenizer.Tokens[i+x-1].Value.ToUpper()=="NOT")
                            &&(
                                (_tokenizer.Tokens[i + x - 2].Value.ToUpper() == "WHERE")
                                || (_tokenizer.Tokens[i + x - 2].Value.ToUpper() == "OR")
                                || (_tokenizer.Tokens[i + x - 2].Value.ToUpper() == "AND")        
                            )
                        )
					   ){
                           if ((_tokenizer.Tokens[i + x].Value != "(") && (_tokenizer.Tokens[i+x].Value.ToUpper()!="NOT"))
                           {
                               int start = i + x;
                               x++;
                               while (x < _subQueryIndexes[i])
                               {
                                   if (_subQueryIndexes.ContainsKey(i + x + 1))
                                       x += _subQueryIndexes[i + x + 1];
                                   if ((_tokenizer.Tokens[i + x].Value.ToUpper() == "OR")
                                       || (_tokenizer.Tokens[i + x].Value.ToUpper() == "AND")
                                       || (_tokenizer.Tokens[i + x].Value.ToUpper() == "GROUP")
                                       || (_tokenizer.Tokens[i + x].Value.ToUpper() == "ORDER"))
                                   {
                                       break;
                                   }
                                   x++;
                               }
                               conditionIndexes.Add(start, i + x);
                           }
					}
                    if ((x < _subQueryIndexes[i])&&
                        (
                            (_tokenizer.Tokens[i + x].Value.ToUpper() == "GROUP") ||
                            (_tokenizer.Tokens[i + x].Value.ToUpper() == "ORDER")
                        ))
                        break;
					x++;
				}
			}

            int endIndex = i + _subQueryIndexes[i];
            foreach (int subIndex in Utility.SortDictionaryKeys(_subQueryIndexes.Keys))
            {
                if (subIndex > i && subIndex <= endIndex)
                {                    
                    if (!translations.ContainsKey(subIndex))
                    {
                        if (parentTableAliases.Count != 0)
                        {
                            Dictionary<string, string> newList = new Dictionary<string, string>();
                            foreach (string str in parentTableAliases.Keys)
                                newList.Add(str, parentTableAliases[str]);
                            foreach (string str in tableAliases.Keys)
                            {
                                if (newList.ContainsKey(str))
                                    newList.Remove(str);
                                newList.Add(str, tableAliases[str]);
                            }
                            string subquery = TranslateSubQuery(subIndex, newList, ref parameters, ref translations);
                            if (!translations.ContainsKey(subIndex))
                                translations.Add(subIndex, subquery);
                        }
                        else
                        {
                            string subquery = TranslateSubQuery(subIndex, tableAliases, ref parameters, ref translations);
                            if (!translations.ContainsKey(subIndex))
                                translations.Add(subIndex, subquery);
                        }
                    }
                }
            }
            List<int> whereFieldIndexes = new List<int>();
            foreach (int index in Utility.SortDictionaryKeys(conditionIndexes.Keys))
            {
                whereFieldIndexes.AddRange(ExtractFieldsFromCondition(index, conditionIndexes[index]));
            }
			Dictionary<string, List<string>> fieldList = new Dictionary<string, List<string>>();
			string tables = CreateTableQuery(i,fromIndex,tableDeclarations, fieldIndexes,ref fieldList,whereFieldIndexes,joinConditionIndexes,tableAliases,parentTableAliases);
			string fields = TranslateFields(i,fieldIndexes, tableDeclarations, fieldAliases, tableAliases,fieldList,parentTableAliases);
			string wheres = TranslateWhereConditions(whereIndex,conditionIndexes,tableAliases,tableDeclarations,fieldList,parentTableAliases);
            if ((whereFieldIndexes.Count == 0) && (whereIndex > 0) && (whereIndex < _subQueryIndexes[i]))
            {
                wheres = " ";
                while (x < _subQueryIndexes[i])
                {
                    wheres += _tokenizer.Tokens[x + i].Value + " ";
                    x++;
                }
            }
            else if (x < _subQueryIndexes[i])
            {
                if (wheres == "")
                    wheres = " ";
                while (x < _subQueryIndexes[i])
                {
                    wheres += _tokenizer.Tokens[x + i].Value + " ";
                    x++;
                }
            }
            string ending = "";
            if (wheres.ToUpper().Contains(" GROUP BY ") || wheres.ToUpper().Contains(" ORDER BY "))
            {
                int endingStart = wheres.ToUpper().IndexOf(" GROUP BY ");
                if ((endingStart==-1)||(wheres.ToUpper().IndexOf(" ORDER BY ")>endingStart))
                    endingStart = wheres.ToUpper().IndexOf(" ORDER BY ");
                ending = wheres.Substring(endingStart);
                wheres = wheres.Substring(0, endingStart);
                QueryTokenizer qt = new QueryTokenizer(ending.Trim());
                qt.parse();
                if (qt.Tokens.Count > 0)
                    wheres += " " + qt.Tokens[0].Value+" ";
                for (int y = 1; y < qt.Tokens.Count; y++)
                {
                    if (
                        (qt.Tokens[y - 1].Value == ",") ||
                        (qt.Tokens[y - 1].Value == "(") ||
                        (qt.Tokens[y - 1].Value.ToUpper() == "BY") ||
                        ((y+1<qt.Tokens.Count)&&(qt.Tokens[y + 1].Type == TokenType.OPERATOR)) ||
                        (qt.Tokens[y - 1].Type == TokenType.OPERATOR) ||
                        (qt.Tokens[y - 1].Value.ToUpper() == "WHEN") ||
                        (qt.Tokens[y - 1].Value.ToUpper() == "THEN") ||
                        (qt.Tokens[y - 1].Value.ToUpper() == "ELSE")
                        )
                    {
                        wheres+=TranslateGroupOrderByFieldName(y, tableDeclarations, fieldAliases, tableAliases, fieldList, qt)+" ";
                    }
                    else
                        wheres += qt.Tokens[y].Value+ " ";
                }
            }
			return fields+" FROM " + tables+wheres;
		}
		#endregion

        #region GroupOrderByTranslating
        private string TranslateGroupOrderByFieldName(int index, List<int> tableDeclarations, Dictionary<int, string> fieldAliases, Dictionary<string, string> tableAliases, Dictionary<string, List<string>> fieldList,QueryTokenizer tokenizer)
        {
            QueryToken field = tokenizer.Tokens[index];
            string tableName = "";
            string fieldName = tokenizer.Tokens[index].Value;
            string alias = "";
            if (field.Value.Contains("."))
            {
                alias = field.Value.Substring(0, field.Value.IndexOf("."));
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
            string ret = field.Value;
            Type t = LocateTableType(tableName);
            if (t != null)
            {
                sTable map = _conn.Pool.Mapping[t];
                if (fieldName.Contains("."))
                {
                    while (fieldName.Contains("."))
                    {
                        PropertyInfo pi = t.GetProperty(fieldName.Substring(0, fieldName.IndexOf(".")), Utility._BINDING_FLAGS);
                        map = _conn.Pool.Mapping[(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)];
                        fieldName = fieldName.Substring(fieldName.IndexOf(".") + 1);
                    }
                    if (fieldName != "*")
                    {
                        if (map.GetRelationForProperty(fieldName).HasValue)
                        {
                            ret = "";
                            foreach (string str in fieldList[field.Value])
                            {
                                ret += field.Value.Replace(".", "_") + "." + str + ", ";
                            }
                        }
                        else
                            ret = TranslateParentFieldName(field.Value.Substring(0, field.Value.LastIndexOf(".")).Replace(".", "_"),fieldName,t,-1);
                    }
                    else
                    {
                        ret = field.Value.Substring(0, field.Value.LastIndexOf(".")).Replace(".", "_") + ".*";
                    }
                }
                else
                {
                    if (fieldName == "*")
                    {
                        ret = "*";
                    }
                    else
                    {
                        if (map.GetRelationForProperty(fieldName).HasValue && !map.IsEnumProperty(fieldName))
                        {
                            ret = "";
                            foreach (string str in fieldList[field.Value])
                            {
                                ret += field.Value.Replace(".", "_") + "." + str + ", ";
                            }
                        }
                        else
                            ret = TranslateParentFieldName(alias,fieldName,t,-1);
                    }
                }
            }
            return ret;
        }
        #endregion
		
		#region ConditionTranslating
        private string TranslateWhereConditions(int whereIndex, Dictionary<int, int> conditionIndexes, Dictionary<string, string> tableAliases, List<int> tableDeclarations, Dictionary<string, List<string>> fieldList, Dictionary<string, string> parentTableAliases)
        {
			string ret = "";
			if (whereIndex>0){
                string tmp = "Condition Indexes:\n";
                foreach (int i in Utility.SortDictionaryKeys(conditionIndexes.Keys))
                {
                    tmp += i.ToString() + " --> " + conditionIndexes[i].ToString() + "\n";
                }
                Logger.LogLine(tmp);
                tmp = "Condition Texts:\n";
                foreach (int i in Utility.SortDictionaryKeys(conditionIndexes.Keys))
                {
                    int end = conditionIndexes[i];
                    if (end >= _tokenizer.Tokens.Count)
                        end = _tokenizer.Tokens.Count - 1;
                    tmp += _tokenizer.Tokens[i].Value + " ... " + _tokenizer.Tokens[end].Value + "\n";
                }
                Logger.LogLine(tmp);
				int lastIndex=whereIndex;
				foreach (int index in Utility.SortDictionaryKeys(conditionIndexes.Keys)){
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
							condition+=_tokenizer.Tokens[cnt].Value+" ";
						}else if (started)
							break;
						cnt++;
					}
					bool isTabled=false;
					foreach (int x in fields){
						isTabled=isTabled||IsFieldConditionTable(x,tableAliases,tableDeclarations,parentTableAliases);
					}
					if (isTabled){
                        string conditionTemplate = "";
                        int conCnter = 0;
                        List<int> changedIndexes = new List<int>();
                        for (int x = index; x < conditionIndexes[index]; x++)
                        {
                            if (fields.Contains(x) || (_tokenizer.Tokens[x].Type == TokenType.VARIABLE))
                            {
                                conditionTemplate += "{" + conCnter.ToString() + "} ";
                                conCnter++;
                                changedIndexes.Add(x);
                            }
                            else
                                conditionTemplate += _tokenizer.Tokens[x].Value + " ";
                        }
                        changedIndexes.Sort();
                        Type t1 = LocateTableTypeAtIndex(fields[0], tableAliases, tableDeclarations,parentTableAliases);
                        Type t2=null;
                        if (fields.Count == 2)
                        {
                            t2 = LocateTableTypeAtIndex(fields[1], tableAliases, tableDeclarations,parentTableAliases);
                            if (!t1.Equals(t2) && !t1.IsSubclassOf(t2) && !t2.IsSubclassOf(t1))
                                throw new Exception("Unable to compare two table objects that are not of the same type.");
                        }
                        string addition = "";
                        sTable tm = _conn.Pool.Mapping[t1];
                        List<string> tmpFields1 = TranslateConditionField(fields[0], tableDeclarations, tableAliases, fieldList,parentTableAliases);
                        List<string> tmpFields2 = null;
                        if (t2!=null)
                            tmpFields2 = TranslateConditionField(fields[1], tableDeclarations, tableAliases, fieldList,parentTableAliases);
                        string origParam = "";
                        foreach (int y in changedIndexes)
                        {
                            if (_tokenizer.Tokens[y].Type == TokenType.VARIABLE)
                            {
                                origParam = _tokenizer.Tokens[y].Value;
                                break;
                            }
                        }
                        switch (condition.TrimEnd().ToUpper())
                        {
                            case "NOT IN":
                            case "IN":
                                if (tm.PrimaryKeyFields.Length == 1)
                                {
                                    string list = "";
                                    for (int x = fields[0] + condition.Split(' ').Length; x < conditionIndexes[index]; x++)
                                    {
                                        if (_tokenizer.Tokens[x].Type == TokenType.VARIABLE)
                                        {
                                            list += "****" + _tokenizer.Tokens[x].Value + "**** ";
                                        }
                                        else
                                        {
                                            list += _tokenizer.Tokens[x].Value + " ";
                                        }
                                    }
                                    if (!list.StartsWith("("))
                                    {
                                        list = "(" + list + ")";
                                    }
                                    int cntr = 0;
                                    string field1 = tmpFields1[0];
                                    foreach (string str in tm.PrimaryKeyFields)
                                    {
                                        while (!field1.Contains(str) && (cntr < tmpFields1.Count))
                                        {
                                            field1 = tmpFields1[cntr];
                                            cntr++;
                                        }
                                    }
                                    addition+=string.Format(conditionTemplate,field1,list);
                                }
                                else
                                {
                                    //TODO: Need to implement code to handle all variations of using a table value, including if an arrayed parameter of tables is passed in for IN
                                    System.Diagnostics.Debug.WriteLine("Current query: " + ret);
                                    throw new Exception("Unable to handle IN conditions using entire tables with complex primary keys at this time.");
                                }
                                break;
                            case "=":
                            case "<":
                            case "<=":
                            case ">=":
                            case ">":
                            case "NOT LIKE":
                            case "LIKE":
                                string joint = " OR ";
                                if (condition.Trim() == "=" || (condition.TrimEnd().ToUpper() == "LIKE") || (condition.TrimEnd().ToUpper()=="NOT LIKE"))
                                    joint = " AND ";
                                addition = "( ";
                                foreach (string str in tm.PrimaryKeyFields)
                                {
                                    int cntr = 0;
                                    string field1 = tmpFields1[0];
                                    while (!field1.Contains(str) && (cntr < tmpFields1.Count))
                                    {
                                        field1 = tmpFields1[cntr];
                                        cntr++;
                                    }
                                    string field2 = "";
                                    if (fields.Count == 2)
                                    {
                                        cntr = 0;
                                        field2 = tmpFields2[0];
                                        while (!field2.EndsWith(str) && (cntr < tmpFields2.Count))
                                        {
                                            field2 = tmpFields2[cntr];
                                            cntr++;
                                        }
                                    }
                                    else
                                        field2 = origParam + "_" + tm.GetPropertyNameForField(str); ;
                                    if (changedIndexes[0] == fields[0])
                                        addition += "( " + string.Format(conditionTemplate, field1, field2) + " )" + joint;
                                    else
                                        addition += "( " + string.Format(conditionTemplate, field2, field1) + " )" + joint;
                                }
                                addition = addition.Substring(0, addition.Length - joint.Length);
                                addition += " )";
                                break;
                        }
                        ret += addition+" ";
					}else{
						for(int x=index;x<conditionIndexes[index];x++){
							if (fields.Contains(x)){
								ret+=TranslateConditionField(x,tableDeclarations,tableAliases,fieldList,parentTableAliases)[0]+" ";
							}else
								ret+=_tokenizer.Tokens[x].Value+" ";
						}
					}
					lastIndex=conditionIndexes[index];
				}
			}
			return ret;
		}

        private Type LocateTableTypeAtIndex(int index, Dictionary<string, string> tableAliases, List<int> tableDeclarations, Dictionary<string, string> parentTableAliases)
        {
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
            else if (parentTableAliases.ContainsKey(tableName))
                tableName = parentTableAliases[tableName];
            Type t = LocateTableType(tableName);
            if (t != null)
            {
                PropertyInfo pi = null;
                if (fieldName.Contains("."))
                {
                    while (fieldName.Contains("."))
                    {
                        pi = t.GetProperty(fieldName.Substring(0, fieldName.IndexOf(".")), Utility._BINDING_FLAGS);
                        t = (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType);
                        fieldName = fieldName.Substring(fieldName.IndexOf(".") + 1);
                    }
                    t = (_conn.Pool.Mapping.IsMappableType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)) 
                        ? (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType) : t);
                }
                else
                {
                    pi = t.GetProperty(fieldName, Utility._BINDING_FLAGS);
                    t = (_conn.Pool.Mapping.IsMappableType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType))
                        ? (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType) : t);
                }
            }
            return t;
        }

        private bool IsFieldConditionTable(int index, Dictionary<string, string> tableAliases, List<int> tableDeclarations, Dictionary<string, string> parentTableAliases)
        {
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
            else if (parentTableAliases.ContainsKey(tableName))
                tableName = parentTableAliases[tableName];
			Type t = LocateTableType(tableName);
			if (fieldName.EndsWith(".*"))
				return true;
			if (t != null)
			{
                PropertyInfo pi = null;
				if (fieldName.Contains("."))
				{
					while (fieldName.Contains("."))
					{
                        pi = t.GetProperty(fieldName.Substring(0, fieldName.IndexOf(".")), Utility._BINDING_FLAGS_WITH_INHERITANCE);
                        t = (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType);
						fieldName = fieldName.Substring(fieldName.IndexOf(".") + 1);
					}
                    pi = t.GetProperty(fieldName, Utility._BINDING_FLAGS);
					if (_conn.Pool.Mapping.IsMappableType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)))
						return true;
					else
						return false;
				}
				else
				{
                    pi = t.GetProperty(fieldName, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                    if (_conn.Pool.Mapping.IsMappableType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)) && !(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType).IsEnum)
						return true;
					else
						return false;
				}
			}
			return false;
		}


        private List<string> TranslateConditionField(int index, List<int> tableDeclarations, Dictionary<string, string> tableAliases, Dictionary<string, List<string>> fieldList, Dictionary<string, string> parentTableAliases)
		{
			List<string> ret = new List<string>();
			QueryToken field = _tokenizer.Tokens[index];
			string tableName = "";
			string fieldName = _tokenizer.Tokens[index].Value;
            string tableAlias = "";
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
            tableAlias = tableName;
            if (tableAliases.ContainsKey(tableName))
                tableName = tableAliases[tableName];
            else if (parentTableAliases.ContainsKey(tableName))
                tableName = parentTableAliases[tableName];
			Type t = LocateTableType(tableName);
			if (t != null)
			{
                sTable map = _conn.Pool.Mapping[t];
				if (fieldName.Contains("."))
				{
                    PropertyInfo pi = null;
					while (fieldName.Contains("."))
					{
                        pi = t.GetProperty(fieldName.Substring(0, fieldName.IndexOf(".")), Utility._BINDING_FLAGS_WITH_INHERITANCE);
                        map = _conn.Pool.Mapping[(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)];
						fieldName = fieldName.Substring(fieldName.IndexOf(".") + 1);
                        t = (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType);
					}
					if (fieldName != "*")
					{
                        pi = t.GetProperty(fieldName, Utility._BINDING_FLAGS);
						if (_conn.Pool.Mapping.IsMappableType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)))
						{
							ret.RemoveAt(0);
							foreach (string str in fieldList[field.Value])
							{
								ret.Add(field.Value.Replace(".", "_") + "." + str);
							}
						}else
						{
							ret.RemoveAt(0);
                            ret.Add(TranslateParentFieldName(field.Value.Substring(0, field.Value.LastIndexOf(".")).Replace(".", "_"),fieldName,t,-1));
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
                        PropertyInfo pi = t.GetProperty(fieldName, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                        if (_conn.Pool.Mapping.IsMappableType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)) && !(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType).IsEnum)
						{
							ret.RemoveAt(0);
							foreach (string str in fieldList[field.Value])
							{
								ret.Add(field.Value.Replace(".", "_") + "." + str);
							}
						}else{
							ret.RemoveAt(0);
                            ret.Add(TranslateParentFieldName(tableAlias,fieldName,t,-1));
						}
					}
				}
			}
			return ret;
		}
		
		private List<int> ExtractFieldsFromCondition(int start,int end){
			List<int> ret = new List<int>();
			for (int x=start;x<end;x++){
                if (_tokenizer.Tokens[x - 1].Value.ToUpper() == "IN")
                    break;
                else if (_subQueryIndexes.ContainsKey(x))
                    break;
				if ((_tokenizer.Tokens[x].Type == TokenType.KEY) &&
				    (
				    	(_tokenizer.Tokens[x - 1].Value.ToUpper() == "CASE") ||
				    	(_tokenizer.Tokens[x - 1].Value.ToUpper() == "CAST") ||
				    	(_tokenizer.Tokens[x - 1].Value == ",") ||
				    	((_tokenizer.Tokens[x - 1].Value == "(") && (_tokenizer.Tokens[x-2].Value.ToUpper()!="IN") )||
				    	(_tokenizer.Tokens[x - 1].Type == TokenType.OPERATOR) ||
				    	(_tokenizer.Tokens[x - 1].Value.ToUpper() == "WHEN") ||
				    	(_tokenizer.Tokens[x - 1].Value.ToUpper() == "THEN") ||
				    	(_tokenizer.Tokens[x - 1].Value.ToUpper() == "ELSE") ||
                        (_tokenizer.Tokens[x-1].Value.ToUpper()=="WHERE")||
				    	((x+1<end)&&
				    	 (
				    	 	(_tokenizer.Tokens[x+1].Value.ToUpper()=="NOT")||
				    	 	(_tokenizer.Tokens[x+1].Value.ToUpper()=="IN")||
				    	 	(_tokenizer.Tokens[x+1].Value.ToUpper()=="LIKE")||
                            (_tokenizer.Tokens[x+1].Value.ToUpper()=="IS")||
                            (_tokenizer.Tokens[x + 1].Type == TokenType.OPERATOR)
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
		private string CreateTableQuery(int queryIndex,int fromIndex,List<int> tableDeclarations,List<int> fieldIndexes,ref Dictionary<string,List<string>> fieldLists,List<int> whereFieldIndexes,List<int> joinConditionIndexes,Dictionary<string,string> tableAliases,Dictionary<string,string> parentTableAliases)
		{
            if (fromIndex == -1)
                return "";
			string ret = "";
            int previousIndex = fromIndex+1;
			Dictionary<string, List<string>> joins = new Dictionary<string, List<string>>();
			foreach (int x in tableDeclarations)
			{
                for (int y = previousIndex; y < x; y++)
                {
                    if (_subQueryIndexes.ContainsKey(y))
                    {
                        for (int a = 0; a < _subQueryIndexes[y]; a++)
                            ret += _tokenizer.Tokens[y + a].Value + " ";
                        y += _subQueryIndexes[y];
                    }
                    if (joinConditionIndexes.Contains(y))
                        ret += TranslateConditionField(y, tableDeclarations, tableAliases, fieldLists, parentTableAliases)[0]+" ";
                    else
                        ret += _tokenizer.Tokens[y].Value + " ";
                }
                previousIndex = x+1;
				Type t = LocateTableType(_tokenizer.Tokens[x].Value);
				if (t == null)
					throw new CannotLocateTable(_tokenizer.Tokens[x].Value);
				else
				{
					if (_conn == null)
						_conn = ConnectionPoolManager.GetConnection(t).getConnection();
                    _conn.Pool.Updater.InitType(t, _conn);
					sTable map = _conn.Pool.Mapping[t];
					ret += map.Name + " ";
					string alias = _tokenizer.Tokens[x].Value;
                    if ((x + 1 < _tokenizer.Tokens.Count) && (_tokenizer.Tokens[x + 1].Value != ",") && (_tokenizer.Tokens[x + 1].Value.ToUpper() != "WHERE")
                        && (_tokenizer.Tokens[x + 1].Value.ToUpper() != "LEFT") && (_tokenizer.Tokens[x + 1].Value.ToUpper() != "RIGHT") && (_tokenizer.Tokens[x + 1].Value.ToUpper() != "INNER") && (_tokenizer.Tokens[x + 1].Value.ToUpper() != "OUTER"))
                    {
                        previousIndex++;
                        alias = _tokenizer.Tokens[x + 1].Value;
                    }
					List<string> tmpJoins = new List<string>();
					foreach (int index in fieldIndexes)
					{
						if (_tokenizer.Tokens[index].Value.StartsWith(alias))
							tmpJoins = TraceJoins(tmpJoins,  t, _tokenizer.Tokens[index].Value.Substring(alias.Length + 1), alias,ref fieldLists);
					}
					foreach (int index in whereFieldIndexes){
						if (_tokenizer.Tokens[index].Value.StartsWith(alias))
							tmpJoins=TraceJoins(tmpJoins,t,_tokenizer.Tokens[index].Value.Substring(alias.Length+1),alias,ref fieldLists);
					}
                    foreach (int index in joinConditionIndexes)
                    {
                        if (_tokenizer.Tokens[index].Value.StartsWith(alias))
                            tmpJoins=TraceJoins(tmpJoins, t, _tokenizer.Tokens[index].Value.Substring(alias.Length + 1), alias, ref fieldLists);
                    }
					if (alias != _tokenizer.Tokens[x].Value)
					{
						joins.Add(map.Name + " " + alias, tmpJoins);
						ret += alias;
					}
					else
						joins.Add(map.Name + " ", tmpJoins);
				}
			}
			foreach (string str in joins.Keys)
			{
				string joinString = "";
				foreach (string s in joins[str])
					joinString += s + " ";
				ret = ret.Replace(str, str + " " + joinString);
			}
            int z = previousIndex;
            if (_subQueryIndexes.ContainsKey(z))
            {
                for (int x = 0; x < _subQueryIndexes[z]; x++)
                    ret += _tokenizer.Tokens[z + x].Value + " ";
                z += _subQueryIndexes[z];
            }
            if ((z < _subQueryIndexes[queryIndex] + queryIndex))
            {
                while (_tokenizer.Tokens[z].Value.ToUpper() != "WHERE"
                    && _tokenizer.Tokens[z].Value.ToUpper() != "GROUP"
                    && _tokenizer.Tokens[z].Value.ToUpper() != "ORDER"
                    && (z < _subQueryIndexes[queryIndex] + queryIndex))
                {
                    if (joinConditionIndexes.Contains(z))
                        ret += TranslateConditionField(z, tableDeclarations, tableAliases, fieldLists, parentTableAliases)[0]+" ";
                    else
                        ret += _tokenizer.Tokens[z].Value + " ";
                    z++;
                    if ((z >= _subQueryIndexes[queryIndex] + queryIndex))
                        break;
                    if (_subQueryIndexes.ContainsKey(z))
                    {
                        for (int x = 0; x < _subQueryIndexes[z]; x++)
                            ret += _tokenizer.Tokens[z + x].Value + " ";
                        z += _subQueryIndexes[z];
                    }
                }
            }
			return ret;
		}

		private List<string> TraceJoins(List<string> joins, Type baseType, string field, string alias,ref Dictionary<string,List<string>> fieldLists)
		{
			string origField = field;
			string origAlias = alias;
            Type cur = baseType;
            bool parentIsLeftJoin = false;
            sTable map;
            PropertyInfo pi;
            INullable nlb = null;
			if (field.Contains("."))
			{
				while (field.Contains("."))
				{
                    pi = cur.GetProperty(field.Substring(0, field.IndexOf(".")), Utility._BINDING_FLAGS_WITH_INHERITANCE);
                    string talias = alias;
                    if (!pi.DeclaringType.Equals(cur))
                    {
                        Type pType = cur;
                        while (!pi.DeclaringType.Equals(pType))
                        {
                            pType = pType.BaseType;
                            _conn.Pool.Updater.InitType(pType, _conn);
                            sTable parentMap = _conn.Pool.Mapping[pType];
                            string iJoin = " " + (parentIsLeftJoin ? "LEFT JOIN" : "INNER JOIN") + " " + parentMap.Name + " " + talias + "_prnt ON ";
                            foreach (string key in parentMap.PrimaryKeyFields)
                                iJoin += talias + "." + key + " = " + talias + "_prnt." + key + " AND ";
                            iJoin = iJoin.Substring(0, iJoin.Length - 4);
                            if (!joins.Contains(iJoin))
                                joins.Add(iJoin);
                            talias += "_prnt";
                            cur = pType;
                        }
                    }
                    foreach (object obj in pi.GetCustomAttributes(false))
                    {
                        if (obj is INullable)
                        {
                            nlb = (INullable)obj;
                            break;
                        }
                    }
                    _conn.Pool.Updater.InitType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType), _conn);
                    sTable eMap = _conn.Pool.Mapping[(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)];
                    map = _conn.Pool.Mapping[cur];
					string className = field.Substring(0, field.IndexOf("."));
					string innerJoin = " "+(parentIsLeftJoin ? "LEFT JOIN" : "INNER JOIN")+" ";
                    if (nlb.Nullable)
                        innerJoin = " LEFT JOIN ";
                    string tbl = _conn.queryBuilder.SelectAll((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType), null);
                    List<string> fields = new List<string>();
                    string fieldString = tbl.Substring(tbl.IndexOf("SELECT") + "SELECT".Length);
                    fieldString = fieldString.Substring(0, fieldString.IndexOf("FROM"));
                    foreach (string str in fieldString.Split(','))
                    {
                        if (str.Length > 0)
                            fields.Add(str.Substring(str.LastIndexOf(".") + 1));
                    }
                    if (!fieldLists.ContainsKey(origAlias + "." + pi.Name))
                        fieldLists.Add(origAlias + "." + pi.Name, fields);
					if (pi.PropertyType.IsArray)
					{
                        sTable iMap = _conn.Pool.Mapping[cur, pi.Name];
						innerJoin += iMap.Name + " " + alias + "_intermediate_" + className + " ON ";
                        foreach (sTableField f in iMap.Fields)
                        {
                            if (f.ClassProperty=="PARENT")
                                innerJoin += " " + talias + "." + f.ExternalField + " = " + alias + "_intermediate_" + className + "." + f.Name + " AND ";
                        }
						innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                        if (!joins.Contains(innerJoin))
                            joins.Add(innerJoin);
                        if (nlb.Nullable)
                            innerJoin = " LEFT JOIN (" + tbl + ") " + alias + "_" + className + " ON ";
                        else
                            innerJoin = " "+(parentIsLeftJoin ? "LEFT JOIN" : "INNER JOIN")+" (" + tbl + ") " + alias + "_" + className + " ON ";
                        List<string> pkeys = new List<string>(eMap.PrimaryKeyFields);
                        foreach (sTableField f in iMap.Fields)
                        {
                            if (f.ClassProperty=="CHILD" && f.ExternalField!=null && pkeys.Contains(f.ExternalField))
                                innerJoin += " " + alias + "_intermediate_" + className + "." + f.Name + " = " + alias + "_" + className + "." + f.ExternalField + " AND ";
                        }
						innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
					}
					else
					{
                        innerJoin += "(" + tbl + ") " + alias + "_" + className + " ON ";
						foreach (sTableField f in map[pi.Name])
							innerJoin += " " + talias + "." + f.Name + " = " + alias + "_" + className + "." + f.ExternalField + " AND ";
						innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
					}
					alias += "_" + field.Substring(0, field.IndexOf("."));
					field = field.Substring(field.IndexOf(".") + 1);
					if (!joins.Contains(innerJoin))
						joins.Add(innerJoin);

                    cur = (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType);
                    parentIsLeftJoin |= nlb.Nullable;
				}
			}
            map = _conn.Pool.Mapping[cur];
            if (field == "*")
            {
                foreach (string prop in map.PrimaryKeyProperties)
                {
                    pi = cur.GetProperty(prop, Utility._BINDING_FLAGS);
                    foreach (object obj in pi.GetCustomAttributes(false))
                    {
                        if (obj is INullable)
                        {
                            nlb = (INullable)obj;
                            break;
                        }
                    }
                    if (_conn.Pool.Mapping.IsMappableType((pi.PropertyType.IsArray? pi.PropertyType.GetElementType() : pi.PropertyType)))
                    {
                        _conn.Pool.Updater.InitType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType), _conn);
                        sTable eMap = _conn.Pool.Mapping[(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)];
                        string innerJoin = " INNER JOIN ";
                        if (nlb.Nullable)
                            innerJoin = " LEFT JOIN ";
                        string tbl = _conn.queryBuilder.SelectAll((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType), null);
                        string fieldString = tbl.Substring(tbl.IndexOf("SELECT") + "SELECT".Length);
                        List<string> fields = new List<string>();
                        fieldString = fieldString.Substring(0, fieldString.IndexOf("FROM"));
                        foreach (string str in fieldString.Split(','))
                        {
                            if (str.Length > 0)
                                fields.Add(str.Substring(str.LastIndexOf(".") + 1));
                        }
                        if (!fieldLists.ContainsKey(origAlias + "." + origField))
                            fieldLists.Add(origAlias + "." + origField.Substring(0, origField.Length - field.Length) + "." + field.Substring(0, field.IndexOf(".")), fields);
                        if (pi.PropertyType.IsArray)
                        {
                            sTable iMap = _conn.Pool.Mapping[cur, pi.Name];
                            innerJoin += iMap.Name + " " + alias + "_intermediate_" + prop+ " ON ";
                            foreach (sTableField f in iMap.Fields)
                            {
                                if (f.ClassProperty != null)
                                    innerJoin += " " + alias + "." + f.ExternalField + " = " + alias + "_intermediate_" + prop + "." + f.Name + " AND ";
                            }
                            innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                            if (!joins.Contains(innerJoin))
                                joins.Add(innerJoin);
                            if (nlb.Nullable)
                                innerJoin = " LEFT JOIN (" + tbl + ") " + alias + "_" + prop+ " ON ";
                            else
                                innerJoin = " " + (parentIsLeftJoin ? "LEFT JOIN" : "INNER JOIN") + " (" + tbl + ") " + alias + "_" + prop + " ON ";
                            List<string> pkeys = new List<string>(eMap.PrimaryKeyFields);
                            foreach (sTableField f in iMap.Fields)
                            {
                                if (f.ClassProperty == null && f.ExternalField != null && pkeys.Contains(f.ExternalField))
                                    innerJoin += " " + alias + "_intermediate_" + prop + "." + f.Name + " = " + alias + "_" + prop+ "." + f.ExternalField + " AND ";
                            }
                            innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                        }
                        else
                        {
                            innerJoin += "(" + tbl + ") " + alias + "_" + prop + " ON ";
                            foreach (sTableField f in map[prop])
                                innerJoin += " " + alias + "." + f.Name + " = " + alias + "_" + prop + "." + f.ExternalField + " AND ";
                            innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                        }
                        if (!joins.Contains(innerJoin))
                            joins.Add(innerJoin);
                    }
                    else if (!pi.DeclaringType.Equals(cur))
                    {
                        Type pType = cur;
                        while (!pi.DeclaringType.Equals(pType))
                        {
                            pType = pType.BaseType;
                            _conn.Pool.Updater.InitType(pType, _conn);
                            sTable parentMap = _conn.Pool.Mapping[pType];
                            string iJoin = " " + (parentIsLeftJoin ? "LEFT JOIN" : "INNER JOIN") + " " + parentMap.Name + " " + alias + "_prnt ON ";
                            foreach (string key in parentMap.PrimaryKeyFields)
                                iJoin += alias + "." + key + " = " + alias + "_prnt." + key + " AND ";
                            iJoin = iJoin.Substring(0, iJoin.Length - 4);
                            if (!joins.Contains(iJoin))
                                joins.Add(iJoin);
                            alias += "_prnt";
                            cur = pType;
                        }
                    }
                }
            }
            else
            {
                pi = cur.GetProperty(field, Utility._BINDING_FLAGS);
                if (pi == null)
                {
                    Type pType = cur.BaseType;
                    while (_conn.Pool.Mapping.IsMappableType(pType))
                    {
                        _conn.Pool.Updater.InitType(pType, _conn);
                        sTable parentMap = _conn.Pool.Mapping[pType];
                        string iJoin = " " + (parentIsLeftJoin ? "LEFT JOIN" : "INNER JOIN") + " " + parentMap.Name + " " + alias + "_prnt ON ";
                        foreach (string key in parentMap.PrimaryKeyFields)
                            iJoin += alias + "." + key + " = " + alias + "_prnt." + key + " AND ";
                        iJoin = iJoin.Substring(0, iJoin.Length - 4);
                        if (!joins.Contains(iJoin))
                            joins.Add(iJoin);
                        alias += "_prnt";
                        pi = pType.GetProperty(field, Utility._BINDING_FLAGS);
                        cur = pType;
                        if (pi != null)
                            break;
                        pType = pType.BaseType;
                    }
                }
                foreach (object obj in pi.GetCustomAttributes(false))
                {
                    if (obj is INullable)
                    {
                        nlb = (INullable)obj;
                        break;
                    }
                }
                if (_conn.Pool.Mapping.IsMappableType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)) && !(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType).IsEnum)
                {
                    _conn.Pool.Updater.InitType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType), _conn);
                    sTable eMap = _conn.Pool.Mapping[(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)];
                    string innerJoin = " INNER JOIN ";
                    if (nlb.Nullable)
                        innerJoin = " LEFT JOIN ";
                    string tbl = _conn.queryBuilder.SelectAll((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType), null);
                    string fieldString = tbl.Substring(tbl.IndexOf("SELECT") + "SELECT".Length);
                    List<string> fields = new List<string>();
                    fieldString = fieldString.Substring(0, fieldString.IndexOf("FROM"));
                    foreach (string str in fieldString.Split(','))
                    {
                        if (str.Length > 0)
                            fields.Add(str.Substring(str.LastIndexOf(".") + 1));
                    }
                    if (!fieldLists.ContainsKey(origAlias + "." + origField))
                        fieldLists.Add(origAlias + "." + origField, fields);
                    if (pi.PropertyType.IsArray)
                    {
                        sTable iMap = _conn.Pool.Mapping[cur, pi.Name];
                        innerJoin += iMap.Name + " " + alias + "_intermediate_" + field + " ON ";
                        foreach (sTableField f in iMap.Fields)
                        {
                            if (f.ClassProperty=="PARENT")
                                innerJoin += " " + alias + "." + f.ExternalField + " = " + alias + "_intermediate_" + field + "." + f.Name + " AND ";
                        }
                        innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                        if (!joins.Contains(innerJoin))
                            joins.Add(innerJoin);
                        if (nlb.Nullable)
                            innerJoin = " LEFT JOIN (" + tbl + ") " + alias + "_" + field + " ON ";
                        else
                            innerJoin = " " + (parentIsLeftJoin ? "LEFT JOIN" : "INNER JOIN") + " (" + tbl + ") " + alias + "_" + field + " ON ";
                        List<string> pkeys = new List<string>(eMap.PrimaryKeyFields);
                        foreach (sTableField f in iMap.Fields)
                        {
                            if (f.ClassProperty =="CHILD")
                                innerJoin += " " + alias + "_intermediate_" + field + "." + f.Name + " = " + alias + "_" + field + "." + f.ExternalField + " AND ";
                        }
                        innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                    }
                    else
                    {
                        innerJoin += "(" + tbl + ") " + alias + "_" + field + " ON ";
                        foreach (sTableField f in map[field])
                            innerJoin += " " + alias + "." + f.Name + " = " + alias + "_" + field + "." + f.ExternalField + " AND ";
                        innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                    }
                    if (!joins.Contains(innerJoin))
                        joins.Add(innerJoin);
                }
                else if (!pi.DeclaringType.Equals(cur))
                {
                    Type pType = cur;
                    while (!pi.DeclaringType.Equals(pType))
                    {
                        pType = pType.BaseType;
                        _conn.Pool.Updater.InitType(pType, _conn);
                        sTable parentMap = _conn.Pool.Mapping[pType];
                        string iJoin = " " + (parentIsLeftJoin ? "LEFT JOIN" : "INNER JOIN") + " " + parentMap.Name + " " + alias + "_prnt ON ";
                        foreach (string key in parentMap.PrimaryKeyFields)
                            iJoin += alias + "." + key + " = " + alias + "_prnt." + key + " AND ";
                        iJoin = iJoin.Substring(0, iJoin.Length - 4);
                        if (!joins.Contains(iJoin))
                            joins.Add(iJoin);
                        alias += "_prnt";
                        cur = pType;
                    }
                }
                else if (pi.PropertyType.IsArray)
                {
                    sTable iMap = _conn.Pool.Mapping[cur, pi.Name];
                    string innerJoin = (nlb.Nullable || parentIsLeftJoin ? " LEFT JOIN " : " INNER JOIN ")+iMap.Name + " " + alias + "_" + pi.Name+ " ON ";
                    foreach (sTableField f in iMap.Fields)
                    {
                        if (f.ClassProperty=="PARENT")
                            innerJoin += " " + alias + "." + f.ExternalField + " = " + alias + "_" + pi.Name + "." + f.Name + " AND ";
                    }
                    innerJoin = innerJoin.Substring(0, innerJoin.Length - 5);
                    if (!joins.Contains(innerJoin))
                        joins.Add(innerJoin);
                }
            }
			return joins;
		}

        private string TranslateParentFieldName(string alias,string field, Type table,int ordinal)
        {
            string ret = null;
            PropertyInfo pi = table.GetProperty(field, Utility._BINDING_FLAGS_WITH_INHERITANCE);
            if (pi == null)
            {
                if (alias == "")
                    return field;
                return alias + "." + field;
            }
            while (!pi.DeclaringType.Equals(table))
            {
                table = table.BaseType;
                alias += "_prnt";
            }
            if (pi.DeclaringType.Equals(table))
            {
                if ((ordinal != -1) && pi.PropertyType.IsEnum)
                {
                    if (_enumFields.ContainsKey(ordinal))
                        _enumFields.Remove(ordinal);
                    Logger.LogLine("Assigning field at ordinal: " + ordinal.ToString() + " as an enumeration type: " + pi.PropertyType.FullName);
                    _enumFields.Add(ordinal, pi.PropertyType);
                }
                if (pi.PropertyType.IsArray)
                    ret = alias + "_" + field + "." + _conn.Pool.Translator.GetIntermediateValueFieldName(table,pi,_conn);
                else
                    ret = alias + "." + _conn.Pool.Mapping[table][pi.Name][0].Name;
            }
            return ret;
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
        private string TranslateFields(int subqueryIndex, List<int> fieldIndexes, List<int> tableDeclarations, Dictionary<int, string> fieldAliases, Dictionary<string, string> tableAliases, Dictionary<string, List<string>> fieldList, Dictionary<string, string> parentTableAliases)
		{
            _fieldNames.Clear();
            int ordinal = 0;
            int preOrdinal = 0;
			string ret = "";
			int previousIndex = subqueryIndex+1;
            ret += _tokenizer.Tokens[subqueryIndex].Value + " ";
			foreach (int x in fieldIndexes){
				for (int y = previousIndex; y<x ; y++)
				{
                    if (_subQueryIndexes.ContainsKey(y))
                    {
                        for (int a = 0; a < _subQueryIndexes[y]; a++)
                            ret += _tokenizer.Tokens[y + a].Value + " ";
                        y += _subQueryIndexes[y];
                    }
                    if (subqueryIndex == 0)
                    {
                        if (_tokenizer.Tokens[y - 1].Value == ")" && _tokenizer.Tokens[y].Value == ",")
                        {
                            ret += " AS " + _conn.WrapAlias("FIELD_" + ordinal.ToString()) + ", ";
                            if (_fieldNames.ContainsKey(ordinal))
                                ordinal++;
                            _fieldNames.Add(ordinal, "FIELD_" + ordinal.ToString());
                            ordinal++;
                            preOrdinal++;
                        }
                        else if (_tokenizer.Tokens[y - 1].Value == ")" && _tokenizer.Tokens[y].Value.ToUpper() == "AS" && (_tokenizer.Tokens[y + 2].Value == "," || _tokenizer.Tokens[y + 2].Value.ToUpper() == "FROM"))
                        {
                            ret += " AS " + _conn.WrapAlias(_tokenizer.Tokens[y + 1].Value.Trim("'\"".ToCharArray()));
                            if (_fieldNames.ContainsKey(ordinal))
                                ordinal++;
                            _fieldNames.Add(ordinal, _tokenizer.Tokens[y + 1].Value.Trim("'\"".ToCharArray()));
                            ordinal++;
                            preOrdinal++;
                            y += 1;
                        }
                        else
                            ret += _tokenizer.Tokens[y].Value + " ";
                    }else
                        ret += _tokenizer.Tokens[y].Value + " ";
				}
                if (ret.TrimEnd(' ').EndsWith(","))
                    ordinal++;
				string fieldAlias;
                if (ordinal > preOrdinal + 1)
                    ordinal = preOrdinal + 1;
                if (subqueryIndex == 0)
                {
                    ret += TranslateFieldName(ordinal, x, out fieldAlias, tableDeclarations, fieldAliases, tableAliases, fieldList,parentTableAliases) + " ";
                    if (_tokenizer.Tokens[x + 1].Value != "," && _tokenizer.Tokens[x + 1].Value.ToUpper() != "AS" && _tokenizer.Tokens[x + 1].Value.ToUpper() != "FROM")
                    {
                        if (_fieldNames.ContainsKey(ordinal))
                        {
                            if (_fieldNames[ordinal] == fieldAlias)
                            {
                                _fieldNames.Remove(ordinal);
                                ordinal--;
                            }
                        }
                        fieldAlias = "";
                    }
                    preOrdinal = ordinal;
                }
                else
                {
                    preOrdinal = ordinal;
                    ret += TranslateFieldName(-1, x, out fieldAlias, tableDeclarations, fieldAliases, tableAliases, fieldList,parentTableAliases) + " ";
                }
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
                    int incr = 1;
                    if ((fieldAlias != "") && ((_tokenizer.Tokens[x + 1].Value == ",") || (_tokenizer.Tokens[x + 1].Value == "FROM")))
                        ret += " AS " + _conn.WrapAlias(fieldAlias);
                    else if (_tokenizer.Tokens[x + 1].Value.ToUpper() == "AS" && _tokenizer.Tokens[x + 2].Value == fieldAlias)
                    {
                        if (_tokenizer.Tokens[x + 3].Value.ToUpper() == "FROM")
                        {
                            ret += _tokenizer.Tokens[x + 1].Value + " " + _tokenizer.Tokens[x + 2].Value + " ";
                            incr = 3;
                        }
                        else
                        {
                            ret += _tokenizer.Tokens[x + 1].Value + " " + _tokenizer.Tokens[x + 2].Value + " " + _tokenizer.Tokens[x + 3].Value;
                            incr = 4;
                        }
                    }
					previousIndex = x + incr;
				}
                if (ret.TrimEnd(' ').EndsWith(","))
                    ordinal++;
                if (ordinal > preOrdinal + 1)
                    ordinal = preOrdinal + 1;
			}
			int z = previousIndex;
            if (_subQueryIndexes.ContainsKey(z))
            {
                for (int x = 0; x < _subQueryIndexes[z]; x++)
                    ret += _tokenizer.Tokens[z + x].Value + " ";
                z += _subQueryIndexes[z];
            }
			while (_tokenizer.Tokens[z].Value.ToUpper() != "FROM")
			{
                if (subqueryIndex == 0)
                {
                    if (_tokenizer.Tokens[z - 1].Value == ")" && _tokenizer.Tokens[z].Value == ",")
                    {
                        ret += " AS " + _conn.WrapAlias("FIELD_" + ordinal.ToString()) + ", ";
                        if (_fieldNames.ContainsKey(ordinal))
                            ordinal++;
                        _fieldNames.Add(ordinal, "FIELD_" + ordinal.ToString());
                        ordinal++;
                        preOrdinal++;
                    }
                    else if (_tokenizer.Tokens[z - 1].Value == ")" && _tokenizer.Tokens[z].Value.ToUpper() == "AS" && (_tokenizer.Tokens[z + 2].Value == "," || _tokenizer.Tokens[z + 2].Value.ToUpper() == "FROM"))
                    {
                        ret += " AS " + _conn.WrapAlias(_tokenizer.Tokens[z + 1].Value.Trim("'\"".ToCharArray()));
                        if (_fieldNames.ContainsKey(ordinal))
                            ordinal++;
                        _fieldNames.Add(ordinal, _tokenizer.Tokens[z + 1].Value.Trim("'\"".ToCharArray()));
                        ordinal++;
                        preOrdinal++;
                        z += 1;
                    }
                    else
                        ret += _tokenizer.Tokens[z].Value + " ";
                }else
                    ret += _tokenizer.Tokens[z].Value + " ";
				z++;
                if (_subQueryIndexes.ContainsKey(z))
                {
                    for (int x = 0; x < _subQueryIndexes[z]; x++)
                        ret += _tokenizer.Tokens[z + x].Value + " ";
                    z += _subQueryIndexes[z];
                }
			}
			return ret;
		}

        private string TranslateFieldName(int ordinal, int index, out string fieldAlias, List<int> tableDeclarations, Dictionary<int, string> fieldAliases, Dictionary<string, string> tableAliases, Dictionary<string, List<string>> fieldList, Dictionary<string, string> parentTableAliases)
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
            string tableAlias = tableName;
			if (fieldAliases.ContainsKey(index))
				fieldAlias = fieldAliases[index];
            if (tableAliases.ContainsKey(tableName))
                tableName = tableAliases[tableName];
            else if (parentTableAliases.ContainsKey(tableName))
                tableName = parentTableAliases[tableName];
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
				sTable map = _conn.Pool.Mapping[t];
                PropertyInfo pi = null;
				if (fieldName.Contains("."))
				{
					while (fieldName.Contains("."))
					{
                        pi = t.GetProperty(fieldName.Substring(0, fieldName.IndexOf(".")), Utility._BINDING_FLAGS_WITH_INHERITANCE);
                        t = (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType);
                        map = _conn.Pool.Mapping[t];
						fieldName = fieldName.Substring(fieldName.IndexOf(".") + 1);
					}
					if (fieldName != "*")
					{
                        while ((pi = t.GetProperty(fieldName, Utility._BINDING_FLAGS)) == null)
                        {
                            t=t.BaseType;
                            map = _conn.Pool.Mapping[t];
                        }
                        if (_conn.Pool.Mapping.IsMappableType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)))
                        {
                            if (ordinal != -1)
                            {
                                _tableFieldCounts.Add(ordinal, fieldList[field.Value].Count);
                                _tableFields.Add(fieldAlias, (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType));
                            }
                            ret = "";
                            foreach (string str in fieldList[field.Value])
                            {
                                ret += field.Value.Replace(".", "_") + "." + str + " AS " + _conn.WrapAlias(fieldAlias + "_" + str) + ", ";
                            }
                        }
                        else
                            ret = TranslateParentFieldName(field.Value.Substring(0, field.Value.LastIndexOf(".")).Replace(".", "_"), fieldName,t,ordinal);
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
                        pi = t.GetProperty(fieldName, Utility._BINDING_FLAGS);
                        if (pi == null)
                            pi = t.GetProperty(fieldName, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                        if (_conn.Pool.Mapping.IsMappableType((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)) && !(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType).IsEnum)
                        {
                            ret = "";
                            if (ordinal != -1)
                            {
                                _tableFieldCounts.Add(ordinal, fieldList[field.Value].Count);
                                _tableFields.Add(fieldAlias, (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType));
                            }
                            foreach (string str in fieldList[field.Value])
                            {
                                ret += field.Value.Replace(".", "_") + "." + str + " AS " + _conn.WrapAlias(fieldAlias + "_" + str) + ", ";
                            }
                        }
                        else
                        {
                            ret = TranslateParentFieldName(tableAlias, fieldName, t,ordinal);
                        }
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
            if (_connectionPassed)
                _conn.Close();
            else
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
            return _conn.GetChars(TranslateFieldIndex(i), fieldoffset, buffer, bufferoffset, length);
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

        public object GetValue(int i){
            if (IsDBNull(i))
                return null;
            if (_tableFieldCounts.ContainsKey(i))
            {
                Table t = (Table)LazyProxy.Instance(_tableFields[_fieldNames[i]].GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]));
                sTableField[] flds = _conn.Pool.Mapping[_tableFields[_fieldNames[i]]].Fields;
                int index = 0;
                i = TranslateFieldIndex(i);
                foreach (sTableField fld in flds)
                {
                    PropertyInfo pi = t.GetType().GetProperty(fld.ClassProperty, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                    if (!pi.PropertyType.IsArray)
                    {
                        if (_conn.Pool.Mapping.IsMappableType(pi.PropertyType) && !pi.PropertyType.IsEnum)
                        {
                            if (t.GetField(pi.Name) == null)
                            {
                                Table tmp = (Table)LazyProxy.Instance(pi.PropertyType.GetConstructor(Type.EmptyTypes).Invoke(new object[0]));
                                tmp.LoadStatus=LoadStatus.Partial;
                                t.SetField(fld.Name,tmp );
                            }
                            Table tbl = (Table)t.GetField(pi.Name);
                            foreach (sTableField f in _conn.Pool.Mapping[tbl.GetType()].Fields)
                            {
                                if (fld.ExternalField == f.Name)
                                {
                                    t.RecurSetPropertyValue(f.Name, _conn, _conn.GetName(i + index), tbl);
                                    index++;
                                    break;
                                }
                            }
                        }
                        else
                        {
                            if (pi.PropertyType.IsEnum)
                                t.SetField(fld.ClassProperty, _conn.Pool.GetEnumValue(pi.PropertyType, _conn.GetInt32(i+index)));
                            else
                                t.SetField(fld.ClassProperty, _conn[i + index]);
                            index++;
                        }
                    }
                }
                t.LoadStatus = LoadStatus.Complete;
                return t;
            }
            else if (_enumFields.ContainsKey(i))
            {
                return _conn.Pool.GetEnumValue(_enumFields[i], _conn.GetInt32(TranslateFieldIndex(i)));
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
            if (!_tableFieldCounts.ContainsKey(i))
                return _conn.IsDBNull(TranslateFieldIndex(i));
            else
            {
                int start = TranslateFieldIndex(i);
                for (int x = 0; x < _tableFieldCounts[i]; x++)
                {
                    if (!_conn.IsDBNull(x + start))
                        return false;
                }
                return true;
            }
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
