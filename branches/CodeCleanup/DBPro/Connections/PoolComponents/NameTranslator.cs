using System;
using System.Collections.Generic;
using System.Text;

namespace Org.Reddragonit.Dbpro.Connections.PoolComponents
{
    internal class NameTranslator
    {
        private ConnectionPool _pool;
        private Dictionary<string, string> _nameTranslations = new Dictionary<string, string>();

        public NameTranslator(ConnectionPool pool)
        {
            _pool = pool;
            _nameTranslations = new Dictionary<string, string>();
        }

        public string CorrectName(string currentName)
        {
            if (_nameTranslations.ContainsValue(currentName.ToUpper()))
            {
                foreach (string str in _nameTranslations.Keys)
                {
                    if (_nameTranslations[str] == currentName.ToUpper())
                        return str.ToUpper();
                }
                return null;
            }
            else if (_nameTranslations.ContainsKey(currentName))
                return currentName;
            else
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
                ret = ShortenName(ret);
                if (_nameTranslations.ContainsKey(ret))
                {
                    int _nameCounter = 0;
                    while (_nameTranslations.ContainsKey(ret.Substring(0, _pool.MaxFieldNameLength - 1 - (_nameCounter.ToString().Length)) + "_" + _nameCounter.ToString()))
                    {
                        _nameCounter++;
                    }
                    ret = ret.Substring(0, _pool.MaxFieldNameLength - 1 - (_nameCounter.ToString().Length));
                    ret += "_" + _nameCounter.ToString();
                }
                if (!_nameTranslations.ContainsKey(ret))
                    _nameTranslations.Add(ret, currentName.ToUpper());
                return ret.ToUpper();
            }
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
