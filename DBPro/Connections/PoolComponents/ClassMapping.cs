using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using Org.Reddragonit.Dbpro.Structure.Attributes;
using Org.Reddragonit.Dbpro.Virtual.Attributes;

namespace Org.Reddragonit.Dbpro.Connections.PoolComponents
{
    internal class ClassMapping
    {
        private Dictionary<Type, sTable> _classMaps;
        private Dictionary<Type,Dictionary<string, sTable>> _intermediateTables;
        private Dictionary<Type, sTable> _versionMaps;
        private Dictionary<Type, VersionField.VersionTypes> _versionTypes;
        private Dictionary<Type, sTable> _virtualTables;

        private ConnectionPool _pool;

        public ClassMapping(ConnectionPool pool, List<Type> tables,List<Type> virtualTables)
        {
            _pool = pool;
            _classMaps = new Dictionary<Type, sTable>();
            _intermediateTables = new Dictionary<Type, Dictionary<string, sTable>>();
            _versionMaps = new Dictionary<Type, sTable>();
            _virtualTables = new Dictionary<Type, sTable>();
            _versionTypes = new Dictionary<Type, VersionField.VersionTypes>();
            Dictionary<string, sTable> intermediates;
            foreach (Type tbl in tables)
            {
                if (!_classMaps.ContainsKey(tbl))
                {
                    _classMaps.Add(tbl, _ConstructTable(tbl, out intermediates));
                    if (intermediates.Count > 0)
                        _intermediateTables.Add(tbl, intermediates);
                }
            }
            foreach (Type vt in virtualTables)
            {
                List<sTableField> fields = new List<sTableField>();
                foreach (PropertyInfo pi in vt.GetProperties())
                {
                    if (pi.GetCustomAttributes(typeof(VirtualField), true).Length > 0)
                        fields.Add(new sTableField(pool.CorrectName(pi.Name),pi.Name,null,FieldType.STRING,-1,false));
                }
                _virtualTables.Add(vt, new sTable(pool.CorrectName("VW_" + _ConvertCamelCaseName(vt.Name)), fields.ToArray(), new sTableRelation[0], new string[0], null));
            }
        }

        private sTable _ConstructTable(Type tbl, out Dictionary<string, sTable> intermediates)
        {
            List<sTableField> fields = new List<sTableField>(); 
            List<sTableRelation> relations = new List<sTableRelation>();
            List<string> primaryKeyFields = new List<string>();
            intermediates = new Dictionary<string, sTable>();
            string tblName = _pool.CorrectName(_ExtractTableName(tbl));
            List<PropertyInfo> selfReferenceProperties = new List<PropertyInfo>();
            List<PropertyInfo> arrayProperties = new List<PropertyInfo>();
            List<string> foriegnProperties = new List<string>();
            List<PropertyInfo> versionProperties = new List<PropertyInfo>();
            VersionField.VersionTypes? _versionType = null;
            string autogenField = null;

            foreach (PropertyInfo pi in tbl.GetProperties(Utility._BINDING_FLAGS))
            {
                foreach (object obj in pi.GetCustomAttributes(true))
                {
                    if (obj is Field)
                    {
                        if (((Field)obj).Name == null)
                            ((Field)obj).InitFieldName(pi);
                    }
                    if (obj is IField)
                    {
                        if (pi.PropertyType.IsEnum)
                        {
                            if (_pool.Enums[(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)] == null)
                            {
                                Type etype = (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType);
                                sTable etbl = new sTable(_pool.CorrectName("ENUM_" + _ConvertCamelCaseName(etype.FullName.Substring(etype.FullName.LastIndexOf(".")+1).Replace("+", ""))),
                                    new sTableField[]{
                                        new sTableField(_pool.CorrectName("ID"),null,null,FieldType.INTEGER,4,false),
                                        new sTableField(_pool.CorrectName("VALUE"),null,null,FieldType.STRING,500,false)
                                    },
                                    new sTableRelation[0],
                                    new string[] { "ID" },
                                    "ID");
                                _classMaps.Add(etype, etbl);
                                _pool.Enums.Add(etype, etbl.Name);
                            }
                        }
                        if (!pi.PropertyType.IsArray)
                        {
                            Logger.LogLine("Adding Field (" + pi.Name + ")");
                            fields.Add(new sTableField(_pool.CorrectName(_ConvertCamelCaseName(pi.Name)), pi.Name, (pi.PropertyType.IsEnum ? "ID" : null),((IField)obj).Type,((IField)obj).Length,((IField)obj).Nullable));
                            if (obj is IPrimaryKeyField)
                            {
                                primaryKeyFields.Add(fields[fields.Count - 1].Name);
                                if (((IPrimaryKeyField)obj).AutoGen)
                                    autogenField = fields[fields.Count - 1].Name;
                            }
                            if (pi.PropertyType.IsEnum)
                                relations.Add(new sTableRelation(_pool.Enums[pi.PropertyType], pi.Name, null, ForeignField.UpdateDeleteAction.CASCADE, ForeignField.UpdateDeleteAction.CASCADE,((IField)obj).Nullable));
                        }
                        else
                            arrayProperties.Add(pi);
                    }
                    else if (obj is IForeignField)
                    {
                        foriegnProperties.Add(pi.Name);
                        if (pi.PropertyType.IsArray)
                            arrayProperties.Add(pi);
                        else
                        {
                            Logger.LogLine("Adding Foreign Field (" + pi.Name + ")");
                            Type ty = Utility.LocateType(pi.ToString().Substring(0, pi.ToString().IndexOf(" ")).Replace("[]", ""));
                            if (!ty.Equals(tbl))
                            {
                                if (!_classMaps.ContainsKey(ty))
                                {
                                    Dictionary<string, sTable> imediates = new Dictionary<string, sTable>();
                                    _classMaps.Add(ty, _ConstructTable(ty, out imediates));
                                    if (imediates.Count > 0)
                                        _intermediateTables.Add(ty, imediates);
                                }
                                sTable ext = _classMaps[ty];
                                string addOn = _ConvertCamelCaseName(pi.Name);
                                foreach (string prop in ext.PrimaryKeyFields)
                                {
                                    foreach (sTableField fld in ext[prop])
                                    {
                                        fields.Add(new sTableField(_pool.CorrectName(addOn + "_" + fld.Name),pi.Name,fld.Name,fld.Type,fld.Length,fld.Nullable));
                                        if (obj is IPrimaryKeyField)
                                            primaryKeyFields.Add(fields[fields.Count - 1].Name);
                                    }
                                }
                                relations.Add(new sTableRelation(ext.Name, pi.Name, null, ((IForeignField)obj).OnDelete, ((IForeignField)obj).OnUpdate,((IForeignField)obj).Nullable));
                            }
                            else
                                selfReferenceProperties.Add(pi);
                        }
                    }
                    if (obj is IVersionField)
                    {
                        if (!_versionType.HasValue)
                            _versionType = ((IVersionField)obj).VersionType;
                        else if (_versionType.Value != ((IVersionField)obj).VersionType)
                            throw new Exception("Cannot use two different version  types in the same table.");
                        versionProperties.Add(pi);
                    }
                }
            }

            foreach (PropertyInfo pi in selfReferenceProperties)
            {
                string addOn = _ConvertCamelCaseName(pi.Name);
                foreach (string str in primaryKeyFields)
                {
                    int cnt = fields.Count;
                    for(int x=0;x<cnt;x++)
                    {
                        sTableField fld = fields[x];
                        if (fld.Name==str)
                            fields.Add(new sTableField(_pool.CorrectName(addOn + "_" + str), pi.Name, str,fld.Type,fld.Length,fld.Nullable));
                    }
                }
                foreach (object obj in pi.GetCustomAttributes(true))
                {
                    if (obj is IForeignField)
                    {
                        relations.Add(new sTableRelation(tblName, pi.Name, null, ((IForeignField)obj).OnDelete, ((IForeignField)obj).OnUpdate,((IForeignField)obj).Nullable));
                        break;
                    }
                }
            }

            if (arrayProperties.Count > 0)
            {
                List<sTableField> afields = new List<sTableField>();
                foreach (sTableField sf in fields)
                {
                    if (primaryKeyFields.Contains(sf.Name))
                        afields.Add(new sTableField(_pool.CorrectName("PARENT_"+sf.Name),sf.ClassProperty,sf.Name,sf.Type,sf.Length,sf.Nullable));
                }
                afields.Add(new sTableField(_pool.CorrectName(_ConvertCamelCaseName("VALUE_INDEX")), null, null,FieldType.INTEGER,4,false));
                List<string> apKeys = new List<string>();
                apKeys.AddRange(primaryKeyFields);
                apKeys.Add(afields[afields.Count - 1].Name);
                foreach (PropertyInfo pi in arrayProperties)
                {
                    if (!foriegnProperties.Contains(pi.Name))
                    {
                        IField fld = null;
                        foreach (object obj in pi.GetCustomAttributes(false))
                        {
                            if (obj is IField)
                            {
                                fld = (IField)obj;
                                if (fld is Field && fld.Name == null)
                                    ((Field)fld).InitFieldName(pi);
                                break;
                            }
                        }
                        if (pi.PropertyType.IsEnum)
                        {
                            afields.Add(new sTableField(_pool.CorrectName(_ConvertCamelCaseName("ID")), null,_pool.CorrectName(_ConvertCamelCaseName("ID")), fld.Type, fld.Length,fld.Nullable));
                            intermediates.Add(pi.Name, new sTable(_pool.CorrectName(tblName + "_" + _ConvertCamelCaseName(pi.Name)), afields.ToArray(),
                                new sTableRelation[] { new sTableRelation(_pool.Enums[pi.PropertyType.GetElementType()], null, null, ForeignField.UpdateDeleteAction.CASCADE, ForeignField.UpdateDeleteAction.CASCADE, fld.Nullable) }, apKeys.ToArray(), afields[afields.Count - 2].Name));
                        }
                        else
                        {
                            afields.Add(new sTableField(_pool.CorrectName(_ConvertCamelCaseName("VALUE")), null, null, fld.Type, fld.Length,fld.Nullable));
                            intermediates.Add(pi.Name, new sTable(_pool.CorrectName(tblName + "_" + _ConvertCamelCaseName(pi.Name)), afields.ToArray(), null, apKeys.ToArray(), afields[afields.Count - 2].Name));
                        }
                        relations.Add(new sTableRelation(intermediates[pi.Name].Name, pi.Name, null, ForeignField.UpdateDeleteAction.CASCADE, ForeignField.UpdateDeleteAction.CASCADE,fld.Nullable));
                        afields.RemoveAt(afields.Count - 1);
                    }
                    else
                    {
                        IForeignField iff = null;
                        foreach (object obj in pi.GetCustomAttributes(false))
                        {
                            if (obj is IForeignField)
                            {
                                iff = (IForeignField)obj;
                                break;
                            }
                        }
                        if (!_classMaps.ContainsKey(pi.PropertyType.GetElementType()))
                        {
                            Dictionary<string, sTable> imediates = new Dictionary<string, sTable>();
                            _classMaps.Add(pi.PropertyType.GetElementType(), _ConstructTable(pi.PropertyType.GetElementType(), out imediates));
                            if (imediates.Count > 0)
                                _intermediateTables.Add(pi.PropertyType.GetElementType(), imediates);
                        }
                        sTable sExt = _classMaps[pi.PropertyType.GetElementType()];
                        List<sTableField> extFields = new List<sTableField>();
                        extFields.AddRange(afields);
                        foreach (string str in sExt.PrimaryKeyProperties)
                        {
                            foreach (sTableField f in sExt[str])
                                extFields.Add(new sTableField(_pool.CorrectName("CHILD_" +f.Name),null,f.Name,f.Type,f.Length,f.Nullable));
                        }
                        intermediates.Add(pi.Name, new sTable(_pool.CorrectName(tblName + "_" + _ConvertCamelCaseName(pi.Name)), extFields.ToArray(),
                            null, apKeys.ToArray(),afields[afields.Count-1].Name));
                        relations.Add(new sTableRelation(sExt.Name, pi.Name, intermediates[pi.Name].Name, ForeignField.UpdateDeleteAction.CASCADE, ForeignField.UpdateDeleteAction.CASCADE,iff.Nullable));
                    }
                }
            }

            if (_versionType.HasValue)
            {
                _versionTypes.Add(tbl, _versionType.Value);
                List<sTableField> vfields = new List<sTableField>();
                List<string> vpkeys = new List<string>();
                foreach (PropertyInfo pi in versionProperties)
                {
                    foreach (sTableField stf in fields)
                    {
                        if (!primaryKeyFields.Contains(stf.Name))
                        {
                            if (Utility.StringsEqual(stf.ClassProperty, pi.Name))
                                vfields.Add(stf);
                        }
                    }
                }
                vpkeys.AddRange(primaryKeyFields);
                foreach (sTableField f in fields)
                {
                    if (vpkeys.Contains(f.Name))
                        vfields.Add(f);
                }
                switch (_versionType.Value)
                {
                    case VersionField.VersionTypes.NUMBER:
                        vfields.Add(new sTableField(_pool.CorrectName(tbl.Name + "_VERSION_ID"), null, null, FieldType.INTEGER, 4,false));
                        break;
                    case VersionField.VersionTypes.DATESTAMP:
                        vfields.Add(new sTableField(_pool.CorrectName(tbl.Name + "_VERSION_ID"), null, null, FieldType.DATETIME, 12,false));
                        break;
                }
                vpkeys.Add(vfields[vfields.Count - 1].Name);
                _versionMaps.Add(tbl, new sTable(_pool.CorrectName(tblName + "_VERSION"), vfields.ToArray(), null, vpkeys.ToArray(), vfields[vfields.Count - 1].Name));
            }

            if (!tbl.BaseType.Equals(typeof(Org.Reddragonit.Dbpro.Structure.Table)))
            {
                if (tbl.BaseType.IsSubclassOf(typeof(Org.Reddragonit.Dbpro.Structure.Table)))
                {
                    if (!_classMaps.ContainsKey(tbl.BaseType))
                    {
                        Dictionary<string, sTable> imediates = new Dictionary<string, sTable>();
                        _classMaps.Add(tbl.BaseType, _ConstructTable(tbl.BaseType, out imediates));
                        if (imediates.Count > 0)
                            _intermediateTables.Add(tbl.BaseType, imediates);
                    }
                    sTable pTbl = _classMaps[tbl.BaseType];
                    List<string> pkeys = new List<string>(pTbl.PrimaryKeyFields);
                    foreach (sTableField fld in pTbl.Fields)
                    {
                        if (pkeys.Contains(fld.Name))
                        {
                            fields.Add(fld);
                            primaryKeyFields.Add(fld.Name);
                        }
                    }
                }
            }
            return new sTable(tblName, fields.ToArray(), relations.ToArray(), primaryKeyFields.ToArray(),autogenField);
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

        public sTable this[Type table]
        {
            get { return _classMaps[table]; }
        }

        public sTable this[Type table, string property]
        {
            get{return _intermediateTables[table][property];}
        }

        public bool IsMappableType(Type table)
        {
            return _classMaps.ContainsKey(table);
        }

        public bool HasVersionTable(Type table)
        {
            return _virtualTables.ContainsKey(table);
        }

        public sTable GetVersionTable(Type table,out VersionField.VersionTypes versionType){
            versionType = _versionTypes[table];
            return _virtualTables[table];
        }

        public bool PropertyHasIntermediateTable(Type table, string property)
        {
            if (_intermediateTables.ContainsKey(table))
            {
                return _intermediateTables[table].ContainsKey(property);
            }
            return false;
        }

        public sTable GetVirtualTable(Type type)
        {
            return _virtualTables[type];
        }

        public List<Type> Types
        {
            get
            {
                List<Type> ret = new List<Type>();
                foreach (Type t in _classMaps.Keys)
                    ret.Add(t);
                return ret;
            }
        }

        public List<Type> VirtualTypes
        {
            get
            {
                List<Type> ret = new List<Type>();
                foreach (Type t in _virtualTables.Keys)
                    ret.Add(t);
                return ret;
            }
        }
    }
}
