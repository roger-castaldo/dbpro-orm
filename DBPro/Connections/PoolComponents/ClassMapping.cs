using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Connections.PoolComponents
{
    internal class ClassMapping
    {
        private Dictionary<Type, sTable> _classMaps;
        private Dictionary<Type,Dictionary<string, sTable>> _intermediateTables;
        private Dictionary<Type, sTable> _versionMaps;
        private Dictionary<Type, VersionField.VersionTypes> _versionTypes;

        private ConnectionPool _pool;

        public ClassMapping(Connection conn, List<Type> tables)
        {
            _pool = conn.Pool;
            _classMaps = new Dictionary<Type, sTable>();
            _intermediateTables = new Dictionary<Type, Dictionary<string, sTable>>();
            _versionMaps = new Dictionary<Type, sTable>();
            _versionTypes = new Dictionary<Type, VersionField.VersionTypes>();
            Dictionary<string, sTable> intermediates;
            foreach (Type tbl in tables)
            {
                if (!_classMaps.ContainsKey(tbl))
                {
                    _classMaps.Add(tbl, _ConstructTable(tbl,conn, out intermediates));
                    if (intermediates.Count > 0)
                        _intermediateTables.Add(tbl, intermediates);
                }
            }
        }

        private sTable _ConstructTable(Type tbl,Connection conn, out Dictionary<string, sTable> intermediates)
        {
            List<sTableField> fields = new List<sTableField>(); 
            List<sTableRelation> relations = new List<sTableRelation>();
            List<string> primaryKeyFields = new List<string>();
            intermediates = new Dictionary<string, sTable>();
            string tblName = _pool.Translator.GetTableName(tbl,conn);
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
                        bool isEnum = Utility.IsEnum(pi.PropertyType);
                        if (!isEnum && pi.PropertyType.IsArray)
                            isEnum = Utility.IsEnum(pi.PropertyType.GetElementType());
                        if (isEnum)
                        {
                            if (_pool.Enums[(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)] == null)
                            {
                                Type etype = (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType);
                                string eName = _pool.Translator.GetTableName(etype, conn);
                                sTable etbl = new sTable(eName,
                                    new sTableField[]{
                                        new sTableField(_pool.Translator.GetEnumIDFieldName(etype,conn),null,null,FieldType.INTEGER,4,false),
                                        new sTableField(_pool.Translator.GetEnumValueFieldName(etype,conn),null,null,FieldType.STRING,500,false)
                                    },
                                    null,
                                    new sTableRelation[0],
                                    new string[] { _pool.Translator.GetEnumIDFieldName(etype, conn) },
                                    _pool.Translator.GetEnumIDFieldName(etype, conn));
                                _classMaps.Add(etype, etbl);
                                _pool.Enums.Add(etype, etbl.Name);
                            }
                        }
                        if (!pi.PropertyType.IsArray
                            || pi.PropertyType.Equals(typeof(byte[])))
                        {
                            Logger.LogLine("Adding Field (" + pi.Name + ")");
                            fields.Add(new sTableField(_pool.Translator.GetFieldName(tbl,pi,conn), pi.Name, (Utility.IsEnum(pi.PropertyType) ? "ID" : null),((IField)obj).Type,((IField)obj).Length,((IField)obj).Nullable));
                            if (obj is IPrimaryKeyField)
                            {
                                primaryKeyFields.Add(fields[fields.Count - 1].Name);
                                if (((IPrimaryKeyField)obj).AutoGen)
                                    autogenField = fields[fields.Count - 1].Name;
                            }
                            if (Utility.IsEnum(pi.PropertyType))
                                relations.Add(new sTableRelation(_pool.Enums[pi.PropertyType], pi.Name, ForeignField.UpdateDeleteAction.CASCADE, ForeignField.UpdateDeleteAction.CASCADE,((IField)obj).Nullable));
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
                                    _classMaps.Add(ty, _ConstructTable(ty,conn, out imediates));
                                    if (imediates.Count > 0)
                                        _intermediateTables.Add(ty, imediates);
                                }
                                sTable ext = _classMaps[ty];
                                foreach (string prop in ext.PrimaryKeyProperties)
                                {
                                    foreach (sTableField fld in ext[prop])
                                    {
                                        fields.Add(new sTableField(_pool.Translator.GetFieldName(tbl,pi,fld.Name,conn),pi.Name,fld.Name,fld.Type,fld.Length,fld.Nullable));
                                        if (obj is IPrimaryKeyField)
                                            primaryKeyFields.Add(fields[fields.Count - 1].Name);
                                    }
                                }
                                relations.Add(new sTableRelation(ext.Name, pi.Name, ((IForeignField)obj).OnDelete, ((IForeignField)obj).OnUpdate,((IForeignField)obj).Nullable));
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

            if (!tbl.BaseType.Equals(typeof(Org.Reddragonit.Dbpro.Structure.Table)))
            {
                if (tbl.BaseType.IsSubclassOf(typeof(Org.Reddragonit.Dbpro.Structure.Table)))
                {
                    if (!_classMaps.ContainsKey(tbl.BaseType))
                    {
                        Dictionary<string, sTable> imediates = new Dictionary<string, sTable>();
                        _classMaps.Add(tbl.BaseType, _ConstructTable(tbl.BaseType, conn, out imediates));
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

            foreach (PropertyInfo pi in selfReferenceProperties)
            {
                foreach (string str in primaryKeyFields)
                {
                    int cnt = fields.Count;
                    for(int x=0;x<cnt;x++)
                    {
                        sTableField fld = fields[x];
                        if (fld.Name==str)
                            fields.Add(new sTableField(_pool.Translator.GetFieldName(tbl,pi,fld.Name,conn), pi.Name, str,fld.Type,fld.Length,fld.Nullable));
                    }
                }
                foreach (object obj in pi.GetCustomAttributes(true))
                {
                    if (obj is IForeignField)
                    {
                        relations.Add(new sTableRelation(tblName, pi.Name, ((IForeignField)obj).OnDelete, ((IForeignField)obj).OnUpdate,((IForeignField)obj).Nullable));
                        break;
                    }
                }
            }

            if (arrayProperties.Count > 0)
            {
                foreach (PropertyInfo pi in arrayProperties)
                {
                    string itblName = _pool.Translator.GetIntermediateTableName(tbl, pi, conn);
                    List<sTableField> afields = new List<sTableField>();
                    foreach (sTableField sf in fields)
                    {
                        if (primaryKeyFields.Contains(sf.Name))
                            afields.Add(new sTableField(_pool.Translator.GetIntermediateFieldName(tbl,pi,sf.Name,true,conn),"PARENT", sf.Name, sf.Type, sf.Length, sf.Nullable));
                    }
                    afields.Add(new sTableField(_pool.Translator.GetIntermediateIndexFieldName(tbl,pi), null, null, FieldType.INTEGER, 4, false));
                    List<string> apKeys = new List<string>();
                    apKeys.AddRange(primaryKeyFields);
                    apKeys.Add(afields[afields.Count - 1].Name);
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
                        if (Utility.IsEnum(pi.PropertyType.GetElementType()))
                        {
                            afields.Add(new sTableField(_pool.Translator.GetIntermediateValueFieldName(tbl,pi), "CHILD", _classMaps[pi.PropertyType.GetElementType()].Fields[0].Name, fld.Type, fld.Length, fld.Nullable));
                            intermediates.Add(pi.Name, new sTable(itblName, afields.ToArray(),null,
                                new sTableRelation[] { 
                                    new sTableRelation(tbl.Name,"PARENT",ForeignField.UpdateDeleteAction.CASCADE,ForeignField.UpdateDeleteAction.CASCADE,false),
                                    new sTableRelation(_pool.Enums[pi.PropertyType.GetElementType()], "CHILD", ForeignField.UpdateDeleteAction.CASCADE, ForeignField.UpdateDeleteAction.CASCADE, fld.Nullable) 
                                }, apKeys.ToArray(), afields[afields.Count - 2].Name));
                        }
                        else
                        {
                            afields.Add(new sTableField(_pool.Translator.GetIntermediateValueFieldName(tbl, pi), null, null, fld.Type, fld.Length, fld.Nullable));
                            intermediates.Add(pi.Name, new sTable(itblName, afields.ToArray(),null, null, apKeys.ToArray(), afields[afields.Count - 2].Name));
                        }
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
                        List<sTableField> extFields = new List<sTableField>();
                        string extTableName = "";
                        if (pi.PropertyType.GetElementType() == tbl)
                        {
                            extTableName = tblName;
                            extFields.AddRange(afields);
                            foreach(string str in primaryKeyFields){
                                foreach(sTableField f in fields){
                                    if (f.Name == str)
                                    {
                                        extFields.Add(new sTableField(_pool.Translator.GetIntermediateFieldName(tbl, pi, f.Name, false, conn), "CHILD", f.Name, f.Type, f.Length, f.Nullable));
                                        break;
                                    }
                                }
                            }
                        }
                        else
                        {
                            if (!_classMaps.ContainsKey(pi.PropertyType.GetElementType()))
                            {
                                Dictionary<string, sTable> imediates = new Dictionary<string, sTable>();
                                _classMaps.Add(pi.PropertyType.GetElementType(), _ConstructTable(pi.PropertyType.GetElementType(), conn, out imediates));
                                if (imediates.Count > 0)
                                    _intermediateTables.Add(pi.PropertyType.GetElementType(), imediates);
                            }
                            sTable sExt = _classMaps[pi.PropertyType.GetElementType()];
                            extTableName = sExt.Name;
                            extFields.AddRange(afields);
                            foreach (string str in sExt.PrimaryKeyProperties)
                            {
                                foreach (sTableField f in sExt[str])
                                    extFields.Add(new sTableField(_pool.Translator.GetIntermediateFieldName(tbl, pi, f.Name, false, conn), "CHILD", f.Name, f.Type, f.Length, f.Nullable));
                            }
                        }
                        intermediates.Add(pi.Name, new sTable(itblName, extFields.ToArray(), null,
                                new sTableRelation[]{
                                new sTableRelation(tblName,"PARENT",iff.OnUpdate,iff.OnDelete,iff.Nullable),
                                new sTableRelation(extTableName,"CHILD",iff.OnUpdate,iff.OnDelete,iff.Nullable)
                            },
                                apKeys.ToArray(), afields[afields.Count - 1].Name));
                    }
                }
            }

            if (_versionType.HasValue)
            {
                string vtblName = _pool.Translator.GetVersionTableName(tbl, conn);
                _versionTypes.Add(tbl, _versionType.Value);
                List<sTableField> vfields = new List<sTableField>();
                List<string> vpkeys = new List<string>();
                switch (_versionType.Value)
                {
                    case VersionField.VersionTypes.NUMBER:
                        vfields.Add(new sTableField(_pool.Translator.GetVersionFieldIDName(tbl, conn), null, null, FieldType.INTEGER, 4, false));
                        break;
                    case VersionField.VersionTypes.DATESTAMP:
                        vfields.Add(new sTableField(_pool.Translator.GetVersionFieldIDName(tbl, conn), null, null, FieldType.DATETIME, 12, false));
                        break;
                }
                vpkeys.Add(vfields[vfields.Count - 1].Name);
                foreach (PropertyInfo pi in versionProperties)
                {
                    foreach (sTableField stf in fields)
                    {
                        if (!primaryKeyFields.Contains(stf.Name))
                        {
                            if (Utility.StringsEqual(stf.ClassProperty, pi.Name))
                                vfields.Add(new sTableField(stf.Name,null,null,stf.Type,stf.Length,stf.Nullable));
                        }
                    }
                }
                vpkeys.AddRange(primaryKeyFields);
                foreach (sTableField f in fields)
                {
                    if (vpkeys.Contains(f.Name))
                        vfields.Add(new sTableField(f.Name,"PARENT",f.Name,f.Type,f.Length,f.Nullable));
                }
                _versionMaps.Add(tbl, new sTable(vtblName, vfields.ToArray(),null,new sTableRelation[]{new sTableRelation(tblName,"PARENT",ForeignField.UpdateDeleteAction.CASCADE,ForeignField.UpdateDeleteAction.CASCADE,false)}, vpkeys.ToArray(), vfields[vfields.Count - 1].Name));
            }

            return new sTable(tblName, fields.ToArray(),arrayProperties, relations.ToArray(), primaryKeyFields.ToArray(), autogenField);
        }

        public sTable this[Type table]
        {
            get { return _classMaps[table]; }
        }

        public Type this[string tableName]
        {
            get
            {
                foreach (Type t in _classMaps.Keys)
                {
                    if (_classMaps[t].Name == tableName)
                        return t;
                }
                return null;
            }
        }

        public PropertyInfo this[string tableName, string tableField]
        {
            get
            {
                Type t = this[tableName];
                if (t != null)
                {
                    foreach (sTableField fld in this[t].Fields)
                    {
                        if (fld.Name == tableField && fld.ClassProperty!=null)
                            return t.GetProperty(fld.ClassProperty);
                    }
                }
                return null;
            }
        }

        public sTable this[Type table, string property]
        {
            get{return _intermediateTables[table][property];}
        }

        public Type GetTypeForVersionTable(string tableName)
        {
            foreach (Type t in _versionMaps.Keys)
            {
                if (_versionMaps[t].Name == tableName)
                    return t;
            }
            return null;
        }

        public bool IsMappableType(Type table)
        {
            return _classMaps.ContainsKey(table);
        }

        public bool HasVersionTable(Type table)
        {
            return _versionMaps.ContainsKey(table);
        }

        public sTable GetVersionTable(Type table,out VersionField.VersionTypes versionType){
            versionType = _versionTypes[table];
            return _versionMaps[table];
        }

        public bool PropertyHasIntermediateTable(Type table, string property)
        {
            if (_intermediateTables.ContainsKey(table))
            {
                return _intermediateTables[table].ContainsKey(property);
            }
            return false;
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

        internal Type GetTypeForIntermediateTable(string tableName, out PropertyInfo pi)
        {
            pi = null;
            foreach (Type t in _intermediateTables.Keys)
            {
                foreach (string str in _intermediateTables[t].Keys)
                {
                    if (_intermediateTables[t][str].Name == tableName)
                    {
                        pi = t.GetProperty(str, Utility._BINDING_FLAGS);
                        return t;
                    }
                }
            }
            return null;
        }

        internal bool IsVersionTable(string tableName)
        {
            foreach (Type t in _versionMaps.Keys)
            {
                if (_versionMaps[t].Name == tableName)
                    return true;
            }
            return false;
        }
    }
}
