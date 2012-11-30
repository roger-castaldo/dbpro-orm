using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Virtual.Attributes;
using System.Reflection;
using Org.Reddragonit.Dbpro.Connections.ClassSQL;
using Org.Reddragonit.Dbpro.Structure.Attributes;

namespace Org.Reddragonit.Dbpro.Connections.PoolComponents
{
    internal class StructureUpdater
    {
        private List<Type> _createdTypes;
        public List<Type> CreatedTypes
        {
            get { return _createdTypes; }
        }

        private ConnectionPool _pool;
        private List<ExtractedTableMap> _tables;
        private List<Trigger> _triggers;
        private List<Generator> _generators;
        private List<IdentityField> _identities;
        private List<View> _views;
        private List<StoredProcedure> _procedures;
        private Dictionary<Type, List<EnumTranslationPair>> _translations;

        public StructureUpdater(ConnectionPool pool,Dictionary<Type,List<EnumTranslationPair>> translations)
        {
            _pool = pool;
            _createdTypes = new List<Type>();
            _translations = translations;
        }

        public void Init(Connection conn)
        {
            _tables = new List<ExtractedTableMap>();
            _triggers = new List<Trigger>();
            _generators = new List<Generator>();
            _identities = new List<IdentityField>();
            _views = new List<View>();
            _procedures = new List<StoredProcedure>();
            _createdTypes = new List<Type>();
            conn.ExecuteQuery(conn.queryBuilder.SelectTriggers());
            while (conn.Read())
            {
                _triggers.Add(new Trigger((string)conn[0], (string)conn[1], (string)conn[2]));
            }
            conn.Close();
            conn.ExecuteQuery(conn.queryBuilder.SelectProcedures());
            while (conn.Read())
            {
                _procedures.Add(new StoredProcedure(conn[0].ToString(), conn[1].ToString(), conn[2].ToString(), conn[3].ToString(), conn[4].ToString()));
            }
            conn.Close();
            conn.ExecuteQuery(conn.queryBuilder.SelectViews());
            while (conn.Read())
                _views.Add(new View((string)conn[0], (string)conn[1]));
            conn.Close();
            conn.ExecuteQuery(conn.queryBuilder.SelectTableNames());
            while (conn.Read())
            {
                _tables.Add(new ExtractedTableMap((string)conn[0]));
            }
            conn.Close();
            for (int x = 0; x < _tables.Count; x++)
            {
                ExtractedTableMap etm = _tables[x];
                etm.Indices = conn.queryBuilder.ExtractTableIndexes(etm.TableName, conn);
                conn.ExecuteQuery(conn.queryBuilder.SelectTableFields(etm.TableName));
                while (conn.Read())
                {
                    etm.Fields.Add(new ExtractedFieldMap(conn[0].ToString(), conn[1].ToString(),
                                                         long.Parse(conn[2].ToString()), bool.Parse(conn[3].ToString()), bool.Parse(conn[4].ToString()),
                                                         bool.Parse(conn[5].ToString())));
                }
                conn.Close();
                conn.ExecuteQuery(conn.queryBuilder.SelectForeignKeys(etm.TableName));
                while (conn.Read())
                {
                    etm.ForeignFields.Add(new ForeignRelationMap(conn[5].ToString(), conn[0].ToString(), conn[1].ToString(),
                                                                 conn[2].ToString(), conn[3].ToString(), conn[4].ToString()));
                }
                conn.Close();
                _tables.RemoveAt(x);
                _tables.Insert(x, etm);
            }
            if (conn.UsesGenerators)
            {
                conn.ExecuteQuery(conn.queryBuilder.SelectGenerators());
                while (conn.Read())
                {
                    _generators.Add(new Generator((string)conn[0]));
                }
                conn.Close();
                for (int x = 0; x < _generators.Count; x++)
                {
                    Generator gen = _generators[x];
                    conn.ExecuteQuery(conn.queryBuilder.GetGeneratorValue(gen.Name));
                    conn.Read();
                    gen.Value = long.Parse(conn[0].ToString());
                    conn.Close();
                    _generators.RemoveAt(x);
                    _generators.Insert(x, gen);
                }
            }
            if (conn.UsesIdentities)
            {
                conn.ExecuteQuery(conn.queryBuilder.SelectIdentities());
                while (conn.Read())
                {
                    _identities.Add(new IdentityField((string)conn[0], (string)conn[1], (string)conn[2], (string)conn[3]));
                }
                conn.Close();
            }
        }

        public void InitType(Type type,Connection conn)
        {
            Utility.WaitOne(_createdTypes);
            if (!_createdTypes.Contains(type))
                _CreateTablesForType(type,conn);
            Utility.Release(_createdTypes);
        }

        private void _CreateTablesForType(Type type, Connection conn)
        {
            List<Type> types = new List<Type>();
            types.Add(type);
            if (_pool.Mapping.IsVirtualTable(type))
            {
                foreach (PropertyInfo pi in type.GetProperties(Utility._BINDING_FLAGS))
                {
                    if (pi.GetCustomAttributes(typeof(VirtualField), true).Length > 0)
                    {
                        VirtualField vf = (VirtualField)pi.GetCustomAttributes(typeof(VirtualField), true)[0];
                        if (!types.Contains(vf.ReferencingTable))
                        {
                            types.Add(vf.ReferencingTable);
                            types = _RecurLoadTypesForTable(_pool.Mapping[vf.ReferencingTable], types);
                        }
                    }
                }
            }
            else
                types = _RecurLoadTypesForTable(_pool.Mapping[type], types);
            for (int x = 1; x < types.Count; x++)
            {
                if (_createdTypes.Contains(types[x]))
                {
                    types.RemoveAt(x);
                    x--;
                }
            }
            List<ExtractedTableMap> tables;
            List<Trigger> triggers;
            List<Generator> generators;
            List<IdentityField> identities;
            List<View> views;
            List<StoredProcedure> procedures;
            ExtractExpectedStructure(ref types, out tables, out triggers, out generators, out identities, out views, out procedures, conn);
            List<string> createdTables = _UpdateStructure(tables, triggers, generators, identities, views, procedures,conn);
            foreach (Type t in types)
            {
                if (t.IsEnum)
                {
                    if (createdTables.Contains(_pool.Enums[t]))
                        _pool.Enums.InsertEnumIntoTable(t, conn);
                    else
                        _pool.Enums.LoadEnumsFromTable(t, conn);
                }
            }
            _createdTypes.AddRange(types);
            if (!_pool.DebugMode && (_translations.Count > 0))
            {
                foreach (Type t in types)
                {
                    if (_translations.ContainsKey(t))
                    {
                        foreach (EnumTranslationPair etp in _translations[t])
                        {
                            conn.ExecuteNonQuery(String.Format(
                                "UPDATE {0} SET {1} = '{3}' WHERE {1} = '{2}'",
                                new object[]{
                                _pool.Enums[t],
                                _pool.Translator.GetEnumValueFieldName(t,conn),
                                etp.OriginalName,
                                etp.NewName}
                            ));
                            conn.Close();
                        }
                    }
                    Dictionary<string, int> enumValuesMap = new Dictionary<string, int>();
                    Dictionary<int, string> enumReverseValuesMap = new Dictionary<int, string>();
                    List<string> enumNames = new List<string>(Enum.GetNames(t));
                    List<int> deletes = new List<int>();
                    conn.ExecuteQuery(String.Format("SELECT ID,{1} FROM {0}",
                        _pool.Enums[t],
                        _pool.Translator.GetEnumValueFieldName(t, conn)));
                    while (conn.Read())
                    {
                        if (enumNames.Contains(conn[1].ToString()))
                        {
                            enumValuesMap.Add(conn[1].ToString(), (int)conn[0]);
                            enumReverseValuesMap.Add((int)conn[0], conn[1].ToString());
                            enumNames.Remove(conn[1].ToString());
                        }
                        else
                            deletes.Add((int)conn[0]);
                    }
                    conn.Close();
                    if (deletes.Count > 0)
                    {
                        foreach (int i in deletes)
                        {
                            conn.ExecuteNonQuery(String.Format("DELETE FROM {0} WHERE ID = {1}",
                                _pool.Enums[t],
                                i));
                            conn.Close();
                        }
                    }
                    if (enumNames.Count > 0)
                    {
                        foreach (string str in enumNames)
                        {
                            conn.ExecuteNonQuery(String.Format("INSERT INTO {0}({1}) VALUES('{2}')",
                                _pool.Enums[t],
                                _pool.Translator.GetEnumValueFieldName(t, conn),
                                str));
                            conn.Close();
                            conn.ExecuteQuery(String.Format("SELECT ID FROM {0} WHERE {1}='{2}'",
                                _pool.Enums[t],
                                _pool.Translator.GetEnumValueFieldName(t, conn),
                                str));
                            conn.Read();
                            enumValuesMap.Add(str, (int)conn[0]);
                            enumReverseValuesMap.Add((int)conn[0], str);
                            conn.Close();
                        }
                    }
                    conn.Commit();
                    _pool.Enums.AssignMapValues(t, enumValuesMap, enumReverseValuesMap);
                }
            }
        }

        private List<Type> _RecurLoadTypesForTable(sTable tbl, List<Type> types)
        {
            Type t = _pool.Mapping[tbl.Name];
            if (!t.BaseType.Equals(typeof(Org.Reddragonit.Dbpro.Structure.Table)) && _pool.Mapping.IsMappableType(t.BaseType))
                types = _RecurLoadTypesForTable(_pool.Mapping[t.BaseType], types);
            if (!types.Contains(t))
                types.Add(t);
            foreach (string prop in tbl.ForeignTableProperties)
            {
                sTableRelation rel = tbl.GetRelationForProperty(prop).Value;
                if (rel.ExternalTable != tbl.Name)
                    types = _RecurLoadTypesForTable(_pool.Mapping[_pool.Mapping[rel.ExternalTable]], types);
            }
            foreach (string prop in tbl.Properties)
            {
                if (_pool.Mapping.PropertyHasIntermediateTable(t, prop))
                {
                    sTable itbl = _pool.Mapping[t, prop];
                    if (itbl.Relations[1].ExternalTable != tbl.Name)
                        types = _RecurLoadTypesForTable(_pool.Mapping[_pool.Mapping[itbl.Relations[1].ExternalTable]], types);
                }
            }
            return types;
        }

        private void ExtractExpectedStructure(ref List<Type> types, out List<ExtractedTableMap> tables, out List<Trigger> triggers, out List<Generator> generators, out List<IdentityField> identities, out List<View> views, out List<StoredProcedure> procedures, Connection conn)
        {
            List<View> tviews;
            List<StoredProcedure> tprocs;
            tables = new List<ExtractedTableMap>();
            triggers = new List<Trigger>();
            generators = new List<Generator>();
            identities = new List<IdentityField>();
            views = new List<View>();
            procedures = new List<StoredProcedure>();
            List<Trigger> tmpTriggers = new List<Trigger>();
            List<Generator> tmpGenerators = new List<Generator>();
            List<IdentityField> tmpIdentities = new List<IdentityField>();
            List<StoredProcedure> tmpProcedures = new List<StoredProcedure>();

            Dictionary<string, string> AutoDeleteParentTables = new Dictionary<string, string>();

            for (int x = 0; x < types.Count;x++ )
            {
                Type type = types[x];
                List<Type> ttypes;
                ConnectionPoolManager.GetAdditionalsForTable(_pool, types[x], out tviews, out tprocs, out ttypes);
                views.AddRange(tviews);
                procedures.AddRange(tprocs);
                foreach (Type t in ttypes)
                {
                    if (!types.Contains(t) && !_createdTypes.Contains(t))
                        types.Add(t);
                }
                if (_pool.Mapping.IsVirtualTable(type))
                    views.Add(_CreateViewForVirtualTable(type, conn));
                else
                {
                    sTable tm = _pool.Mapping[type];
                    ExtractedTableMap etm = new ExtractedTableMap(tm.Name);
                    if (type.IsEnum)
                    {
                        foreach (sTableField f in tm.Fields)
                            etm.Fields.Add(new ExtractedFieldMap(f.Name, conn.TranslateFieldType(f.Type, f.Length), f.Length, tm.AutoGenField == f.Name, false, tm.AutoGenField == f.Name));
                    }
                    else
                    {
                        Table tbl = (Table)type.GetCustomAttributes(typeof(Table), false)[0];
                        foreach (TableIndex ti in type.GetCustomAttributes(typeof(TableIndex), false))
                        {
                            List<string> tfields = new List<string>();
                            foreach (string str in ti.Fields)
                            {
                                sTableField[] flds = tm[str];
                                if (flds.Length == 0)
                                    tfields.Add(str);
                                else
                                {
                                    foreach (sTableField f in flds)
                                        tfields.Add(f.Name);
                                }
                            }
                            etm.Indices.Add(new Index(_pool.Translator.GetIndexName(type, ti.Name, conn), tfields.ToArray(), ti.Unique, ti.Ascending));
                        }
                        List<string> pProps = new List<string>(tm.PrimaryKeyProperties);
                        foreach (string prop in tm.Properties)
                        {
                            PropertyInfo pi = type.GetProperty(prop, Utility._BINDING_FLAGS_WITH_INHERITANCE);
                            if (!pi.PropertyType.IsArray)
                            {
                                sTableRelation? rel = tm.GetRelationForProperty(prop);
                                foreach (sTableField f in tm[prop])
                                {
                                    etm.Fields.Add(new ExtractedFieldMap(f.Name, conn.TranslateFieldType(f.Type, f.Length), f.Length, pProps.Contains(prop), (rel.HasValue ? rel.Value.Nullable : f.Nullable), (tm.AutoGenField != null ? f.Name == tm.AutoGenField : false)));
                                    if (rel.HasValue)
                                        etm.ForeignFields.Add(new ForeignRelationMap(type.Name + "_" + prop, f.Name, rel.Value.ExternalTable, f.ExternalField, rel.Value.OnDelete.ToString(), rel.Value.OnUpdate.ToString()));
                                }
                            }
                            else
                            {
                                sTable iMap = _pool.Mapping[type, prop];
                                ExtractedTableMap ietm = new ExtractedTableMap(iMap.Name);
                                List<string> ipKeys = new List<string>(iMap.PrimaryKeyFields);
                                string extTable = (_pool.Mapping.IsMappableType(pi.PropertyType.GetElementType()) ? _pool.Mapping[pi.PropertyType.GetElementType()].Name : null);
                                foreach (sTableField f in iMap.Fields)
                                {
                                    ietm.Fields.Add(new ExtractedFieldMap(f.Name, conn.TranslateFieldType(f.Type, f.Length), f.Length, ipKeys.Contains(f.Name), false, (iMap.AutoGenField != null ? iMap.AutoGenField == f.Name : false)));
                                    if (Utility.StringsEqual("PARENT", f.ClassProperty))
                                        ietm.ForeignFields.Add(new ForeignRelationMap(type.Name + "_" + prop + (_pool.Mapping.IsMappableType(pi.PropertyType.GetElementType()) ? "_intermediate" : ""), f.Name, etm.TableName, f.ExternalField, ForeignField.UpdateDeleteAction.CASCADE.ToString(), ForeignField.UpdateDeleteAction.CASCADE.ToString()));
                                    if (Utility.StringsEqual("CHILD", f.ClassProperty))
                                        ietm.ForeignFields.Add(new ForeignRelationMap(type.Name + "_" + prop, f.Name, extTable, f.ExternalField, ForeignField.UpdateDeleteAction.CASCADE.ToString(), ForeignField.UpdateDeleteAction.CASCADE.ToString()));
                                }
                                tables.Add(ietm);
                            }
                        }
                        if (_pool.Mapping.IsMappableType(type.BaseType))
                        {
                            sTable pMap = _pool.Mapping[type.BaseType];
                            foreach (string str in pMap.PrimaryKeyFields)
                                etm.ForeignFields.Add(new ForeignRelationMap(type.Name + "_parent", str, pMap.Name, str, ForeignField.UpdateDeleteAction.CASCADE.ToString(), ForeignField.UpdateDeleteAction.CASCADE.ToString()));
                            if (tbl.AutoDeleteParent)
                                AutoDeleteParentTables.Add(tm.Name, pMap.Name);
                        }
                        if (_pool.Mapping.HasVersionTable(type))
                        {
                            Org.Reddragonit.Dbpro.Structure.Attributes.VersionField.VersionTypes vt;
                            sTable vtm = _pool.Mapping.GetVersionTable(type, out vt);
                            ExtractedTableMap vetm = new ExtractedTableMap(vtm.Name);
                            List<string> vpkeys = new List<string>(tm.PrimaryKeyFields);
                            foreach (sTableField f in vtm.Fields)
                            {
                                vetm.Fields.Add(new ExtractedFieldMap(f.Name, conn.TranslateFieldType(f.Type, f.Length), f.Length, vpkeys.Contains(f.Name), !vpkeys.Contains(f.Name), vtm.AutoGenField == f.Name));
                                if (vpkeys.Contains(f.Name))
                                    vetm.ForeignFields.Add(new ForeignRelationMap(type.Name + "_version", f.Name, tm.Name, f.Name, ForeignField.UpdateDeleteAction.CASCADE.ToString(), ForeignField.UpdateDeleteAction.CASCADE.ToString()));
                            }
                            triggers.AddRange(conn.GetVersionTableTriggers(vetm, vt));
                            tables.Add(vetm);
                        }
                    }
                    tables.Add(etm);
                    bool keyDifferent = false;
                    foreach (ExtractedTableMap e in _tables)
                    {
                        if (etm.TableName == e.TableName)
                        {
                            foreach (ExtractedFieldMap efm in etm.PrimaryKeys)
                            {
                                bool foundField = false;
                                foreach (ExtractedFieldMap ee in e.PrimaryKeys)
                                {
                                    if (ee.FieldName == efm.FieldName)
                                    {
                                        foundField = true;
                                        if ((ee.PrimaryKey != efm.PrimaryKey) || (ee.PrimaryKey && efm.PrimaryKey && ((ee.Type != efm.Type) || (ee.Size != efm.Size))))
                                            keyDifferent = true;
                                        break;
                                    }
                                }
                                if (!foundField)
                                    keyDifferent = true;
                                if (keyDifferent)
                                    break;
                            }
                            if (keyDifferent)
                                break;
                        }
                    }
                    if (keyDifferent)
                    {
                        foreach (ExtractedTableMap e in _tables)
                        {
                            if (e.RelatedTables.Contains(etm.TableName))
                            {
                                Type t = _pool.Mapping[e.TableName];
                                if (t != null)
                                {
                                    if (!types.Contains(t) && !_createdTypes.Contains(t))
                                        types.Add(t);
                                }
                            }
                        }
                    }
                }
            }
            foreach (ExtractedTableMap etm in tables)
            {
                Logger.LogLine(etm.TableName + ":");
                foreach (ExtractedFieldMap efm in etm.Fields)
                    Logger.LogLine("\t" + efm.FieldName + " - " + efm.PrimaryKey.ToString());
                if (!_pool.Mapping.IsVersionTable(etm.TableName))
                {
                    foreach (ExtractedFieldMap efm in etm.PrimaryKeys)
                    {
                        if (efm.AutoGen)
                        {
                            conn.GetAddAutogen(etm, out tmpIdentities, out tmpGenerators, out tmpTriggers, out tmpProcedures);
                            if (tmpGenerators != null)
                                generators.AddRange(tmpGenerators);
                            if (tmpTriggers != null)
                                triggers.AddRange(tmpTriggers);
                            if (tmpIdentities != null)
                                identities.AddRange(tmpIdentities);
                            if (tmpProcedures != null)
                                procedures.AddRange(tmpProcedures);
                            break;
                        }
                    }
                    if (AutoDeleteParentTables.ContainsKey(etm.TableName))
                    {
                        ExtractedTableMap ptm = new ExtractedTableMap();
                        foreach (ExtractedTableMap m in tables)
                        {
                            if (AutoDeleteParentTables[etm.TableName] == m.TableName)
                            {
                                ptm = m;
                                break;
                            }
                        }
                        if (ptm.TableName == null)
                        {
                            sTable ptbl =  _pool.Mapping[_pool.Mapping[AutoDeleteParentTables[etm.TableName]]];
                            ptm = new ExtractedTableMap(ptbl.Name);
                            List<string> pkeys = new List<string>(ptbl.PrimaryKeyFields);
                            foreach (sTableField f in ptbl.Fields)
                                ptm.Fields.Add(new ExtractedFieldMap(f.Name, conn.TranslateFieldType(f.Type, f.Length), f.Length, pkeys.Contains(f.Name), f.Nullable));
                        }
                        triggers.AddRange(conn.GetDeleteParentTrigger(etm, ptm));
                    }
                }
            }
        }

        private View _CreateViewForVirtualTable(Type virtualTable, Connection conn)
        {
            Type t = VirtualTableAttribute.GetMainTableTypeForVirtualTable(virtualTable);
            sTable tbl = _pool.Mapping.GetVirtualTable(virtualTable);
            string query = "SELECT ";
            foreach (string prop in tbl.Properties)
            {
                PropertyInfo pi = virtualTable.GetProperty(prop, Utility._BINDING_FLAGS);
                VirtualField vf = (VirtualField)pi.GetCustomAttributes(typeof(VirtualField), false)[0];
                query += "main." + vf.FieldName + " AS "+tbl[prop][0].Name+", ";
            }
            query = query.Substring(0, query.Length - 2);
            query += " FROM " + t.Name + " main";
            ClassQuery cq = new ClassQuery(t.Namespace, query);
            return new View(tbl.Name,cq.QueryString);
        }

        private List<string> _UpdateStructure(List<ExtractedTableMap> tables, List<Trigger> triggers, List<Generator> generators, List<IdentityField> identities, List<View> views, List<StoredProcedure> procedures,Connection conn)
        {
            List<string> ret = new List<string>();

            List<Trigger> dropTriggers = new List<Trigger>();
            List<Trigger> createTriggers = new List<Trigger>();
            List<StoredProcedure> createProcedures = new List<StoredProcedure>();
            List<StoredProcedure> updateProcedures = new List<StoredProcedure>();
            List<Generator> createGenerators = new List<Generator>();
            List<string> constraintDrops = new List<string>();
            List<string> constraintCreations = new List<string>();
            List<PrimaryKey> primaryKeyDrops = new List<PrimaryKey>();
            List<PrimaryKey> primaryKeyCreations = new List<PrimaryKey>();
            List<ForeignKey> foreignKeyDrops = new List<ForeignKey>();
            List<ForeignKey> foreignKeyCreations = new List<ForeignKey>();
            List<IdentityField> createIdentities = new List<IdentityField>();
            List<IdentityField> setIdentities = new List<IdentityField>();
            Dictionary<string, List<Index>> dropIndexes = new Dictionary<string, List<Index>>();
            Dictionary<string, List<Index>> createIndexes = new Dictionary<string, List<Index>>();
            List<View> createViews = new List<View>();
            List<View> dropViews = new List<View>();

            _ExtractPrimaryKeyCreationsDrops(tables, out primaryKeyDrops, out primaryKeyCreations);

            _CompareTriggers(triggers, out dropTriggers, out createTriggers);

            _CompareStoredProcedures(procedures, out createProcedures, out updateProcedures);

            _CompareGenerators(generators, out createGenerators);

            _ExtractConstraintDropsCreates(tables, conn, out constraintDrops, out constraintCreations);

            _ExtractForeignKeyCreatesDrops(tables, out foreignKeyDrops, out foreignKeyCreations);

            _CompareIdentities(identities, out createIdentities, out setIdentities);

            _ExtractIndexCreationsDrops(tables, out dropIndexes, out createIndexes);

            _CompareViews(views, out createViews, out dropViews);

            List<string> tableCreations = new List<string>();
            List<string> tableAlterations = new List<string>();

            foreach (ExtractedTableMap map in tables)
            {
                bool foundTable = false;
                for(int x=0;x<_tables.Count;x++){
                    if (map.TableName == _tables[x].TableName)
                    {
                        foundTable = true;
                        foreach (ExtractedFieldMap efm in map.Fields)
                        {
                            bool foundField = false;
                            foreach (ExtractedFieldMap ee in _tables[x].Fields)
                            {
                                if (efm.FieldName == ee.FieldName)
                                {
                                    foundField = true;
                                    if (((efm.Type != ee.Type) || (efm.Size != ee.Size)) &&
                                        !((efm.Type == "BLOB") && (ee.Type == "BLOB")))
                                    {
                                        if (efm.PrimaryKey && ee.PrimaryKey)
                                        {
                                            primaryKeyDrops.Add(new PrimaryKey(map));
                                            primaryKeyCreations.Add(new PrimaryKey(map));
                                        }
                                        else 
                                        {
                                            foreach (ForeignRelationMap frms in _tables[x].ForeignFields)
                                            {
                                                if (frms.InternalField == efm.FieldName)
                                                {
                                                    foreignKeyDrops.Add(new ForeignKey(_tables[x], frms.ExternalTable, frms.ID));
                                                    foreignKeyCreations.Add(new ForeignKey(_tables[x], frms.ExternalTable, frms.ID));
                                                    break;
                                                }
                                            }
                                        }
                                        tableAlterations.Add(conn.queryBuilder.AlterFieldType(map.TableName, efm, ee));
                                    }
                                    if (efm.Nullable != ee.Nullable)
                                    {
                                        if (efm.PrimaryKey && ee.PrimaryKey)
                                        {
                                            primaryKeyDrops.Add(new PrimaryKey(map));
                                            primaryKeyCreations.Add(new PrimaryKey(map));
                                        }
                                        else
                                        {
                                            foreach (ForeignRelationMap frms in _tables[x].ForeignFields)
                                            {
                                                if (frms.InternalField == efm.FieldName)
                                                {
                                                    foreignKeyDrops.Add(new ForeignKey(_tables[x], frms.ExternalTable, frms.ID));
                                                    foreignKeyCreations.Add(new ForeignKey(_tables[x], frms.ExternalTable, frms.ID));
                                                    break;
                                                }
                                            }
                                        }
                                        if (!efm.Nullable)
                                            constraintCreations.Add(conn.queryBuilder.CreateNullConstraint(map.TableName, efm));
                                        else
                                            constraintDrops.Add(conn.queryBuilder.DropNullConstraint(map.TableName, efm));
                                    }
                                    break;
                                }
                            }
                            if (!foundField)
                                tableAlterations.Add(conn.queryBuilder.CreateColumn(map.TableName, efm));
                        }
                        foreach (ExtractedFieldMap efm in _tables[x].Fields)
                        {
                            bool foundField = false;
                            foreach (ExtractedFieldMap ee in map.Fields)
                            {
                                if (efm.FieldName == ee.FieldName)
                                {
                                    foundField = true;
                                    break;
                                }
                            }
                            if (!foundField)
                            {
                                if (efm.PrimaryKey)
                                {
                                    primaryKeyDrops.Add(new PrimaryKey(map));
                                    primaryKeyCreations.Add(new PrimaryKey(map));
                                }
                                else
                                {
                                    foreach (ForeignRelationMap frms in _tables[x].ForeignFields)
                                    {
                                        if (frms.InternalField == efm.FieldName)
                                        {
                                            foreignKeyDrops.Add(new ForeignKey(_tables[x], frms.ExternalTable, frms.ID));
                                            break;
                                        }
                                    }
                                }
                                tableAlterations.Add(conn.queryBuilder.DropColumn(map.TableName, efm.FieldName));
                            }
                        }
                    }
                }
                if (!foundTable)
                {
                    ret.Add(map.TableName);
                    tableCreations.Add(conn.queryBuilder.CreateTable(map));
                }
            }

            foreach (PrimaryKey pk in primaryKeyDrops)
            {
                foreach (Trigger t in _triggers)
                {
                    if (t.Conditions.Contains("FOR " + pk.Name + " "))
                        dropTriggers.Add(t);
                }
                foreach (Trigger t in triggers)
                {
                    if (t.Conditions.Contains("FOR " + pk.Name + " "))
                        createTriggers.Add(t);
                }
            }

            _CleanUpForeignKeys(ref foreignKeyDrops);
            _CleanUpForeignKeys(ref foreignKeyCreations);

            List<string> alterations = new List<string>();
            //add drops to alterations
            alterations.AddRange(constraintDrops);
            alterations.Add(" COMMIT;");

            foreach (View vw in dropViews)
                alterations.Add(conn.queryBuilder.DropView(vw.Name));
            alterations.Add(" COMMIT;");

            foreach (Trigger trig in dropTriggers)
                alterations.Add(conn.queryBuilder.DropTrigger(trig.Name));
            alterations.Add(" COMMIT;");

            foreach (string str in dropIndexes.Keys)
            {
                foreach (Index ind in dropIndexes[str])
                    alterations.Add(conn.queryBuilder.DropTableIndex(str, ind.Name));
            }
            alterations.Add(" COMMIT;");

            foreach (ForeignKey fk in foreignKeyDrops)
                alterations.Add(conn.queryBuilder.DropForeignKey(fk.InternalTable, fk.ExternalTable, fk.ExternalFields[0], fk.InternalFields[0]));
            alterations.Add(" COMMIT;");

            foreach (PrimaryKey pk in primaryKeyDrops)
            {
                foreach (string field in pk.Fields)
                    alterations.Add(conn.queryBuilder.DropPrimaryKey(pk));
            }
            alterations.Add(" COMMIT;");

            alterations.AddRange(tableAlterations);
            alterations.Add(" COMMIT;");

            alterations.AddRange(tableCreations);
            alterations.Add(" COMMIT;");

            foreach (View vw in createViews)
                alterations.Add(conn.queryBuilder.CreateView(vw));
            alterations.Add(" COMMIT;");

            //add creations to alterations
            alterations.AddRange(constraintCreations);
            alterations.Add(" COMMIT;");

            foreach (PrimaryKey pk in primaryKeyCreations)
                alterations.Add(conn.queryBuilder.CreatePrimaryKey(pk));
            alterations.Add(" COMMIT;");

            foreach (ForeignKey fk in foreignKeyCreations)
                alterations.Add(conn.queryBuilder.CreateForeignKey(fk));
            alterations.Add(" COMMIT;");

            foreach (string str in createIndexes.Keys)
            {
                foreach (Index ind in createIndexes[str])
                    alterations.Add(conn.queryBuilder.CreateTableIndex(str, ind.Fields, ind.Name, ind.Unique, ind.Ascending));
            }
            alterations.Add(" COMMIT;");

            foreach (StoredProcedure proc in updateProcedures)
                alterations.Add(conn.queryBuilder.UpdateProcedure(proc));
            alterations.Add(" COMMIT;");

            foreach (StoredProcedure proc in createProcedures)
                alterations.Add(conn.queryBuilder.CreateProcedure(proc));
            alterations.Add(" COMMIT;");

            foreach (Generator gen in createGenerators)
            {
                alterations.Add(conn.queryBuilder.CreateGenerator(gen.Name));
                alterations.Add(conn.queryBuilder.SetGeneratorValue(gen.Name, gen.Value));
            }
            alterations.Add(" COMMIT;");

            foreach (Trigger trig in createTriggers)
            {
                alterations.Add(conn.queryBuilder.CreateTrigger(trig));
            }
            alterations.Add(" COMMIT;");

            foreach (IdentityField idf in createIdentities)
                alterations.Add(conn.queryBuilder.CreateIdentityField(idf));
            alterations.Add(" COMMIT;");

            foreach (IdentityField idf in setIdentities)
                alterations.Add(conn.queryBuilder.SetIdentityFieldValue(idf));
            alterations.Add(" COMMIT;");

            for (int x = 0; x < alterations.Count; x++)
            {
                if (alterations[x].Contains(";ALTER"))
                {
                    string tmp = alterations[x];
                    alterations.RemoveAt(x);
                    alterations.Insert(x, tmp.Substring(0, tmp.IndexOf(";ALTER") + 1));
                    alterations.Insert(x + 1, tmp.Substring(tmp.IndexOf(";ALTER") + 1));
                }
                else if (alterations[x].Contains(";\nALTER"))
                {
                    string tmp = alterations[x];
                    alterations.RemoveAt(x);
                    alterations.Insert(x, tmp.Substring(0, tmp.IndexOf(";\nALTER") + 1));
                    alterations.Insert(x + 1, tmp.Substring(tmp.IndexOf(";\nALTER") + 1));
                }
                if (alterations[x].StartsWith("ALTER") && !alterations[x].TrimEnd(new char[] { '\n', ' ', '\t' }).EndsWith(";"))
                {
                    alterations[x] = alterations[x] + ";";
                }
            }

            Utility.RemoveDuplicateStrings(ref alterations, new string[] { " COMMIT;" });

            if (!Utility.OnlyContains(alterations,new string[]{"COMMIT;"}))
            {
                try
                {
                    if (_pool.DebugMode)
                    {
                        foreach (string str in alterations)
                        {
                            if (!str.EndsWith(";"))
                                Logger.LogLine(str + ";");
                            else
                                Logger.LogLine(str);
                        }
                    }
                    else
                    {
                        foreach (string str in alterations)
                        {
                            if (str.Length > 0)
                            {
                                if (str == " COMMIT;")
                                    conn.Commit();
                                else if (str.EndsWith(" COMMIT;"))
                                {
                                    conn.ExecuteNonQuery(str.Substring(0, str.Length - 8));
                                    conn.Commit();
                                }
                                else
                                    conn.ExecuteNonQuery(str);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Logger.LogLine(e.Message);
                    Logger.LogLine(e.StackTrace);
                    throw e;
                }
            }
            conn.Commit();

            _pool.Translator.ApplyAllDescriptions(tables, triggers, generators, identities, views, procedures,conn);
            conn.Commit();

            return ret;
        }

        private void _CleanUpForeignKeys(ref List<ForeignKey> foreignKeys)
        {
            for (int x = 0; x < foreignKeys.Count; x++)
            {
                ForeignKey key = foreignKeys[x];
                if (key.ExternalFields.Count > 1)
                {
                    if (key.ExternalFields.IndexOf(key.ExternalFields[0], 1) >= 1)
                    {
                        List<string> externalFields = new List<string>();
                        List<string> internalFields = new List<string>();
                        for (int y = 0; y < key.ExternalFields.Count; y++)
                        {
                            string str = key.ExternalFields[y];
                            if (externalFields.Contains(str))
                            {
                                foreignKeys.Add(new ForeignKey(key.InternalTable, internalFields,
                                                               key.ExternalTable, externalFields,
                                                               key.OnUpdate, key.OnDelete));
                                internalFields = new List<string>();
                                externalFields = new List<string>();
                            }
                            externalFields.Add(str);
                            internalFields.Add(key.InternalFields[y]);
                        }
                        foreignKeys.Add(new ForeignKey(key.InternalTable, internalFields,
                                                       key.ExternalTable, externalFields,
                                                       key.OnUpdate, key.OnDelete));
                        foreignKeys.RemoveAt(x);
                        x--;
                    }
                }
            }
        }

        private void _CompareViews(List<View> views, out List<View> createViews, out List<View> dropViews)
        {
            createViews = new List<View>();
            dropViews = new List<View>();

            for (int x = 0; x < views.Count; x++)
            {
                bool add = true;
                for (int y = 0; y < _views.Count; y++)
                {
                    if (views[x].Name == _views[y].Name)
                    {
                        add = false;
                        if (_views[y].Query != views[x].Query)
                        {
                            dropViews.Add(views[x]);
                            createViews.Add(views[x]);
                        }
                        _views.RemoveAt(y);
                        break;
                    }
                }
                if (add)
                    createViews.Add(views[x]);
            }
        }

        private void _ExtractIndexCreationsDrops(List<ExtractedTableMap> tables, out Dictionary<string, List<Index>> dropIndexes, out Dictionary<string, List<Index>> createIndexes)
        {
            dropIndexes = new Dictionary<string, List<Index>>();
            createIndexes = new Dictionary<string, List<Index>>();
            List<Index> indAdd = new List<Index>();
            List<Index> indDel = new List<Index>();

            foreach (ExtractedTableMap etm in tables)
            {
                bool found = false;
                for(int x=0;x<_tables.Count;x++)
                {
                    ExtractedTableMap e = _tables[x];
                    if (e.TableName == etm.TableName)
                    {
                        found = true;
                        indAdd = new List<Index>();
                        indDel = new List<Index>();
                        foreach (Index ind in etm.Indices)
                        {
                            bool foundindex = false;
                            foreach (Index i in e.Indices)
                            {
                                if (i.Name == ind.Name)
                                {
                                    foundindex = true;
                                    if (!i.Equals(ind))
                                    {
                                        indDel.Add(i);
                                        indAdd.Add(ind);
                                    }
                                    break;
                                }
                            }
                            if (!foundindex)
                                indAdd.Add(ind);
                        }
                        foreach (Index ind in e.Indices)
                        {
                            bool foundindex = false;
                            foreach (Index i in etm.Indices)
                            {
                                if (i.Name == ind.Name)
                                {
                                    foundindex = true;
                                    break;
                                }
                            }
                            if (!foundindex)
                                indDel.Add(ind);
                        }
                        if (indAdd.Count > 0)
                            createIndexes.Add(etm.TableName, indAdd);
                        if (indDel.Count > 0)
                            dropIndexes.Add(etm.TableName, indDel);
                    }
                }
                if (!found)
                    createIndexes.Add(etm.TableName, etm.Indices);
            }
        }

        private void _CompareIdentities(List<IdentityField> identities, out List<IdentityField> createIdentities, out List<IdentityField> setIdentities)
        {
            createIdentities = new List<IdentityField>();
            setIdentities = new List<IdentityField>();

            foreach (IdentityField idf in identities)
            {
                bool create = true;
                for (int x = 0; x < _identities.Count; x++)
                {
                    if ((idf.TableName == _identities[x].TableName)
                        && (idf.FieldName == _identities[x].FieldName)
                        && (idf.FieldType == _identities[x].FieldType))
                    {
                        create = false;
                        if (idf.CurValue != _identities[x].CurValue)
                            setIdentities.Add(idf);
                        _identities.RemoveAt(x);
                        break;
                    }
                }
                if (create)
                    createIdentities.Add(idf);
            }
        }

        private void _ExtractForeignKeyCreatesDrops(List<ExtractedTableMap> tables, out List<ForeignKey> foreignKeyDrops, out List<ForeignKey> foreignKeyCreations)
        {
            foreignKeyDrops = new List<ForeignKey>();
            foreignKeyCreations = new List<ForeignKey>();

            foreach (ExtractedTableMap etm in tables)
            {
                bool found = false;
                foreach (ExtractedTableMap e in _tables)
                {
                    if (etm.TableName == e.TableName)
                    {
                        found = true;
                        foreach (string tableName in etm.RelatedTables)
                        {
                            foreach (List<ForeignRelationMap> exfrms in etm.RelatedFieldsForTable(tableName))
                            {
                                bool foundRelation = false;
                                foreach (List<ForeignRelationMap> curfrms in e.RelatedFieldsForTable(tableName))
                                {
                                    if (exfrms.Count == curfrms.Count)
                                    {
                                        bool foundField = true;
                                        foreach (ForeignRelationMap exfrm in exfrms)
                                        {
                                            foundField = false;
                                            foreach (ForeignRelationMap curfrm in curfrms)
                                            {
                                                if ((exfrm.InternalField == curfrm.InternalField) && (exfrm.ExternalField == curfrm.ExternalField))
                                                {
                                                    foundField = ((etm.GetField(exfrm.InternalField).Type == e.GetField(curfrm.InternalField).Type) && (etm.GetField(exfrm.InternalField).Size == e.GetField(curfrm.InternalField).Size) && (etm.GetField(exfrm.InternalField).Nullable == e.GetField(curfrm.InternalField).Nullable) && (etm.GetField(exfrm.InternalField).PrimaryKey == e.GetField(curfrm.InternalField).PrimaryKey));
                                                    break;
                                                }
                                            }
                                            if (!foundField)
                                                break;
                                        }
                                        if (foundField)
                                        {
                                            foundRelation = true;
                                            break;
                                        }else
                                            foreignKeyDrops.Add(new ForeignKey(etm.TableName,new List<string>(new string[]{curfrms[0].InternalField}),tableName,new List<string>(new string[]{curfrms[0].ExternalField}),curfrms[0].OnUpdate,curfrms[0].OnDelete));
                                    }else
                                        foreignKeyDrops.Add(new ForeignKey(etm,tableName,curfrms[0].ID));
                                }
                                if (!foundRelation)
                                    foreignKeyCreations.Add(new ForeignKey(etm, tableName, exfrms[0].ID));
                            }
                        }
                        break;
                    }
                }
                if (!found)
                {
                    foreach (string tableName in etm.RelatedTables)
                    {
                        foreach (List<ForeignRelationMap> frms in etm.RelatedFieldsForTable(tableName))
                        {
                            foreignKeyCreations.Add(new ForeignKey(etm, tableName, frms[0].ID));
                        }
                    }
                }
            }
        }

        private void _ExtractPrimaryKeyCreationsDrops(List<ExtractedTableMap> tables, out List<PrimaryKey> primaryKeyDrops, out List<PrimaryKey> primaryKeyCreations)
        {
            primaryKeyDrops = new List<PrimaryKey>();
            primaryKeyCreations = new List<PrimaryKey>();

            foreach (ExtractedTableMap etm in tables)
            {
                bool found = false;
                foreach (ExtractedTableMap e in _tables)
                {
                    if (etm.TableName == e.TableName)
                    {
                        found = true;
                        bool keyDifferent = false;
                        foreach (ExtractedFieldMap efm in etm.PrimaryKeys)
                        {
                            bool foundField = false;
                            foreach (ExtractedFieldMap ee in e.PrimaryKeys)
                            {
                                if (ee.FieldName == efm.FieldName)
                                {
                                    foundField = true;
                                    if ((ee.PrimaryKey != efm.PrimaryKey) || (ee.PrimaryKey && efm.PrimaryKey && ((ee.Type != efm.Type) || (ee.Size != efm.Size))))
                                        keyDifferent = true;
                                    break;
                                }
                            }
                            if (!foundField)
                                keyDifferent = true;
                            if (keyDifferent)
                                break;
                        }
                        if (keyDifferent)
                        {
                            primaryKeyDrops.Add(new PrimaryKey(e));
                            primaryKeyCreations.Add(new PrimaryKey(etm));
                        }
                    }
                }
                if (!found)
                {
                    if (etm.PrimaryKeys.Count > 0)
                        primaryKeyCreations.Add(new PrimaryKey(etm));
                }
            }
        }

        private void _ExtractConstraintDropsCreates(List<ExtractedTableMap> tables, Connection conn, out List<string> constraintDrops, out List<string> constraintCreations)
        {
            constraintDrops = new List<string>();
            constraintCreations = new List<string>();

            foreach (ExtractedTableMap etm in tables)
            {
                bool found = false;
                foreach (ExtractedTableMap e in _tables)
                {
                    if (etm.TableName == e.TableName)
                    {
                        found = true;
                        foreach (ExtractedFieldMap efm in etm.Fields)
                        {
                            bool foundField = false;
                            foreach (ExtractedFieldMap ee in e.Fields)
                            {
                                if (efm.FieldName == ee.FieldName)
                                {
                                    foundField = true;
                                    if (efm.Nullable && !ee.Nullable)
                                        constraintDrops.Add(conn.queryBuilder.DropNullConstraint(etm.TableName, efm));
                                    else if (!efm.Nullable && ee.Nullable)
                                        constraintCreations.Add(conn.queryBuilder.CreateNullConstraint(etm.TableName, efm));
                                    break;
                                }
                            }
                            if (!foundField && !efm.Nullable)
                                constraintCreations.Add(conn.queryBuilder.CreateNullConstraint(etm.TableName, efm));
                        }
                        break;
                    }
                }
                if (!found)
                {
                    foreach (ExtractedFieldMap efm in etm.Fields)
                    {
                        if (!efm.Nullable)
                            constraintCreations.Add(conn.queryBuilder.CreateNullConstraint(etm.TableName, efm));
                    }
                }
            }
        }

        private void _CompareGenerators(List<Generator> generators, out List<Generator> createGenerators)
        {
            createGenerators = new List<Generator>();

            foreach (Generator gen in generators)
            {
                bool create = true;
                for (int x = 0; x < _generators.Count; x++)
                {
                    if (Utility.StringsEqualIgnoreWhitespace(gen.Name, _generators[x].Name))
                    {
                        create = false;
                        _generators.RemoveAt(x);
                        break;
                    }
                }
                if (create)
                    createGenerators.Add(gen);
            }
        }

        private void _CompareStoredProcedures(List<StoredProcedure> procedures, out List<StoredProcedure> createProcedures, out List<StoredProcedure> updateProcedures)
        {
            createProcedures = new List<StoredProcedure>();
            updateProcedures = new List<StoredProcedure>();

            foreach (StoredProcedure proc in procedures)
            {
                bool create = true;
                for (int x = 0; x < _procedures.Count; x++)
                {
                    if (Utility.StringsEqualIgnoreWhitespace(proc.ProcedureName, _procedures[x].ProcedureName))
                    {
                        create = false;
                        if (!Utility.StringsEqualIgnoreWhitespace(proc.DeclareLines, _procedures[x].DeclareLines)
                            || !Utility.StringsEqualIgnoreWhitespace(proc.ReturnLine, _procedures[x].ReturnLine)
                            || !Utility.StringsEqualIgnoreWhitespace(proc.ParameterLines, _procedures[x].ParameterLines)
                            || !Utility.StringsEqualIgnoreWhitespace(proc.Code, _procedures[x].Code))
                            updateProcedures.Add(proc);
                        if (!_pool.IsCoreStoredProcedure(_procedures[x]))
                            _procedures.RemoveAt(x);
                    }
                }
                if (create)
                    createProcedures.Add(proc);
            }
        }

        private void _CompareTriggers(List<Trigger> triggers, out List<Trigger> dropTriggers, out List<Trigger> createTriggers)
        {
            createTriggers = new List<Trigger>();
            dropTriggers = new List<Trigger>();
            foreach (Trigger t in triggers)
            {
                bool create = true;
                for (int y = 0; y < _triggers.Count; y++)
                {
                    if (Utility.StringsEqualIgnoreWhitespace(t.Name, _triggers[y].Name))
                    {
                        if (!Utility.StringsEqualIgnoreCaseWhitespace(t.Conditions, _triggers[y].Conditions)
                            || !Utility.StringsEqualIgnoreCaseWhitespace(t.Code, _triggers[y].Code))
                            dropTriggers.Add(_triggers[y]);
                        else
                            create = false;
                        _triggers.RemoveAt(y);
                        break;
                    }
                }
                if (create)
                    createTriggers.Add(t);
            }
        }
    }
}
