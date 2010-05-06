using ICSharpCode.SharpZipLib.Zip;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Structure.Mapping;

namespace Org.Reddragonit.Dbpro.Backup
{
    public class BackupManager
    {

        public static bool BackupDataToStream(ConnectionPool pool, ref Stream outputStream)
        {
            Logger.LogLine("Locking down "+pool.ConnectionName+" database for backing up...");
            Connection c = pool.LockDownForBackupRestore();
            c.StartTransaction();
            Logger.LogLine("Database locked down for backing up");
            List<Type> types = ClassMapper.TableTypesForConnection(pool.ConnectionName);
            List<Type> enums = new List<Type>();
            List<Type> basicTypes = new List<Type>();
            List<Type> complexTypes = new List<Type>();
            TableMap map;
            Logger.LogLine("Loading all required types...");
            foreach (Type t in types)
            {
                map = ClassMapper.GetTableMap(t);
                foreach (InternalFieldMap ifm in map.Fields)
                {
                    if (ifm.FieldType == Org.Reddragonit.Dbpro.Structure.Attributes.FieldType.ENUM)
                    {
                        if (!enums.Contains(ifm.ObjectType))
                            enums.Add(ifm.ObjectType);
                    }
                }
                if (map.ForeignTables.Count > 0)
                {
                    if (!complexTypes.Contains(t))
                    {
                        recurAddRelatedTypes(ref basicTypes, ref complexTypes, map, t);
                    }
                }
                else
                {
                    if (!basicTypes.Contains(t))
                        basicTypes.Add(t);
                }
            }

            //begin data dumping and outputting
            ZipOutputStream zs = new ZipOutputStream(outputStream);
            byte[] buff = new byte[1024];
            XmlDocument doc;
            XmlElement elem;
            MemoryStream ms;
            int len;
            object obj;
            int cnt = 0;

            //output all enumerations
            Logger.LogLine("Extracting enumerations for backup...");
            foreach (Type t in enums)
            {
                doc = new XmlDocument();
                elem = doc.CreateElement("enums");
                doc.AppendChild(elem);
                foreach (string str in Enum.GetNames(t))
                {
                    elem = doc.CreateElement("enum");
                    elem.Attributes.Append(CreateAttributeWithValue(doc, "Name", str));
                    elem.Attributes.Append(CreateAttributeWithValue(doc,"Value",pool.GetEnumID(t,str).ToString()));
                    doc.DocumentElement.AppendChild(elem);
                }
                zs.PutNextEntry(new ZipEntry(cnt.ToString("0000000")+"_"+t.FullName + ".xml"));
                cnt++;
                ms = new MemoryStream(XMLCompressor.CompressXMLDocument(doc));
                for (long x = 0; x < ms.Length; x += 1024)
                {
                    len = ms.Read(buff, 0, 1024);
                    zs.Write(buff, 0, len);
                }
            }

            //output all basic types
            Logger.LogLine("Backing up the basic types for database...");
            foreach (Type t in basicTypes)
            {
                Logger.LogLine("Backing up basic type: " + t.FullName);
                doc = new XmlDocument();
                elem = doc.CreateElement("entries");
                doc.AppendChild(elem);
                map = ClassMapper.GetTableMap(t);
                Logger.LogLine("Extracting basic type: " + t.FullName + " from the database and writing it to the xml document.");
                foreach (Table table in c.SelectAll(t))
                {
                    elem = doc.CreateElement("entry");
                    foreach (TableMap.FieldNamePair fnp in map.FieldNamePairs)
                    {
                        obj = table.GetField(fnp.ClassFieldName);
                        if (obj != null)
                            elem.AppendChild(CreateElementWithValue(doc, fnp.ClassFieldName, obj));
                    }
                    doc.DocumentElement.AppendChild(elem);
                }
                Logger.LogLine("Compressing basic type: " + t.FullName + " data and appending it into the zip file.");
                zs.PutNextEntry(new ZipEntry(cnt.ToString("0000000") + "_" + t.FullName + ".xml"));
                cnt++;
                ms = new MemoryStream(XMLCompressor.CompressXMLDocument(doc));
                for (long x = 0; x < ms.Length; x += 1024)
                {
                    len = ms.Read(buff, 0, 1024);
                    zs.Write(buff, 0, len);
                }
                c.ResetConnection(false);
            }

            //output all complex types
            Logger.LogLine("Backing up complex types for database...");
            foreach (Type t in complexTypes)
            {
                Logger.LogLine("Backing up complex type: " + t.FullName);
                doc = new XmlDocument();
                elem = doc.CreateElement("entries");
                doc.AppendChild(elem);
                map = ClassMapper.GetTableMap(t);
                Logger.LogLine("Extracting complex type: " + t.FullName + " from the database and writing it to the xml document.");
                foreach (Table table in c.SelectAll(t))
                {
                    elem = doc.CreateElement("entry");
                    foreach (TableMap.FieldNamePair fnp in map.FieldNamePairs)
                    {
                        obj = table.GetField(fnp.ClassFieldName);
                        if (obj != null)
                        {
                            if (map[fnp] is ExternalFieldMap)
                            {
                                if (map[fnp].IsArray)
                                {
                                    elem.AppendChild(doc.CreateElement(fnp.ClassFieldName));
                                    foreach (object o in (Array)obj)
                                    {
                                        elem.ChildNodes[elem.ChildNodes.Count - 1].AppendChild(CreateRealtedXMLElement((Table)o, doc, "child"));
                                    }
                                }else
                                    elem.AppendChild(CreateRealtedXMLElement((Table)obj, doc,fnp.ClassFieldName));
                            }else
                                elem.AppendChild(CreateElementWithValue(doc, fnp.ClassFieldName, obj));
                        }
                    }
                    doc.DocumentElement.AppendChild(elem);
                }
                Logger.LogLine("Compressing complex type: " + t.FullName + " data and appending it into the zip file.");
                zs.PutNextEntry(new ZipEntry(cnt.ToString("0000000") + "_" + t.FullName + ".xml"));
                cnt++;
                ms = new MemoryStream(XMLCompressor.CompressXMLDocument(doc));
                for (long x = 0; x < ms.Length; x += 1024)
                {
                    len = ms.Read(buff, 0, 1024);
                    zs.Write(buff, 0, len);
                }
                c.ResetConnection(false);
            }

            zs.Flush();
            zs.Close();
            c.Reset();
            Logger.LogLine("Backup of database complete, re-enabling pool.");
            pool.UnlockPoolPostBackupRestore();
            c.CloseConnection();
            return true;
        }

        private static XmlElement CreateRealtedXMLElement(Table val,XmlDocument doc,string name)
        {
            XmlElement ret = doc.CreateElement(name);
            TableMap map = ClassMapper.GetTableMap(val.GetType());
            object obj;
            foreach (TableMap.FieldNamePair fnp in map.FieldNamePairs)
            {
                if (map[fnp].PrimaryKey)
                {
                    obj = val.GetField(fnp.ClassFieldName);
                    if (obj != null)
                    {
                        if (map[fnp] is ExternalFieldMap)
                            ret.AppendChild(CreateRealtedXMLElement((Table)obj, doc, fnp.ClassFieldName));
                        else
                            ret.AppendChild(CreateElementWithValue(doc, fnp.ClassFieldName, obj));
                    }
                }
            }
            return ret;
        }

        private static XmlElement CreateElementWithValue(XmlDocument doc,string name,object value)
        {
            XmlElement ret = doc.CreateElement(name);
            XmlSerializer serl = XmlSerializer.FromTypes(new Type[] { value.GetType() })[0];
            MemoryStream ms = new MemoryStream();
            serl.Serialize(ms, value);
            ret.InnerXml = System.Text.Encoding.ASCII.GetString(ms.ToArray());
            return ret;
        }

        private static XmlAttribute CreateAttributeWithValue(XmlDocument doc, string name, string value)
        {
            XmlAttribute ret = doc.CreateAttribute(name);
            ret.Value = value;
            return ret;
        }

        private static void recurAddRelatedTypes(ref List<Type> basicTypes, ref List<Type> complexTypes,TableMap map,Type type)
        {
            if (complexTypes.Contains(type))
                return;
            foreach (Type t in map.ForeignTables)
            {
                if (!t.Equals(type))
                {
                    TableMap tm = ClassMapper.GetTableMap(t);
                    if (tm.ForeignTables.Count > 0)
                        recurAddRelatedTypes(ref basicTypes, ref complexTypes, tm, t);
                    else
                    {
                        if (!basicTypes.Contains(t))
                            basicTypes.Add(t);
                    }
                }
            }
            if (!complexTypes.Contains(type))
            {
                complexTypes.Add(type);
            }
        }

        public static bool RestoreDataFromStream(ConnectionPool pool, ref Stream inputStream)
        {
            Connection c = pool.LockDownForBackupRestore();
            //disable autogen fields as well as all relationship constraints
            c.DisableAutogens();
            foreach (Type ty in ClassMapper.TableTypesForConnection(pool.ConnectionName))
                c.DeleteAll(ty);
            pool.DisableRelationships(c);
            c.Commit();

            ZipInputStream zis = new ZipInputStream(inputStream);
            ZipEntry ze = null;
            MemoryStream ms;
            XmlDocument doc;
            string type;
            BinaryWriter bw;
            byte[] buff = new byte[1024];
            Type t;
            Dictionary<string, int> enumMap;
            Dictionary<int, string> reverseMap;
            int len;
            Table tbl;

            while ((ze=zis.GetNextEntry())!=null)
            {
                type = ze.Name.Substring(ze.Name.IndexOf("_") + 1);
                type=type.Substring(0,type.Length-4);
                ms = new MemoryStream();
                bw = new BinaryWriter(ms);
                while (bw.BaseStream.Length<ze.Size)
                {
                    len = zis.Read(buff, 0, 1024);
                    bw.Write(buff,0,len);
                }
                ms.Position = 0;
                doc = XMLCompressor.DecompressXMLDocument(ms);
                Logger.LogLine("Extracted xml data for type " + type);
                t = Utility.LocateType(type);
                if (t.IsEnum)
                {
                    Logger.LogLine("Processing enum data into database...");
                    c.ExecuteNonQuery("DELETE FROM " + pool._enumTableMaps[t]);
                    enumMap = new Dictionary<string, int>();
                    reverseMap = new Dictionary<int, string>();
                    foreach (XmlNode node in doc.DocumentElement.ChildNodes)
                    {
                        c.ExecuteNonQuery("INSERT INTO " + pool._enumTableMaps[t] + " VALUES(" + c.CreateParameterName("id") + "," + c.CreateParameterName("value") + ");",
                            new System.Data.IDbDataParameter[]{
                                c.CreateParameter(c.CreateParameterName("id"),int.Parse(node.Attributes["Value"].Value)),
                                c.CreateParameter(c.CreateParameterName("value"),node.Attributes["Name"].Value)
                            });
                        enumMap.Add(node.Attributes["Name"].Value, int.Parse(node.Attributes["Value"].Value));
                        reverseMap.Add(int.Parse(node.Attributes["Value"].Value), node.Attributes["Name"].Value);
                    }
                    pool._enumReverseValuesMap.Remove(t);
                    pool._enumValuesMap.Remove(t);
                    pool._enumReverseValuesMap.Add(t, reverseMap);
                    pool._enumValuesMap.Add(t, enumMap);
                    Logger.LogLine("Enum data has been imported.");
                }
                else
                {
                    Logger.LogLine("Processing object data into database...");
                    c.DeleteAll(t);
                    TableMap map = ClassMapper.GetTableMap(t);
                    foreach (XmlNode node in doc.DocumentElement.ChildNodes)
                    {
                        tbl = (Table)t.GetConstructor(Type.EmptyTypes).Invoke(new object[] { });
                        foreach (XmlNode n in node.ChildNodes)
                        {
                            Logger.LogLine("Inner Field Value ("+n.Name+"): " + n.InnerXml);
                            if (map[n.Name] is ExternalFieldMap){
                                Logger.LogLine("Processing external field...");
                                tbl.SetField(n.Name, ExtractTableValue(n,map[n.Name].ObjectType,map[n.Name].IsArray));
                            }else{
                                try
                                {
                                    if (map[n.Name].ObjectType == null)
                                        Logger.LogLine("Object type for field " + n.Name + " is null");
                                    if (map[n.Name] == null)
                                        Logger.LogLine("The field: " + n.Name + " was not locating in the table map.");
                                    Logger.LogLine("Processing internal field of the type "+map[n.Name].ObjectType.FullName+" ...");
                                    if (XmlSerializer.FromTypes(new Type[] { map[n.Name].ObjectType }).Length == 0)
                                        Logger.LogLine("The field: " + n.Name + " has no xml seriliazer available.");
                                    object obj = XmlSerializer.FromTypes(new Type[] { map[n.Name].ObjectType })[0].Deserialize(new MemoryStream(System.Text.ASCIIEncoding.ASCII.GetBytes(n.InnerXml)));
                                    if (obj == null)
                                        Logger.LogLine("The field: " + n.Name + " is being delivered a null value.");
                                    Logger.LogLine("Setting the field value for the field: " + n.Name);
                                    tbl.SetField(n.Name, obj);
                                }
                                catch (Exception e)
                                {
                                    Logger.LogLine(e);
                                    throw e;
                                }
                            }
                        }
                        Logger.LogLine("Table data loaded from xml, saving to database...");
                        c.SaveWithAutogen(tbl);
                    }
                    Logger.LogLine("Object data has been imported.");
                }
            }

            zis.Close();
            c.Commit();

            //reset all relationships and autogen fields
            pool.EnableRelationships(c);
            c.EnableAndResetAutogens();
            c.Commit();
            pool.UnlockPoolPostBackupRestore();
            return true;
        }

        private static object ExtractTableValue(XmlNode node, Type t,bool isArray)
        {
            if (isArray)
            {
                if (node.ChildNodes.Count == 0)
                    return null;
                Type ty = Utility.LocateType(t.FullName.Replace("[]", ""));
                ArrayList tmp = new ArrayList();
                foreach (XmlNode n in node.ChildNodes)
                {
                    tmp.Add(ExtractTableValue(n, ty, false));
                }
                Array ret = Array.CreateInstance(ty, node.ChildNodes.Count);
                tmp.CopyTo(ret);
                return ret;
            }
            else
            {
                Table ret = (Table)t.GetConstructor(Type.EmptyTypes).Invoke(new object[] { });
                TableMap map = ClassMapper.GetTableMap(t);
                foreach (XmlNode n in node.ChildNodes)
                {
                    Logger.LogLine((map[n.Name] is ExternalFieldMap).ToString());
                    if (map[n.Name] is ExternalFieldMap)
                    {
                        Logger.LogLine("Processing external field...");
                        ret.SetField(n.Name, ExtractTableValue(n, map[n.Name].ObjectType, map[n.Name].IsArray));
                    }
                    else
                    {
                        Logger.LogLine("Processing internal field...");
                        ret.SetField(n.Name, XmlSerializer.FromTypes(new Type[] { map[n.Name].ObjectType })[0].Deserialize(new MemoryStream(System.Text.ASCIIEncoding.ASCII.GetBytes(n.InnerXml))));
                    }
                }
                return ret;
            }
        }
    }
}
