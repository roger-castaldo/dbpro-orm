using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using Org.Reddragonit.Dbpro.Connections;
using Org.Reddragonit.Dbpro.Structure;
using Org.Reddragonit.Dbpro.Connections.PoolComponents;
using System.Reflection;

namespace Org.Reddragonit.Dbpro.Backup
{
    public class BackupManager
    {

        public static bool BackupDataToStream(ConnectionPool pool, ref Stream outputStream)
        {
            Logger.LogLine("Locking down "+pool.ConnectionName+" database for backing up...");
            Connection c = pool.LockDownForBackupRestore();
            System.Threading.Thread.Sleep(500);
            c.StartTransaction();
            Logger.LogLine("Database locked down for backing up");
            List<Type> types = pool.Mapping.Types;
            List<Type> enums = new List<Type>();
            List<Type> basicTypes = new List<Type>();
            List<Type> complexTypes = new List<Type>();
            sTable tbl;
            Logger.LogLine("Loading all required types...");
            foreach (Type t in types)
            {
                tbl = pool.Mapping[t];
                foreach(sTableField fld in tbl.Fields)
                {
                    if (fld.Type == Org.Reddragonit.Dbpro.Structure.Attributes.FieldType.ENUM)
                    {
                        PropertyInfo pi = t.GetProperty(fld.ClassProperty, Utility._BINDING_FLAGS);
                        if (!enums.Contains((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType)))
                            enums.Add((pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType));
                    }
                }
                if (tbl.ForeignTableProperties.Length > 0)
                {
                    if (!complexTypes.Contains(t))
                        recurAddRelatedTypes(ref basicTypes, ref complexTypes, tbl, t,pool);
                }
                else
                {
                    if (!basicTypes.Contains(t))
                        basicTypes.Add(t);
                }
            }

            //begin data dumping and outputting
            ZipFile zf = new ZipFile(outputStream,false);
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
                zf.AppendFile(cnt.ToString("0000000") + "_" + t.FullName + ".xml", XMLCompressor.CompressXMLDocument(doc));
                cnt++;
            }

            //output all basic types
            Logger.LogLine("Backing up the basic types for database...");
            foreach (Type t in basicTypes)
            {
                Logger.LogLine("Backing up basic type: " + t.FullName);
                doc = new XmlDocument();
                elem = doc.CreateElement("entries");
                doc.AppendChild(elem);
                tbl = pool.Mapping[t];
                Logger.LogLine("Extracting basic type: " + t.FullName + " from the database and writing it to the xml document.");
                foreach (Table table in c.SelectAll(t))
                {
                    elem = doc.CreateElement("entry");
                    foreach (string str in tbl.Properties)
                    {
                        obj = table.GetField(str);
                        if (obj != null)
                            elem.AppendChild(CreateElementWithValue(doc, str, obj));
                    }
                    doc.DocumentElement.AppendChild(elem);
                }
                Logger.LogLine("Compressing basic type: " + t.FullName + " data and appending it into the zip file.");
                zf.AppendFile(cnt.ToString("0000000") + "_" + t.FullName + ".xml", XMLCompressor.CompressXMLDocument(doc));
                cnt++;
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
                tbl = pool.Mapping[t];
                Logger.LogLine("Extracting complex type: " + t.FullName + " from the database and writing it to the xml document.");
                foreach (Table table in c.SelectAll(t))
                {
                    elem = doc.CreateElement("entry");
                    foreach (string str in tbl.Properties)
                    {
                        obj = table.GetField(str);
                        if (obj != null)
                        {
                            if (tbl.GetRelationForProperty(str).HasValue)
                            {
                                PropertyInfo pi = t.GetProperty(str, Utility._BINDING_FLAGS);
                                if (pi.PropertyType.IsArray)
                                {
                                    elem.AppendChild(doc.CreateElement(str));
                                    foreach (object o in (Array)obj)
                                    {
                                        elem.ChildNodes[elem.ChildNodes.Count - 1].AppendChild(CreateRealtedXMLElement((Table)o, doc, "child",pool));
                                    }
                                }else
                                    elem.AppendChild(CreateRealtedXMLElement((Table)obj, doc,str,pool));
                            }else
                                elem.AppendChild(CreateElementWithValue(doc, str, obj));
                        }
                    }
                    doc.DocumentElement.AppendChild(elem);
                }
                Logger.LogLine("Compressing complex type: " + t.FullName + " data and appending it into the zip file.");
                zf.AppendFile(cnt.ToString("0000000") + "_" + t.FullName + ".xml", XMLCompressor.CompressXMLDocument(doc));
                cnt++;
                c.ResetConnection(false);
            }

            zf.Flush();
            zf.Close();
            c.Reset();
            Logger.LogLine("Backup of database complete, re-enabling pool.");
            c.Disconnect();
            pool.UnlockPoolPostBackupRestore();
            return true;
        }

        private static XmlElement CreateRealtedXMLElement(Table val,XmlDocument doc,string name,ConnectionPool pool)
        {
            XmlElement ret = doc.CreateElement(name);
            sTable map = pool.Mapping[val.GetType()];
            object obj;
            foreach (string str in map.PrimaryKeyProperties)
            {
                obj = val.GetField(str);
                if (obj != null)
                {
                    if (pool.Mapping.IsMappableType(obj.GetType()))
                        ret.AppendChild(CreateRealtedXMLElement((Table)obj, doc, str,pool));
                    else
                        ret.AppendChild(CreateElementWithValue(doc, str, obj));
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

        private static void recurAddRelatedTypes(ref List<Type> basicTypes, ref List<Type> complexTypes,sTable map,Type type,ConnectionPool pool)
        {
            if (complexTypes.Contains(type))
                return;
            foreach (string str in map.ForeignTableProperties)
            {
                PropertyInfo pi = type.GetProperty(str, Utility._BINDING_FLAGS);
                Type t = (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType);
                if (!t.Equals(type))
                {
                    sTable tm = pool.Mapping[t];
                    if (tm.ForeignTableProperties.Length > 0)
                        recurAddRelatedTypes(ref basicTypes, ref complexTypes, tm, t,pool);
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
            System.Threading.Thread.Sleep(500);
            //disable autogen fields as well as all relationship constraints
            c.StartTransaction();
            c.DisableAutogens();
            System.Threading.Thread.Sleep(500);
            c.Commit();
            pool.EmptyAllTables(c);
            System.Threading.Thread.Sleep(500);
            c.Commit();
            //pool.DisableRelationships(c);
            //System.Threading.Thread.Sleep(500);
            //c.Commit();
            System.Threading.Thread.Sleep(500);

            ZipFile zf = new ZipFile(inputStream, true);
            XmlDocument doc;
            string type;
            Type t;
            Dictionary<string, int> enumMap;
            Dictionary<int, string> reverseMap;
            int len;
            Table tbl;

            foreach (string str in zf.Keys)
            {
                type = str.Substring(str.IndexOf("_") + 1);
                type=type.Substring(0,type.Length-4);
                doc = XMLCompressor.DecompressXMLDocument(new MemoryStream(zf[str]));
                Logger.LogLine("Extracted xml data for type " + type);
                t = Utility.LocateType(type);
                if (t.IsEnum)
                {
                    Logger.LogLine("Processing enum data into database...");
                    pool.Enums.WipeOutEnums(c);
                    enumMap = new Dictionary<string, int>();
                    reverseMap = new Dictionary<int, string>();
                    foreach (XmlNode node in doc.DocumentElement.ChildNodes)
                    {
                        pool.Enums.InsertToDB(t, int.Parse(node.Attributes["Value"].Value), node.Attributes["Name"].Value, c);
                        enumMap.Add(node.Attributes["Name"].Value, int.Parse(node.Attributes["Value"].Value));
                        reverseMap.Add(int.Parse(node.Attributes["Value"].Value), node.Attributes["Name"].Value);
                    }
                    pool.Enums.AssignMapValues(t, enumMap, reverseMap);
                    Logger.LogLine("Enum data has been imported.");
                }
                else
                {
                    Logger.LogLine("Processing object data into database...");
                    c.DeleteAll(t);
                    sTable map = pool.Mapping[t];
                    List<string> extProps = new List<string>(map.ForeignTableProperties);
                    foreach (XmlNode node in doc.DocumentElement.ChildNodes)
                    {
                        tbl = (Table)t.GetConstructor(Type.EmptyTypes).Invoke(new object[] { });
                        foreach (XmlNode n in node.ChildNodes)
                        {
                            Logger.LogLine("Inner Field Value ("+n.Name+"): " + n.InnerXml);
                            PropertyInfo pi = t.GetProperty(n.Name, Utility._BINDING_FLAGS);
                            if (extProps.Contains(n.Name)){
                                Logger.LogLine("Processing external field...");
                                tbl.SetField(n.Name, ExtractTableValue(n,pi.PropertyType,pool));
                            }else{
                                try
                                {
                                    Logger.LogLine("Processing internal field of the type "+(pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType).FullName+" ...");
                                    if (XmlSerializer.FromTypes(new Type[] { (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType) }).Length == 0)
                                        Logger.LogLine("The field: " + n.Name + " has no xml seriliazer available.");
                                    object obj = XmlSerializer.FromTypes(new Type[] { (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType) })[0].Deserialize(new MemoryStream(System.Text.ASCIIEncoding.ASCII.GetBytes(n.InnerXml)));
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
                //force garbage collection to occur to prevent memory leaks
                GC.Collect();
            }

            zf.Close();
            c.Commit();

            //reset all relationships and autogen fields
            //pool.EnableRelationships(c);
            //System.Threading.Thread.Sleep(500);
            //c.Commit();
            c.EnableAndResetAutogens();
            System.Threading.Thread.Sleep(500);
            c.Commit();
            c.Disconnect();
            pool.UnlockPoolPostBackupRestore();
            return true;
        }

        private static object ExtractTableValue(XmlNode node, Type t,ConnectionPool pool)
        {
            bool isArray = t.IsArray;
            if (isArray)
            {
                t = t.GetElementType();
                if (node.ChildNodes.Count == 0)
                    return null;
                ArrayList tmp = new ArrayList();
                foreach (XmlNode n in node.ChildNodes)
                {
                    tmp.Add(ExtractTableValue(n, t,pool));
                }
                Array ret = Array.CreateInstance(t, node.ChildNodes.Count);
                tmp.CopyTo(ret);
                return ret;
            }
            else
            {
                Table ret = (Table)t.GetConstructor(Type.EmptyTypes).Invoke(new object[] { });
                sTable map = pool.Mapping[t];
                List<string> fProps = new List<string>(map.ForeignTableProperties);
                foreach (XmlNode n in node.ChildNodes)
                {
                    PropertyInfo pi = t.GetProperty(n.Name, Utility._BINDING_FLAGS);
                    if (fProps.Contains(n.Name))
                    {
                        Logger.LogLine("Processing external field...");
                        ret.SetField(n.Name, ExtractTableValue(n, pi.PropertyType,pool));
                    }
                    else
                    {
                        Logger.LogLine("Processing internal field...");
                        ret.SetField(n.Name, XmlSerializer.FromTypes(new Type[] { (pi.PropertyType.IsArray ? pi.PropertyType.GetElementType() : pi.PropertyType) })[0].Deserialize(new MemoryStream(System.Text.ASCIIEncoding.ASCII.GetBytes(n.InnerXml))));
                    }
                }
                return ret;
            }
        }
    }
}
