using System;
using System.Collections.Generic;
using System.Text;
using Org.Reddragonit.Dbpro.Connections;
using System.IO;
using Org.Reddragonit.Dbpro.Structure.Mapping;
using Ionic.Zip;
using System.Xml;
using System.Xml.Serialization;
using Org.Reddragonit.Dbpro.Structure;

namespace Org.Reddragonit.Dbpro.Backup
{
    public class BackupManager
    {

        public static bool BackupDataToStream(ConnectionPool pool, ref Stream outputStream)
        {
            Connection c = pool.LockDownForBackupRestore();
            List<Type> types = ClassMapper.TableTypesForConnection(pool.ConnectionName);
            List<Type> enums = new List<Type>();
            List<Type> basicTypes = new List<Type>();
            List<Type> complexTypes = new List<Type>();
            TableMap map;
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

            //output all enumerations
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
                }
                zs.PutNextEntry(t.FullName + ".xml");
                ms = new MemoryStream(XMLCompressor.CompressXMLDocument(doc));
                for (long x = 0; x < ms.Length; x += 1024)
                {
                    len = ms.Read(buff, 0, 1024);
                    zs.Write(buff, 0, len);
                }
            }

            //output all basic types
            foreach (Type t in basicTypes)
            {
                doc = new XmlDocument();
                elem = doc.CreateElement("entries");
                doc.AppendChild(elem);
                map = ClassMapper.GetTableMap(t);
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
                zs.PutNextEntry(t.FullName + ".xml");
                ms = new MemoryStream(XMLCompressor.CompressXMLDocument(doc));
                for (long x = 0; x < ms.Length; x += 1024)
                {
                    len = ms.Read(buff, 0, 1024);
                    zs.Write(buff, 0, len);
                }
            }

            //output all complex types
            foreach (Type t in complexTypes)
            {
                doc = new XmlDocument();
                elem = doc.CreateElement("entries");
                doc.AppendChild(elem);
                map = ClassMapper.GetTableMap(t);
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
                zs.PutNextEntry(t.FullName + ".xml");
                ms = new MemoryStream(XMLCompressor.CompressXMLDocument(doc));
                for (long x = 0; x < ms.Length; x += 1024)
                {
                    len = ms.Read(buff, 0, 1024);
                    zs.Write(buff, 0, len);
                }
            }


            zs.Flush();
            zs.Close();
            pool.UnlockPoolPostBackupRestore();
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

    }
}
