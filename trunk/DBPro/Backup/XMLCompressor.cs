using System;
using System.Xml;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Org.Reddragonit.Dbpro.Backup
{
    public class XMLCompressor
    {
        private enum BYTE_TAGS
        {
            OPEN_ELEMENT = 1,
            CLOSE_ELEMENT = 2,
            DECLARE_ELEMENT = 3,
            OPEN_ATTRIBUTE = 4,
            CLOSE_ATTRIBUTE = 5,
            DECLARE_ATTRIBUTE = 6,
            DEFINE_ELEMENTS_BEGIN = 7,
            DEFINE_ELEMENTS_END = 8,
            DEFINE_ATTRIBUTES_START=9,
            DEFINE_ATTRIBUTES_END = 10,
            BEGIN_DOCUMENT = 11,
            END_DOCUMENT = 12
        }

        public static XmlDocument DecompressXMLDocument(Stream ms)
        {
            BinaryReader br = new BinaryReader(ms);
            StringBuilder sb = new StringBuilder();

            while (br.PeekChar() != (int)BYTE_TAGS.DEFINE_ELEMENTS_BEGIN)
            {
                br.ReadChar();
            }


            //Load elemental translations
            br.ReadChar();
            bool elem_use_short = br.ReadByte() == (byte)2;
            Dictionary<int, string> elementTrans = new Dictionary<int, string>();
            while (true)
            {
                if (br.ReadByte() == (byte)BYTE_TAGS.DEFINE_ELEMENTS_END)
                    break;
                if (elem_use_short)
                    elementTrans.Add((int)br.ReadInt16(), System.Text.ASCIIEncoding.ASCII.GetString(br.ReadBytes(br.ReadInt32())));
                else
                    elementTrans.Add((int)br.ReadByte(),System.Text.ASCIIEncoding.ASCII.GetString(br.ReadBytes(br.ReadInt32())));
            }
            
            //Load attribute translations
            br.ReadByte();
            bool att_use_short = br.ReadByte() == (byte)2;
            Dictionary<int, string> attTrans = new Dictionary<int, string>();
            while (true)
            {
                if (br.ReadByte() == (byte)BYTE_TAGS.DEFINE_ATTRIBUTES_END)
                    break;
                if (att_use_short)
                    attTrans.Add((int)br.ReadInt16(), System.Text.ASCIIEncoding.ASCII.GetString(br.ReadBytes(br.ReadInt32())));
                else
                    attTrans.Add((int)br.ReadByte(), System.Text.ASCIIEncoding.ASCII.GetString(br.ReadBytes(br.ReadInt32())));
            }

            //begin loading and translation of the document back to xml
            br.ReadByte();
            XmlDocument ret = new XmlDocument();
            while (br.BaseStream.Position < br.BaseStream.Length - 1)
            {
                XmlElement tmp = LoadElement(ref br, ret,att_use_short,elem_use_short,elementTrans,attTrans);
                if (tmp != null)
                    ret.AppendChild(tmp);
            }

            return ret;
        }

        private static XmlElement LoadElement(ref BinaryReader br, XmlDocument doc,bool att_use_short,bool elem_use_short,Dictionary<int,string> elemTrans,Dictionary<int,string> attTrans)
        {
            if (br.ReadByte() == (byte)BYTE_TAGS.OPEN_ELEMENT)
            {
                XmlElement ret=null;
                if (elem_use_short)
                    ret = doc.CreateElement(elemTrans[(int)br.ReadInt16()]);
                else
                    ret = doc.CreateElement(elemTrans[(int)br.ReadByte()]);
                int len = br.ReadInt32();
                if (len != 0)
                    ret.InnerText = System.Text.ASCIIEncoding.ASCII.GetString(br.ReadBytes(len));
                while (br.PeekChar() == (int)BYTE_TAGS.OPEN_ATTRIBUTE)
                {
                    br.ReadByte();
                    XmlAttribute att = null;
                    if (att_use_short)
                        att = doc.CreateAttribute(attTrans[(int)br.ReadInt16()]);
                    else
                        att = doc.CreateAttribute(attTrans[(int)br.ReadByte()]);
                    att.Value = System.Text.ASCIIEncoding.ASCII.GetString(br.ReadBytes(br.ReadInt32()));
                    ret.Attributes.Append(att);
                    br.ReadByte();
                }
                while (br.PeekChar() == (int)BYTE_TAGS.OPEN_ELEMENT)
                {
                    ret.AppendChild(LoadElement(ref br, doc, att_use_short, elem_use_short, elemTrans, attTrans));
                }
                br.ReadByte();
                return ret;
            }
            return null;
        }

        //Base file structure is <xml tag followed by new line>
        //then being definitions byte, define each element and attribute with lengths, also 
        //define total number of different types.
        public static byte[] CompressXMLDocument(XmlDocument doc)
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter bw = new BinaryWriter(ms);
            List<string> attributes = new List<string>();
            List<string> elements = new List<string>();
            recurLocateAttributesAndElements(ref attributes, ref elements, (XmlElement)doc.DocumentElement);
            Dictionary<string, int> elementTrans = new Dictionary<string, int>();
            Dictionary<string, int> attributeTrans = new Dictionary<string, int>();

            //outputting elements header
            bw.Write((byte)BYTE_TAGS.DEFINE_ELEMENTS_BEGIN);
            bool elem_use_short = false;
            if (elements.Count < 250)
                bw.Write((byte)1);
            else if (elements.Count > 250)
            {
                elem_use_short = true;
                bw.Write((byte)2);
            }
            int cnt = 0;
            foreach (string str in elements)
            {
                bw.Write((byte)BYTE_TAGS.DECLARE_ELEMENT);
                if (elements.Count < 250)
                {
                    bw.Write((byte)cnt);
                    elementTrans.Add(str, cnt);
                }
                else if (elements.Count > 250)
                {
                    bw.Write((short)cnt);
                    elementTrans.Add(str, cnt);
                }
                bw.Write(str.Length);
                bw.Write(System.Text.ASCIIEncoding.ASCII.GetBytes(str));
                cnt++;
            }
            bw.Write((byte)BYTE_TAGS.DEFINE_ELEMENTS_END);
            elements = null;

            //outputting attributes header
            bool att_use_short = false;
            bw.Write((byte)BYTE_TAGS.DEFINE_ATTRIBUTES_START);
            if (attributes.Count < 250)
                bw.Write((byte)1);
            else if (attributes.Count > 250)
            {
                att_use_short = true;
                bw.Write((byte)2);
            }
            cnt = 0;
            foreach (string str in attributes)
            {
                bw.Write((byte)BYTE_TAGS.DECLARE_ELEMENT);
                if (attributes.Count < 250)
                {
                    bw.Write((byte)cnt);
                    attributeTrans.Add(str, cnt);
                }
                else if (attributes.Count > 250)
                {
                    bw.Write((short)cnt);
                    attributeTrans.Add(str,cnt);
                }
                bw.Write(str.Length);
                bw.Write(System.Text.ASCIIEncoding.ASCII.GetBytes(str));
                cnt++;
            }
            bw.Write((byte)BYTE_TAGS.DEFINE_ATTRIBUTES_END);
            attributes = null;

            bw.Write((byte)BYTE_TAGS.BEGIN_DOCUMENT);

            //translate all element/attribute data into bytes with strings.
            recurOutputElement(ref bw, attributeTrans, elementTrans, (XmlElement)doc.DocumentElement,att_use_short,elem_use_short);

            bw.Write((byte)BYTE_TAGS.END_DOCUMENT);

            return ms.ToArray();
        }

        private static void recurOutputElement(ref BinaryWriter bw, Dictionary<string, int> attributeTrans, Dictionary<string, int> elementTrans, XmlElement elem,bool att_use_short,bool elem_use_short)
        {
            bw.Write((byte)BYTE_TAGS.OPEN_ELEMENT);
            if (elem_use_short)
                bw.Write((short)elementTrans[elem.Name]);
            else
                bw.Write((byte)elementTrans[elem.Name]);

            string val = null;
            foreach (XmlNode node in elem.ChildNodes)
            {
                if (node.NodeType == XmlNodeType.Text)
                {
                    val = node.Value;
                    break;
                }
            }

            if (val != null)
            {
                bw.Write(val.Length);
                bw.Write(System.Text.ASCIIEncoding.ASCII.GetBytes(val));
            }
            else
                bw.Write((int)0);

            foreach (XmlAttribute att in elem.Attributes)
            {
                bw.Write((byte)BYTE_TAGS.OPEN_ATTRIBUTE);
                if (att_use_short)
                    bw.Write((short)attributeTrans[att.Name]);
                else
                    bw.Write((byte)attributeTrans[att.Name]);
                bw.Write(att.Value.Length);
                bw.Write(System.Text.ASCIIEncoding.ASCII.GetBytes(att.Value));
                bw.Write((byte)BYTE_TAGS.CLOSE_ATTRIBUTE);
            }

            foreach (XmlNode el in elem.ChildNodes)
            {
                if (el is XmlElement)
                    recurOutputElement(ref bw, attributeTrans, elementTrans, (XmlElement)el, att_use_short, elem_use_short);
            }

            bw.Write((byte)BYTE_TAGS.CLOSE_ELEMENT);
        }

        private static void recurLocateAttributesAndElements(ref List<string> attributes, ref List<string> elements, XmlElement element)
        {
            if (!elements.Contains(element.Name))
                elements.Add(element.Name);
            foreach (XmlAttribute att in element.Attributes)
            {
                if (!attributes.Contains(att.Name))
                    attributes.Add(att.Name);
            }
            foreach (XmlNode elem in element.ChildNodes)
            {
                if (elem is XmlElement)
                    recurLocateAttributesAndElements(ref attributes, ref elements, (XmlElement)elem);
            }
            if (element.NextSibling != null)
                recurLocateAttributesAndElements(ref attributes, ref elements, (XmlElement)element.NextSibling);
        }
    }
}
