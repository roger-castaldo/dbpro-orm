using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.IO.Compression;

namespace Org.Reddragonit.Dbpro.Backup
{
    internal class ZipFile
    {
        internal static readonly DateTime TheEpoch = new DateTime(1970, 1, 1, 0, 0, 0);

        private enum FileTypes
        {
            File = '0',
            Directory = '5'
        }

        private struct sHeader
        {

            private string _name;
            public string Name
            {
                get { return _name; }
            }

            private int _mode;
            public int Mode
            {
                get { return _mode; }
            }

            private int _uid;
            public int UID
            {
                get { return _uid; }
            }

            private int _gid;
            public int gid
            {
                get { return _gid; }
            }

            private long _size;
            public long Size
            {
                get { return _size; }
            }

            private DateTime _modifyTime;
            public DateTime ModifyTime
            {
                get { return _modifyTime; }
            }

            private FileTypes _type;
            public FileTypes Type
            {
                get { return _type; }
            }

            private string _linkName;

            private short _version;
            public short Version
            {
                get { return _version; }
            }

            private string _uName;
            public string UName
            {
                get { return _uName; }
            }

            private string _gName;
            public string GName
            {
                get { return _gName; }
            }

            private string _prefix;
            public string Prefix
            {
                get { return _prefix; }
            }

            public sHeader(string name, byte[] data)
                : this(DateTime.Now,name,"/",'/')
            {
                _name = name;
                _type = FileTypes.File;
                _size = data.Length;
            }

            private sHeader(DateTime modifyTime, string name, string basePath, char dirChar)
            {
                _type = FileTypes.File;
                _size = 0;
                _mode = 511;
                _uid = 61;
                _gid = 61;
                _uName = "root";
                _gName = "root";
                _modifyTime = modifyTime;
                name = (name.EndsWith(dirChar.ToString()) ? name.Substring(0, name.Length - 1) : name);
                name = name.Substring(basePath.Length);
                _prefix = "";
                if (name.Contains(dirChar.ToString()))
                {
                    _prefix = name.Substring(0, name.LastIndexOf(dirChar));
                    name = name.Substring(name.LastIndexOf(dirChar) + 1);
                }
                _name = name;
                _linkName = "";
                _version = BitConverter.ToInt16(ASCIIEncoding.ASCII.GetBytes("  "), 0);
                if (_name.Length > 100)
                    throw new Exception("Unable to compress when file/directory name is longer than 100 characters.");
                if (_prefix.Length > 155)
                    throw new Exception("Unable to compress when the path for a file/directory is longer than 100 characters.");
            }

            public sHeader(byte[] headerData)
            {
                _name = ASCIIEncoding.ASCII.GetString(headerData,0,100).TrimEnd('\0');
                _mode = 0;
                _uid = 0;
                _gid = 0;
                _size = 0;
                _modifyTime = TheEpoch;
                _type = (FileTypes)headerData[156];
                _linkName = ASCIIEncoding.ASCII.GetString(headerData, 157, 100).TrimEnd('\0');
                _version = BitConverter.ToInt16(headerData, 263);
                _uName = ASCIIEncoding.ASCII.GetString(headerData, 265, 32).TrimEnd('\0');
                _gName = ASCIIEncoding.ASCII.GetString(headerData, 297, 32).TrimEnd('\0');
                _prefix = ASCIIEncoding.ASCII.GetString(headerData, 345, 155).TrimEnd('\0');
                _mode = StringToInt(ASCIIEncoding.ASCII.GetString(headerData,100,8));
                _uid = StringToInt(ASCIIEncoding.ASCII.GetString(headerData, 108, 8));
                _gid = StringToInt(ASCIIEncoding.ASCII.GetString(headerData, 116, 8));
                _size = StringToLong(ASCIIEncoding.ASCII.GetString(headerData, 124, 12));
                _modifyTime = TheEpoch.AddSeconds(StringToLong(ASCIIEncoding.ASCII.GetString(headerData, 136, 12)));
            }

            public byte[] Bytes
            {
                get
                {
                    byte[] ret = new byte[512];
                    ASCIIEncoding.ASCII.GetBytes(_name.PadRight(100, '\0')).CopyTo(ret, 0);
                    ASCIIEncoding.ASCII.GetBytes(IntToString(_mode)).CopyTo(ret, 100);
                    ASCIIEncoding.ASCII.GetBytes(IntToString(_uid)).CopyTo(ret, 108);
                    ASCIIEncoding.ASCII.GetBytes(IntToString(_gid)).CopyTo(ret, 116);
                    ASCIIEncoding.ASCII.GetBytes(LongToString(_size)).CopyTo(ret, 124);
                    ASCIIEncoding.ASCII.GetBytes(LongToString((long)(_modifyTime.Subtract(TheEpoch).TotalSeconds))).CopyTo(ret, 136);
                    ASCIIEncoding.ASCII.GetBytes("        ").CopyTo(ret, 148);
                    ret[156] = (byte)_type;
                    ASCIIEncoding.ASCII.GetBytes(_linkName.PadRight(100, '\0')).CopyTo(ret, 157);
                    ASCIIEncoding.ASCII.GetBytes("ustar").CopyTo(ret, 257);
                    BitConverter.GetBytes(_version).CopyTo(ret, 263);
                    ASCIIEncoding.ASCII.GetBytes(_uName.PadRight(32, '\0')).CopyTo(ret, 265);
                    ASCIIEncoding.ASCII.GetBytes(_gName.PadRight(32, '\0')).CopyTo(ret, 297);
                    ASCIIEncoding.ASCII.GetBytes(IntToString(0)).CopyTo(ret, 329);
                    ASCIIEncoding.ASCII.GetBytes(IntToString(0)).CopyTo(ret, 337);
                    ASCIIEncoding.ASCII.GetBytes(_prefix.PadRight(155, '\0')).CopyTo(ret, 345);
                    long headerCheckSum = 0;
                    foreach (byte b in ret)
                    {
                        if ((b & 0x80) == 0x80)
                            headerCheckSum -= (long)(b ^ 0x80);
                        else
                            headerCheckSum += (long)b;
                    }
                    ASCIIEncoding.ASCII.GetBytes(AddChars(Convert.ToString(headerCheckSum, 8), 6, '0', true)).CopyTo(ret, 148);
                    return ret;
                }
            }

            private string IntToString(int val)
            {
                return AddChars(Convert.ToString(val, 8), 7, '0', true);
            }

            private int StringToInt(string val)
            {
                return Convert.ToInt32(val.TrimStart('0'),8);
            }

            private string LongToString(long val)
            {
                return AddChars(Convert.ToString(val, 8), 11, '0', true);
            }

            private long StringToLong(string val)
            {
                return Convert.ToInt64(val.TrimStart('0'), 11);
            }

            private string AddChars(string str, int num, char ch, bool isLeading)
            {
                int neededZeroes = num - str.Length;
                while (neededZeroes > 0)
                {
                    if (isLeading)
                        str = ch + str;
                    else
                        str = str + ch;
                    --neededZeroes;
                }
                return str;
            }
        }

        private const byte _FILE_ID_TAG = 48;
        private const byte _DIRECTORY_ID_TAG = 53;

        private Stream _strm;
        private BinaryWriter _bw;
        private BinaryReader _br;

        private Dictionary<string, byte[]> _files;

        public Dictionary<string, byte[]>.KeyCollection Keys
        {
            get { return _files.Keys; }
        }

        public byte[] this[string name]
        {
            get { return _files[name]; }
        }

        public ZipFile(Stream strm,bool read)
        {
            _strm = strm;
            if (read)
                _bw = new BinaryWriter(new GZipStream(_strm, CompressionMode.Compress, true));
            else
            {
                _br = new BinaryReader(new GZipStream(_strm, CompressionMode.Decompress, true));
                _files = new Dictionary<string, byte[]>();
                while (_br.BaseStream.Position < _br.BaseStream.Length)
                {
                    sHeader head = new sHeader(_br.ReadBytes(512));
                    _files.Add(head.Name, _br.ReadBytes((int)head.Size));
                }
            }
        }

        public void AppendFile(string filename, byte[] fileData)
        {
            _bw.Write(new sHeader(filename,fileData).Bytes);
            BinaryReader br = new BinaryReader(new MemoryStream(fileData));
            while (br.BaseStream.Position < br.BaseStream.Length)
            {
                byte[] data = br.ReadBytes(512);
                if (data.Length < 512)
                {
                    _bw.Write(data);
                    for (int x = 0; x < 512 - data.Length; x++)
                        _bw.Write((byte)0);
                }
                else
                    _bw.Write(data);
            }
        }

        public void Flush()
        {
            _bw.Flush();
        }

        public void Close()
        {
            if (_bw != null)
                _bw.Close();
            else
                _br.Close();
        }
    }
}
