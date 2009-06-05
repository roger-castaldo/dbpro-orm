/*
 * Created by SharpDevelop.
 * User: Roger
 * Date: 11/05/2009
 * Time: 8:03 PM
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.IO;
using System.Reflection;
using System.Threading;

namespace Org.Reddragonit.Dbpro
{
	internal static class Initializer
	{
		private const string BasePath="Org.Reddragonit.Dbpro.lib.";
		
		private static bool _isLoaded=false;
		private static Mutex _mut = new Mutex(false);
		
		internal static void LoadAll()
		{
			_mut.WaitOne();
			if (!_isLoaded){
				Assembly ass = typeof(Initializer).Assembly;
				System.Diagnostics.Debug.WriteLine("Assembly Path: "+ass.Location);
				foreach (string str in ass.GetManifestResourceNames())
				{
					if (str.StartsWith(BasePath))
					{
						/*if (str.Contains("Npgsql"))
						{
							if (str.Contains("resources"))
							{
								new DirectoryInfo(ass.Location.Replace("DBPro.DLL","")+str.Replace(BasePath,"").Replace(".Npgsql.resources.dll","")).Create();
								WriteLibraryToFileSystem(str,ass.Location.Replace("DBPro.DLL","")+str.Replace(BasePath,"").Replace(".Npgsql.resources.dll","")+"\\Npgsql.resources.dll");
							}else
								WriteLibraryToFileSystem(str,ass.Location.Replace("DBPro.DLL","")+str.Replace(BasePath,""));
						}else*/
							LoadInternalLibrary(str);
					}
				}
				System.Diagnostics.Debug.WriteLine("Loaded Assemblies...");
				foreach(Assembly a in AppDomain.CurrentDomain.GetAssemblies())
				{
					System.Diagnostics.Debug.WriteLine(a.FullName);
				}
				_isLoaded=true;
			}
			_mut.ReleaseMutex();
		}
		
		private static void WriteLibraryToFileSystem(string path,string outputPath)
		{
			System.Diagnostics.Debug.WriteLine("Writing: "+path+" to "+outputPath);
			Stream str = typeof(Initializer).Assembly.GetManifestResourceStream(path);
			BinaryReader br = new BinaryReader(str);
			BinaryWriter bw = new BinaryWriter(new FileStream(outputPath,FileMode.Create));
			while (br.BaseStream.Position<br.BaseStream.Length)
				bw.Write(br.ReadBytes(1024));
			bw.Flush();
			bw.Close();
			if (!path.EndsWith("Npgsql.resources.dll"))
				Assembly.LoadFile(outputPath);
		}
		
		private static void LoadInternalLibrary(string path)
		{
			Stream str = typeof(Initializer).Assembly.GetManifestResourceStream(path);
			MemoryStream ms = new MemoryStream();
			BinaryReader br = new BinaryReader(str);
			BinaryWriter bw = new BinaryWriter(ms);
			while (br.BaseStream.Position<br.BaseStream.Length)
				bw.Write(br.ReadBytes(1024));
			bw.Flush();
			Assembly.Load(ms.ToArray());
		}
	}
}
