"""Create a clean immutable local candidate; never deploys or changes an existing server."""
from pathlib import Path
import argparse,hashlib,json,shutil,subprocess,zipfile,os

ROOT=Path(__file__).resolve().parents[1]
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--host-root',type=Path,required=True);p.add_argument('--host-source-root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);args=p.parse_args()
 host=args.host_root.resolve();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
 shutil.copytree(host,out/'server')
 collector=ROOT/'collector-plugin/build/libs/cnkd-match-analytics-0.1.0.jar';service=ROOT/'report-service/build/libs/cnkd-analytics-service-0.1.0.jar'
 assert collector.exists() and service.exists()
 shutil.copy2(collector,out/'server/data/plugins'/collector.name)
 java=Path(os.environ['JAVA_HOME'])/'bin'/('java.exe' if os.name=='nt' else 'java') if os.environ.get('JAVA_HOME') else Path(shutil.which('java') or 'java')
 with (out/'plugin-approval.log').open('wb') as log:
  subprocess.run([str(java),'-jar',str(out/'server/plugin-sdk/tools/cnkd-plugin-tool.jar'),'lock',str(out/'server/data/plugins'),'--server-api','1.3.0','--approved-by','analytics-release-candidate','--output',str(out/'server/data/plugins/plugin-lock.json'),'--force'],stdout=log,stderr=subprocess.STDOUT,check=True)
 backup=out/'server/data/plugins/plugin-lock.json.bak'
 if backup.exists():backup.unlink()
 hostInfo=json.loads((out/'server/RELEASE-INFO.json').read_text(encoding='utf-8-sig'))
 hostInfo['pluginLock']=json.loads((out/'server/data/plugins/plugin-lock.json').read_text(encoding='utf-8'));(out/'server/RELEASE-INFO.json').write_text(json.dumps(hostInfo,ensure_ascii=False,indent=2),encoding='utf-8')
 with zipfile.ZipFile(out/'server/Server-All.jar') as z:
  m=z.read('META-INF/MANIFEST.MF').decode().replace('\r\n ','');assert 'Implementation-Version: 1.1.24' in m
  assert not any(n.startswith('data/') for n in z.namelist())
 assert not (out/'server/data/Settings.bin').exists()
 assert not (out/'server/data/plugins/data').exists()
 svc=out/'analytics';svc.mkdir()
 shutil.copy2(service,svc/service.name)
 config={'dataDirectory':'../analytics-data','journalDirectory':'../server/data/plugins/data/cnkd.match.analytics/journal','port':28480,'maximumStorageMiB':2048,'hotDays':90,'archiveDays':365,'playerLinksEnabled':False}
 (svc/'config.json').write_text(json.dumps(config,ensure_ascii=False,indent=2),encoding='utf-8')
 (svc/'start-windows.bat').write_bytes(('@echo off\ncd /d "%~dp0"\nif not defined CNKD_UDS_TMP set "CNKD_UDS_TMP=%SystemDrive%\\tmp\\cnkd-hps-uds"\nif not exist "%CNKD_UDS_TMP%" mkdir "%CNKD_UDS_TMP%"\njava "-Djdk.net.unixdomain.tmpdir=%CNKD_UDS_TMP%" -Xmx512m -Dfile.encoding=UTF-8 -jar cnkd-analytics-service-0.1.0.jar config.json\nset "CNKD_ANALYTICS_EXIT=%ERRORLEVEL%"\npause\nexit /b %CNKD_ANALYTICS_EXIT%\n').replace('\n','\r\n').encode('utf-8'))
 (svc/'start.sh').write_bytes(b'#!/bin/sh\nset -eu\ncd "$(dirname "$0")"\nexec java -Xmx512m -Dfile.encoding=UTF-8 -jar cnkd-analytics-service-0.1.0.jar config.json\n')
 for f in ['README.md','LICENSE','NOTICE.md']:shutil.copy2(ROOT/f,out/f)
 shutil.copy2(collector,out/collector.name)
 info={'product':'CNKD Postgame Analytics','pluginVersion':'0.1.0','hostVersion':'1.1.24','pluginApiVersion':'1.3.0','hostSha256':sha(out/'server/Server-All.jar'),'pluginSha256':sha(collector),'serviceSha256':sha(service),'pluginSourceCommit':subprocess.run(['git','rev-parse','HEAD'],cwd=ROOT,capture_output=True,text=True,check=True).stdout.strip(),'status':'LOCAL_CANDIDATE; SEE VALIDATION-CN.md','productionDeployed':False}
 (out/'RELEASE-INFO.json').write_text(json.dumps(info,ensure_ascii=False,indent=2),encoding='utf-8')
 sources=out/'sources';sources.mkdir()
 subprocess.run(['git','archive','--format=zip','--output='+str(sources/'cnkd-match-analytics-source.zip'),'HEAD'],cwd=ROOT,check=True)
 hostRepo=args.host_source_root.resolve()
 subprocess.run(['git','archive','--format=zip','--output='+str(sources/'cnkd-next-host-source.zip'),hostInfo['sourceRevision']],cwd=hostRepo,check=True)
 (sources/'host-build-state.patch').write_bytes(subprocess.run(['git','diff','--binary',hostInfo['sourceRevision']],cwd=hostRepo,capture_output=True,check=True).stdout)
 print(json.dumps({'output':str(out),'info':info},ensure_ascii=False,indent=2))

if __name__=='__main__':main()
