package net.cnkd.analytics;
import com.google.gson.*;
import com.sun.net.httpserver.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

/** Loopback, authenticated private postgame UI/API. Never starts or controls a game process. */
public final class ReportService implements AutoCloseable {
    private final Path root,journal;private final AnalysisStore store;private final HttpServer http;
    private final ScheduledExecutorService scanner=Executors.newSingleThreadScheduledExecutor();
    private final ThreadPoolExecutor workers=new ThreadPoolExecutor(2,4,30,TimeUnit.SECONDS,new ArrayBlockingQueue<>(32),new ThreadPoolExecutor.AbortPolicy());
    private final Map<String,Long> sessions=new ConcurrentHashMap<>();private final Map<String,long[]> requests=new LinkedHashMap<>();
    private final ThreadPoolExecutor exports=new ThreadPoolExecutor(1,1,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(4),new ThreadPoolExecutor.AbortPolicy());
    private final Map<String,ExportJob> exportJobs=new ConcurrentHashMap<>();private final long maximumExportBytes;
    private final Map<String,String> sharedExports=new ConcurrentHashMap<>();
    private final String collectorToken;private volatile boolean playerLinksEnabled;
    private final Map<String,Integer> waitingBlocks=new HashMap<>();
    private static final class ExportJob {
        final String id=UUID.randomUUID().toString(),match,format;final long expires=System.currentTimeMillis()+3600000;
        volatile String state="QUEUED",error="";volatile Path file;
        ExportJob(String match,String format){this.match=match;this.format=format;}
        Map<String,Object> view(){return Map.of("id",id,"state",state,"error",error,"format",format,"matchId",match,"expiresAt",expires);}
    }
    private final String token;private volatile String failure="";private volatile long rejectedBlocks;private volatile boolean closing;private long lastCleanup;private volatile int hotDays,archiveDays;
    private Path configurationPath;private long configurationRevision;
    public ReportService(Path root,Path journal,int port,long maxBytes,int hotDays,int archiveDays)throws Exception{
        this.root=root.toAbsolutePath().normalize();this.journal=journal.toAbsolutePath().normalize();this.hotDays=hotDays;this.archiveDays=archiveDays;
        this.maximumExportBytes=maxBytes;
        if(hotDays<1||archiveDays<hotDays)throw new IllegalArgumentException("Retention range");
        Files.createDirectories(this.root);Path key=this.root.resolve("admin-token.txt");
        if(!Files.exists(key)){byte[] random=new byte[32];new SecureRandom().nextBytes(random);Files.writeString(key,HexFormat.of().formatHex(random),StandardOpenOption.CREATE_NEW);try{Files.setPosixFilePermissions(key,java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));}catch(UnsupportedOperationException ignored){}}
        token=Files.readString(key).trim();if(token.length()<32)throw new IllegalArgumentException("Admin token too short");
        Path collectorKey=this.root.resolve("collector-token.txt");if(!Files.exists(collectorKey)){byte[] random=new byte[32];new SecureRandom().nextBytes(random);Files.writeString(collectorKey,HexFormat.of().formatHex(random),StandardOpenOption.CREATE_NEW);try{Files.setPosixFilePermissions(collectorKey,java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));}catch(UnsupportedOperationException ignored){}}
        collectorToken=Files.readString(collectorKey).trim();if(collectorToken.length()<32)throw new IllegalArgumentException("Collector token too short");
        store=new AnalysisStore(this.root,maxBytes);http=HttpServer.create(new InetSocketAddress("127.0.0.1",port),32);http.setExecutor(workers);http.createContext("/",this::handle);
    }
    public int port(){return http.getAddress().getPort();}
    private synchronized Map<String,Object> configuration(){return Map.of("hotDays",hotDays,"archiveDays",archiveDays,"playerLinksEnabled",playerLinksEnabled,"maximumStorageMiB",maximumExportBytes/1024/1024,"revision",configurationRevision);}
    private synchronized void configuration(JsonObject update)throws Exception{
        if(update.get("revision").getAsLong()!=configurationRevision)throw new IllegalStateException("CONFIG_REVISION_CONFLICT");
        int hot=update.get("hotDays").getAsInt(),archive=update.get("archiveDays").getAsInt();boolean links=update.get("playerLinksEnabled").getAsBoolean();
        if(hot<1||hot>3650||archive<hot||archive>3650)throw new IllegalArgumentException("保留期范围1至3650天，归档期限不能短于热数据");
        Path destination=configurationPath==null?root.resolve("runtime-policy.json"):configurationPath;
        JsonObject next=Files.exists(destination)?JsonParser.parseString(Files.readString(destination)).getAsJsonObject():new JsonObject();next.addProperty("hotDays",hot);next.addProperty("archiveDays",archive);next.addProperty("playerLinksEnabled",links);next.addProperty("configurationRevision",configurationRevision+1);
        Path temp=destination.resolveSibling(destination.getFileName()+".tmp");Files.writeString(temp,Journal.JSON.toJson(next));try(var channel=java.nio.channels.FileChannel.open(temp,StandardOpenOption.WRITE)){channel.force(true);}Files.move(temp,destination,StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);
        hotDays=hot;archiveDays=archive;playerLinksEnabled=links;configurationRevision++;synchronized(store){store.execute("INSERT INTO audit VALUES(?,?,?)",System.currentTimeMillis(),"CONFIG_UPDATED",Long.toString(configurationRevision));}
    }
    private Map<String,Object> queueExport(String mid,String format)throws Exception{
        store.report(mid);if(!Set.of("jsonl.gz","jsonl","csv","html").contains(format))throw new IllegalArgumentException("Invalid format");
        for(ExportJob job:new ArrayList<>(exportJobs.values()))if(job.expires<System.currentTimeMillis()){if(job.file!=null)Files.deleteIfExists(job.file);exportJobs.remove(job.id);}
        if(exportJobs.size()>=16)throw new IllegalStateException("EXPORT_JOB_BUDGET");
        ExportJob job=new ExportJob(mid,format);exportJobs.put(job.id,job);
        try{exports.execute(()->{
            Path temporary=root.resolve("exports").resolve(".writing-"+job.id),target=root.resolve("exports").resolve(job.id+"."+format);job.state="RUNNING";
            try{Files.createDirectories(temporary.getParent());final long remaining;try(var files=Files.walk(root)){remaining=maximumExportBytes-files.filter(Files::isRegularFile).mapToLong(p->{try{return Files.size(p);}catch(Exception e){return maximumExportBytes;}}).sum();}if(remaining<1024)throw new java.io.IOException("EXPORT_STORAGE_BUDGET");try(var file=Files.newOutputStream(temporary,StandardOpenOption.CREATE_NEW);var limited=new java.io.FilterOutputStream(file){long count;@Override public void write(int b)throws java.io.IOException{if(++count>remaining)throw new java.io.IOException("EXPORT_STORAGE_BUDGET");out.write(b);}@Override public void write(byte[] b,int off,int len)throws java.io.IOException{if((count+=len)>remaining)throw new java.io.IOException("EXPORT_STORAGE_BUDGET");out.write(b,off,len);}}){store.export(mid,format,limited);}Files.move(temporary,target,StandardCopyOption.ATOMIC_MOVE);job.file=target;job.state="READY";}
            catch(Exception e){job.error=e.getClass().getSimpleName()+": "+e.getMessage();job.state="FAILED";try{Files.deleteIfExists(temporary);}catch(Exception ignored){}}
        });}catch(RejectedExecutionException full){exportJobs.remove(job.id);throw new IllegalStateException("EXPORT_QUEUE_FULL");}
        return job.view();
    }
    public void start(){http.start();scanner.scheduleWithFixedDelay(this::scan,0,1,TimeUnit.SECONDS);}
    private void scan(){if(closing)return;try{
        if(Files.isDirectory(journal)&&!Files.isSymbolicLink(journal)){
            List<Map.Entry<Path,JsonObject>> candidates=new ArrayList<>();
            try(var files=Files.list(journal)){
                // Collector names blocks by stream/first sequence; never select an arbitrary hash prefix.
                var selected=files.filter(p->p.getFileName().toString().startsWith("chunk-")&&p.getFileName().toString().endsWith(".json")).sorted().limit(256).toList();
                for(Path path:selected){
                    try{JsonObject payload=Journal.decode(path);if(payload.getAsJsonArray("records").isEmpty())throw new IllegalArgumentException("EMPTY_BLOCK");candidates.add(Map.entry(path,payload));}catch(RuntimeException invalid){quarantine(path,invalid.getMessage());}
                }
            }
            candidates.sort(Comparator.comparing((Map.Entry<Path,JsonObject> x)->x.getValue().get("streamId").getAsString()).thenComparingLong(x->x.getValue().getAsJsonArray("records").get(0).getAsJsonObject().get("sequence").getAsLong()));
            for(var candidate:candidates){Path path=candidate.getKey();try{
                String hash=JsonParser.parseString(Files.readString(path)).getAsJsonObject().get("sha256").getAsString();store.ingest(candidate.getValue(),hash);Files.delete(path);waitingBlocks.remove(path.getFileName().toString());
            }catch(IllegalArgumentException|com.google.gson.JsonParseException invalid){quarantine(path,invalid.getMessage());}catch(IllegalStateException waiting){if(!waiting.getMessage().equals("WAITING_SESSION"))quarantine(path,waiting.getMessage());else if(waitingBlocks.merge(path.getFileName().toString(),1,Integer::sum)>60){quarantine(path,"SESSION_STILL_UNAVAILABLE");waitingBlocks.remove(path.getFileName().toString());}}}
            Path live=journal.resolve("live.json");
            if(Files.exists(live)){
                JsonObject marker=JsonParser.parseString(Files.readString(live)).getAsJsonObject();String stream=marker.get("streamId").getAsString();
                if(System.currentTimeMillis()-Files.getLastModifiedTime(live).toMillis()>15000&&marker.has("pid")){
                    var process=ProcessHandle.of(marker.get("pid").getAsLong());
                    boolean originalAlive=process.isPresent()&&process.get().isAlive()&&process.get().info().startInstant().map(x->x.toEpochMilli()==marker.get("processStartedAt").getAsLong()).orElse(true);
                    if(!originalAlive)store.interrupted(stream);
                }
                synchronized(store){for(var prior:store.rows("SELECT DISTINCT stream FROM matches WHERE state='RECORDING' AND stream<>?",stream))store.interrupted(prior.get("stream").toString());}
            }
        }
        if(System.currentTimeMillis()-lastCleanup>3600000){cleanup();lastCleanup=System.currentTimeMillis();}failure="";
    }catch(Exception e){failure=e.getClass().getSimpleName()+": "+e.getMessage();}}
    private void quarantine(Path path,String reason)throws Exception{Files.move(path,path.resolveSibling(path.getFileName()+".rejected-"+UUID.randomUUID()));rejectedBlocks++;failure="REJECTED_BLOCK: "+reason;}
    private void cleanup()throws Exception{
        synchronized(store){for(var m:store.rows("SELECT id,terminal,archive_sha FROM matches WHERE terminal IS NOT NULL AND pinned=0 AND state<>'PURGED'")){
            long ended=JsonParser.parseString(m.get("terminal").toString()).getAsJsonObject().get("endedAt").getAsLong();long age=System.currentTimeMillis()-ended;
            if(age>hotDays*86400000L&&m.get("archive_sha")==null)store.archive(m.get("id").toString());
            if(age>archiveDays*86400000L){Path file=root.resolve("archives").resolve(m.get("id")+".jsonl.gz");Files.deleteIfExists(file);store.execute("DELETE FROM points WHERE match_id=?",m.get("id"));store.execute("UPDATE matches SET state='PURGED',archive_sha=NULL WHERE id=?",m.get("id"));}
        }}
    }
    private static boolean equal(String a,String b){return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8),b.getBytes(StandardCharsets.UTF_8));}
    private void authorize(HttpExchange e){
        String cookie=e.getRequestHeaders().getFirst("Cookie");String session=cookie==null?"":Arrays.stream(cookie.split(";")).map(String::trim).filter(x->x.startsWith("analytics_session=")).map(x->x.substring(18)).findFirst().orElse("");
        long until=sessions.getOrDefault(session,0L);String bearer=e.getRequestHeaders().getFirst("Authorization");if(until<System.currentTimeMillis()&&(bearer==null||!equal(bearer,"Bearer "+token)))throw new SecurityException("需要管理员登录");
    }
    private synchronized void budget(HttpExchange e){
        long now=System.currentTimeMillis();String key=e.getRemoteAddress().getAddress().getHostAddress();
        if(requests.size()>256)requests.clear();long[] state=requests.computeIfAbsent(key,k->new long[]{now,0});if(now-state[0]>60000){state[0]=now;state[1]=0;}
        if(++state[1]>600)throw new IllegalStateException("REQUEST_BUDGET");
    }
    private static Map<String,String> query(URI u){var out=new HashMap<String,String>();if(u.getRawQuery()!=null)for(String p:u.getRawQuery().split("&")){String[] a=p.split("=",2);out.put(URLDecoder.decode(a[0],StandardCharsets.UTF_8),a.length==1?"":URLDecoder.decode(a[1],StandardCharsets.UTF_8));}return out;}
    private static long num(Map<String,String> q,String key,long def){return q.containsKey(key)?Long.parseLong(q.get(key)):def;}
    private static JsonObject body(HttpExchange e)throws Exception{byte[] data=e.getRequestBody().readNBytes(65537);if(data.length>65536)throw new IllegalArgumentException("Request too large");return JsonParser.parseString(new String(data,StandardCharsets.UTF_8)).getAsJsonObject();}
    private void send(HttpExchange e,int status,Object value)throws Exception{byte[] bytes=Journal.JSON.toJson(value).getBytes(StandardCharsets.UTF_8);e.getResponseHeaders().set("Content-Type","application/json; charset=utf-8");e.sendResponseHeaders(status,bytes.length);try(var out=e.getResponseBody()){out.write(bytes);}}
    private void handle(HttpExchange e){try{
        budget(e);e.getResponseHeaders().set("Cache-Control","no-store");e.getResponseHeaders().set("X-Content-Type-Options","nosniff");e.getResponseHeaders().set("Referrer-Policy","no-referrer");
        String path=e.getRequestURI().getPath(),method=e.getRequestMethod();
        if(!Set.of("GET","POST").contains(method)){send(e,405,Map.of("error","METHOD_NOT_ALLOWED"));return;}
        if(method.equals("POST")){
            String origin=e.getRequestHeaders().getFirst("Origin"),host=e.getRequestHeaders().getFirst("Host");
            if(origin!=null&&!origin.equals("http://"+host)&&!origin.equals("https://"+host))throw new SecurityException("来源不匹配");
            if(!Optional.ofNullable(e.getRequestHeaders().getFirst("Content-Type")).orElse("").startsWith("application/json"))throw new IllegalArgumentException("JSON required");
        }
        if(path.equals("/api/session")&&method.equals("POST")){
            if(!equal(AnalysisStore.required(body(e),"token"),token))throw new SecurityException("凭据无效");
            sessions.entrySet().removeIf(x->x.getValue()<System.currentTimeMillis());if(sessions.size()>=64)throw new IllegalStateException("SESSION_BUDGET");
            String s=UUID.randomUUID().toString();sessions.put(s,System.currentTimeMillis()+8*3600000L);e.getResponseHeaders().add("Set-Cookie","analytics_session="+s+"; Path=/; HttpOnly; SameSite=Strict");send(e,200,Map.of("ok",true));return;
        }
        if(Set.of("/collector/v1/participant-link","/collector/v1/latest-link").contains(path)&&method.equals("POST")){
            if(!playerLinksEnabled)throw new SecurityException("玩家报告入口未启用");String auth=e.getRequestHeaders().getFirst("Authorization");if(auth==null||!equal(auth,"Bearer "+collectorToken))throw new SecurityException("采集应用凭据无效");JsonObject request=body(e);
            send(e,200,path.endsWith("latest-link")?store.latestShare(AnalysisStore.required(request,"connectionHash")):store.share(AnalysisStore.required(request,"matchId"),AnalysisStore.required(request,"connectionHash"),24));return;
        }
        if(path.matches("/r/[a-f0-9]{64}")&&method.equals("GET")){
            String token=path.substring(3),mid=store.sharedMatch(token),jobId=sharedExports.get(token);ExportJob job=jobId==null?null:exportJobs.get(jobId);
            if(job==null||job.expires<System.currentTimeMillis()){Map<String,Object> queued=queueExport(mid,"html");jobId=queued.get("id").toString();if(sharedExports.size()>1024)sharedExports.clear();sharedExports.put(token,jobId);job=exportJobs.get(jobId);}
            e.getResponseHeaders().set("Content-Type","text/html; charset=utf-8");e.getResponseHeaders().set("Content-Security-Policy","default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'");
            if(job.state.equals("FAILED"))throw new IllegalStateException("REPORT_GENERATION_FAILED");
            if(!job.state.equals("READY")){byte[] bytes="<!doctype html><meta charset=utf-8><meta name=viewport content=width=device-width,initial-scale=1><meta http-equiv=refresh content=1><title>赛后报告</title><p>正在生成只读赛后报告，请稍候…</p>".getBytes(StandardCharsets.UTF_8);e.sendResponseHeaders(202,bytes.length);try(var out=e.getResponseBody()){out.write(bytes);}return;}
            e.sendResponseHeaders(200,Files.size(job.file));try(var in=Files.newInputStream(job.file);var out=e.getResponseBody()){in.transferTo(out);}return;
        }
        if(path.equals("/")||path.equals("/app.js")||path.equals("/style.css")){
            if(!method.equals("GET")){send(e,405,Map.of("error","METHOD_NOT_ALLOWED"));return;}
            String resource=path.equals("/")?"index.html":path.substring(1);byte[] bytes;try(var in=ReportService.class.getResourceAsStream("/web/"+resource)){if(in==null)throw new java.io.FileNotFoundException(resource);bytes=in.readAllBytes();}
            e.getResponseHeaders().set("Content-Security-Policy","default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'");e.getResponseHeaders().set("Content-Type",resource.endsWith("js")?"text/javascript; charset=utf-8":resource.endsWith("css")?"text/css; charset=utf-8":"text/html; charset=utf-8");e.sendResponseHeaders(200,bytes.length);try(var out=e.getResponseBody()){out.write(bytes);}return;
        }
        authorize(e);Map<String,String> q=query(e.getRequestURI());
        if(path.equals("/api/status")&&method.equals("GET")){send(e,200,Map.of("version","0.1.0","ingestFailure",failure,"rejectedBlocks",rejectedBlocks,"journal",journal.toString(),"reportMode","POSTGAME_ONLY"));return;}
        if(path.equals("/api/config")){if(method.equals("POST"))configuration(body(e));send(e,200,configuration());return;}
        if(path.equals("/api/logout")&&method.equals("POST")){sessions.clear();e.getResponseHeaders().add("Set-Cookie","analytics_session=; Max-Age=0; Path=/; HttpOnly; SameSite=Strict");send(e,200,Map.of("ok",true));return;}
        if(path.equals("/api/matches")&&method.equals("GET")){send(e,200,store.list((int)num(q,"offset",0),(int)num(q,"count",50)));return;}
        if(path.startsWith("/api/exports/")&&method.equals("GET")){
            String[] exportParts=path.split("/");if(exportParts.length<4||exportParts.length>5)throw new java.util.NoSuchElementException();ExportJob job=exportJobs.get(exportParts[3]);if(job==null||job.expires<System.currentTimeMillis())throw new java.util.NoSuchElementException();
            if(exportParts.length==4){send(e,200,job.view());return;}
            if(!exportParts[4].equals("download"))throw new java.util.NoSuchElementException();if(!job.state.equals("READY"))throw new IllegalStateException("EXPORT_NOT_READY");
            e.getResponseHeaders().set("Content-Type","application/octet-stream");e.getResponseHeaders().set("Content-Disposition","attachment; filename=\""+job.match+"."+job.format+"\"");e.sendResponseHeaders(200,Files.size(job.file));try(var file=Files.newInputStream(job.file);var output=e.getResponseBody()){file.transferTo(output);}return;
        }
        String[] parts=path.split("/");if(parts.length<4||!parts[1].equals("api")||!parts[2].equals("matches"))throw new java.util.NoSuchElementException();
        String mid=parts[3],action=parts.length==4?"report":parts[4];String metric=q.getOrDefault("metric","income"),group=q.getOrDefault("group","player"),perspective=q.getOrDefault("perspective","");
        if(q.containsKey("revision")&&store.report(mid).get("revision").getAsLong()!=num(q,"revision",0))throw new IllegalStateException("REVISION_CONFLICT");
        if(method.equals("GET")){
            switch(action){
                case "report"->send(e,200,store.report(mid));
                case "quality"->send(e,200,store.report(mid));
                case "comparison"->send(e,200,store.comparison(mid,metric,group,(int)num(q,"maxPoints",500),perspective));
                case "cursor"->send(e,200,store.cursor(mid,metric,num(q,"gameMs",0),group,perspective));
                case "raw"->send(e,200,store.raw(mid,q.getOrDefault("participantId",""),metric,num(q,"afterGameMs",-1),(int)num(q,"count",1500)));
                case "events"->{store.report(mid);synchronized(store){send(e,200,store.rows("SELECT seq,kind,t FROM events WHERE match_id=? ORDER BY seq LIMIT 1000",mid));}}
                case "export"->send(e,202,queueExport(mid,q.getOrDefault("format","jsonl.gz")));
                default->throw new java.util.NoSuchElementException();
            }
        }else{
            JsonObject request=body(e);
            if(action.equals("exports")){send(e,202,queueExport(mid,request.has("format")?request.get("format").getAsString():"jsonl.gz"));return;}
            if(action.equals("share")){send(e,200,store.share(mid,null,24));return;}
            if(action.equals("revoke-shares")){store.revokeShares(mid);send(e,200,Map.of("ok",true));return;}
            switch(action){case "pin"->store.pin(mid,request.get("pinned").getAsBoolean());case "archive"->store.archive(mid);case "restore"->store.restore(mid);case "backup"->store.backup(root.resolve("backups").resolve("backup-"+System.currentTimeMillis()));default->throw new IllegalArgumentException("Unknown action");}
            send(e,200,Map.of("ok",true));
        }
    }catch(SecurityException error){try{send(e,401,Map.of("error",error.getMessage()));}catch(Exception ignored){}}
    catch(java.util.NoSuchElementException error){try{send(e,404,Map.of("error","NOT_FOUND"));}catch(Exception ignored){}}
    catch(IllegalArgumentException error){try{send(e,400,Map.of("error",String.valueOf(error.getMessage())));}catch(Exception ignored){}}
    catch(IllegalStateException error){try{send(e,409,Map.of("error",String.valueOf(error.getMessage())));}catch(Exception ignored){}}
    catch(Exception error){try{send(e,503,Map.of("error","SOURCE_UNAVAILABLE: "+error.getClass().getSimpleName()));}catch(Exception ignored){}}
    finally{e.close();}}
    @Override public synchronized void close()throws Exception{if(closing)return;closing=true;scanner.shutdown();scanner.awaitTermination(5,TimeUnit.SECONDS);http.stop(1);workers.shutdownNow();exports.shutdown();if(!exports.awaitTermination(5,TimeUnit.SECONDS))exports.shutdownNow();store.close();}
    public static void main(String[] args)throws Exception{
        if(args.length!=1)throw new IllegalArgumentException("Usage: java -jar cnkd-analytics-service.jar config.json");
        Path config=Path.of(args[0]).toAbsolutePath();JsonObject c=JsonParser.parseString(Files.readString(config)).getAsJsonObject();
        Path root=Path.of(AnalysisStore.required(c,"dataDirectory")),journal=Path.of(AnalysisStore.required(c,"journalDirectory"));
        ReportService service=new ReportService(root,journal,c.has("port")?c.get("port").getAsInt():28480,c.has("maximumStorageMiB")?c.get("maximumStorageMiB").getAsLong()*1024*1024:2L*1024*1024*1024,c.has("hotDays")?c.get("hotDays").getAsInt():90,c.has("archiveDays")?c.get("archiveDays").getAsInt():365);
        service.playerLinksEnabled=c.has("playerLinksEnabled")&&c.get("playerLinksEnabled").getAsBoolean();
        service.configurationPath=config;service.configurationRevision=c.has("configurationRevision")?c.get("configurationRevision").getAsLong():0;
        Runtime.getRuntime().addShutdownHook(new Thread(()->{try{service.close();}catch(Exception ignored){}}));service.start();
        System.out.println("CNKD Postgame Analytics ready on 127.0.0.1:"+service.port()+"; admin token is stored in dataDirectory/admin-token.txt");
        Thread console=new Thread(()->{try{var reader=new java.io.BufferedReader(new java.io.InputStreamReader(System.in));String line;while((line=reader.readLine())!=null)if(line.trim().equals("exit")){service.close();break;}}catch(Exception ignored){}},"Analytics-Console");console.setDaemon(true);console.start();
    }
}
