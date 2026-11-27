package net.cnkd.analytics;
import net.rwhps.server.plugin.api.*;
import com.google.gson.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** One owned writer. Host records are acknowledged only after an immutable block is durable. */
public final class CollectorPlugin implements CnkdPlugin {
    private PluginContext context;private PluginStatistics statistics;private Path directory,configFile;
    private ScheduledExecutorService writer;private boolean enabled=true;private long maximumBytes=512L*1024*1024;
    private ThreadPoolExecutor links;
    private volatile boolean playerReportsEnabled;
    private volatile String reportServiceUrl="http://127.0.0.1:28480",publicReportBaseUrl="",collectorToken="";
    private long lastAccepted;private volatile String failure="";private volatile boolean closed;
    private JsonArray pending=new JsonArray();private long pendingAt;private long pendingSequence;private int pendingBytes;
    private long heartbeatAt;
    @Override public void onEnable(PluginContext context)throws Exception{
        this.context=context;directory=context.dataDirectory().resolve("journal");configFile=context.dataDirectory().resolve("config.json");Files.createDirectories(context.dataDirectory());
        if(!Files.exists(configFile))Files.writeString(configFile,"{\"enabled\":true,\"maximumJournalMiB\":512,\"playerReportsEnabled\":false,\"reportServiceUrl\":\"http://127.0.0.1:28480\",\"publicReportBaseUrl\":\"\",\"collectorToken\":\"\"}");
        JsonObject cfg=JsonParser.parseString(Files.readString(configFile)).getAsJsonObject();enabled=cfg.get("enabled").getAsBoolean();
        maximumBytes=Math.max(16,Math.min(16384,cfg.get("maximumJournalMiB").getAsLong()))*1024*1024;
        linkConfiguration(cfg);
        statistics=context.statistics();statistics.enabled(enabled);
        writer=Executors.newSingleThreadScheduledExecutor(r->{Thread t=new Thread(r,"CNKD-Analytics-Journal");t.setDaemon(true);return t;});
        context.manage(()->{writer.shutdown();if(!writer.awaitTermination(5,TimeUnit.SECONDS))writer.shutdownNow();});
        links=new ThreadPoolExecutor(1,1,0,TimeUnit.SECONDS,new ArrayBlockingQueue<>(4),r->{Thread t=new Thread(r,"Analytics-Report-Links");t.setDaemon(true);return t;},new ThreadPoolExecutor.AbortPolicy());context.manage(()->{links.shutdownNow();links.awaitTermination(2,TimeUnit.SECONDS);});
        for(String name:new String[]{"report","战报"})context.commands().registerPlayer(name,"","查看自己最近已结算战报",cmd->{
            if(cmd.player().isEmpty())return;
            if(!playerReportsEnabled||collectorToken.isBlank()||publicReportBaseUrl.isBlank()){cmd.reply("赛后报告已由服务器保存。玩家自助入口尚未开启，请联系管理员查看。");return;}
            String hash=Journal.hash(cmd.player().get().id().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            try{links.execute(()->{try{
                JsonObject request=new JsonObject();request.addProperty("connectionHash",hash);
                java.net.HttpURLConnection connection=(java.net.HttpURLConnection)java.net.URI.create(reportServiceUrl+"/collector/v1/latest-link").toURL().openConnection();String token;
                try{byte[] payload=request.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);connection.setRequestMethod("POST");connection.setConnectTimeout(2000);connection.setReadTimeout(5000);connection.setDoOutput(true);connection.setFixedLengthStreamingMode(payload.length);connection.setRequestProperty("Authorization","Bearer "+collectorToken);connection.setRequestProperty("Content-Type","application/json");try(var out=connection.getOutputStream()){out.write(payload);}
                    if(connection.getResponseCode()!=200){cmd.reply("暂无可查看的已结算报告，或分析服务尚未准备完成。请稍后重试。");return;}
                    byte[] body;try(var input=connection.getInputStream()){body=input.readNBytes(16385);}if(body.length>16384)throw new IllegalStateException("Report response too large");token=JsonParser.parseString(new String(body,java.nio.charset.StandardCharsets.UTF_8)).getAsJsonObject().get("token").getAsString();
                }finally{connection.disconnect();}
                if(!token.matches("[a-f0-9]{64}"))throw new IllegalStateException("Invalid report token");cmd.reply("最近一局只读战报（24小时有效，请勿公开转发）："+publicReportBaseUrl+"/r/"+token);
            }catch(Exception error){cmd.reply("报告查询暂时不可用，请稍后重试。");}});cmd.reply("正在查询已结算报告…");}catch(RejectedExecutionException busy){cmd.reply("报告查询繁忙，请稍后重试。");}
        });
        context.commands().registerConsole("analytics","[status|enable|disable|reload]","赛后统计采集控制",cmd->{
            String op=cmd.arguments().isEmpty()?"status":cmd.arguments().get(0);
            if(op.equals("reload")){writer.execute(()->{try{JsonObject next=JsonParser.parseString(Files.readString(configFile)).getAsJsonObject();linkConfiguration(next);enabled=next.get("enabled").getAsBoolean();statistics.enabled(enabled);}catch(Exception error){failure=error.toString();}});cmd.reply("配置重载已排队；查看 analytics status 确认结果。");}
            else if(op.equals("enable")||op.equals("disable")){writer.execute(()->{enabled=op.equals("enable");statistics.enabled(enabled);try{JsonObject next=JsonParser.parseString(Files.readString(configFile)).getAsJsonObject();next.addProperty("enabled",enabled);next.addProperty("maximumJournalMiB",maximumBytes/1024/1024);Path tmp=configFile.resolveSibling("config.tmp");Files.writeString(tmp,next.toString());Files.move(tmp,configFile,StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);}catch(Exception e){failure=e.toString();}});cmd.reply("变更已排队；启用作用于下一局，停用将标记当前局不完整。");}
            else cmd.reply("统计："+statistics.status()+" journal="+directory+" error="+failure);
        });
        writer.scheduleWithFixedDelay(this::tick,0,100,TimeUnit.MILLISECONDS);
    }
    private void linkConfiguration(JsonObject cfg){
        boolean enabled=cfg.has("playerReportsEnabled")&&cfg.get("playerReportsEnabled").getAsBoolean();
        String service=cfg.has("reportServiceUrl")?cfg.get("reportServiceUrl").getAsString():"http://127.0.0.1:28480",base=cfg.has("publicReportBaseUrl")?cfg.get("publicReportBaseUrl").getAsString():"";
        if(enabled){java.net.URI uri=java.net.URI.create(service);if(!Set.of("127.0.0.1","localhost","[::1]").contains(uri.getHost())||!Set.of("http","https").contains(uri.getScheme())||uri.getUserInfo()!=null)throw new IllegalArgumentException("Report service must be loopback");java.net.URI publicUri=java.net.URI.create(base);if(publicUri.getHost()==null||publicUri.getUserInfo()!=null||publicUri.getQuery()!=null||publicUri.getFragment()!=null||!Set.of("http","https").contains(publicUri.getScheme())||!publicUri.getScheme().equals("https")&&!Set.of("127.0.0.1","localhost").contains(publicUri.getHost()))throw new IllegalArgumentException("Public reports require HTTPS");}
        reportServiceUrl=service.replaceAll("/+$","");publicReportBaseUrl=base.replaceAll("/+$","");collectorToken=cfg.has("collectorToken")?cfg.get("collectorToken").getAsString():"";playerReportsEnabled=enabled;
    }
    private synchronized void tick(){
        if(closed)return;
        try{
            if(System.currentTimeMillis()-heartbeatAt>1000){
                JsonObject live=new JsonObject();live.addProperty("streamId",statistics.status().get("bootId").toString());live.addProperty("at",System.currentTimeMillis());live.addProperty("pid",ProcessHandle.current().pid());live.addProperty("processStartedAt",ProcessHandle.current().info().startInstant().map(java.time.Instant::toEpochMilli).orElse(0L));
                Files.createDirectories(directory);Path tmp=directory.resolve(".live.tmp");Files.writeString(tmp,live.toString());Files.move(tmp,directory.resolve("live.json"),StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);heartbeatAt=System.currentTimeMillis();
            }
            for(StatisticsRecord r:statistics.read(256)){
                if(r.sequence()<=lastAccepted)continue;
                if(pending.size()>=256)flush();
                JsonObject row=new JsonObject();row.addProperty("sequence",r.sequence());row.addProperty("matchId",r.matchId());row.addProperty("kind",r.kind());row.add("data",Journal.JSON.toJsonTree(r.data()));
                int size=row.toString().length()*3;if(pendingBytes+size>512*1024 && pending.size()>0)flush();
                if(pending.size()==0)pendingAt=System.currentTimeMillis();pending.add(row);pendingBytes+=size;lastAccepted=r.sequence();pendingSequence=r.sequence();
                if(r.kind().equals("TERMINAL"))flush();
            }
            if(pending.size()>0 && System.currentTimeMillis()-pendingAt>=1000)flush();
            failure="";
        }catch(Exception e){String old=failure;failure=e.getClass().getSimpleName()+": "+e.getMessage();if(!failure.equals(old))context.logger().error("Journal pending; host records remain unacknowledged: "+failure,e);}
    }
    private void flush()throws Exception{
        if(pending.size()==0)return;
        if(pending.size()>256)throw new IllegalStateException("JOURNAL_PENDING_LIMIT");
        Files.createDirectories(directory);long used;try(var files=Files.list(directory)){used=files.filter(Files::isRegularFile).mapToLong(p->{try{return Files.size(p);}catch(Exception e){return maximumBytes;}}).sum();}
        if(used+pendingBytes+4096>maximumBytes)throw new IllegalStateException("JOURNAL_CAPACITY_EXCEEDED");
        JsonObject body=new JsonObject();body.addProperty("schemaVersion",1);body.addProperty("streamId",statistics.status().get("bootId").toString());body.addProperty("committedAt",System.currentTimeMillis());body.add("records",pending);
        Journal.commit(directory,body);statistics.acknowledge(pendingSequence);pending=new JsonArray();pendingBytes=0;
    }
    @Override public synchronized void onDisable()throws Exception{
        // The host freezes SERVER_SHUTDOWN before plugin disable. Drain final records before releasing ownership.
        for(int i=0;i<32 && ((Number)statistics.status().get("pending")).intValue()>0;i++){tick();flush();}
        flush();closed=true;writer.shutdown();
    }
}
