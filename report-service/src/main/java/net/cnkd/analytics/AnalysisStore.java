package net.cnkd.analytics;
import com.google.gson.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.zip.*;
import java.io.*;

/** Single authority for private postgame records; ingestion, queries and archive actions share a transaction lock. */
public final class AnalysisStore implements AutoCloseable {
    final Connection db;final Path root;private final long maximumBytes;
    public AnalysisStore(Path root,long maximumBytes)throws Exception{
        this.root=root;this.maximumBytes=maximumBytes;Files.createDirectories(root);Files.createDirectories(root.resolve("archives"));
        db=DriverManager.getConnection("jdbc:sqlite:"+root.resolve("analytics.db"));
        for(String sql:List.of("PRAGMA journal_mode=WAL","PRAGMA synchronous=FULL","PRAGMA busy_timeout=3000",
            "CREATE TABLE IF NOT EXISTS chunks(hash TEXT PRIMARY KEY)",
            "CREATE TABLE IF NOT EXISTS records(stream TEXT,seq INTEGER,match_id TEXT,hash TEXT,PRIMARY KEY(stream,seq))",
            "CREATE TABLE IF NOT EXISTS matches(id TEXT PRIMARY KEY,stream TEXT,session TEXT,terminal TEXT,state TEXT,partial INTEGER DEFAULT 0,last_t INTEGER DEFAULT -1,first_t INTEGER DEFAULT -1,last_tick INTEGER DEFAULT -1,first_seq INTEGER,last_seq INTEGER,pinned INTEGER DEFAULT 0,revision INTEGER DEFAULT 1,archive_sha TEXT,archive_points INTEGER)",
            "CREATE TABLE IF NOT EXISTS participants(match_id TEXT,id TEXT,body TEXT,endpoint INTEGER,color INTEGER,PRIMARY KEY(match_id,id))",
            "CREATE TABLE IF NOT EXISTS points(match_id TEXT,person TEXT,metric TEXT,t INTEGER,v INTEGER,PRIMARY KEY(match_id,person,metric,t))",
            "CREATE INDEX IF NOT EXISTS points_time ON points(match_id,metric,t)",
            "CREATE TABLE IF NOT EXISTS counters(match_id TEXT,person TEXT,t INTEGER,body TEXT,PRIMARY KEY(match_id,person,t))",
            "CREATE TABLE IF NOT EXISTS events(match_id TEXT,seq INTEGER,kind TEXT,t INTEGER,body TEXT,PRIMARY KEY(match_id,seq))",
            "CREATE TABLE IF NOT EXISTS gaps(match_id TEXT,person TEXT,first_t INTEGER,last_t INTEGER,reason TEXT)",
            "CREATE TABLE IF NOT EXISTS audit(at INTEGER,action TEXT,target TEXT)") )execute(sql);
        execute("CREATE TABLE IF NOT EXISTS shares(hash TEXT PRIMARY KEY,match_id TEXT,expires INTEGER,revoked INTEGER DEFAULT 0)");
    }
    void execute(String sql,Object... values)throws SQLException{try(var p=db.prepareStatement(sql)){bind(p,values);p.execute();}}
    static void bind(PreparedStatement p,Object... values)throws SQLException{for(int i=0;i<values.length;i++)p.setObject(i+1,values[i]);}
    List<Map<String,Object>> rows(String sql,Object... values)throws SQLException{try(var p=db.prepareStatement(sql)){bind(p,values);try(var rs=p.executeQuery()){var result=new ArrayList<Map<String,Object>>();while(rs.next()){var row=new LinkedHashMap<String,Object>();for(int i=1;i<=rs.getMetaData().getColumnCount();i++)row.put(rs.getMetaData().getColumnLabel(i),rs.getObject(i));result.add(row);}return result;}}}
    static long number(Map<String,Object> row,String key){Object n=row.get(key);return n==null?0:((Number)n).longValue();}
    static String required(JsonObject o,String key){if(!o.has(key)||!o.get(key).isJsonPrimitive())throw new IllegalArgumentException("Missing "+key);return o.get(key).getAsString();}
    static long integer(JsonElement value){try{if(!value.isJsonPrimitive()||!value.getAsJsonPrimitive().isNumber())throw new IllegalArgumentException("Integer required");return value.getAsBigDecimal().longValueExact();}catch(ArithmeticException e){throw new IllegalArgumentException("Integer out of range",e);}}
    static boolean flag(JsonObject o,String key){return o.has(key)&&o.get(key).getAsBoolean();}
    static void id(String id){if(!id.matches("[a-f0-9]{32}_[0-9]{1,12}"))throw new IllegalArgumentException("Invalid match ID");}
    long occupied()throws Exception{return (number(rows("PRAGMA page_count").get(0),"page_count")-number(rows("PRAGMA freelist_count").get(0),"freelist_count"))*number(rows("PRAGMA page_size").get(0),"page_size");}
    private long archivesAndExports()throws Exception{long used=0;for(String name:List.of("archives","exports")){Path directory=root.resolve(name);if(Files.exists(directory))try(var files=Files.list(directory)){for(Path p:files.toList())if(Files.isRegularFile(p))used+=Files.size(p);}}return used;}
    public synchronized void ingest(JsonObject chunk,String hash)throws Exception{
        if(!rows("SELECT hash FROM chunks WHERE hash=?",hash).isEmpty())return;
        if(occupied()+archivesAndExports()>maximumBytes)throw new IOException("DATABASE_CAPACITY");
        String stream=required(chunk,"streamId");if(!stream.matches("[a-f0-9]{32}"))throw new IllegalArgumentException("Invalid stream");
        JsonArray records=chunk.getAsJsonArray("records");if(records.size()>256)throw new IllegalArgumentException("Record budget");
        db.setAutoCommit(false);
        try{
            for(JsonElement e:records){
                JsonObject r=e.getAsJsonObject();long seq=integer(r.get("sequence"));if(seq<1)throw new IllegalArgumentException("Invalid sequence");
                String mid=required(r,"matchId"),kind=required(r,"kind"),digest=Journal.hash(r.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));id(mid);
                if(!mid.startsWith(stream+"_"))throw new IllegalArgumentException("Stream/match mismatch");
                var prior=rows("SELECT hash FROM records WHERE stream=? AND seq=?",stream,seq);
                if(!prior.isEmpty()){if(!prior.get(0).get("hash").equals(digest))throw new IllegalArgumentException("SEQUENCE_CONFLICT");continue;}
                JsonObject data=r.getAsJsonObject("data");
                if(kind.equals("SESSION")){
                    if(!rows("SELECT id FROM matches WHERE id=?",mid).isEmpty())throw new IllegalArgumentException("DUPLICATE_SESSION");
                    JsonArray roster=data.getAsJsonArray("participants");if(roster==null||roster.size()<1||roster.size()>100)throw new IllegalArgumentException("Invalid roster");
                    execute("INSERT INTO matches(id,stream,session,state,partial,first_seq,last_seq) VALUES(?,?,?,'RECORDING',?,?,?)",mid,stream,data.toString(),flag(data,"partial")?1:0,seq,seq);
                    Set<String> ids=new HashSet<>();
                    for(JsonElement person:roster){JsonObject p=person.getAsJsonObject();String pid=required(p,"id");if(!pid.matches("p[0-9]{1,3}")||!ids.add(pid))throw new IllegalArgumentException("Invalid participant");execute("INSERT INTO participants(match_id,id,body) VALUES(?,?,?)",mid,pid,p.toString());}
                }else{
                    var found=rows("SELECT * FROM matches WHERE id=?",mid);if(found.isEmpty())throw new IllegalStateException("WAITING_SESSION");var match=found.get(0);
                    if(match.get("terminal")!=null)throw new IllegalArgumentException("RECORD_AFTER_TERMINAL");
                    boolean sequenceGap=seq!=number(match,"last_seq")+1;
                    if(sequenceGap)execute("UPDATE matches SET partial=1 WHERE id=?",mid);
                    if(!Set.of("FRAME","TERMINAL").contains(kind))throw new IllegalArgumentException("Unknown record");
                    long time=integer(data.get("simulationMs")),tick=integer(data.get("tick"));if(time<0||tick<0)throw new IllegalArgumentException("Invalid time");
                    if(time<number(match,"last_t")||tick<number(match,"last_tick"))throw new IllegalArgumentException("SIMULATION_TIME_REVERSED");
                    JsonArray players=data.getAsJsonArray("players");Set<String> observed=new HashSet<>();
                    for(JsonElement player:players){JsonObject p=player.getAsJsonObject();String pid=required(p,"id");
                        if(!observed.add(pid)||rows("SELECT id FROM participants WHERE match_id=? AND id=?",mid,pid).isEmpty())throw new IllegalArgumentException("Unknown/repeated participant");
                        JsonArray values=p.getAsJsonArray("values"),counters=p.getAsJsonArray("counters");if(values.size()!=4||counters.size()!=6)throw new IllegalArgumentException("Invalid statistic shape");
                        for(int i=0;i<4;i++){
                            long value=integer(values.get(i));String metric=Journal.METRICS.get(i);var previous=rows("SELECT t,v FROM points WHERE match_id=? AND person=? AND metric=? ORDER BY t DESC LIMIT 1",mid,pid,metric);
                            boolean closesGap=sequenceGap||data.has("gapFromMs")||!previous.isEmpty()&&!rows("SELECT person FROM gaps WHERE match_id=? AND person=? AND last_t>? AND first_t<=? LIMIT 1",mid,pid,number(previous.get(0),"t"),time).isEmpty();
                            if(previous.isEmpty()||number(previous.get(0),"v")!=value||kind.equals("TERMINAL")||closesGap)execute("INSERT OR REPLACE INTO points VALUES(?,?,?,?,?)",mid,pid,metric,time,value);
                        }
                        for(JsonElement v:counters)if(integer(v)<0)throw new IllegalArgumentException("Negative counter");
                        execute("INSERT OR REPLACE INTO counters VALUES(?,?,?,?)",mid,pid,time,counters.toString());
                        if(p.has("color"))execute("UPDATE participants SET color=? WHERE match_id=? AND id=?",p.get("color").getAsInt(),mid,pid);
                        if(flag(p,"ended"))execute("UPDATE participants SET endpoint=COALESCE(endpoint,?) WHERE match_id=? AND id=?",time,mid,pid);
                    }
                    long expected=rows("SELECT id FROM participants WHERE match_id=?",mid).size();
                    for(var participant:rows("SELECT id FROM participants WHERE match_id=?",mid)){
                        String pid=participant.get("id").toString();
                        if(sequenceGap)gap(mid,pid,Math.max(0,number(match,"last_t")),time,"SEQUENCE_GAP");
                        else if(!observed.contains(pid)){
                            long from=Math.max(0,number(match,"last_t"));
                            if(data.has("seriesCoverageGaps"))for(JsonElement element:data.getAsJsonArray("seriesCoverageGaps")){JsonObject g=element.getAsJsonObject();if(g.get("participantId").getAsString().equals(pid))from=g.get("fromGameMs").getAsLong();}
                            gap(mid,pid,from,time,"PLAYER_NOT_CAPTURED");
                        }
                        else if(data.has("gapFromMs"))execute("INSERT INTO gaps VALUES(?,?,?,?,?)",mid,pid,data.get("gapFromMs").getAsLong(),time,"CAPTURE_INCOMPLETE");
                    }
                    boolean partial=flag(data,"partial")||players.size()!=expected||number(match,"first_t")==-1&&time>100;
                    execute("UPDATE matches SET last_t=?,last_tick=?,last_seq=?,first_t=CASE WHEN first_t<0 THEN ? ELSE first_t END,partial=MAX(partial,?),revision=revision+1 WHERE id=?",time,tick,seq,time,partial?1:0,mid);
                    if(kind.equals("TERMINAL")){
                        String reason=required(data,"reason");boolean interrupted=Set.of("SERVER_SHUTDOWN","SHUTDOWN_SNAPSHOT_UNAVAILABLE","SIMULATION_RESET","SUPERSEDED_START").contains(reason);
                        execute("UPDATE matches SET terminal=?,state=?,partial=MAX(partial,?) WHERE id=?",data.toString(),interrupted?"INTERRUPTED":"STORED",interrupted?1:0,mid);
                    }
                }
                execute("INSERT INTO records VALUES(?,?,?,?)",stream,seq,mid,digest);
                execute("INSERT INTO events VALUES(?,?,?,?,?)",mid,seq,kind,data.has("simulationMs")?data.get("simulationMs").getAsLong():0,data.toString());
            }
            execute("INSERT INTO chunks VALUES(?)",hash);if(occupied()+archivesAndExports()>maximumBytes)throw new IOException("DATABASE_CAPACITY");db.commit();
        }catch(Exception e){db.rollback();throw e;}finally{db.setAutoCommit(true);}
    }
    public synchronized List<Map<String,Object>> list(int offset,int count)throws Exception{
        return rows("SELECT id,state,partial,last_t durationMs,revision,pinned,archive_sha FROM matches ORDER BY rowid DESC LIMIT ? OFFSET ?",Math.min(100,Math.max(1,count)),Math.max(0,offset));
    }
    private void gap(String mid,String pid,long from,long to,String reason)throws Exception{
        var overlapping=rows("SELECT rowid,first_t FROM gaps WHERE match_id=? AND person=? AND reason=? AND last_t>=? AND first_t<=? LIMIT 1",mid,pid,reason,from,to);
        if(overlapping.isEmpty())execute("INSERT INTO gaps VALUES(?,?,?,?,?)",mid,pid,from,to,reason);
        else execute("UPDATE gaps SET first_t=MIN(first_t,?),last_t=MAX(last_t,?) WHERE rowid=?",from,to,overlapping.get(0).get("rowid"));
    }
    Map<String,Object> match(String mid)throws Exception{id(mid);var values=rows("SELECT * FROM matches WHERE id=?",mid);if(values.isEmpty())throw new NoSuchElementException("Match not found");return values.get(0);}
    public synchronized JsonObject report(String mid)throws Exception{
        var m=match(mid);if(m.get("state").equals("RECORDING"))throw new IllegalStateException("REPORT_NOT_SETTLED");
        JsonObject out=JsonParser.parseString(m.get("session").toString()).getAsJsonObject();out.addProperty("matchId",mid);out.addProperty("state",m.get("state").toString());
        out.addProperty("quality",m.get("state").equals("PURGED")?"PURGED":m.get("state").equals("INTERRUPTED")?"INTERRUPTED":number(m,"partial")!=0?"PARTIAL":"COMPLETE");out.addProperty("durationMs",number(m,"last_t"));out.addProperty("revision",number(m,"revision"));out.addProperty("pinned",number(m,"pinned")!=0);out.addProperty("archived",m.get("archive_sha")!=null);out.addProperty("clientIdentityVerified",false);
        if(m.get("terminal")!=null){JsonObject terminal=JsonParser.parseString(m.get("terminal").toString()).getAsJsonObject();terminal.remove("participants");out.add("terminal",terminal);}
        JsonArray participants=new JsonArray();
        for(var p:rows("SELECT * FROM participants WHERE match_id=? ORDER BY CAST(substr(id,2) AS INTEGER)",mid)){
            JsonObject person=JsonParser.parseString(p.get("body").toString()).getAsJsonObject();person.remove("connectionHash");if(p.get("endpoint")!=null)person.addProperty("personalEndMs",number(p,"endpoint"));if(p.get("color")!=null)person.addProperty("color",number(p,"color"));
            var cs=rows("SELECT body FROM counters WHERE match_id=? AND person=? ORDER BY t DESC LIMIT 1",mid,p.get("id"));person.add("counters",cs.isEmpty()?new JsonArray():JsonParser.parseString(cs.get(0).get("body").toString()));participants.add(person);
        }out.add("participants",participants);out.add("coverageGaps",Journal.JSON.toJsonTree(rows("SELECT person participantId,first_t fromGameMs,last_t toGameMs,reason FROM gaps WHERE match_id=? ORDER BY first_t",mid)));return out;
    }
    public synchronized void interrupted(String stream)throws Exception{
        execute("UPDATE matches SET state='INTERRUPTED',partial=1,revision=revision+1 WHERE stream=? AND state='RECORDING'",stream);
    }
    static void metric(String metric){if(!Journal.METRICS.contains(metric))throw new IllegalArgumentException("Unknown metric");}
    public synchronized JsonObject raw(String mid,String pid,String metric,long after,int count)throws Exception{
        JsonObject r=report(mid);metric(metric);if(r.get("archived").getAsBoolean())throw new IllegalStateException("ARCHIVED_RESTORE_REQUIRED");
        if(!pid.matches("p[0-9]{1,3}"))throw new IllegalArgumentException("Unknown participant");
        int limit=Math.min(1500,Math.max(1,count));var points=rows("SELECT t,v FROM points WHERE match_id=? AND person=? AND metric=? AND t>? ORDER BY t LIMIT ?",mid,pid,metric,after,limit+1);boolean more=points.size()>limit;if(more)points.remove(points.size()-1);
        JsonObject out=new JsonObject();out.addProperty("revision",r.get("revision").getAsLong());out.add("points",Journal.JSON.toJsonTree(points));out.add("nextAfterGameMs",more?Journal.JSON.toJsonTree(points.get(points.size()-1).get("t")):JsonNull.INSTANCE);return out;
    }
    public synchronized JsonObject cursor(String mid,String metric,long time,String group,String perspective)throws Exception{
        JsonObject manifest=report(mid);metric(metric);if(manifest.get("archived").getAsBoolean())throw new IllegalStateException("ARCHIVED_RESTORE_REQUIRED");
        if(!Set.of("player","team").contains(group))throw new IllegalArgumentException("Invalid group");
        long end=manifest.get("durationMs").getAsLong();if(!perspective.isBlank()){
            var owner=rows("SELECT endpoint FROM participants WHERE match_id=? AND id=?",mid,perspective);if(owner.isEmpty())throw new IllegalArgumentException("Unknown perspective");if(owner.get(0).get("endpoint")!=null)end=Math.min(end,number(owner.get(0),"endpoint"));
        }if(time<0||time>end)throw new IllegalArgumentException("Cursor outside range");
        Map<String,JsonObject> groups=new LinkedHashMap<>();
        for(JsonElement e:manifest.getAsJsonArray("participants")){
            JsonObject p=e.getAsJsonObject();String pid=p.get("id").getAsString(),id=group.equals("team")?"team-"+p.get("team").getAsInt():pid;
            var value=rows("SELECT t,v FROM points WHERE match_id=? AND person=? AND metric=? AND t<=? ORDER BY t DESC LIMIT 1",mid,pid,metric,time);
            JsonObject g=groups.computeIfAbsent(id,k->{JsonObject n=new JsonObject();n.addProperty("id",k);n.addProperty("name",group.equals("team")?"队伍 "+(p.get("team").getAsInt()+1):p.get("name").getAsString());n.addProperty("value",0L);n.addProperty("quality","EXACT_AT_RECORDED_TIME");return n;});
            // With any missing interval the first release refuses previous-value inference, rather than inventing continuity.
            boolean known=!value.isEmpty();
            if(known&&number(value.get(0),"t")!=time)known=rows("SELECT person FROM gaps WHERE match_id=? AND person=? AND first_t<? AND last_t>? LIMIT 1",mid,pid,time,number(value.get(0),"t")).isEmpty();
            if(!known){g.add("value",JsonNull.INSTANCE);g.addProperty("quality","UNKNOWN");}
            else if(!g.get("value").isJsonNull()){
                g.addProperty("value",g.get("value").getAsLong()+number(value.get(0),"v"));
                if(number(value.get(0),"t")!=time)g.addProperty("quality","PREVIOUS_RECORDED_VALUE");
            }
        }
        JsonObject out=new JsonObject();out.addProperty("gameMs",time);out.addProperty("revision",manifest.get("revision").getAsLong());out.add("series",Journal.JSON.toJsonTree(groups.values()));return out;
    }
    public synchronized JsonObject comparison(String mid,String metric,String group,int max,String perspective)throws Exception{
        JsonObject manifest=report(mid);metric(metric);if(manifest.get("archived").getAsBoolean())throw new IllegalStateException("ARCHIVED_RESTORE_REQUIRED");
        int budget=Math.min(1500,Math.max(10,max));long end=manifest.get("durationMs").getAsLong();
        if(!perspective.isBlank()){var p=rows("SELECT endpoint FROM participants WHERE match_id=? AND id=?",mid,perspective);if(p.isEmpty())throw new IllegalArgumentException("Unknown perspective");if(p.get(0).get("endpoint")!=null)end=Math.min(end,number(p.get(0),"endpoint"));}
        if(!perspective.isBlank())for(JsonElement e:manifest.getAsJsonArray("participants")){
            JsonObject person=e.getAsJsonObject();var counter=rows("SELECT body FROM counters WHERE match_id=? AND person=? AND t<=? ORDER BY t DESC LIMIT 1",mid,person.get("id").getAsString(),end);person.add("counters",counter.isEmpty()?new JsonArray():JsonParser.parseString(counter.get(0).get("body").toString()));
        }
        var roster=rows("SELECT id,body FROM participants WHERE match_id=? ORDER BY CAST(substr(id,2) AS INTEGER)",mid);
        Map<String,Integer> index=new HashMap<>();List<String> groupIds=new ArrayList<>(),groupNames=new ArrayList<>();List<List<Integer>> members=new ArrayList<>();
        for(int i=0;i<roster.size();i++){
            String pid=roster.get(i).get("id").toString();index.put(pid,i);JsonObject person=JsonParser.parseString(roster.get(i).get("body").toString()).getAsJsonObject();
            String gid=group.equals("team")?"team-"+person.get("team").getAsInt():pid;
            int at=groupIds.indexOf(gid);if(at<0){at=groupIds.size();groupIds.add(gid);groupNames.add(group.equals("team")?"队伍 "+(person.get("team").getAsInt()+1):person.get("name").getAsString());members.add(new ArrayList<>());}members.get(at).add(i);
        }
        if(!Set.of("player","team").contains(group))throw new IllegalArgumentException("Invalid group");
        List<Map<String,Object>> gaps=rows("SELECT person,first_t,last_t FROM gaps WHERE match_id=?",mid);
        Long[] values=new Long[roster.size()];long[] observed=new long[roster.size()];Arrays.fill(observed,-1);
        // Keep absolute and share extrema on the same grid, before drawing reduction.
        JointReducer reducer=new JointReducer(end,budget,members.size()*2);long time=-1;
        try(var query=db.prepareStatement("SELECT t,person,v FROM points WHERE match_id=? AND metric=? AND t<=? ORDER BY t,person")){
            bind(query,mid,metric,end);try(var rs=query.executeQuery()){while(rs.next()){
                long next=rs.getLong(1);if(time>=0&&next!=time)joint(reducer,time,values,observed,members,roster,gaps);
                time=next;int slot=index.get(rs.getString(2));values[slot]=rs.getLong(3);observed[slot]=time;
            }}
        }
        if(time>=0)joint(reducer,time,values,observed,members,roster,gaps);
        JsonArray grid=new JsonArray();List<JointReducer.Sample> chosen=reducer.finish();
        for(var sample:chosen){JsonObject point=new JsonObject();point.addProperty("gameMs",sample.time());JsonArray series=new JsonArray();
            for(int i=0;i<members.size();i++){JsonObject line=new JsonObject();line.addProperty("id",groupIds.get(i));line.addProperty("name",groupNames.get(i));line.add("value",sample.values()[i]==null?JsonNull.INSTANCE:new JsonPrimitive(sample.values()[i]));line.addProperty("quality",sample.quality()[i]);series.add(line);}point.add("series",series);grid.add(point);
        }
        JsonObject out=new JsonObject();out.add("manifest",manifest);out.addProperty("metric",metric);out.addProperty("group",group);out.addProperty("rangeEndMs",end);out.addProperty("reduced",reducer.input>chosen.size());out.addProperty("inputTimes",reducer.input);out.addProperty("downsampling","JOINT_FIRST_LAST_PER_SERIES_EXTREMA; RAW_CURSOR_UNREDUCED");out.add("grid",grid);return out;
    }
    private static void joint(JointReducer reducer,long time,Long[] values,long[] observed,List<List<Integer>> groups,List<Map<String,Object>> roster,List<Map<String,Object>> gaps){
        Long[] sums=new Long[groups.size()*2];String[] quality=new String[groups.size()*2];
        for(int i=0;i<groups.size();i++){long sum=0;boolean known=true,exact=true;
            for(int member:groups.get(i)){
                if(values[member]==null){known=false;break;}String id=roster.get(member).get("id").toString();
                if(observed[member]!=time&&gaps.stream().anyMatch(g->g.get("person").equals(id)&&number(g,"first_t")<time&&number(g,"last_t")>observed[member])){known=false;break;}
                sum+=values[member];exact&=observed[member]==time;
            }sums[i]=known?sum:null;quality[i]=!known?"UNKNOWN":exact?"EXACT_AT_RECORDED_TIME":"PREVIOUS_RECORDED_VALUE";
        }
        boolean known=true;double total=0;for(int i=0;i<groups.size();i++){known&=sums[i]!=null;if(sums[i]!=null)total+=Math.max(0,sums[i]);}
        for(int i=0;i<groups.size();i++){sums[i+groups.size()]=!known?null:total==0?0L:Math.round(Math.max(0,sums[i])*1000000.0/total);quality[i+groups.size()]=quality[i];}
        reducer.add(time,sums,quality);
    }

    public synchronized void pin(String mid,boolean value)throws Exception{match(mid);execute("UPDATE matches SET pinned=?,revision=revision+1 WHERE id=?",value?1:0,mid);execute("INSERT INTO audit VALUES(?,?,?)",System.currentTimeMillis(),value?"PIN":"UNPIN",mid);}
    public synchronized Map<String,Object> share(String mid,String connectionHash,int hours)throws Exception{
        report(mid);
        if(connectionHash!=null){boolean participant=false;for(var p:rows("SELECT body FROM participants WHERE match_id=?",mid)){
            JsonObject person=JsonParser.parseString(p.get("body").toString()).getAsJsonObject();if(person.has("connectionHash")&&person.get("connectionHash").getAsString().equals(connectionHash))participant=true;
        }if(!participant)throw new SecurityException("不是该局参赛者");}
        execute("DELETE FROM shares WHERE expires<?",System.currentTimeMillis());if(number(rows("SELECT count(*) n FROM shares").get(0),"n")>=10000)throw new IllegalStateException("SHARE_BUDGET");
        byte[] random=new byte[32];new java.security.SecureRandom().nextBytes(random);String token=HexFormat.of().formatHex(random);long expires=System.currentTimeMillis()+Math.max(1,Math.min(168,hours))*3600000L;
        execute("INSERT INTO shares VALUES(?,?,?,0)",Journal.hash(token.getBytes(java.nio.charset.StandardCharsets.UTF_8)),mid,expires);return Map.of("token",token,"expiresAt",expires,"scope","FULL_POSTGAME_REPORT_READ_ONLY");
    }
    public synchronized String sharedMatch(String token)throws Exception{
        if(!token.matches("[a-f0-9]{64}"))throw new NoSuchElementException();var rows=rows("SELECT match_id FROM shares WHERE hash=? AND expires>? AND revoked=0",Journal.hash(token.getBytes(java.nio.charset.StandardCharsets.UTF_8)),System.currentTimeMillis());if(rows.isEmpty())throw new NoSuchElementException();String mid=rows.get(0).get("match_id").toString();report(mid);return mid;
    }
    public synchronized Map<String,Object> latestShare(String connectionHash)throws Exception{
        if(!connectionHash.matches("[a-f0-9]{64}"))throw new SecurityException("参赛身份无效");
        var found=rows("SELECT m.id FROM matches m JOIN participants p ON p.match_id=m.id WHERE m.state IN ('STORED','INTERRUPTED') AND json_extract(p.body,'$.connectionHash')=? ORDER BY m.rowid DESC LIMIT 1",connectionHash);
        if(found.isEmpty())throw new NoSuchElementException("暂无已结算报告");return share(found.get(0).get("id").toString(),connectionHash,24);
    }
    public synchronized void revokeShares(String mid)throws Exception{match(mid);execute("UPDATE shares SET revoked=1 WHERE match_id=?",mid);}
    public synchronized void export(String mid,String format,OutputStream out)throws Exception{
        if(format.equals("html")){html(mid,out);return;}
        JsonObject manifest=report(mid);boolean gz=format.equals("jsonl.gz");OutputStream target=gz?new GZIPOutputStream(out):out;
        try(BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(target,java.nio.charset.StandardCharsets.UTF_8))){
            if(format.equals("csv"))writer.write("participant,metric,simulationMs,value\n");else{writer.write(Journal.JSON.toJson(Map.of("kind","manifest","data",manifest)));writer.newLine();}
            if(manifest.get("archived").getAsBoolean()){
                if(format.equals("csv"))throw new IllegalStateException("RESTORE_REQUIRED_FOR_CSV");
                Path path=root.resolve("archives").resolve(mid+".jsonl.gz");verifyArchive(mid,path);
                try(var reader=new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path)),java.nio.charset.StandardCharsets.UTF_8))){String line;while((line=reader.readLine())!=null){writer.write(line);writer.newLine();}}
            }else try(var query=db.prepareStatement("SELECT person,metric,t,v FROM points WHERE match_id=? ORDER BY person,metric,t")){bind(query,mid);try(var rs=query.executeQuery()){while(rs.next()){
                if(format.equals("csv"))writer.write(rs.getString(1)+","+rs.getString(2)+","+rs.getLong(3)+","+rs.getLong(4));
                else writer.write(Journal.JSON.toJson(List.of(rs.getString(1),rs.getString(2),rs.getLong(3),rs.getLong(4))));writer.newLine();
            }}}
        }
    }
    private static String escape(String value){return value.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace("\"","&quot;");}
    private void html(String mid,OutputStream output)throws Exception{
        JsonObject report=report(mid);String[] colors={"#087bb5","#ad6500","#078260","#a34485","#796300","#c43d00"};
        try(var w=new BufferedWriter(new OutputStreamWriter(output,java.nio.charset.StandardCharsets.UTF_8))){
            w.write("<!doctype html><html lang=\"zh-CN\"><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>CNKD 赛后报告</title><style>body{font:15px/1.6 system-ui;margin:24px auto;padding:0 18px;max-width:1100px;color:#182839;background:#f4f7fa}section{background:white;padding:18px;margin:20px 0;border:1px solid #cad7e2;border-radius:12px;overflow:auto}table{border-collapse:collapse;white-space:nowrap}td,th{padding:8px;border-bottom:1px solid #d5dfe8}svg{min-width:650px;width:100%;height:280px}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style><h1>CNKD 赛后报告</h1><p>"+escape(report.get("mapName").getAsString())+" · "+escape(mid)+"</p><p>质量："+report.get("quality").getAsString()+"；模拟时长："+report.get("durationMs").getAsLong()+" 毫秒。客户端逐点一致性尚未认证。</p><section><h2>总览</h2><table><tr><th>玩家</th><th>队伍</th>");
            for(String c:Journal.COUNTERS)w.write("<th>"+c+"</th>");w.write("</tr>");
            for(JsonElement e:report.getAsJsonArray("participants")){JsonObject p=e.getAsJsonObject();w.write("<tr><td>"+escape(p.get("name").getAsString())+"</td><td>"+p.get("team").getAsInt()+"</td>");for(JsonElement c:p.getAsJsonArray("counters"))w.write("<td>"+c.getAsLong()+"</td>");w.write("</tr>");}w.write("</table></section>");
            for(String metric:Journal.METRICS){w.write("<section><h2>"+metric+"</h2>");if(report.get("archived").getAsBoolean()){w.write("<p>原始曲线已归档；恢复后重新导出可显示图表。</p></section>");continue;}
                JsonObject graph=comparison(mid,metric,"player",500,"");JsonArray grid=graph.getAsJsonArray("grid");long end=Math.max(1,graph.get("rangeEndMs").getAsLong());double maximum=1;
                for(JsonElement e:grid)for(JsonElement s:e.getAsJsonObject().getAsJsonArray("series"))if(!s.getAsJsonObject().get("value").isJsonNull())maximum=Math.max(maximum,s.getAsJsonObject().get("value").getAsDouble());
                w.write("<p>"+(graph.get("reduced").getAsBoolean()?"联合时间网格精简预览，原始导出与游标保持原始精度。":"原始时间网格。")+" 未知区间断开。</p><svg viewBox=\"0 0 900 280\" role=\"img\" aria-label=\""+metric+"阶梯图\"><path d=\"M55 15V245H880\" stroke=\"#7b8c9f\" fill=\"none\"/>");
                JsonArray roster=report.getAsJsonArray("participants");
                for(int i=0;i<roster.size();i++){String pid=roster.get(i).getAsJsonObject().get("id").getAsString();StringBuilder path=new StringBuilder();boolean connected=false;
                    for(JsonElement point:grid){JsonObject g=point.getAsJsonObject();JsonObject value=null;for(JsonElement e:g.getAsJsonArray("series"))if(e.getAsJsonObject().get("id").getAsString().equals(pid))value=e.getAsJsonObject();if(value==null||value.get("value").isJsonNull()){connected=false;continue;}
                        double x=55+g.get("gameMs").getAsLong()*825.0/end,y=245-value.get("value").getAsDouble()*220/maximum;path.append(connected?"H":"M").append(x).append(connected?"V":",").append(y);connected=true;}
                    w.write("<path d=\""+path+"\" stroke=\""+colors[i%colors.length]+"\" fill=\"none\" stroke-width=\"2\"/>");
                }w.write("</svg><p>");for(int i=0;i<roster.size();i++)w.write("<span style=\"color:"+colors[i%colors.length]+"\">"+escape(roster.get(i).getAsJsonObject().get("name").getAsString())+"</span>　");w.write("</p></section>");
            }w.write("<section><h2>来源与质量</h2><pre>"+escape(Journal.JSON.toJson(report))+"</pre></section></html>");
        }
    }
    private long verifyArchive(String mid,Path path)throws Exception{
        var m=match(mid);String expected=(String)m.get("archive_sha");if(expected==null)throw new IllegalStateException("No archive");
        try(var input=Files.newInputStream(path)){var d=java.security.MessageDigest.getInstance("SHA-256");byte[] b=new byte[65536];int n;while((n=input.read(b))!=-1)d.update(b,0,n);if(!HexFormat.of().formatHex(d.digest()).equals(expected))throw new IOException("ARCHIVE_HASH_MISMATCH");}
        long count=0;try(var reader=new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path)),java.nio.charset.StandardCharsets.UTF_8))){String line;while((line=reader.readLine())!=null){if(line.length()>2048)throw new IOException("ARCHIVE_LINE_LIMIT");JsonArray row=JsonParser.parseString(line).getAsJsonArray();if(row.size()!=4||!Journal.METRICS.contains(row.get(1).getAsString())||row.get(2).getAsLong()<0)throw new IOException("ARCHIVE_INVALID");count++;}}
        if(count!=number(m,"archive_points"))throw new IOException("ARCHIVE_COUNT_MISMATCH");return count;
    }
    public synchronized void archive(String mid)throws Exception{
        var m=match(mid);report(mid);if(number(m,"pinned")!=0)throw new IllegalStateException("PINNED_MATCH");if(m.get("archive_sha")!=null)return;
        Path target=root.resolve("archives").resolve(mid+".jsonl.gz"),temp=target.resolveSibling(".archive-"+UUID.randomUUID());long count=0;
        try{
            try(var writer=new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(temp,StandardOpenOption.CREATE_NEW)),java.nio.charset.StandardCharsets.UTF_8));var q=db.prepareStatement("SELECT person,metric,t,v FROM points WHERE match_id=? ORDER BY person,metric,t")){
                bind(q,mid);try(var rs=q.executeQuery()){while(rs.next()){writer.write(Journal.JSON.toJson(List.of(rs.getString(1),rs.getString(2),rs.getLong(3),rs.getLong(4))));writer.newLine();count++;}}
            }
            if(Files.size(temp)+occupied()>maximumBytes)throw new IOException("ARCHIVE_CAPACITY");
            String digest;try(var in=Files.newInputStream(temp)){var d=java.security.MessageDigest.getInstance("SHA-256");byte[] b=new byte[65536];int n;while((n=in.read(b))!=-1)d.update(b,0,n);digest=HexFormat.of().formatHex(d.digest());}
            try(var ch=java.nio.channels.FileChannel.open(temp,StandardOpenOption.WRITE)){ch.force(true);}
            if(Files.exists(target)){
                try(var in=Files.newInputStream(target)){var d=java.security.MessageDigest.getInstance("SHA-256");byte[] b=new byte[65536];int n;while((n=in.read(b))!=-1)d.update(b,0,n);if(!HexFormat.of().formatHex(d.digest()).equals(digest))throw new IOException("EXISTING_ARCHIVE_CONFLICT");}Files.delete(temp);
            }else Files.move(temp,target,StandardCopyOption.ATOMIC_MOVE);
            db.setAutoCommit(false);
            try{execute("UPDATE matches SET archive_sha=?,archive_points=? WHERE id=?",digest,count,mid);verifyArchive(mid,target);execute("DELETE FROM points WHERE match_id=?",mid);execute("UPDATE matches SET revision=revision+1 WHERE id=?",mid);db.commit();}catch(Exception e){db.rollback();throw e;}finally{db.setAutoCommit(true);}
        }finally{Files.deleteIfExists(temp);}
    }
    public synchronized void restore(String mid)throws Exception{
        var m=match(mid);if(m.get("archive_sha")==null)return;Path file=root.resolve("archives").resolve(mid+".jsonl.gz");long count=verifyArchive(mid,file);
        if(occupied()+count*160>maximumBytes)throw new IOException("RESTORE_CAPACITY");
        db.setAutoCommit(false);
        try(var reader=new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(file)),java.nio.charset.StandardCharsets.UTF_8));var p=db.prepareStatement("INSERT OR IGNORE INTO points VALUES(?,?,?,?,?)")){
            String line;int queued=0;while((line=reader.readLine())!=null){JsonArray r=JsonParser.parseString(line).getAsJsonArray();bind(p,mid,r.get(0).getAsString(),r.get(1).getAsString(),r.get(2).getAsLong(),r.get(3).getAsLong());p.addBatch();if(++queued%512==0)p.executeBatch();}p.executeBatch();
            execute("UPDATE matches SET archive_sha=NULL,revision=revision+1 WHERE id=?",mid);db.commit();
        }catch(Exception e){db.rollback();throw e;}finally{db.setAutoCommit(true);}
        // Preserve the verified archive as a recoverable artifact; re-archiving explicitly verifies/reuses it.
    }
    public synchronized void backup(Path target)throws Exception{
        if(Files.exists(target))throw new IllegalArgumentException("Backup exists");Files.createDirectories(target);
        try(var p=db.createStatement()){p.execute("VACUUM INTO '"+target.resolve("analytics.db").toString().replace("'","''")+"'");}
        try(var files=Files.list(root.resolve("archives"))){for(Path file:files.toList())if(Files.isRegularFile(file))Files.copy(file,target.resolve(file.getFileName()));}
        try(var verify=DriverManager.getConnection("jdbc:sqlite:"+target.resolve("analytics.db"));var s=verify.createStatement();var rs=s.executeQuery("PRAGMA integrity_check")){if(!rs.next()||!rs.getString(1).equals("ok"))throw new IOException("Backup integrity failure");}
    }
    @Override public synchronized void close()throws Exception{db.close();}
}
