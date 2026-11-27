package net.cnkd.analytics;
import com.google.gson.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class AnalysisStoreTest {
    @TempDir Path temp;
    final String stream="a".repeat(32),mid=stream+"_1";
    JsonObject record(long seq,String kind,JsonObject data){JsonObject r=new JsonObject();r.addProperty("sequence",seq);r.addProperty("matchId",mid);r.addProperty("kind",kind);r.add("data",data);return r;}
    JsonObject session(){return JsonParser.parseString("{\"schemaVersion\":1,\"mapName\":\"测试地图\",\"participants\":[{\"id\":\"p0\",\"name\":\"同名\",\"slot\":0,\"team\":0,\"role\":\"HUMAN\"},{\"id\":\"p1\",\"name\":\"同名\",\"slot\":1,\"team\":1,\"role\":\"HUMAN\"}]}").getAsJsonObject();}
    JsonObject frame(long t,int value,boolean terminal){JsonObject d=new JsonObject();d.addProperty("simulationMs",t);d.addProperty("tick",t/16);d.addProperty("partial",false);JsonArray ps=new JsonArray();
        for(int slot=0;slot<2;slot++){JsonObject p=new JsonObject();p.addProperty("id","p"+slot);p.addProperty("slot",slot);p.addProperty("ended",slot==0&&t>=1000);p.add("values",Journal.JSON.toJsonTree(List.of(18,value,3000,value+3000)));p.add("counters",Journal.JSON.toJsonTree(List.of(slot==0?3:0,0,0,slot==1?3:0,0,0)));ps.add(p);}d.add("players",ps);
        if(terminal){d.addProperty("reason","verdict");d.addProperty("endedAt",System.currentTimeMillis());d.addProperty("outcomeConfirmed",true);}return d;}
    JsonObject chunk(JsonObject... records){JsonObject c=new JsonObject();c.addProperty("schemaVersion",1);c.addProperty("streamId",stream);c.add("records",Journal.JSON.toJsonTree(Arrays.asList(records)));return c;}
    void seed(AnalysisStore store)throws Exception{store.ingest(chunk(record(1,"SESSION",session()),record(2,"FRAME",frame(0,500,false)),record(3,"FRAME",frame(1000,900,false)),record(4,"TERMINAL",frame(2000,900,true))),"first");}
    @Test void postgameQueriesPreserveIdentityAndNativeValues()throws Exception{
        try(var store=new AnalysisStore(temp,128*1024*1024)){seed(store);JsonObject report=store.report(mid);assertEquals("COMPLETE",report.get("quality").getAsString());assertEquals(2,report.getAsJsonArray("participants").size());
            var cursor=store.cursor(mid,"armyValue",1500,"player","");assertEquals(900,cursor.getAsJsonArray("series").get(0).getAsJsonObject().get("value").getAsLong());
            assertEquals("PREVIOUS_RECORDED_VALUE",cursor.getAsJsonArray("series").get(0).getAsJsonObject().get("quality").getAsString());
            assertEquals(1800,store.cursor(mid,"armyValue",1500,"team","").getAsJsonArray("series").get(0).getAsJsonObject().get("value").getAsLong()+store.cursor(mid,"armyValue",1500,"team","").getAsJsonArray("series").get(1).getAsJsonObject().get("value").getAsLong());
            assertThrows(IllegalArgumentException.class,()->store.cursor(mid,"income",1500,"player","p0"));
            JsonObject first=store.raw(mid,"p0","armyValue",-1,1);assertEquals(1,first.getAsJsonArray("points").size());assertEquals(0,first.get("nextAfterGameMs").getAsLong());
            assertEquals(2,store.raw(mid,"p0","armyValue",0,100).getAsJsonArray("points").size());
        }
    }
    @Test void journalChecksumsAndAtomicImportRejectConflicts()throws Exception{
        JsonObject payload=chunk(record(1,"SESSION",session()));Path block=Journal.commit(temp.resolve("spool"),payload);assertEquals(payload,Journal.decode(block));
        JsonObject envelope=JsonParser.parseString(Files.readString(block)).getAsJsonObject();envelope.addProperty("payload","{}");Files.writeString(block,envelope.toString());assertThrows(IllegalArgumentException.class,()->Journal.decode(block));
        try(var store=new AnalysisStore(temp.resolve("db"),128*1024*1024)){seed(store);seed(store);assertEquals(1,store.list(0,100).size());JsonObject conflicting=record(1,"SESSION",session());conflicting.getAsJsonObject("data").addProperty("mapName","changed");assertThrows(IllegalArgumentException.class,()->store.ingest(chunk(conflicting),"conflict"));assertEquals("测试地图",store.report(mid).get("mapName").getAsString());}
    }
    @Test void incompleteAndCrashedSessionsNeverPretendComplete()throws Exception{
        try(var store=new AnalysisStore(temp,128*1024*1024)){store.ingest(chunk(record(1,"SESSION",session()),record(2,"FRAME",frame(0,500,false))),"a");assertThrows(IllegalStateException.class,()->store.report(mid));store.interrupted(stream);assertEquals("INTERRUPTED",store.report(mid).get("quality").getAsString());
            store.ingest(chunk(record(5,"TERMINAL",frame(2000,900,true))),"late");assertEquals("PARTIAL",store.report(mid).get("quality").getAsString());assertTrue(store.cursor(mid,"armyValue",1500,"player","").getAsJsonArray("series").get(0).getAsJsonObject().get("value").isJsonNull());}
    }
    @Test void missingOnePlayerDoesNotExpandGapAcrossUnchangedHealthyFrames()throws Exception{
        try(var store=new AnalysisStore(temp,128*1024*1024)){
            JsonObject missing=frame(1000,500,false);missing.getAsJsonArray("players").remove(1);
            missing.add("seriesCoverageGaps",JsonParser.parseString("[{\"participantId\":\"p1\",\"fromGameMs\":984,\"toGameMs\":1000,\"reason\":\"PLAYER_NOT_CAPTURED\"}]"));
            store.ingest(chunk(record(1,"SESSION",session()),record(2,"FRAME",frame(0,500,false)),record(3,"FRAME",missing),record(4,"FRAME",frame(1016,500,false)),record(5,"TERMINAL",frame(2000,500,true))),"gaps");
            assertEquals(984,((Number)store.rows("SELECT first_t FROM gaps WHERE match_id=?",mid).get(0).get("first_t")).longValue());
            assertFalse(store.cursor(mid,"armyValue",500,"player","").getAsJsonArray("series").get(1).getAsJsonObject().get("value").isJsonNull());
            assertTrue(store.cursor(mid,"armyValue",992,"player","").getAsJsonArray("series").get(1).getAsJsonObject().get("value").isJsonNull());
            assertEquals(500,store.cursor(mid,"armyValue",1100,"player","").getAsJsonArray("series").get(1).getAsJsonObject().get("value").getAsLong());
        }
    }
    @Test void archiveRestoreCorruptionAndBackupAreVerified()throws Exception{
        try(var store=new AnalysisStore(temp.resolve("db"),128*1024*1024)){seed(store);store.pin(mid,true);assertThrows(IllegalStateException.class,()->store.archive(mid));store.pin(mid,false);store.archive(mid);assertTrue(store.report(mid).get("archived").getAsBoolean());
            assertThrows(IllegalStateException.class,()->store.raw(mid,"p0","income",-1,20));store.restore(mid);assertEquals(2,store.raw(mid,"p0","income",-1,20).getAsJsonArray("points").size());store.backup(temp.resolve("backup"));assertTrue(Files.size(temp.resolve("backup/analytics.db"))>0);
            store.execute("UPDATE matches SET archive_sha='bad' WHERE id=?",mid);assertThrows(Exception.class,()->store.restore(mid));assertTrue(Files.size(temp.resolve("db/archives/"+mid+".jsonl.gz"))>0);}
    }
    @Test void participantLinksAreSettledScopedExpiringRevocableAndHideConnectionHashes()throws Exception{
        try(var store=new AnalysisStore(temp,128*1024*1024)){
            String identity="c".repeat(64);JsonObject session=session();session.getAsJsonArray("participants").get(0).getAsJsonObject().addProperty("connectionHash",identity);
            store.ingest(chunk(record(1,"SESSION",session),record(2,"FRAME",frame(0,500,false))),"start");
            assertThrows(IllegalStateException.class,()->store.share(mid,identity,24));
            store.ingest(chunk(record(3,"TERMINAL",frame(2000,500,true))),"end");
            assertThrows(SecurityException.class,()->store.share(mid,"d".repeat(64),24));
            var grant=store.latestShare(identity);String token=grant.get("token").toString();assertEquals(mid,store.sharedMatch(token));assertFalse(store.report(mid).toString().contains(identity));assertFalse(store.report(mid).toString().contains("connectionHash"));
            assertNotEquals(token,store.rows("SELECT hash FROM shares").get(0).get("hash"));store.revokeShares(mid);assertThrows(java.util.NoSuchElementException.class,()->store.sharedMatch(token));
            String second=store.share(mid,identity,1).get("token").toString();store.execute("UPDATE shares SET expires=0");assertThrows(java.util.NoSuchElementException.class,()->store.sharedMatch(second));
        }
    }
}
