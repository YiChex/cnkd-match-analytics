package net.cnkd.analytics;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import com.google.gson.*;
import java.nio.file.*;
import java.net.URI;
import java.net.http.*;
import static org.junit.jupiter.api.Assertions.*;

class ReportServiceTest {
    @TempDir Path temp;
    @Test void authenticatedHttpOnlyPublishesSettledCommittedReports()throws Exception{
        Path spool=temp.resolve("journal"),root=temp.resolve("service");Files.createDirectories(spool);
        AnalysisStoreTest f=new AnalysisStoreTest();
        Journal.commit(spool,f.chunk(f.record(1,"SESSION",f.session()),f.record(2,"FRAME",f.frame(0,500,false))));
        Files.writeString(spool.resolve("chunk-invalid.json"),"{broken");
        try(var service=new ReportService(root,spool,0,128*1024*1024,90,365)){
            service.start();HttpClient client=HttpClient.newHttpClient();String base="http://127.0.0.1:"+service.port();
            assertEquals(401,client.send(HttpRequest.newBuilder(URI.create(base+"/api/matches")).build(),HttpResponse.BodyHandlers.ofString()).statusCode());
            String key=Files.readString(root.resolve("admin-token.txt"));var login=client.send(HttpRequest.newBuilder(URI.create(base+"/api/session")).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString("{\"token\":\""+key+"\"}")).build(),HttpResponse.BodyHandlers.ofString());assertEquals(200,login.statusCode());
            String cookie=login.headers().firstValue("set-cookie").orElseThrow().split(";",2)[0];
            Thread.sleep(1200);
            var active=client.send(HttpRequest.newBuilder(URI.create(base+"/api/matches/"+f.mid)).header("Cookie",cookie).build(),HttpResponse.BodyHandlers.ofString());assertEquals(409,active.statusCode());
            Journal.commit(spool,f.chunk(f.record(3,"TERMINAL",f.frame(2000,900,true))));Thread.sleep(1500);
            var settled=client.send(HttpRequest.newBuilder(URI.create(base+"/api/matches/"+f.mid)).header("Cookie",cookie).build(),HttpResponse.BodyHandlers.ofString());assertEquals(200,settled.statusCode());assertEquals("COMPLETE",JsonParser.parseString(settled.body()).getAsJsonObject().get("quality").getAsString());
            var conflict=client.send(HttpRequest.newBuilder(URI.create(base+"/api/matches/"+f.mid+"/cursor?metric=income&gameMs=0&revision=999")).header("Cookie",cookie).build(),HttpResponse.BodyHandlers.ofString());assertEquals(409,conflict.statusCode());
            var csrf=client.send(HttpRequest.newBuilder(URI.create(base+"/api/matches/"+f.mid+"/pin")).header("Cookie",cookie).header("Content-Type","application/json").header("Origin","https://untrusted.example").POST(HttpRequest.BodyPublishers.ofString("{\"pinned\":true}")).build(),HttpResponse.BodyHandlers.ofString());assertEquals(401,csrf.statusCode());
            assertTrue(login.headers().firstValue("set-cookie").orElseThrow().contains("HttpOnly"));
            var queued=client.send(HttpRequest.newBuilder(URI.create(base+"/api/matches/"+f.mid+"/exports")).header("Cookie",cookie).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString("{\"format\":\"html\"}")).build(),HttpResponse.BodyHandlers.ofString());assertEquals(202,queued.statusCode());
            String job=JsonParser.parseString(queued.body()).getAsJsonObject().get("id").getAsString();JsonObject state=null;
            for(int i=0;i<60;i++){state=JsonParser.parseString(client.send(HttpRequest.newBuilder(URI.create(base+"/api/exports/"+job)).header("Cookie",cookie).build(),HttpResponse.BodyHandlers.ofString()).body()).getAsJsonObject();if(state.get("state").getAsString().equals("READY"))break;Thread.sleep(50);}
            assertEquals("READY",state.get("state").getAsString());
            var download=client.send(HttpRequest.newBuilder(URI.create(base+"/api/exports/"+job+"/download")).header("Cookie",cookie).build(),HttpResponse.BodyHandlers.ofString());assertEquals(200,download.statusCode());assertTrue(download.body().contains("CNKD 赛后报告"));assertTrue(download.body().contains("<svg"));
            var status=JsonParser.parseString(client.send(HttpRequest.newBuilder(URI.create(base+"/api/status")).header("Cookie",cookie).build(),HttpResponse.BodyHandlers.ofString()).body()).getAsJsonObject();assertEquals(1,status.get("rejectedBlocks").getAsInt());
            var grant=client.send(HttpRequest.newBuilder(URI.create(base+"/api/matches/"+f.mid+"/share")).header("Cookie",cookie).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString("{}")).build(),HttpResponse.BodyHandlers.ofString());assertEquals(200,grant.statusCode());String shared=JsonParser.parseString(grant.body()).getAsJsonObject().get("token").getAsString();
            HttpResponse<String> sharedReport=null;for(int i=0;i<60;i++){sharedReport=client.send(HttpRequest.newBuilder(URI.create(base+"/r/"+shared)).build(),HttpResponse.BodyHandlers.ofString());if(sharedReport.statusCode()==200)break;Thread.sleep(50);}assertEquals(200,sharedReport.statusCode());assertTrue(sharedReport.body().contains("<svg"));
            var revoke=client.send(HttpRequest.newBuilder(URI.create(base+"/api/matches/"+f.mid+"/revoke-shares")).header("Cookie",cookie).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString("{}")).build(),HttpResponse.BodyHandlers.ofString());assertEquals(200,revoke.statusCode());assertEquals(404,client.send(HttpRequest.newBuilder(URI.create(base+"/r/"+shared)).build(),HttpResponse.BodyHandlers.ofString()).statusCode());
            var config=client.send(HttpRequest.newBuilder(URI.create(base+"/api/config")).header("Cookie",cookie).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString("{\"revision\":0,\"hotDays\":30,\"archiveDays\":120,\"playerLinksEnabled\":false}")).build(),HttpResponse.BodyHandlers.ofString());assertEquals(200,config.statusCode());assertEquals(1,JsonParser.parseString(config.body()).getAsJsonObject().get("revision").getAsInt());
            var stale=client.send(HttpRequest.newBuilder(URI.create(base+"/api/config")).header("Cookie",cookie).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString("{\"revision\":0,\"hotDays\":1,\"archiveDays\":1,\"playerLinksEnabled\":true}")).build(),HttpResponse.BodyHandlers.ofString());assertEquals(409,stale.statusCode());assertTrue(Files.readString(root.resolve("runtime-policy.json")).contains("120"));
        }
    }
}
