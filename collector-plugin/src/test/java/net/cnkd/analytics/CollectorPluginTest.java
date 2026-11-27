package net.cnkd.analytics;
import net.rwhps.server.plugin.api.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class CollectorPluginTest {
    @TempDir Path temp;
    @Test void diskFailureNeverAcknowledgesUndurableDataAndRecoveryDoesNotDuplicate()throws Exception{
        Path journal=temp.resolve("journal");Files.writeString(journal,"isolated disk failure fixture");
        List<StatisticsRecord> records=new ArrayList<>();for(int i=1;i<=300;i++)records.add(new StatisticsRecord(i,"a".repeat(32)+"_1",i==1?"SESSION":"FRAME",Map.of("participants",List.of())));
        AtomicLong ack=new AtomicLong();List<AutoCloseable> resources=new ArrayList<>();
        PluginStatistics stream=new PluginStatistics(){public void enabled(boolean b){}public List<StatisticsRecord> read(int n){return records.stream().filter(r->r.sequence()>ack.get()).limit(n).collect(java.util.stream.Collectors.toList());}public void acknowledge(long n){ack.set(n);}public Map<String,Object> status(){return Map.of("bootId","a".repeat(32),"pending",ack.get()==0?records.size():0);}public void close(){}};
        PluginCommands commands=(PluginCommands)Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{PluginCommands.class},(p,m,a)->new Registration(){public boolean isActive(){return true;}public void close(){}});
        PluginLogger logger=(PluginLogger)Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{PluginLogger.class},(p,m,a)->null);
        PluginContext context=(PluginContext)Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{PluginContext.class},(p,m,a)->{
            switch(m.getName()){case "dataDirectory":return temp;case "statistics":return stream;case "commands":return commands;case "logger":return logger;case "manage":resources.add((AutoCloseable)a[0]);return null;default:throw new UnsupportedOperationException(m.getName());}
        });
        CollectorPlugin plugin=new CollectorPlugin();
        try{
            plugin.onEnable(context);Thread.sleep(1200);assertEquals(0,ack.get());Files.delete(journal);Files.createDirectory(journal);
            long end=System.currentTimeMillis()+5000;while(ack.get()<300&&System.currentTimeMillis()<end)Thread.sleep(50);assertEquals(300,ack.get());
            plugin.onDisable();
            List<Path> chunks;try(var files=Files.list(journal)){chunks=files.filter(p->p.getFileName().toString().startsWith("chunk-")).collect(java.util.stream.Collectors.toList());}assertEquals(2,chunks.size());int total=0;for(Path chunk:chunks){int size=Journal.decode(chunk).getAsJsonArray("records").size();assertTrue(size<=256);total+=size;}assertEquals(300,total);
        }finally{Collections.reverse(resources);for(AutoCloseable resource:resources)resource.close();}
    }
}
