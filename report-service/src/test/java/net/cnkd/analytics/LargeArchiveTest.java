package net.cnkd.analytics;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.io.OutputStream;
import static org.junit.jupiter.api.Assertions.*;

/** Synthetic capacity verification, explicitly not two hours of game simulation. */
class LargeArchiveTest {
    @TempDir Path temp;
    @Test void moreThanOneMillionPointsStreamThroughArchiveRestoreAndExport()throws Exception{
        try(var store=new AnalysisStore(temp,1024L*1024*1024)){
            AnalysisStoreTest fixture=new AnalysisStoreTest();fixture.seed(store);String mid=fixture.mid;
            store.db.setAutoCommit(false);int count=1_000_020;
            try(var p=store.db.prepareStatement("INSERT OR REPLACE INTO points VALUES(?,?,?,?,?)")){
                for(int i=0;i<count;i++){AnalysisStore.bind(p,mid,"p"+(i%2),Journal.METRICS.get((i/2)%4),5000L+i*16L,i);p.addBatch();if(i%512==511)p.executeBatch();}p.executeBatch();store.db.commit();
            }finally{store.db.setAutoCommit(true);}
            long before=AnalysisStore.number(store.rows("SELECT count(*) n FROM points WHERE match_id=?",mid).get(0),"n");assertTrue(before>1_000_000);
            store.archive(mid);assertEquals(0,AnalysisStore.number(store.rows("SELECT count(*) n FROM points WHERE match_id=?",mid).get(0),"n"));
            store.export(mid,"jsonl.gz",OutputStream.nullOutputStream());store.restore(mid);
            assertEquals(before,AnalysisStore.number(store.rows("SELECT count(*) n FROM points WHERE match_id=?",mid).get(0),"n"));
            store.export(mid,"csv",OutputStream.nullOutputStream());store.archive(mid);assertTrue(store.report(mid).get("archived").getAsBoolean());
        }
    }
}
