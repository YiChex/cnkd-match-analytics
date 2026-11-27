package net.cnkd.analytics;
import com.google.gson.*;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;

/** Checksummed, independently recoverable blocks. This class never touches game state. */
public final class Journal {
    public static final Gson JSON=new GsonBuilder().serializeNulls().create();
    public static final List<String> METRICS=List.of("income","armyValue","buildingValue","totalValue");
    public static final List<String> COUNTERS=List.of("unitsKilled","buildingsKilled","experimentalsKilled","unitsLost","buildingsLost","experimentalsLost");
    public static String hash(byte[] bytes){try{byte[] raw=MessageDigest.getInstance("SHA-256").digest(bytes);StringBuilder s=new StringBuilder();for(byte b:raw)s.append(String.format("%02x",b&255));return s.toString();}catch(Exception e){throw new IllegalStateException(e);}}
    public static Path commit(Path directory,JsonObject payload)throws Exception{
        Files.createDirectories(directory);String value=JSON.toJson(payload),hash=hash(value.getBytes(StandardCharsets.UTF_8));
        JsonObject envelope=new JsonObject();envelope.addProperty("schemaVersion",1);envelope.addProperty("sha256",hash);envelope.addProperty("payload",value);
        JsonArray records=payload.getAsJsonArray("records");if(records==null||records.size()==0)throw new IllegalArgumentException("Empty journal block");
        long first=records.get(0).getAsJsonObject().get("sequence").getAsLong();
        Path destination=directory.resolve("chunk-"+payload.get("streamId").getAsString()+"-"+String.format("%020d",first)+"-"+hash+".json");byte[] bytes=JSON.toJson(envelope).getBytes(StandardCharsets.UTF_8);
        if(bytes.length>4*1024*1024)throw new IllegalArgumentException("JOURNAL_BLOCK_TOO_LARGE");
        if(Files.exists(destination)){decode(destination);return destination;}
        Path temp=directory.resolve(".writing-"+UUID.randomUUID());
        try{
            Files.write(temp,bytes,StandardOpenOption.CREATE_NEW);
            try(FileChannel channel=FileChannel.open(temp,StandardOpenOption.WRITE)){channel.force(true);}
            for(int i=0;;i++)try{Files.move(temp,destination,StandardCopyOption.ATOMIC_MOVE);break;}catch(AccessDeniedException e){if(i>=20)throw e;Thread.sleep(25);}
        }finally{Files.deleteIfExists(temp);}
        return destination;
    }
    public static JsonObject decode(Path file)throws Exception{
        if(Files.isSymbolicLink(file)||Files.size(file)>4*1024*1024)throw new IllegalArgumentException("INVALID_JOURNAL_FILE");
        JsonObject e=JsonParser.parseString(Files.readString(file)).getAsJsonObject();String payload=e.get("payload").getAsString();
        if(e.get("schemaVersion").getAsInt()!=1||!hash(payload.getBytes(StandardCharsets.UTF_8)).equals(e.get("sha256").getAsString()))throw new IllegalArgumentException("JOURNAL_HASH_MISMATCH");
        JsonObject p=JsonParser.parseString(payload).getAsJsonObject();
        if(p.get("schemaVersion").getAsInt()!=1||!p.get("streamId").getAsString().matches("[a-f0-9]{32}")||p.getAsJsonArray("records").size()>256)throw new IllegalArgumentException("JOURNAL_SCHEMA_INVALID");
        return p;
    }
    private Journal(){}
}
