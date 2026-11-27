package net.cnkd.analytics;
import java.util.*;

/** Bounded joint grid retaining first/last and each visible series' extrema in each time bucket. */
final class JointReducer {
    record Sample(long time,Long[] values,String[] quality){}
    final int buckets,dimensions;final long end;long input;
    final List<TreeMap<Long,Sample>> selected;final Sample[][] minima,maxima;final Sample[] first,last;
    JointReducer(long end,int maximum,int dimensions){this.end=Math.max(1,end);this.dimensions=dimensions;buckets=Math.max(1,(maximum-2)/Math.max(2,2*dimensions+2));selected=new ArrayList<>();for(int i=0;i<buckets;i++)selected.add(new TreeMap<>());minima=new Sample[buckets][dimensions];maxima=new Sample[buckets][dimensions];first=new Sample[buckets];last=new Sample[buckets];}
    void add(long time,Long[] values,String[] quality){input++;int bucket=(int)Math.min(buckets-1,time*buckets/end);Sample sample=new Sample(time,values.clone(),quality.clone());if(first[bucket]==null)first[bucket]=sample;last[bucket]=sample;
        for(int i=0;i<dimensions;i++)if(values[i]!=null){if(minima[bucket][i]==null||values[i]<minima[bucket][i].values[i])minima[bucket][i]=sample;if(maxima[bucket][i]==null||values[i]>maxima[bucket][i].values[i])maxima[bucket][i]=sample;}
    }
    List<Sample> finish(){TreeMap<Long,Sample> out=new TreeMap<>();for(int b=0;b<buckets;b++){if(first[b]!=null)out.put(first[b].time,first[b]);if(last[b]!=null)out.put(last[b].time,last[b]);for(int i=0;i<dimensions;i++){if(minima[b][i]!=null)out.put(minima[b][i].time,minima[b][i]);if(maxima[b][i]!=null)out.put(maxima[b][i].time,maxima[b][i]);}}return List.copyOf(out.values());}
}
