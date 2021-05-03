package application;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

import java.util.HashMap;

public class MyMapReduce implements MapReduce {
    private static final long serialVersionUID = 1L;

    public static void main(String[] args) {
        if (args.length < 1) {
            usage();
            return;
        }
        Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
        long t1 = System.currentTimeMillis();
        j.startJob(new MyMapReduce());
        long t2 = System.currentTimeMillis();
        System.out.println("time in ms =" + (t2 - t1));
        System.exit(0);
    }

    public void reduce(FormatReader reader, FormatWriter writer) {
        HashMap<String, Integer> hm = new HashMap<>();
        KV kv;
        while ((kv = reader.read()) != null) {
            if (hm.containsKey(kv.k))
                hm.put(kv.k, hm.get(kv.k) + Integer.parseInt(kv.v));
            else
                hm.put(kv.k, Integer.parseInt(kv.v));
        }
        for (String k : hm.keySet())
            writer.write(new KV(k, hm.get(k).toString()));
    }

    public static void usage() {
        System.out.println("Usage : MyMapReduce <id>");
    }

    // MapReduce program that computes word counts
    public void map(FormatReader reader, FormatWriter writer) {

        HashMap<String, Integer> hm = new HashMap<>();
        KV kv;
        while ((kv = reader.read()) != null) {
            String[] tokens = kv.v.split(" ");
            for (String tok : tokens) {
                if (hm.containsKey(tok))
                    hm.put(tok, hm.get(tok).intValue() + 1);
                else
                    hm.put(tok, 1);
            }
        }
        for (String k : hm.keySet())
            writer.write(new KV(k, hm.get(k).toString()));
    }
}
