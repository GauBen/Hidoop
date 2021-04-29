package application;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

import java.util.HashMap;

public class QuasiMonteCarlo implements MapReduce {

    private static final long serialVersionUID = 1L;

    public int offset;
    public int size;

    public QuasiMonteCarlo(int offset, int size){
        this.offset = offset;
        this.size = size;
    }

    @Override
    public void map(FormatReader reader, FormatWriter writer) {
        final HaltonSequence haltonsequence = new HaltonSequence(offset);
        int numInside = 0;
        int numOutside = 0;

        for(long i = 0; i < size; i++) {
            //generate points in a unit square
            final double[] point = haltonsequence.nextPoint();

            //count points inside/outside of the inscribed circle of the square
            final double x = point[0] - 0.5;
            final double y = point[1] - 0.5;
            if (x*x + y*y > 0.25) {
                numOutside++;
            } else {
                numInside++;
            }

            //report status
            i++;
            if (i % 1000 == 0) {
                //context.setStatus("Generated " + i + " samples.");
            }
        }

        //output map results
        /*
        writer.write(new BooleanWritable(true), new LongWritable(numInside));
        writer.write(new BooleanWritable(false), new LongWritable(numOutside));
         */

        writer.write(new KV("true", String.valueOf(numInside)));
        writer.write(new KV("false", String.valueOf(numOutside)));

    }

    @Override
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
        System.out.println("Usage : QuasiMonteCarlo <size>");
    }

    public static void main(String args[]) {
        if (args.length < 1) {
            usage();
            return;
        }
        Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
        long t1 = System.currentTimeMillis();
        j.startJob(new QuasiMonteCarlo(0, 1000000));
        long t2 = System.currentTimeMillis();
        System.out.println("time in ms =" + (t2 - t1));
        System.exit(0);
    }



    /** 2-dimensional Halton sequence {H(i)},
     * where H(i) is a 2-dimensional point and i >= 1 is the index.
     * Halton sequence is used to generate sample points for Pi estimation.
     */
    private static class HaltonSequence {
        /** Bases */
        static final int[] P = {2, 3};
        /** Maximum number of digits allowed */
        static final int[] K = {63, 40};

        private long index;
        private double[] x;
        private double[][] q;
        private int[][] d;

        /** Initialize to H(startindex),
         * so the sequence begins with H(startindex+1).
         */
        HaltonSequence(long startindex) {
            index = startindex;
            x = new double[K.length];
            q = new double[K.length][];
            d = new int[K.length][];
            for(int i = 0; i < K.length; i++) {
                q[i] = new double[K[i]];
                d[i] = new int[K[i]];
            }

            for(int i = 0; i < K.length; i++) {
                long k = index;
                x[i] = 0;

                for(int j = 0; j < K[i]; j++) {
                    q[i][j] = (j == 0? 1.0: q[i][j-1])/P[i];
                    d[i][j] = (int)(k % P[i]);
                    k = (k - d[i][j])/P[i];
                    x[i] += d[i][j] * q[i][j];
                }
            }
        }

        /** Compute next point.
         * Assume the current point is H(index).
         * Compute H(index+1).
         *
         * @return a 2-dimensional point with coordinates in [0,1)^2
         */
        double[] nextPoint() {
            index++;
            for(int i = 0; i < K.length; i++) {
                for(int j = 0; j < K[i]; j++) {
                    d[i][j]++;
                    x[i] += q[i][j];
                    if (d[i][j] < P[i]) {
                        break;
                    }
                    d[i][j] = 0;
                    x[i] -= (j == 0? 1.0: q[i][j-1]);
                }
            }
            return x;
        }
    }
}
