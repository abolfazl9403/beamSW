package abolfazl.younesi.bolts;
import abolfazl.younesi.App;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.ArrayDeque;
import java.util.Queue;

public class BWABeam {
    private static final int BLOCK_SIZE = 100; // Set the desired block size
    public static final String HEADER = "BlockIndex, Average";

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> taxiData = pipeline.apply(TextIO.read().from(App.dtc + "\\chunk_1.csv"));

        PCollection<KV<Integer, Double>> averages = taxiData
                .apply(ParDo.of(new CalculateBlockAverage()))
                .apply(Window.<KV<Integer, Double>>into(FixedWindows.of(Duration.standardMinutes(1))));

        PCollection<String> averagesAsStrings = averages.apply(ParDo.of(new KVToStringFn()));

        averagesAsStrings
                .apply(TextIO.write()
                        .to(App.dtc + "\\BWA")
                        .withHeader(HEADER)
                        .withNumShards(4));

        pipeline.run().waitUntilFinish();
    }

    public static class CalculateBlockAverage extends DoFn<String, KV<Integer, Double>> {
        private final Queue<Double> blockData;
        private double blockSum;
        private int blockIndex;
        private boolean isHeaderSkipped; // Add this line

        public CalculateBlockAverage() {
            this.blockData = new ArrayDeque<>(BLOCK_SIZE);
            this.blockSum = 0;
            this.blockIndex = 0;
            this.isHeaderSkipped = false; // Add this line
        }

        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<KV<Integer, Double>> receiver) {
            if (!isHeaderSkipped) { // Add this block
                isHeaderSkipped = true;
                return;
            }

            double totalAmount = parseTotalAmount(line);

            blockData.add(totalAmount);
            blockSum += totalAmount;

            if (blockData.size() > BLOCK_SIZE) {
                double removedData = blockData.poll();
                blockSum -= removedData;
            }

            if (blockData.size() == BLOCK_SIZE) {
                double average = blockSum / blockData.size();
                receiver.output(KV.of(blockIndex, average));
                blockIndex++;
                blockData.clear();
                blockSum = 0;
            }
        }

        private double parseTotalAmount(String line) {
            // Assuming the input line is in the format: "value1,value2,...,value10,totalAmount,value11"
            String[] values = line.split(",");
            return Double.parseDouble(values[10]);
        }
    }

    public static class KVToStringFn extends DoFn<KV<Integer, Double>, String> {
        @ProcessElement
        public void processElement(@Element KV<Integer, Double> element, OutputReceiver<String> receiver) {
            receiver.output(element.getKey() + "," + element.getValue());
        }
    }
}