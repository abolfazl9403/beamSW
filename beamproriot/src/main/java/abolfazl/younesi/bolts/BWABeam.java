package abolfazl.younesi.bolts;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

// Class for calculating block window average
public class BWABeam extends DoFn<TaxiData, KV<Integer, Double>> {
    private int blockSize;
    private double blockSum;  // Sum of total amounts in the current block
    private int fileIndex; // To keep track of processed files

    // Constructor to initialize block window average calculator
    public void BlockWindowAverageFn(int blockSize) {
        this.blockSize = blockSize;
        this.blockSum = 0;
        this.fileIndex = 1; // Initialize file index
    }

    // Method to add taxi data to the current block
    @ProcessElement
    public void processElement(@Element TaxiData data, OutputReceiver<KV<Integer, Double>> out) {
        blockSum += data.getTotalAmount();
        if (fileIndex % blockSize == 0) {
            out.output(KV.of(fileIndex / blockSize, blockSum / blockSize));
            blockSum = 0;
        }
        fileIndex++;
    }
}
