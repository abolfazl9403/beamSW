package abolfazl.younesi;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class AppCommand {
    public interface Options extends StreamingOptions {
        @Description("Input text to print.")
        @Default.String("My, input, text, 2020202")
        String getInputText();
        void setInputText(String value);

        @Description("Delimiter to separate input elements.")
        @Default.String(",")
        String getDelimiter();
        void setDelimiter(String value);
    }

    public static PCollection<String> buildPipeline(Pipeline pipeline, Options options) {
        System.out.println("Building pipeline...");
        PCollection<String> collection = pipeline
                .apply("Create elements", Create.of(Arrays.asList(options.getInputText().split(options.getDelimiter()))));
        System.out.println("Pipeline built successfully!");
        return collection
                .apply("Print elements",
                        MapElements.into(TypeDescriptors.strings()).via(x -> {
                            System.out.println("Printing element: " + x);
                            return x;
                        }));
    }

    public static void main(String[] args) {
        System.out.println("Starting application...");
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        System.out.println("Options created successfully!");

        // Extract input text and delimiter from command line arguments
        String inputText = options.getInputText();
        String delimiter = options.getDelimiter();
        System.out.println("Input text: " + inputText);
        System.out.println("Delimiter: " + delimiter);

        var pipeline = Pipeline.create(options);
        System.out.println("Pipeline created successfully!");

        AppCommand.buildPipeline(pipeline, options);
        System.out.println("Pipeline built and executed!");

        pipeline.run().waitUntilFinish();
        System.out.println("Pipeline execution completed!");
    }
}
