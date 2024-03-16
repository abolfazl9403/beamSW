package abolfazl.younesi;

import java.util.Arrays;
import java.util.Scanner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class App2 {
    public interface Options extends StreamingOptions {
        @Description("Input text to print.")
        @Default.String("My, input, text")
        String getInputText();
        void setInputText(String value);

        @Description("Delimiter to separate input elements.")
        @Default.String(",")
        String getDelimiter();
        void setDelimiter(String value);
    }

    public static PCollection<String> buildPipeline(Pipeline pipeline, Options options) {
        return pipeline
                .apply("Create elements", Create.of(Arrays.asList(options.getInputText().split(options.getDelimiter()))))
                .apply("Print elements",
                        MapElements.into(TypeDescriptors.strings()).via(x -> {
                            System.out.println(x);
                            return x;
                        }));
    }

    public static void main(String[] args) {
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        // Get input text and delimiter from the user
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter input text (default: " + options.getInputText() + "): ");
        String inputText = scanner.nextLine();
        if (!inputText.isEmpty()) {
            options.setInputText(inputText);
        }

        System.out.print("Enter delimiter (default: " + options.getDelimiter() + "): ");
        String delimiter = scanner.nextLine();
        if (!delimiter.isEmpty()) {
            options.setDelimiter(delimiter);
        }

        var pipeline = Pipeline.create(options);
        App2.buildPipeline(pipeline, options);
        pipeline.run().waitUntilFinish();

        // Close the scanner
        scanner.close();
    }
}
