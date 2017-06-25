package com.dataartisans;

import org.apache.flink.streaming.util.StreamingProgramTestBase;

/**
 * Tests for {@link App}.
 */
public class AppTests
{
    public static class ShouldExecuteSuccessfully extends StreamingProgramTestBase {

        public static final String INPUT_DATA_AS_LINEWISE_JSON = ""
                + "{\"time\":1498404088692,\"someData\":\"some data value\"}";

        private String inputPath;
        private String outputPath;

        @Override
        protected void preSubmit() throws Exception {
            inputPath = createTempFile("AppTest_Input.txt", INPUT_DATA_AS_LINEWISE_JSON);
            outputPath = getTempDirPath("AppTest_Output");
        }

        @Override
        protected void postSubmit() throws Exception {
            compareResultsByLinesInMemory(INPUT_DATA_AS_LINEWISE_JSON, outputPath);
        }

        @Override
        protected void testProgram() throws Exception {
            App.main(new String[]{
                    "--inputPath", inputPath,
                    "--outputPath", outputPath});
        }
    }
}