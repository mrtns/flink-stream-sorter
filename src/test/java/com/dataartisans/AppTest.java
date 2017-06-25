package com.dataartisans;

import org.apache.flink.streaming.util.StreamingProgramTestBase;

/**
 * {@link App} program should execute successfully.
 */
public class AppTest extends StreamingProgramTestBase
{
    private String inputPath;
    private String outputPath;

    @Override
    protected void preSubmit() throws Exception {
        inputPath = createTempFile("AppTest_Input.txt", AppData.EVENT_STREAM_AS_LINEWISE_JSON);
        outputPath = getTempDirPath("AppTest_Output");
    }

    @Override
    protected void postSubmit() throws Exception {
        compareResultsByLinesInMemory(AppData.EVENT_STREAM_AS_LINEWISE_JSON, outputPath);
    }

    @Override
    protected void testProgram() throws Exception {
        App.main(new String[]{
                "--inputPath", inputPath,
                "--outputPath", outputPath});
    }
}