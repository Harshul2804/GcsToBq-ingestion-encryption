import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class BQGCS {


    public static void main(String[] args) throws Exception{

        DataflowPipelineOptions dataflowPipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        dataflowPipelineOptions.setRegion("us-central1");
        dataflowPipelineOptions.setProject("dmgcp-del-108");
        dataflowPipelineOptions.setServiceAccount("sa-dataflow@dmgcp-del-108.iam.gserviceaccount.com");
        dataflowPipelineOptions.setJobName("BQGCS1391");
        dataflowPipelineOptions.setStagingLocation("gs://training_freshers/harshul/Staging");
        dataflowPipelineOptions.setTempLocation("gs://training_freshers/harshul/Temp");
        dataflowPipelineOptions.setNetwork("dm-primary-network");
        dataflowPipelineOptions.setSubnetwork("https://www.googleapis.com/compute/v1/projects/dm-network-host-project/regions/us-central1/subnetworks/subnet-dm-delivery-us-central1");
        dataflowPipelineOptions.setRunner(DataflowRunner.class);
        dataflowPipelineOptions.setUsePublicIps(Boolean.FALSE);

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        TableId tableId = TableId.of("dmgcp-del-108", "training_freshers", "assign-5");
        long size = bigquery.getTable(tableId).getNumBytes();
        final long MAX_FILE_SIZE = 20L * 1024L * 1024L;
        int shards = (int) (size / MAX_FILE_SIZE);
        if ((size % MAX_FILE_SIZE) > 0L) {
            ++shards;
        }
        System.out.println("Table Size : " + size);
        System.out.println("Shards     : " + shards);


        Pipeline p = Pipeline.create(dataflowPipelineOptions);
        PCollection<TableRow> Tdata = p.apply("Read from BQ",BigQueryIO.readTableRows().from("dmgcp-del-108:training_freshers.DF_gtbqJava_1391"));
        Tdata.apply("Get CSV Record", ParDo.of(new GetCSVRec()))
                .apply("Write to text", TextIO.write().to("gs://training_freshers/harshul/sensetiveData").withSuffix(".csv").withNumShards(shards));
        p.run().waitUntilFinish();

    }


}
