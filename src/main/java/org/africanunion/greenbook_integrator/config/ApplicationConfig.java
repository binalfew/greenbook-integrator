package org.africanunion.greenbook_integrator.config;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.africanunion.greenbook_integrator.records.Department;
import org.africanunion.greenbook_integrator.records.Office;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Configuration
public class ApplicationConfig {
    private static final Logger log = LoggerFactory.getLogger(ApplicationConfig.class);

    private Path downloadBlob(String connectionString, String containerName, String fileName) throws IOException {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        BlobClient blobClient = containerClient.getBlobClient(fileName);

        if (!blobClient.exists()) {
            throw new IllegalStateException("❌ Required file " + fileName + " not found in container " + containerName);
        }

        Path tempFilePath = Files.createTempFile(fileName.replace(".csv", "") + "-", ".csv");
        blobClient.downloadToFile(tempFilePath.toString(), true);
        log.info("📁 Downloaded {} to temp file: {}", fileName, tempFilePath);
        return tempFilePath;
    }

    private void validateHeader(Path file, String... expectedHeaders) throws IOException {
        String headerLine;
        try (var lines = Files.lines(file)) {
            headerLine = lines.findFirst().orElse("");
        }

        String[] actual = headerLine.split(",");
        if (actual.length != expectedHeaders.length) {
            throw new IllegalStateException("❌ Invalid header column count in " + file);
        }
        for (int i = 0; i < expectedHeaders.length; i++) {
            if (!actual[i].trim().equalsIgnoreCase(expectedHeaders[i])) {
                throw new IllegalStateException("❌ Invalid header in " + file +
                        ". Expected '" + expectedHeaders[i] + "', found '" + actual[i] + "'");
            }
        }
    }

    // --- Readers ---
    @Bean
    @StepScope
    FlatFileItemReader<Office> officeReader(
            @Value("${azure.storage.connection-string}") String connectionString,
            @Value("${azure.storage.container-name}") String containerName) throws IOException {
        FlatFileItemReader<Office> reader = new FlatFileItemReaderBuilder<Office>()
                .name("officeReader")
                .delimited()
                .names("id", "name")
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new Office(fieldSet.readString("name")))
                .build();

        // Resource will be set later in beforeStep()
        reader.setStrict(false);
        return reader;
    }

    @Bean
    @StepScope
    FlatFileItemReader<Department> departmentReader(
            @Value("${azure.storage.connection-string}") String connectionString,
            @Value("${azure.storage.container-name}") String containerName) throws IOException {

        FlatFileItemReader<Department> reader = new FlatFileItemReaderBuilder<Department>()
                .name("departmentReader")
                .delimited()
                .names("id", "name")
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new Department(fieldSet.readString("name")))
                .build();

        reader.setStrict(false);
        return reader;
    }

    // --- Writers ---
    @Bean
    ItemWriter<Office> officeWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Office>()
                .dataSource(dataSource)
                .sql("""
                            INSERT INTO "Office" ("id", "name", "createdAt", "updatedAt") 
                            VALUES (?, ?, ?, ?) 
                            ON CONFLICT ("name") 
                            DO UPDATE SET "updatedAt" = EXCLUDED."updatedAt"
                        """)
                .itemPreparedStatementSetter((item, ps) -> {
                    java.time.LocalDateTime now = java.time.LocalDateTime.now();
                    ps.setObject(1, java.util.UUID.randomUUID()); // new UUID
                    ps.setString(2, item.name());
                    ps.setObject(3, now);
                    ps.setObject(4, now);
                })
                .assertUpdates(false) // ✅ allow UPSERTs that don't change anything
                .build();
    }

    @Bean
    ItemWriter<Department> departmentWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Department>()
                .dataSource(dataSource)
                .sql("""
                            INSERT INTO "Department" ("id", "name") 
                            VALUES (?, ?) 
                            ON CONFLICT ("name") DO NOTHING
                        """)
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setObject(1, java.util.UUID.randomUUID()); // new UUID
                    ps.setString(2, item.name());
                })
                .assertUpdates(false) // ✅ allow UPSERTs that don't change anything
                .build();
    }


    // --- Steps ---
    @Bean
    Step officeStep(JobRepository repository,
                    PlatformTransactionManager transactionManager,
                    FlatFileItemReader<Office> officeReader,
                    ItemWriter<Office> officeWriter,
                    @Value("${azure.storage.connection-string}") String connectionString,
                    @Value("${azure.storage.container-name}") String containerName) {

        return new StepBuilder("officeStep", repository)
                .<Office, Office>chunk(10, transactionManager)
                .reader(officeReader)
                .writer(officeWriter)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        try {
                            Path file = downloadBlob(connectionString, containerName, "offices.csv");
                            validateHeader(file, "id", "name");
                            officeReader.setResource(new FileSystemResource(file));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .build();
    }

    @Bean
    Step departmentStep(JobRepository repository,
                        PlatformTransactionManager transactionManager,
                        FlatFileItemReader<Department> departmentReader,
                        ItemWriter<Department> departmentWriter,
                        @Value("${azure.storage.connection-string}") String connectionString,
                        @Value("${azure.storage.container-name}") String containerName) {

        return new StepBuilder("departmentStep", repository)
                .<Department, Department>chunk(10, transactionManager)
                .reader(departmentReader)
                .writer(departmentWriter)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        try {
                            Path file = downloadBlob(connectionString, containerName, "departments.csv");
                            validateHeader(file, "id", "name");
                            departmentReader.setResource(new FileSystemResource(file));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
                .build();
    }

    // --- Job ---
    @Bean
    Job job(JobRepository repository, Step officeStep, Step departmentStep) {
        return new JobBuilder("importJob", repository)
                .incrementer(new RunIdIncrementer())
                .start(officeStep)
                .next(departmentStep)
                .build();
    }
}
