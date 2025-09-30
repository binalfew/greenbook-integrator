package org.africanunion.greenbook_integrator.config;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.africanunion.greenbook_integrator.records.Office;
import org.africanunion.greenbook_integrator.records.Department;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.lang.NonNull;

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

        // Always close the stream
        try (var lines = Files.lines(file)) {
            headerLine = lines.findFirst().orElse("");
        }

        String[] actual = headerLine.split(",");

        if (actual.length != expectedHeaders.length) {
            throw new IllegalStateException("❌ Invalid header column count in " + file + ". Expected " + expectedHeaders.length + " columns, found " + actual.length);
        }

        for (int i = 0; i < expectedHeaders.length; i++) {
            if (!actual[i].trim().equalsIgnoreCase(expectedHeaders[i])) {
                throw new IllegalStateException("❌ Invalid header in " + file + ". Expected '" + expectedHeaders[i] + "', found '" + actual[i] + "'");
            }
        }
    }

    // --- Readers ---
    @Bean
    ItemReader<Office> officeReader(@Value("${azure.storage.connection-string}") String connectionString, @Value("${azure.storage.container-name}") String containerName) throws IOException {
        Path file = downloadBlob(connectionString, containerName, "offices.csv");
        validateHeader(file, "id", "name");

        return new FlatFileItemReaderBuilder<Office>()
                .resource(new FileSystemResource(file))
                .name("officeReader")
                .delimited()
                .names("id", "name") // ignore id
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new Office(fieldSet.readString("name")))
                .build();
    }

    @Bean
    ItemReader<Department> departmentReader(@Value("${azure.storage.connection-string}") String connectionString, @Value("${azure.storage.container-name}") String containerName) throws IOException {
        Path file = downloadBlob(connectionString, containerName, "departments.csv");
        validateHeader(file, "id", "name");

        return new FlatFileItemReaderBuilder<Department>()
                .resource(new FileSystemResource(file))
                .name("departmentReader")
                .delimited()
                .names("id", "name") // ignore id
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new Department(fieldSet.readString("name")))
                .build();
    }

    // --- Writers ---
    @Bean
    ItemWriter<Office> officeWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Office>()
                .dataSource(dataSource)
                .sql("INSERT INTO Office (name) VALUES (?)")
                .itemPreparedStatementSetter((item, ps) -> ps.setString(1, item.name()))
                .build();
    }

    @Bean
    ItemWriter<Department> departmentWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Department>()
                .dataSource(dataSource)
                .sql("INSERT INTO Department (name) VALUES (?)")
                .itemPreparedStatementSetter((item, ps) -> ps.setString(1, item.name()))
                .build();
    }

    // --- Steps ---
    @Bean
    Step officeStep(JobRepository repository, PlatformTransactionManager transactionManager, ItemReader<Office> officeReader, ItemWriter<Office> officeWriter) {
        return new StepBuilder("officeStep", repository)
                .<Office, Office>chunk(10, transactionManager)
                .reader(officeReader)
                .writer(officeWriter)
                .listener(stepCleanupListener("offices.csv"))
                .build();
    }

    @Bean
    Step departmentStep(JobRepository repository, PlatformTransactionManager transactionManager, ItemReader<Department> departmentReader, ItemWriter<Department> departmentWriter) {
        return new StepBuilder("departmentStep", repository)
                .<Department, Department>chunk(10, transactionManager)
                .reader(departmentReader)
                .writer(departmentWriter)
                .listener(stepCleanupListener("departments.csv"))
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

    // --- Cleanup Listener (per step) ---
    private StepExecutionListener stepCleanupListener(String fileName) {
        return new StepExecutionListener() {
            private Path tempFilePath;

            @Override
            public void beforeStep(@NonNull StepExecution stepExecution) {
                // store path in ExecutionContext for afterStep
                tempFilePath = stepExecution.getExecutionContext().containsKey(fileName)
                        ? (Path) stepExecution.getExecutionContext().get(fileName)
                        : null;
            }

            @Override
            public ExitStatus afterStep(@NonNull StepExecution stepExecution) {
                if (tempFilePath != null) {
                    try {
                        Files.deleteIfExists(tempFilePath);
                        log.info("🧹 Temp file {} deleted after step.", tempFilePath);
                    } catch (IOException e) {
                        log.warn("⚠️ Failed to delete temp file {}", tempFilePath, e);
                    }
                }
                return stepExecution.getExitStatus();
            }
        };
    }
}
