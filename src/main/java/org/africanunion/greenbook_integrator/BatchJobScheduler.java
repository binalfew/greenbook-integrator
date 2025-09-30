package org.africanunion.greenbook_integrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class BatchJobScheduler {

    private static final Logger log = LoggerFactory.getLogger(BatchJobScheduler.class);

    private final JobLauncher jobLauncher;
    private final JobExplorer jobExplorer;
    private final Job job;

    public BatchJobScheduler(JobLauncher jobLauncher, JobExplorer jobExplorer, Job job) {
        this.jobLauncher = jobLauncher;
        this.jobExplorer = jobExplorer;
        this.job = job;
    }

    @Scheduled(cron = "${batch.job.cron}")
    public void runBatchJob() throws Exception {
        if (!jobExplorer.findRunningJobExecutions(job.getName()).isEmpty()) {
            log.info("⏸ Job {} is already running. Skipping this trigger.", job.getName());
            return;
        }

        String runId = String.valueOf(System.currentTimeMillis());

        log.info("============================================");
        log.info("🚀 JOB START [{}] - {}", runId, new Date());
        log.info("============================================");

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis()) // ensure uniqueness
                .toJobParameters();

        JobExecution execution = jobLauncher.run(job, jobParameters);

        log.info("--------------------------------------------");
        log.info("✅ JOB END [{}] - Status: {}", runId, execution.getStatus());
        log.info("--------------------------------------------\n");
    }
}
