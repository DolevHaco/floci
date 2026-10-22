package io.github.hectorvent.floci.services.sagemaker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.DockerClient;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.dns.EmbeddedDnsServer;
import io.github.hectorvent.floci.core.common.docker.ContainerBuilder;
import io.github.hectorvent.floci.core.common.docker.ContainerDetector;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerLogStreamer;
import io.github.hectorvent.floci.core.common.docker.ContainerSpec;
import io.github.hectorvent.floci.core.common.docker.DockerHostResolver;
import io.github.hectorvent.floci.services.s3.S3Service;
import io.github.hectorvent.floci.services.sagemaker.model.SageMakerEntities.TrainingJobResource;
import io.github.hectorvent.floci.testing.DockerArchiveUploadFixture;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A state reset runs the container teardown, which shuts the worker pool down, then
 * {@code clear()}, and {@code afterReset()} last, even when the wipe or a {@code clear()}
 * failed. Shutdown runs only the teardown.
 */
class SageMakerTrainingRunnerTest {
    private final DockerClient docker = mock(DockerClient.class);
    private final ContainerLifecycleManager lifecycle = mock(ContainerLifecycleManager.class);
    private final ContainerLogStreamer logStreamer = mock(ContainerLogStreamer.class);

    @Test
    void teardownStopsThePoolAndAfterResetRestoresIt() {
        SageMakerTrainingRunner runner = runner();
        assertTrue(runner.acceptsWork());

        runner.stopManagedContainers();
        assertFalse(runner.acceptsWork());

        // A reset whose wipe threw skips clear(); afterReset() alone must bring the pool back.
        runner.afterReset();
        assertTrue(runner.acceptsWork());
        runner.afterReset();
        assertTrue(runner.acceptsWork());

        runner.stopManagedContainers();
    }

    @Test
    void trainingJobsSubmittedAfterAResetStillRun() throws InterruptedException {
        SageMakerTrainingRunner runner = runner();
        runner.stopManagedContainers();
        runner.clear();
        runner.afterReset();

        SageMakerService service = mock(SageMakerService.class);
        CountDownLatch reported = new CountDownLatch(1);
        TrainingJobResource job = new TrainingJobResource();
        job.trainingJobName = "after-reset";
        job.region = "us-east-1";
        job.algorithmSpecification = Map.of("TrainingImage", "busybox:stable");
        // A latch rather than verify(timeout): updateTrainingJob is synchronized, and a timed
        // verify holds the mock's monitor while it polls, blocking the very call it waits for.
        doAnswer(invocation -> {
            reported.countDown();
            return true;
        }).when(service).updateTrainingJob(job);
        runner.runAsync(job, service);

        // The mocked Docker helpers fail the run at once, and every run that executes reports its
        // outcome, so the report proves the new pool accepted and ran the work.
        assertTrue(reported.await(5, TimeUnit.SECONDS));
        runner.stopManagedContainers();
    }

    @Test
    void trainingLogsAreForwardedToTheJobsAccount() throws InterruptedException {
        SageMakerTrainingRunner runner = dockerBackedRunner();
        TrainingJobResource job = job("account-logs", "111111111111", "us-east-1");
        SageMakerService service = mock(SageMakerService.class);
        // Two reports: the start of training, then the outcome once the unstubbed inspect fails.
        CountDownLatch reported = new CountDownLatch(2);
        doAnswer(invocation -> {
            reported.countDown();
            return true;
        }).when(service).updateTrainingJob(job);

        runner.runAsync(job, service, 0);

        assertTrue(reported.await(5, TimeUnit.SECONDS));
        verify(logStreamer).attachForAccount("111111111111", "container", "/aws/sagemaker/TrainingJobs",
                "account-logs/algo-1", "us-east-1", "sagemaker:account-logs");
        runner.stopManagedContainers();
    }

    private static SageMakerTrainingRunner runner() {
        return new SageMakerTrainingRunner(mock(ContainerBuilder.class), mock(ContainerLifecycleManager.class),
                mock(ContainerLogStreamer.class), mock(EmulatorConfig.class, RETURNS_DEEP_STUBS),
                mock(ContainerDetector.class), mock(S3Service.class), new ObjectMapper(),
                mock(SageMakerGpuResolver.class));
    }

    /** A runner that builds real container specs and drives a mocked lifecycle and Docker client. */
    private SageMakerTrainingRunner dockerBackedRunner() {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.services().dockerNetwork()).thenReturn(Optional.empty());
        when(config.services().sagemaker().dockerNetwork()).thenReturn(Optional.empty());
        when(config.docker().resourceNamespace()).thenReturn(Optional.empty());
        when(config.docker().logMaxSize()).thenReturn("10m");
        when(config.docker().logMaxFile()).thenReturn("3");
        new DockerArchiveUploadFixture(docker, "container");
        when(lifecycle.create(any(ContainerSpec.class))).thenReturn("container");
        when(lifecycle.getDockerClient()).thenReturn(docker);
        ContainerBuilder builder = new ContainerBuilder(config, mock(DockerHostResolver.class), mock(EmbeddedDnsServer.class));
        return new SageMakerTrainingRunner(builder, lifecycle, logStreamer, config, mock(ContainerDetector.class),
                mock(S3Service.class), new ObjectMapper(), mock(SageMakerGpuResolver.class));
    }

    private static TrainingJobResource job(String name, String accountId, String region) {
        TrainingJobResource job = new TrainingJobResource();
        job.trainingJobName = name;
        job.trainingJobArn = "arn:aws:sagemaker:" + region + ":" + accountId + ":training-job/" + name;
        job.accountId = accountId;
        job.region = region;
        job.algorithmSpecification = Map.of("TrainingImage", "busybox:stable");
        job.outputDataConfig = Map.of("S3OutputPath", "s3://bucket/output");
        return job;
    }
}
