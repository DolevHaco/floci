package io.github.hectorvent.floci.services.kinesis;

import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.storage.InMemoryStorage;
import io.github.hectorvent.floci.services.kinesis.model.KinesisShard;
import io.github.hectorvent.floci.services.kinesis.model.KinesisStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class KinesisClosedShardRoutingServiceTest {

    private static final String REGION = "us-east-1";
    private static final String STREAM = "routing";
    private static final byte[] DATA = {1};

    private KinesisService service;

    @BeforeEach
    void setUp() {
        service = new KinesisService(new InMemoryStorage<>(), new InMemoryStorage<>(),
                new RegionResolver(REGION, "000000000000"));
    }

    @Test
    void putRecordFollowsAnOpenSplitChild() throws Exception {
        KinesisStream stream = service.createStream(STREAM, 1, REGION);
        service.putRecord(STREAM, DATA, "c", REGION);
        split(stream.getShards().getFirst());

        assertOpenDestinationAndUnchangedClosedRecords(stream, "c", openShardCovering(stream, "c"));
    }

    @Test
    void putRecordTraversesClosedSplitChildren() throws Exception {
        KinesisStream stream = splitTwoGenerations();

        assertOpenDestinationAndUnchangedClosedRecords(stream, "b", openShardCovering(stream, "b"));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void putRecordPrefersAnOpenDirectChildOverADeeperDescendant(int closedChildIndex) throws Exception {
        KinesisStream stream = service.createStream(STREAM, 1, REGION);
        split(stream.getShards().getFirst());
        split(stream.getShards().get(closedChildIndex));

        // The key lands on the one open shard whose hash range holds MD5(partitionKey),
        // whether that shard is the unsplit child or a grandchild of the split sibling.
        assertOpenDestinationAndUnchangedClosedRecords(stream, "d", openShardCovering(stream, "d"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"d", "e"})
    void putRecordTraversesEitherMergeParentThenAClosedChild(String partitionKey) throws Exception {
        KinesisStream stream = service.createStream(STREAM, 2, REGION);
        service.mergeShards(STREAM, stream.getShards().get(0).getShardId(),
                stream.getShards().get(1).getShardId(), REGION);
        split(stream.getShards().get(2));

        assertOpenDestinationAndUnchangedClosedRecords(stream, partitionKey, openShardCovering(stream, partitionKey));
    }

    @Test
    void putRecordTraversesSplitThenMergeThenSplit() throws Exception {
        KinesisStream stream = service.createStream(STREAM, 1, REGION);
        split(stream.getShards().getFirst());
        service.mergeShards(STREAM, stream.getShards().get(1).getShardId(),
                stream.getShards().get(2).getShardId(), REGION);
        split(stream.getShards().get(3));

        assertOpenDestinationAndUnchangedClosedRecords(stream, "f", openShardCovering(stream, "f"));
    }

    @Test
    void putRecordUsesAReachableChildInsteadOfAnEarlierUnrelatedOpenShard() throws Exception {
        KinesisStream stream = service.createStream(STREAM, 2, REGION);
        service.putRecordWithShardId(STREAM, DATA, "d", "0", REGION);
        split(stream.getShards().getFirst());

        assertOpenDestinationAndUnchangedClosedRecords(stream, "d", openShardCovering(stream, "d"));
        assertEquals(1, stream.getShards().get(0).recordCount());
    }

    @Test
    void putRecordRejectsAClosedShardWithoutDescendants() {
        KinesisStream stream = service.createStream(STREAM, 1, REGION);
        stream.getShards().getFirst().setClosed(true);

        assertNoDestination(stream, "b");
    }

    @Test
    void putRecordDoesNotFallBackToAnUnrelatedOpenShard() throws Exception {
        KinesisStream stream = service.createStream(STREAM, 2, REGION);
        KinesisShard closed = stream.getShards().get(0);
        KinesisShard open = stream.getShards().get(1);
        closed.setClosed(true);

        // A key in the closed shard's range is rejected. It is not written to the other open shard.
        assertNoDestination(stream, partitionKeyInRange(closed));
        assertEquals(0, open.recordCount());

        String openKey = partitionKeyInRange(open);
        KinesisService.PutRecordResult result = service.putRecordWithShardId(STREAM, DATA, openKey, REGION);
        assertEquals(open.getShardId(), result.shardId());
        assertEquals(0, closed.recordCount());
    }

    @Test
    void putRecordRejectsACycleWithoutHangingOrAppending() {
        KinesisStream stream = service.createStream(STREAM, 2, REGION);
        KinesisShard first = stream.getShards().get(0);
        KinesisShard second = stream.getShards().get(1);
        first.setClosed(true);
        second.setClosed(true);
        first.setParentShardId(second.getShardId());
        second.setAdjacentParentShardId(first.getShardId());

        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> assertNoDestination(stream, "b"));
    }

    @Test
    void explicitHashKeysKeepRoutingToOpenRangeBoundariesAfterRepeatedSplits() {
        KinesisStream stream = splitTwoGenerations();
        List<KinesisShard> openShards = stream.getShards().stream().filter(shard -> !shard.isClosed()).toList();

        for (KinesisShard shard : openShards) {
            for (String hash : List.of(shard.getHashKeyRange().startingHashKey(),
                    shard.getHashKeyRange().endingHashKey())) {
                KinesisService.PutRecordResult result = service.putRecordWithShardId(STREAM, DATA, "b", hash, REGION);
                assertEquals(shard.getShardId(), result.shardId());
            }
        }
        assertTrue(stream.getShards().stream().filter(KinesisShard::isClosed)
                .allMatch(shard -> shard.recordCount() == 0));
    }

    @Test
    void explicitAccountProducerAlsoTraversesClosedDescendants() {
        KinesisStream stream = splitTwoGenerations();

        String sequence = service.putRecordForAccount("000000000000", STREAM, DATA, "b", REGION);

        assertTrue(stream.getShards().stream().filter(KinesisShard::isClosed)
                .allMatch(shard -> shard.recordCount() == 0));
        assertTrue(stream.getShards().stream().filter(shard -> !shard.isClosed())
                .flatMap(shard -> shard.getRecords().stream())
                .anyMatch(record -> sequence.equals(record.getSequenceNumber())));
    }

    private KinesisStream splitTwoGenerations() {
        KinesisStream stream = service.createStream(STREAM, 1, REGION);
        split(stream.getShards().getFirst());
        List<KinesisShard> children = stream.getShards().stream().filter(shard -> !shard.isClosed()).toList();
        children.forEach(this::split);
        return stream;
    }

    private void split(KinesisShard shard) {
        BigInteger start = new BigInteger(shard.getHashKeyRange().startingHashKey());
        BigInteger end = new BigInteger(shard.getHashKeyRange().endingHashKey());
        service.splitShard(STREAM, shard.getShardId(), start.add(end).add(BigInteger.ONE)
                .divide(BigInteger.TWO).toString(), REGION);
    }

    private void assertOpenDestinationAndUnchangedClosedRecords(KinesisStream stream, String partitionKey,
                                                               KinesisShard expectedDestination) {
        Map<String, Integer> closedCounts = stream.getShards().stream().filter(KinesisShard::isClosed)
                .collect(Collectors.toMap(KinesisShard::getShardId, KinesisShard::recordCount));
        KinesisService.PutRecordResult result = service.putRecordWithShardId(STREAM, DATA, partitionKey, REGION);
        KinesisShard destination = stream.getShards().stream()
                .filter(shard -> result.shardId().equals(shard.getShardId())).findFirst().orElseThrow();

        assertFalse(destination.isClosed());
        assertEquals(expectedDestination.getShardId(), result.shardId());
        assertEquals(result.sequenceNumber(), destination.getRecords().getLast().getSequenceNumber());
        stream.getShards().stream().filter(KinesisShard::isClosed)
                .forEach(shard -> assertEquals(closedCounts.get(shard.getShardId()).intValue(), shard.recordCount()));
    }

    private void assertNoDestination(KinesisStream stream, String partitionKey) {
        AwsException error = assertThrows(AwsException.class,
                () -> service.putRecordWithShardId(STREAM, DATA, partitionKey, REGION));
        assertEquals("InternalFailure", error.getErrorCode());
        assertEquals(500, error.getHttpStatus());
        assertTrue(stream.getShards().stream().allMatch(shard -> shard.recordCount() == 0));
    }

    private static KinesisShard openShardCovering(KinesisStream stream, String partitionKey) throws Exception {
        BigInteger hashKey = md5HashKey(partitionKey);
        List<KinesisShard> covering = stream.getShards().stream()
                .filter(shard -> !shard.isClosed())
                .filter(shard -> covers(shard, hashKey))
                .toList();
        assertEquals(1, covering.size(), partitionKey);
        return covering.getFirst();
    }

    private static String partitionKeyInRange(KinesisShard shard) throws Exception {
        for (int i = 0; i < 10_000; i++) {
            String key = "range-" + i;
            if (covers(shard, md5HashKey(key))) {
                return key;
            }
        }
        fail("no partition key fell in " + shard.getShardId());
        return "";
    }

    private static boolean covers(KinesisShard shard, BigInteger hashKey) {
        BigInteger start = new BigInteger(shard.getHashKeyRange().startingHashKey());
        BigInteger end = new BigInteger(shard.getHashKeyRange().endingHashKey());
        return hashKey.compareTo(start) >= 0 && hashKey.compareTo(end) <= 0;
    }

    private static BigInteger md5HashKey(String partitionKey) throws Exception {
        return new BigInteger(1, MessageDigest.getInstance("MD5")
                .digest(partitionKey.getBytes(StandardCharsets.UTF_8)));
    }
}
