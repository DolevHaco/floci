package io.github.hectorvent.floci.services.s3;

import io.github.hectorvent.floci.testing.S3EnforceAuthProfile;
import io.github.hectorvent.floci.testutil.S3RequestSigner;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static io.restassured.RestAssured.given;

/**
 * S3's own bucket-policy check, with IAM enforcement off, checks an object request as the actions
 * the enforcement filter does: the version action a versionId selects, s3:GetObject beside
 * s3:GetObjectAttributes, and the actions an object created with tags or an ACL, or copied from a
 * tagged object, needs besides s3:PutObject, offered the tag and Object Lock keys the request
 * sets. Each case is an explicit Deny in the bucket policy refusing the bucket owner.
 */
@QuarkusTest
@TestProfile(S3EnforceAuthProfile.class)
class S3ObjectOperationsBucketPolicyIntegrationTest {

    private static final S3RequestSigner OWNER = S3RequestSigner.signedAs("test", "test");

    @Test
    void aRequestNamingAVersionIsCheckedAsTheVersionAction() {
        String bucket = bucket(true);
        String versionId = owner().header("x-amz-tagging", "env=a").body("v1").when().put(bucket + "/doc")
                .then().statusCode(200).extract().header("x-amz-version-id");
        owner().queryParam("annotation", "").queryParam("annotationName", "labels").body("x")
                .when().put(bucket + "/doc").then().statusCode(200);
        policy(bucket, deny("s3:GetObjectVersionTagging", "s3:PutObjectVersionTagging",
                "s3:DeleteObjectVersionTagging", "s3:GetObjectVersionAcl", "s3:GetObjectVersionAnnotation"));

        owner().queryParam("tagging", "").queryParam("versionId", versionId).when().get(bucket + "/doc")
                .then().statusCode(403);
        owner().queryParam("tagging", "").queryParam("versionId", versionId)
                .body("<Tagging><TagSet/></Tagging>").when().put(bucket + "/doc").then().statusCode(403);
        owner().queryParam("tagging", "").queryParam("versionId", versionId).when().delete(bucket + "/doc")
                .then().statusCode(403);
        owner().queryParam("acl", "").queryParam("versionId", versionId).when().get(bucket + "/doc")
                .then().statusCode(403);
        owner().queryParam("annotation", "").queryParam("annotationName", "labels")
                .queryParam("versionId", versionId).when().get(bucket + "/doc").then().statusCode(403);

        // Without a versionId the base actions are checked, which the policy leaves alone.
        owner().queryParam("tagging", "").when().get(bucket + "/doc").then().statusCode(200);
        owner().queryParam("acl", "").when().get(bucket + "/doc").then().statusCode(200);
        owner().queryParam("annotation", "").queryParam("annotationName", "labels")
                .when().get(bucket + "/doc").then().statusCode(200);
    }

    @Test
    void getObjectAttributesNeedsTheReadAsWell() {
        String bucket = bucket(false);
        owner().body("data").when().put(bucket + "/doc").then().statusCode(200);

        policy(bucket, deny("s3:GetObject"));
        attributes(bucket + "/doc").then().statusCode(403);

        policy(bucket, deny("s3:GetObjectAttributes"));
        attributes(bucket + "/doc").then().statusCode(403);
        owner().when().get(bucket + "/doc").then().statusCode(200);
    }

    @Test
    void anObjectCreatedWithTagsOrAnAclNeedsThoseActions() {
        String bucket = bucket(false);
        policy(bucket, deny("s3:PutObjectTagging", "s3:PutObjectAcl"));

        owner().header("x-amz-tagging", "env=a").body("tagged").when().put(bucket + "/tagged")
                .then().statusCode(403);
        owner().header("x-amz-acl", "bucket-owner-full-control").body("acl").when().put(bucket + "/acl")
                .then().statusCode(403);
        owner().header("x-amz-tagging", "env=a").queryParam("uploads", "").when().post(bucket + "/upload")
                .then().statusCode(403);
        owner().body("plain").when().put(bucket + "/plain").then().statusCode(200);
    }

    @Test
    void aCopyOfATaggedObjectNeedsPutObjectTagging() {
        String bucket = bucket(false);
        owner().header("x-amz-tagging", "env=a").body("tagged").when().put(bucket + "/tagged")
                .then().statusCode(200);
        owner().body("plain").when().put(bucket + "/plain").then().statusCode(200);
        policy(bucket, deny("s3:PutObjectTagging"));

        owner().header("x-amz-copy-source", bucket + "/tagged").when().put(bucket + "/copy")
                .then().statusCode(403);
        owner().header("x-amz-copy-source", bucket + "/plain").when().put(bucket + "/copy")
                .then().statusCode(200);
    }

    @Test
    void anObjectCreationOffersItsTagAndObjectLockKeys() {
        String bucket = lockedBucket();
        policy(bucket, denyWhen("{\"StringEquals\":{\"s3:RequestObjectTag/env\":\"secret\"}}",
                "s3:PutObjectTagging")
                + "," + denyWhen("{\"StringEquals\":{\"s3:object-lock-mode\":\"COMPLIANCE\"}}",
                "s3:PutObjectRetention")
                + "," + denyWhen("{\"ForAnyValue:StringEquals\":{\"s3:RequestObjectTagKeys\":\"blocked\"}}",
                "s3:PutObject"));
        String until = Instant.now().plus(Duration.ofDays(5)).toString();

        owner().header("x-amz-tagging", "env=secret").body("x").when().put(bucket + "/secret")
                .then().statusCode(403);
        owner().header("x-amz-tagging", "env=secret").queryParam("uploads", "").when().post(bucket + "/upload")
                .then().statusCode(403);
        owner().header("x-amz-tagging", "env=open").body("x").when().put(bucket + "/open")
                .then().statusCode(200);
        owner().header("x-amz-tagging", "blocked=yes").body("x").when().put(bucket + "/blocked")
                .then().statusCode(403);
        owner().header("x-amz-object-lock-mode", "COMPLIANCE").header("x-amz-object-lock-retain-until-date", until)
                .body("x").when().put(bucket + "/compliance").then().statusCode(403);
        owner().header("x-amz-object-lock-mode", "GOVERNANCE").header("x-amz-object-lock-retain-until-date", until)
                .body("x").when().put(bucket + "/governance").then().statusCode(200);
    }

    private static RequestSpecification owner() {
        return given().filter(OWNER);
    }

    private static Response attributes(String path) {
        return owner().queryParam("attributes", "").header("x-amz-object-attributes", "ETag").when().get(path);
    }

    /** A new bucket, with versioning on when asked, as its path. */
    private static String bucket(boolean versioned) {
        String bucket = "/s3ops-policy-" + UUID.randomUUID();
        owner().when().put(bucket).then().statusCode(200);
        if (versioned) {
            owner().body("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
                    .when().put(bucket + "?versioning").then().statusCode(200);
        }
        return bucket;
    }

    /** A new bucket with Object Lock enabled, as its path. */
    private static String lockedBucket() {
        String bucket = "/s3ops-policy-" + UUID.randomUUID();
        owner().header("x-amz-bucket-object-lock-enabled", "true").when().put(bucket).then().statusCode(200);
        return bucket;
    }

    private static String denyWhen(String condition, String... actions) {
        String deny = deny(actions);
        return deny.substring(0, deny.length() - 1) + ",\"Condition\":" + condition + "}";
    }

    private static String deny(String... actions) {
        return "{\"Effect\":\"Deny\",\"Principal\":\"*\",\"Action\":[\"" + String.join("\",\"", actions)
                + "\"],\"Resource\":\"arn:aws:s3:::%s/*\"}";
    }

    private static void policy(String bucket, String statement) {
        owner().contentType("application/json")
                .body("{\"Version\":\"2012-10-17\",\"Statement\":[" + statement.replace("%s", bucket.substring(1))
                        + "]}")
                .when().put(bucket + "?policy").then().statusCode(200);
    }
}
