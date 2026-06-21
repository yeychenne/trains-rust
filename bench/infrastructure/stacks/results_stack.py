"""TrainsBenchResultsStack — S3 bucket for the trains-cli binary + per-node logs.

Versioned, server-side-encrypted, lifecycle 90d. Bucket policy permits
the bench instance role to read /bin/* and write /results/* — defined
in the compute stack, not here, to keep cross-stack IAM scoping local
to the resources that need it.
"""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import RemovalPolicy, Stack
from aws_cdk import aws_s3 as s3
from constructs import Construct


class TrainsBenchResultsStack(Stack):
    bucket: s3.Bucket

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.bucket = s3.Bucket(
            self, "ResultsBucket",
            # Versioning protects against an `aws s3 rm` accident — the
            # per-run results stay recoverable for 90 days.
            versioned=True,
            # Each bench deploy can be destroyed cleanly without leaving
            # orphaned data — the lifecycle rule below expires objects
            # after 90 d so even a paused bench self-cleans.
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire-old-results",
                    enabled=True,
                    expiration=cdk.Duration.days(90),
                    noncurrent_version_expiration=cdk.Duration.days(7),
                ),
            ],
        )

        cdk.CfnOutput(
            self, "ResultsBucketName",
            value=self.bucket.bucket_name,
            description=(
                "S3 bucket for bench artefacts. Set in coordinator env as "
                "TRAINS_BENCH_RESULTS_BUCKET."
            ),
            export_name="TrainsBenchResultsBucket",
        )
