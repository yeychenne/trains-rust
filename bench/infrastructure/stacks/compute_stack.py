"""TrainsBenchComputeStack — EC2 ring nodes + coordinator + IAM.

Provisions `ring_size` instances tagged Role=ring + NodeId=N, plus
one coordinator instance tagged Role=coordinator. All instances share
an IAM instance profile granting SSM Managed Instance Core + scoped
S3 access on the results bucket only.

The coordinator's UserData clones the TRAINS source, builds
`trains-cli` (arm64 native, since the coordinator is itself arm64),
and uploads the binary to s3://<results-bucket>/bin/trains. Ring
nodes' UserData is minimal — they wait for SSM RunCommand from the
coordinator to start the bench.
"""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constructs import Construct

# Pinned upstream commit so the bench is reproducible. Operator updates
# this when intentionally re-benching against a newer TRAINS revision.
TRAINS_REPO_URL = "https://github.com/yeychenne/trains-rust.git"
TRAINS_REPO_REF = "main"


class TrainsBenchComputeStack(Stack):
    coordinator: ec2.Instance
    ring_nodes: list[ec2.Instance]

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.Vpc,
        security_group: ec2.SecurityGroup,
        placement_group_name: str,
        ring_size: int,
        instance_type_str: str,
        ring_instance_type_str: str | None,
        results_bucket: s3.Bucket,
        az_spread: str,
        enable_ena_express: bool,
        trains_max_frame_len_mb: int = 16,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The ring nodes may use a different (larger) instance type
        # than the coordinator. Default: same as instance_type_str.
        ring_instance_type_str = ring_instance_type_str or instance_type_str

        # ── AMI: pinned AL2023 arm64 per region ───────────────────────
        # We pin AMI IDs directly (vs. MachineImage.from_ssm_parameter or
        # latestAmazonLinux2023) because this account's IAM environment
        # denies `ssm:GetParameter` on the public `/aws/service/*`
        # namespace ("No access to '/aws/' namespace" — observed
        # 2026-05-23 on first Compute deploy attempt). Likely an
        # org-level SCP overriding AdministratorAccess for the
        # `/aws/` SSM tree.
        #
        # Resolve a fresh AMI ID with:
        #   aws ec2 describe-images --owners amazon --region <region> \
        #     --filters "Name=name,Values=al2023-ami-2023.*-kernel-*-arm64" \
        #               "Name=architecture,Values=arm64" \
        #               "Name=state,Values=available" \
        #               "Name=virtualization-type,Values=hvm" \
        #     --query 'reverse(sort_by(Images, &CreationDate))[:3].[CreationDate,ImageId,Name]'
        # These IDs are stale by design — refresh every ~2 months or
        # when AL2023 publishes a security update we want to pick up.
        AL2023_ARM64_AMIS = {
            # al2023-ami-2023.11.20260514.0-kernel-6.18-arm64 (2026-05-15)
            "us-east-1": "ami-015be099dd3d0d058",
        }
        AL2023_X86_64_AMIS = {
            # al2023-ami-2023.11.20260514.0-kernel-6.18-x86_64 (2026-05-15)
            "us-east-1": "ami-02b2c1b57c5105166",
        }

        # Arch detection by instance-type family prefix.
        # Graviton families end in g/gn/gd → arm64.
        # Intel/AMD families: c7i, c6i, m7i, m6i, r7i, r6i, c7a, m7a, r7a → x86_64.
        def _arch_for(instance_type_str: str) -> str:
            t = instance_type_str.split(".")[0].lower()
            arm64_families = {
                "t4g", "c7g", "c7gn", "c7gd",
                "m7g", "m7gd", "r7g", "r7gd",
                "c8g", "m8g", "r8g",
            }
            return "arm64" if t in arm64_families else "x86_64"

        coord_arch = _arch_for(instance_type_str)
        ring_arch = _arch_for(ring_instance_type_str)

        # We support mixed-arch deployments BUT not for the binary
        # path: coordinator's UserData builds + uploads ONE binary, ring
        # nodes download the same. If arches differ, the binary won't
        # run on one side. Refuse that config — operator must pick a
        # single arch.
        if coord_arch != ring_arch:
            raise SystemExit(
                f"FATAL: mixed arch — coordinator={instance_type_str} ({coord_arch}) "
                f"vs ring={ring_instance_type_str} ({ring_arch}). The build pipeline "
                f"produces ONE binary; both must match. Pick consistent instance types."
            )

        ami_dict = AL2023_X86_64_AMIS if coord_arch == "x86_64" else AL2023_ARM64_AMIS
        if self.region not in ami_dict:
            raise SystemExit(
                f"FATAL: no pinned AL2023 {coord_arch} AMI for region {self.region}."
            )
        ami = ec2.MachineImage.generic_linux(ami_dict)

        coordinator_instance_type = ec2.InstanceType(instance_type_str)
        ring_instance_type = ec2.InstanceType(ring_instance_type_str)

        # ── IAM role: SSM Managed + scoped S3 ─────────────────────────
        role = iam.Role(
            self, "BenchInstanceRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            description="trains-bench: SSM + scoped S3 (read bin, write results)",
        )
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonSSMManagedInstanceCore"
            )
        )
        # Allow read of /bin/* and write of /results/* on the bucket.
        # No `s3:DeleteObject` — the lifecycle rule handles aging.
        results_bucket.grant_read(role, "bin/*")
        results_bucket.grant_put(role, "results/*")
        results_bucket.grant_put(role, "identities/*")
        results_bucket.grant_read(role, "identities/*")

        if enable_ena_express:
            # Ring nodes self-enable ENA Express SRD on their primary
            # NIC during UserData via ModifyNetworkInterfaceAttribute.
            role.add_to_policy(
                iam.PolicyStatement(
                    actions=["ec2:ModifyNetworkInterfaceAttribute"],
                    resources=["*"],
                )
            )

        # Coordinator additionally needs ec2:DescribeInstances (peer
        # discovery), ssm:SendCommand (bench trigger), s3:ListBucket
        # (idempotent uploads), and ce:GetCostAndUsage (budget check).
        coordinator_extra = iam.Policy(
            self, "CoordinatorExtraPolicy",
            statements=[
                iam.PolicyStatement(
                    actions=["ec2:DescribeInstances"],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "ssm:SendCommand",
                        "ssm:GetCommandInvocation",
                        "ssm:ListCommandInvocations",
                    ],
                    resources=["*"],  # SSM SendCommand requires "*" for documents
                ),
                iam.PolicyStatement(
                    actions=["s3:ListBucket"],
                    resources=[results_bucket.bucket_arn],
                ),
                iam.PolicyStatement(
                    actions=["ce:GetCostAndUsage"],
                    resources=["*"],
                ),
            ],
        )

        # Separate role for coordinator so its extra perms don't leak
        # to ring nodes (least privilege).
        coordinator_role = iam.Role(
            self, "CoordinatorRole",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            description="trains-bench coordinator: SSM + EC2 discover + Cost Explorer",
        )
        coordinator_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonSSMManagedInstanceCore"
            )
        )
        results_bucket.grant_read_write(coordinator_role)
        coordinator_extra.attach_to_role(coordinator_role)

        # ── Coordinator UserData ──────────────────────────────────────
        # Build trains-cli on first boot, upload to S3. Idempotent: a
        # `cdk deploy` re-run doesn't rebuild unless the binary is gone.
        # The bench supports compile-time TRAINS_RING_SIZE / TRAINS_NUM_TRAINS
        # via build.rs. We bake them into the coordinator's build so the
        # binary matches the deployed ring size. Operators can override
        # via the `trainsRingSize` context var (default = ring_size).
        trains_build_ring_size = ring_size
        trains_build_num_trains = 2  # matches default; both nodes 0 + 1 issue
        coordinator_userdata = ec2.UserData.for_linux()
        coordinator_userdata.add_commands(
            "set -euxo pipefail",
            # Cloud-init doesn't always export HOME → `source $HOME/.cargo/env`
            # under `set -u` trips. Discovered first deploy attempt 2026-05-23.
            "export HOME=/root",
            "export CARGO_HOME=/root/.cargo",
            "export PATH=/root/.cargo/bin:$PATH",
            # 2 GB swap. Required because t4g.micro coord has only 1 GB
            # RAM and `cargo build --release --bin trains` peaks ~1 GB
            # per rustc process — OOM-killed observed 2026-05-23 on
            # T4G-ring-3-bw (Phase E builds were lucky; pushing
            # CARGO_BUILD_JOBS=1 also works but swap is more general
            # and protects Phase G chaos runs too). Larger coords
            # (c7gn.medium, c7i.large) don't strictly need this but
            # fallocate is ~1 sec at boot and harmless if unused.
            "fallocate -l 2G /swapfile && chmod 600 /swapfile && "
            "mkswap /swapfile && swapon /swapfile && "
            "echo '/swapfile none swap sw 0 0' >> /etc/fstab",
            # Belt-and-braces: cap cargo parallelism on tiny coords so
            # we don't spike past swap either. t4g.micro = 1 vCPU so
            # this is effectively a no-op there but avoids 8 concurrent
            # rustc instances on a larger coord with little RAM.
            "export CARGO_BUILD_JOBS=1",
            "dnf install -y gcc git tar gzip",
            # Rust toolchain (native to whatever arch the coordinator is).
            "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | "
            "sh -s -- -y --default-toolchain stable --profile minimal",
            # Pull TRAINS source. If the upstream repo is PUBLIC, prefer
            # git clone (always-fresh HEAD). If PRIVATE, the operator
            # must pre-stage a tarball at s3://<results-bucket>/src/
            # trains-src.tar.gz (see bench/scripts/stage-source.sh).
            # Try git first, fall back to S3 tarball.
            f"if git clone --depth 1 --branch {TRAINS_REPO_REF} {TRAINS_REPO_URL} /opt/trains-src 2>/dev/null; then "
            f"  echo 'TRAINS_SOURCE=git'; "
            f"else "
            f"  echo 'TRAINS_SOURCE=s3 (repo private — falling back to staged tarball)'; "
            f"  mkdir -p /opt/trains-src; "
            f"  aws s3 cp s3://{results_bucket.bucket_name}/src/trains-src.tar.gz /tmp/trains-src.tar.gz; "
            f"  tar xzf /tmp/trains-src.tar.gz -C /opt/trains-src; "
            f"fi",
            "cd /opt/trains-src",
            # Bake the compile-time ring-size constants into the binary.
            # trains-core panics if --id ≥ RING_SIZE; the binary must
            # match the deployed ring. (build.rs reads these env vars.)
            f"export TRAINS_RING_SIZE={trains_build_ring_size}",
            f"export TRAINS_NUM_TRAINS={trains_build_num_trains}",
            # 2026-05-23: MAX_FRAME_LEN is now compile-time tunable via
            # TRAINS_MAX_FRAME_LEN_MB. 16 MB default = t4g.medium-sized
            # NIC headroom; raise for big-NIC bandwidth saturation runs
            # (c7gn.8xlarge / c7i.16xlarge with ENA Express).
            f"export TRAINS_MAX_FRAME_LEN_MB={trains_max_frame_len_mb}",
            "cargo build --release --bin trains",
            # Upload to S3 (idempotent: SSE-S3 + Versioning preserve history)
            f"aws s3 cp target/release/trains s3://{results_bucket.bucket_name}/bin/trains",
            # Smoke-check: trains --version must run
            "target/release/trains --version > /tmp/trains-version.txt 2>&1 || true",
            f"aws s3 cp /tmp/trains-version.txt s3://{results_bucket.bucket_name}/coordinator/trains-version.txt",
            # Mark coordinator ready
            "touch /var/lib/trains-bench-coordinator-ready",
        )

        # ── Ring node UserData ─────────────────────────────────────────
        # Install iperf3 (now possible with NAT), prep dirs, optionally
        # enable ENA Express SRD on the primary network interface.
        # ENA Express requires:
        #   - Instance type that supports it (c7gn.*, c7i.16xlarge+, etc)
        #   - Same-AZ + cluster placement group (we have that for
        #     azSpread=single)
        #   - Explicit enable per-NIC via aws ec2 modify-network-interface-attribute
        # Both peers of a flow must have it enabled for SRD to kick in.
        ring_userdata = ec2.UserData.for_linux()
        ring_userdata.add_commands(
            "set -euxo pipefail",
            "export HOME=/root",
            # iperf3 (legacy) + sockperf (the new baseline tool —
            # see bench research 2026-05-24: iperf3 measures bulk
            # bandwidth, sockperf measures sub-ns percentile latency
            # with ping-pong + under-load modes). sockperf isn't in
            # AL2023 default repos; build from source — small, fast.
            "dnf install -y iperf3 gcc gcc-c++ make autoconf automake libtool git",
            "mkdir -p /opt/trains-bench /var/log/trains-bench /opt/sockperf-src",
            # Build sockperf (one-time, ~1 min on a fast instance):
            "git clone --depth 1 https://github.com/Mellanox/sockperf.git /opt/sockperf-src || true",
            "(cd /opt/sockperf-src && ./autogen.sh && ./configure --prefix=/usr/local && make -j$(nproc) && make install) "
            "|| echo 'sockperf build failed; continuing — bench will skip baseline'",
            "sockperf --version 2>&1 | head -1 || true",
        )
        if enable_ena_express:
            # Enable ENA Express on the primary network interface.
            # Discover ENI id via IMDSv2, then call modify-NIC-attribute.
            ring_userdata.add_commands(
                "TOKEN=$(curl -sf -X PUT 'http://169.254.169.254/latest/api/token' -H 'X-aws-ec2-metadata-token-ttl-seconds: 60')",
                "ENI=$(curl -sf -H \"X-aws-ec2-metadata-token: $TOKEN\" http://169.254.169.254/latest/meta-data/network/interfaces/macs/ | head -1 | tr -d /)",
                "ENI_ID=$(curl -sf -H \"X-aws-ec2-metadata-token: $TOKEN\" http://169.254.169.254/latest/meta-data/network/interfaces/macs/${ENI}/interface-id)",
                "REGION=$(curl -sf -H \"X-aws-ec2-metadata-token: $TOKEN\" http://169.254.169.254/latest/meta-data/placement/region)",
                "echo \"Enabling ENA Express on ENI=$ENI_ID in region=$REGION\"",
                "aws ec2 modify-network-interface-attribute --region $REGION --network-interface-id $ENI_ID --ena-srd-specification 'EnaSrdEnabled=true,EnaSrdUdpSpecification={EnaSrdUdpEnabled=true}'",
                "echo ENA_EXPRESS_ENABLED",
                # Pull + run AWS's official validation script for ENA
                # Express tunings (MTU, BQL, autocork, TCP buffers,
                # congestion control). Writes report to
                # /var/log/ena-express-check.log for forensics.
                "curl -sfo /opt/trains-bench/check-ena-express-settings.sh "
                "https://raw.githubusercontent.com/amzn/amzn-ec2-ena-utilities/main/ena-express/check-ena-express-settings.sh "
                "|| echo 'check-ena-express-settings.sh download failed'",
                "chmod +x /opt/trains-bench/check-ena-express-settings.sh 2>/dev/null || true",
                "/opt/trains-bench/check-ena-express-settings.sh > /var/log/ena-express-check.log 2>&1 "
                "|| echo 'ENA Express settings check non-fatal warnings (see /var/log/ena-express-check.log)'",
            )
        ring_userdata.add_commands(
            "touch /var/lib/trains-bench-node-ready",
        )

        # ── Coordinator instance ─────────────────────────────────────
        self.coordinator = ec2.Instance(
            self, "Coordinator",
            vpc=vpc,
            instance_type=coordinator_instance_type,
            machine_image=ami,
            role=coordinator_role,
            security_group=security_group,
            user_data=coordinator_userdata,
            require_imdsv2=True,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
        )
        cdk.Tags.of(self.coordinator).add("Project", "trains-bench")
        cdk.Tags.of(self.coordinator).add("Role", "coordinator")
        cdk.Tags.of(self.coordinator).add("Name", "trains-bench-coordinator")

        # ── Ring nodes ───────────────────────────────────────────────
        self.ring_nodes = []
        subnets = vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        ).subnets

        for i in range(ring_size):
            # Round-robin subnets — for single-AZ all nodes land in the
            # one subnet; for 3-AZ they distribute across us-east-1a/b/c.
            subnet = subnets[i % len(subnets)]
            node = ec2.Instance(
                self, f"RingNode{i}",
                vpc=vpc,
                instance_type=ring_instance_type,
                machine_image=ami,
                role=role,
                security_group=security_group,
                user_data=ring_userdata,
                require_imdsv2=True,
                vpc_subnets=ec2.SubnetSelection(subnets=[subnet]),
            )
            cdk.Tags.of(node).add("Project", "trains-bench")
            cdk.Tags.of(node).add("Role", "ring")
            cdk.Tags.of(node).add("NodeId", str(i))
            cdk.Tags.of(node).add("Name", f"trains-bench-node-{i}")
            self.ring_nodes.append(node)

        # ── Outputs ──────────────────────────────────────────────────
        cdk.CfnOutput(
            self, "CoordinatorInstanceId",
            value=self.coordinator.instance_id,
            description="SSM target for operator: aws ssm start-session --target <id>",
            export_name="TrainsBenchCoordinatorId",
        )
        cdk.CfnOutput(
            self, "RingNodeIds",
            value=",".join(n.instance_id for n in self.ring_nodes),
            description="Comma-separated instance IDs of ring nodes",
        )
        cdk.CfnOutput(
            self, "RingSize",
            value=str(ring_size),
        )
        cdk.CfnOutput(
            self, "AzSpread",
            value=az_spread,
        )
        cdk.CfnOutput(
            self, "InstanceType",
            value=instance_type_str,
        )
        cdk.CfnOutput(
            self, "TrainsRepoRef",
            value=TRAINS_REPO_REF,
            description="Git ref of the TRAINS source built by the coordinator",
        )
