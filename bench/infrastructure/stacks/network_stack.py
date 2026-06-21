"""TrainsBenchNetworkStack — VPC, subnets, security group, placement group.

Single-AZ vs 3-AZ is a deploy-time choice (CDK context `azSpread`):
- single — one subnet in us-east-1a, Cluster Placement Group
  (low-latency, same physical rack class)
- three  — three subnets across us-east-1a/b/c, Spread Placement Group
  (one node per AZ for ring sizes ≤ 3; for larger rings, round-robin)

No internet gateway. No NAT. All bench instance access is through SSM
Session Manager + RunCommand. The VPC has interface endpoints for SSM,
SSM Messages, EC2 Messages, and an S3 gateway endpoint so the bench
can pull the binary + upload results without an IGW.
"""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import Stack
from aws_cdk import aws_ec2 as ec2
from constructs import Construct

VPC_CIDR = "10.50.0.0/16"
RING_PORT = 7777


class TrainsBenchNetworkStack(Stack):
    vpc: ec2.Vpc
    security_group: ec2.SecurityGroup
    placement_group: ec2.PlacementGroup

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        az_spread: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── VPC ──────────────────────────────────────────────────────
        # `max_azs=1` for single-AZ keeps the subnet provisioning tight.
        # For 3-AZ we want exactly 3 — CDK's default of "use all AZs in
        # the region" would pull 6 in us-east-1, which is wasteful for
        # a bench rig.
        #
        # NAT gateway design (added 2026-05-23 afternoon, operator
        # decision): we add 1 NAT gateway in a public subnet so bench
        # instances (in a PRIVATE_WITH_EGRESS subnet) can reach the
        # internet for `rustup`, `git clone`, `dnf install iperf3`.
        # Cost: ~$0.045/hr while running — trivial for a <1h bench.
        # Without NAT, the coordinator UserData failed yesterday on
        # `curl https://sh.rustup.rs` (no internet egress), forcing a
        # manual binary-stage workaround. NAT eliminates that step.
        # Interface endpoints for SSM stay (cheaper than routing SSM
        # traffic through NAT and decouples SSM from internet outages).
        max_azs = 1 if az_spread == "single" else 3
        self.vpc = ec2.Vpc(
            self, "BenchVpc",
            ip_addresses=ec2.IpAddresses.cidr(VPC_CIDR),
            max_azs=max_azs,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="bench-public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="bench-private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
            enable_dns_hostnames=True,
            enable_dns_support=True,
        )

        # ── VPC endpoints (SSM access — keep even with NAT) ──────────
        # SSM via interface endpoints stays free of NAT data charges
        # and provides isolation from the public internet for the
        # control plane. NAT only carries the rust install + git pulls.
        for service, suffix in (
            (ec2.InterfaceVpcEndpointAwsService.SSM, "Ssm"),
            (ec2.InterfaceVpcEndpointAwsService.SSM_MESSAGES, "SsmMessages"),
            (ec2.InterfaceVpcEndpointAwsService.EC2_MESSAGES, "Ec2Messages"),
        ):
            self.vpc.add_interface_endpoint(
                f"{suffix}Endpoint",
                service=service,
                private_dns_enabled=True,
            )

        # S3 gateway endpoint — required for `aws s3 cp` to the results
        # bucket. Gateway endpoints are free (interface ones bill per hr).
        self.vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        # ── Security group ──────────────────────────────────────────
        # Allow all-to-all inside the SG (ring nodes + coordinator
        # talk on the ring port; SSM agent talks to the SSM endpoints
        # over the VPC's primary CIDR; iperf3 uses ephemeral ports).
        # Deny all ingress from outside the SG.
        # NOTE: EC2 SecurityGroup descriptions must be ASCII only.
        # An em-dash here caused CREATE_FAILED on 2026-05-22 first deploy
        # ("Character sets beyond ASCII are not supported"). Keep this
        # field plain ASCII forever.
        self.security_group = ec2.SecurityGroup(
            self, "BenchSg",
            vpc=self.vpc,
            description="trains-bench ring + coordinator (intra-SG all-allow)",
            allow_all_outbound=True,
        )
        # `connections.allow_internally(...)` is a CDK convention for
        # the all-to-all-inside-the-SG pattern — adds an ingress rule
        # with self-source.
        self.security_group.connections.allow_internally(
            ec2.Port.all_traffic(),
            description="ring nodes + coordinator + iperf3",
        )

        # ── Placement group ─────────────────────────────────────────
        # L2 ec2.PlacementGroup (CDK 2.122+) exposes
        # placement_group_name for cross-stack reference. Strategy is
        # CLUSTER (single-AZ, low-latency, same rack class) or SPREAD
        # (3-AZ, max one node per rack — fault isolation).
        if az_spread == "single":
            strategy = ec2.PlacementGroupStrategy.CLUSTER
            spread_level = None
        else:
            strategy = ec2.PlacementGroupStrategy.SPREAD
            spread_level = ec2.PlacementGroupSpreadLevel.RACK
        self.placement_group = ec2.PlacementGroup(
            self, "BenchPlacementGroup",
            strategy=strategy,
            spread_level=spread_level,
        )
        placement_strategy = strategy.value

        cdk.CfnOutput(
            self, "VpcId",
            value=self.vpc.vpc_id,
            description="VPC id for trains-bench",
            export_name="TrainsBenchVpcId",
        )
        cdk.CfnOutput(
            self, "SecurityGroupId",
            value=self.security_group.security_group_id,
            description="Security group for trains-bench",
            export_name="TrainsBenchSgId",
        )
        cdk.CfnOutput(
            self, "PlacementStrategy",
            value=placement_strategy,
            description="Placement strategy in effect for this deploy",
        )
