"""Nullable tenant context and sanitized deduplicated failure counts."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "002_tenant_boundary"
down_revision = "001_legacy_baseline"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("processed_events", sa.Column("tenant_id", postgresql.UUID(as_uuid=True)))
    op.add_column("processed_events", sa.Column("raw_sha256", sa.String(64)))
    op.create_index("ix_processed_events_tenant_id", "processed_events", ["tenant_id"])
    op.create_table(
        "failed_events",
        sa.Column("topic", sa.String(255), primary_key=True),
        sa.Column("partition", sa.Integer(), primary_key=True),
        sa.Column("offset", sa.BigInteger(), primary_key=True),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True)),
        sa.Column("error", sa.String(128), nullable=False),
        sa.Column("record_sha256", sa.String(64), nullable=False),
        sa.Column("failed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_failed_events_tenant_id", "failed_events", ["tenant_id"])


def downgrade():
    bind = op.get_bind()
    if bind.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM processed_events WHERE tenant_id IS NOT NULL)")):
        raise RuntimeError("Downgrade refused: tenant-scoped records require explicit reconciliation")
    op.drop_table("failed_events")
    op.drop_index("ix_processed_events_tenant_id", table_name="processed_events")
    op.drop_column("processed_events", "raw_sha256")
    op.drop_column("processed_events", "tenant_id")
