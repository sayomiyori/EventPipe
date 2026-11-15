"""Explicit schema baseline; existing databases require checked adoption."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "001_legacy_baseline"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    tables = set(sa.inspect(op.get_bind()).get_table_names()) - {"alembic_version"}
    if tables:
        raise RuntimeError("Unversioned existing schema requires checked baseline adoption")
    op.create_table(
        "processed_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(64), nullable=False),
        sa.Column("source", sa.String(255), nullable=False),
        sa.Column("event_type", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=False),
        sa.Column("s3_key", sa.Text(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("enrichments", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_processed_events_event_id", "processed_events", ["event_id"], unique=True)
    op.create_index("ix_processed_events_event_type", "processed_events", ["event_type"])


def downgrade():
    op.drop_table("processed_events")
