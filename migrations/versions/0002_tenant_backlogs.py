"""Give tenants independent pending-row budgets."""

import sqlalchemy as sa
from alembic import op

revision = "0002_tenant_backlogs"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tenant_backlogs",
        sa.Column(
            "tenant_id", sa.Uuid(), sa.ForeignKey("tenants.id"), primary_key=True
        ),
        sa.Column("lane", sa.String(length=10), primary_key=True),
        sa.Column("pending_rows", sa.Integer(), nullable=False),
        sa.CheckConstraint("pending_rows >= 0"),
    )
    op.execute("""
        INSERT INTO tenant_backlogs (tenant_id, lane, pending_rows)
        SELECT r.tenant_id, r.kind, SUM(j.row_count)
        FROM jobs j JOIN import_receipts r ON r.id = j.import_id
        WHERE j.state = 'pending'
        GROUP BY r.tenant_id, r.kind
    """)


def downgrade() -> None:
    op.drop_table("tenant_backlogs")
