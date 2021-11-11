"""added misc data column to walkthrough

Revision ID: 09684af95b86
Revises: 59c31df47970
Create Date: 2021-11-10 15:41:21.172911

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '09684af95b86'
down_revision = '59c31df47970'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('walkthrough_slide', schema=None) as batch_op:
        batch_op.add_column(sa.Column('data', sa.Text(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('walkthrough_slide', schema=None) as batch_op:
        batch_op.drop_column('data')

    # ### end Alembic commands ###
