"""add start date and end date to processor

Revision ID: 1b54730177b4
Revises: b588896794ab
Create Date: 2019-12-09 17:25:22.914227

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1b54730177b4'
down_revision = 'b588896794ab'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('processor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('end_date', sa.Date(), nullable=True))
        batch_op.add_column(sa.Column('start_date', sa.Date(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('processor', schema=None) as batch_op:
        batch_op.drop_column('start_date')
        batch_op.drop_column('end_date')

    # ### end Alembic commands ###