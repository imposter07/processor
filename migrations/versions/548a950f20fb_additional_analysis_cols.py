"""additional analysis cols

Revision ID: 548a950f20fb
Revises: 0fffff1de9f6
Create Date: 2020-07-28 15:49:41.045122

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '548a950f20fb'
down_revision = '0fffff1de9f6'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('processor_analysis', schema=None) as batch_op:
        batch_op.add_column(sa.Column('filter_col', sa.Text(), nullable=True))
        batch_op.add_column(sa.Column('filter_val', sa.Text(), nullable=True))
        batch_op.add_column(sa.Column('split_col', sa.Text(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('processor_analysis', schema=None) as batch_op:
        batch_op.drop_column('split_col')
        batch_op.drop_column('filter_val')
        batch_op.drop_column('filter_col')

    # ### end Alembic commands ###