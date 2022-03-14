"""additional account ids for uploader

Revision ID: 5016ce19958d
Revises: 5104473f174a
Create Date: 2020-11-24 07:20:38.139624

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5016ce19958d'
down_revision = '5104473f174a'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('uploader', schema=None) as batch_op:
        batch_op.add_column(sa.Column('aw_account_id', sa.Text(), nullable=True))
        batch_op.add_column(sa.Column('dcm_account_id', sa.Text(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('uploader', schema=None) as batch_op:
        batch_op.drop_column('dcm_account_id')
        batch_op.drop_column('aw_account_id')

    # ### end Alembic commands ###