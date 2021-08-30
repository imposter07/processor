"""add description to tutorial

Revision ID: 24ab337f83c4
Revises: a64e3b680f0c
Create Date: 2021-08-30 16:59:24.945403

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '24ab337f83c4'
down_revision = 'a64e3b680f0c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('tutorial', schema=None) as batch_op:
        batch_op.add_column(sa.Column('description', sa.Text(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('tutorial', schema=None) as batch_op:
        batch_op.drop_column('description')

    # ### end Alembic commands ###
