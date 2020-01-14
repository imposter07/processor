"""add conversion model

Revision ID: 3bff23082cc4
Revises: fe38ee2a0a3b
Create Date: 2020-01-13 18:12:48.160577

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3bff23082cc4'
down_revision = 'fe38ee2a0a3b'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('conversion',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('conversion_name', sa.Text(), nullable=True),
    sa.Column('key', sa.String(length=64), nullable=True),
    sa.Column('processor_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['processor_id'], ['processor.id'], name=op.f('fk_conversion_processor_id_processor')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_conversion'))
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('conversion')
    # ### end Alembic commands ###
