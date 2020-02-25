"""add ratecard

Revision ID: c46817910e3a
Revises: 4e066ff7a62c
Create Date: 2020-01-12 18:36:48.569776

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c46817910e3a'
down_revision = '4e066ff7a62c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('rate_card',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('owner_id', sa.Integer(), nullable=True),
    sa.Column('name', sa.Text(), nullable=True),
    sa.Column('description', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['owner_id'], ['user.id'], name=op.f('fk_rate_card_owner_id_user')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_rate_card'))
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('rate_card')
    # ### end Alembic commands ###