"""added follow processor

Revision ID: 8e96e5364b0e
Revises: dc928299dedf
Create Date: 2020-01-14 20:35:31.515614

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8e96e5364b0e'
down_revision = 'dc928299dedf'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('processor_followers',
    sa.Column('follower_id', sa.Integer(), nullable=True),
    sa.Column('followed_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['followed_id'], ['processor.id'], name=op.f('fk_processor_followers_followed_id_processor')),
    sa.ForeignKeyConstraint(['follower_id'], ['user.id'], name=op.f('fk_processor_followers_follower_id_user'))
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('processor_followers')
    # ### end Alembic commands ###
