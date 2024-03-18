"""add user_id to project

Revision ID: 20746f8abdd2
Revises: 231fb780259e
Create Date: 2023-10-10 16:33:34.798105

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20746f8abdd2'
down_revision = '231fb780259e'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('project', schema=None) as batch_op:
        batch_op.add_column(sa.Column('user_id', sa.Integer(), nullable=True))
        batch_op.create_foreign_key(batch_op.f('fk_project_user_id_user'), 'user', ['user_id'], ['id'])

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('project', schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f('fk_project_user_id_user'), type_='foreignkey')
        batch_op.drop_column('user_id')

    # ### end Alembic commands ###