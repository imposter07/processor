"""update password_hash

Revision ID: 50b54e3b0d33
Revises: 11f5dbfe1ec8
Create Date: 2024-06-12 19:06:23.751466

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '50b54e3b0d33'
down_revision = '11f5dbfe1ec8'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('brandtracker', schema=None) as batch_op:
        batch_op.add_column(sa.Column('plan_id', sa.Integer(), nullable=True))
        batch_op.create_foreign_key(batch_op.f('fk_brandtracker_plan_id_plan'), 'plan', ['plan_id'], ['id'])

    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.alter_column('password_hash',
               existing_type=sa.VARCHAR(length=128),
               type_=sa.Text(),
               existing_nullable=True)

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.alter_column('password_hash',
               existing_type=sa.Text(),
               type_=sa.VARCHAR(length=128),
               existing_nullable=True)

    with op.batch_alter_table('brandtracker', schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f('fk_brandtracker_plan_id_plan'), type_='foreignkey')
        batch_op.drop_column('plan_id')

    # ### end Alembic commands ###