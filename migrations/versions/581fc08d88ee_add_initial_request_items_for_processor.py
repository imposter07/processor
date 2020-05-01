"""add initial request items for processor

Revision ID: 581fc08d88ee
Revises: 1b54730177b4
Create Date: 2019-12-18 16:08:22.854415

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '581fc08d88ee'
down_revision = '1b54730177b4'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('processor', schema=None) as batch_op:
        batch_op.add_column(sa.Column('first_report_', sa.Date(), nullable=True))
        batch_op.add_column(sa.Column('plan_path', sa.Text(), nullable=True))
        batch_op.add_column(sa.Column('requesting_user_id', sa.Integer(), nullable=True))
        batch_op.create_foreign_key(batch_op.f('fk_processor_requesting_user_id_user'), 'user', ['requesting_user_id'], ['id'])

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('processor', schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f('fk_processor_requesting_user_id_user'), type_='foreignkey')
        batch_op.drop_column('requesting_user_id')
        batch_op.drop_column('plan_path')
        batch_op.drop_column('first_report_')

    # ### end Alembic commands ###