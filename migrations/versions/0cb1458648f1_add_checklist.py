"""add checklist

Revision ID: 0cb1458648f1
Revises: 50146fa3b6f9
Create Date: 2024-03-07 13:23:46.866544

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0cb1458648f1'
down_revision = '50146fa3b6f9'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('checklist',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('plan_id', sa.Integer(), nullable=True),
    sa.Column('name', sa.Text(), nullable=True),
    sa.Column('completed_at', sa.DateTime(), nullable=True),
    sa.Column('completed_by', sa.Integer(), nullable=True),
    sa.Column('complete_msg', sa.Text(), nullable=True),
    sa.Column('checked_at', sa.DateTime(), nullable=True),
    sa.Column('checked_by', sa.Integer(), nullable=True),
    sa.Column('checked_msg', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['checked_by'], ['user.id'], name=op.f('fk_checklist_checked_by_user')),
    sa.ForeignKeyConstraint(['completed_by'], ['user.id'], name=op.f('fk_checklist_completed_by_user')),
    sa.ForeignKeyConstraint(['plan_id'], ['plan.id'], name=op.f('fk_checklist_plan_id_plan')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_checklist'))
    )
    with op.batch_alter_table('checklist', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_checklist_checked_by'), ['checked_by'], unique=False)
        batch_op.create_index(batch_op.f('ix_checklist_completed_by'), ['completed_by'], unique=False)
        batch_op.create_index(batch_op.f('ix_checklist_plan_id'), ['plan_id'], unique=False)

    with op.batch_alter_table('account', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_account_processor_id'), ['processor_id'], unique=False)

    with op.batch_alter_table('dashboard', schema=None) as batch_op:
        batch_op.add_column(sa.Column('include_in_report', sa.Boolean(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('dashboard', schema=None) as batch_op:
        batch_op.drop_column('include_in_report')

    with op.batch_alter_table('account', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_account_processor_id'))

    with op.batch_alter_table('checklist', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_checklist_plan_id'))
        batch_op.drop_index(batch_op.f('ix_checklist_completed_by'))
        batch_op.drop_index(batch_op.f('ix_checklist_checked_by'))

    op.drop_table('checklist')
    # ### end Alembic commands ###
