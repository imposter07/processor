"""add rfp models

Revision ID: beefba65fb1f
Revises: 3f65f3507252
Create Date: 2023-09-05 15:46:48.659135

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'beefba65fb1f'
down_revision = '3f65f3507252'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('rfp_file',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('plan_id', sa.Integer(), nullable=True),
    sa.Column('user_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('name', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['plan_id'], ['plan.id'], name=op.f('fk_rfp_file_plan_id_plan')),
    sa.ForeignKeyConstraint(['user_id'], ['user.id'], name=op.f('fk_rfp_file_user_id_user')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_rfp_file'))
    )
    op.create_table('rfp',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('partner_name', sa.Text(), nullable=True),
    sa.Column('package_name_description', sa.Text(), nullable=True),
    sa.Column('placement_name_description', sa.Text(), nullable=True),
    sa.Column('ad_size_wxh', sa.Text(), nullable=True),
    sa.Column('ad_type', sa.Text(), nullable=True),
    sa.Column('device', sa.Text(), nullable=True),
    sa.Column('country', sa.Text(), nullable=True),
    sa.Column('start_date', sa.Date(), nullable=True),
    sa.Column('end_date', sa.Date(), nullable=True),
    sa.Column('buy_model', sa.Text(), nullable=True),
    sa.Column('planned_impressions', sa.Integer(), nullable=True),
    sa.Column('planned_units', sa.Text(), nullable=True),
    sa.Column('cpm_cost_per_unit', sa.Numeric(), nullable=True),
    sa.Column('planned_net_cost', sa.Numeric(), nullable=True),
    sa.Column('planned_sov', sa.Numeric(), nullable=True),
    sa.Column('reporting_source', sa.Text(), nullable=True),
    sa.Column('ad_serving_type', sa.Text(), nullable=True),
    sa.Column('targeting', sa.Text(), nullable=True),
    sa.Column('placement_phase', sa.Text(), nullable=True),
    sa.Column('placement_objective', sa.Text(), nullable=True),
    sa.Column('kpi', sa.Text(), nullable=True),
    sa.Column('sizmek_id', sa.Text(), nullable=True),
    sa.Column('rfp_file_id', sa.Integer(), nullable=True),
    sa.Column('partner_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['partner_id'], ['partner.id'], name=op.f('fk_rfp_partner_id_partner')),
    sa.ForeignKeyConstraint(['rfp_file_id'], ['rfp_file.id'], name=op.f('fk_rfp_rfp_file_id_rfp_file')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_rfp'))
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('rfp')
    op.drop_table('rfp_file')
    # ### end Alembic commands ###
