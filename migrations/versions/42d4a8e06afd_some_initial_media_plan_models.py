"""some initial media plan models

Revision ID: 42d4a8e06afd
Revises: 09684af95b86
Create Date: 2021-12-20 10:16:09.313637

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '42d4a8e06afd'
down_revision = '09684af95b86'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('plan_rule',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.Text(), nullable=True),
    sa.Column('order', sa.Integer(), nullable=True),
    sa.Column('type', sa.String(length=128), nullable=True),
    sa.Column('rule_info', sa.JSON(), nullable=True),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_plan_rule'))
    )
    with op.batch_alter_table('plan_rule', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_plan_rule_name'), ['name'], unique=False)

    op.create_table('plan',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=128), nullable=True),
    sa.Column('description', sa.String(length=128), nullable=True),
    sa.Column('client_requests', sa.Text(), nullable=True),
    sa.Column('restrictions', sa.Text(), nullable=True),
    sa.Column('objective', sa.Text(), nullable=True),
    sa.Column('user_id', sa.Integer(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('start_date', sa.Date(), nullable=True),
    sa.Column('end_date', sa.Date(), nullable=True),
    sa.Column('total_budget', sa.Numeric(), nullable=True),
    sa.Column('rate_card_id', sa.Integer(), nullable=True),
    sa.Column('campaign_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['campaign_id'], ['campaign.id'], name=op.f('fk_plan_campaign_id_campaign')),
    sa.ForeignKeyConstraint(['rate_card_id'], ['rate_card.id'], name=op.f('fk_plan_rate_card_id_rate_card')),
    sa.ForeignKeyConstraint(['user_id'], ['user.id'], name=op.f('fk_plan_user_id_user')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_plan'))
    )
    with op.batch_alter_table('plan', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_plan_name'), ['name'], unique=False)

    op.create_table('partner',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(length=128), nullable=True),
    sa.Column('plan_id', sa.Integer(), nullable=True),
    sa.Column('total_budget', sa.Numeric(), nullable=True),
    sa.Column('estimated_cpm', sa.Numeric(), nullable=True),
    sa.Column('estimated_cpc', sa.Numeric(), nullable=True),
    sa.Column('start_date', sa.Date(), nullable=True),
    sa.Column('end_date', sa.Date(), nullable=True),
    sa.ForeignKeyConstraint(['plan_id'], ['plan.id'], name=op.f('fk_partner_plan_id_plan')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_partner'))
    )
    with op.batch_alter_table('partner', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_partner_name'), ['name'], unique=False)

    op.create_table('processor_plan',
    sa.Column('processor_id', sa.Integer(), nullable=True),
    sa.Column('plan_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['plan_id'], ['plan.id'], name=op.f('fk_processor_plan_plan_id_plan')),
    sa.ForeignKeyConstraint(['processor_id'], ['processor.id'], name=op.f('fk_processor_plan_processor_id_processor'))
    )
    op.create_table('project_number_plan',
    sa.Column('project_id', sa.Integer(), nullable=True),
    sa.Column('plan_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['plan_id'], ['plan.id'], name=op.f('fk_project_number_plan_plan_id_plan')),
    sa.ForeignKeyConstraint(['project_id'], ['project.id'], name=op.f('fk_project_number_plan_project_id_project'))
    )
    op.create_table('partner_placements',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.Text(), nullable=True),
    sa.Column('start_date', sa.Date(), nullable=True),
    sa.Column('end_date', sa.Date(), nullable=True),
    sa.Column('partner_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['partner_id'], ['partner.id'], name=op.f('fk_partner_placements_partner_id_partner')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_partner_placements'))
    )
    with op.batch_alter_table('partner_placements', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_partner_placements_name'), ['name'], unique=False)

    with op.batch_alter_table('post', schema=None) as batch_op:
        batch_op.add_column(sa.Column('plan_id', sa.Integer(), nullable=True))
        batch_op.create_foreign_key(batch_op.f('fk_post_plan_id_plan'), 'plan', ['plan_id'], ['id'])

    with op.batch_alter_table('task', schema=None) as batch_op:
        batch_op.add_column(sa.Column('plan_id', sa.Integer(), nullable=True))
        batch_op.create_foreign_key(batch_op.f('fk_task_plan_id_plan'), 'plan', ['plan_id'], ['id'])

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('task', schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f('fk_task_plan_id_plan'), type_='foreignkey')
        batch_op.drop_column('plan_id')

    with op.batch_alter_table('post', schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f('fk_post_plan_id_plan'), type_='foreignkey')
        batch_op.drop_column('plan_id')

    with op.batch_alter_table('partner_placements', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_partner_placements_name'))

    op.drop_table('partner_placements')
    op.drop_table('project_number_plan')
    op.drop_table('processor_plan')
    with op.batch_alter_table('partner', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_partner_name'))

    op.drop_table('partner')
    with op.batch_alter_table('plan', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_plan_name'))

    op.drop_table('plan')
    with op.batch_alter_table('plan_rule', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_plan_rule_name'))

    op.drop_table('plan_rule')
    # ### end Alembic commands ###
