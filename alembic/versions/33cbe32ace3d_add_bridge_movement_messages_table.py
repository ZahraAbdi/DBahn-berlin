"""add_bridge_movement_messages_table

Revision ID: 33cbe32ace3d
Revises: fcb01b080dc9
Create Date: 2025-12-15 13:42:50.907986

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '33cbe32ace3d'
down_revision: Union[str, Sequence[str], None] = 'fcb01b080dc9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create bridge table
    op.create_table(
        'bridge_movement_messages',
        sa. Column('movement_id', sa. Integer(), nullable=False),
        sa.Column('message_key', sa.Integer(), nullable=False),
        sa.Column('message_sequence', sa.SmallInteger(), nullable=False),
        sa.ForeignKeyConstraint(['message_key'], ['dim_message.message_key'], 
                                name='fk_message', ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['movement_id'], ['fact_train_movement.movement_id'], 
                                name='fk_movement', ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('movement_id', 'message_key')
    )
    
    # Create indexes
    op.create_index('idx_bridge_movement', 'bridge_movement_messages', ['movement_id'])
    op.create_index('idx_bridge_message', 'bridge_movement_messages', ['message_key'])


def downgrade() -> None:
    """Downgrade schema."""
    # Drop indexes
    op. drop_index('idx_bridge_message', table_name='bridge_movement_messages')
    op.drop_index('idx_bridge_movement', table_name='bridge_movement_messages')
    
    # Drop table
    op.drop_table('bridge_movement_messages')
