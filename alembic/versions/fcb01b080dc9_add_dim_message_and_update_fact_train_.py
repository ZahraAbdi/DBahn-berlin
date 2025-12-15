"""add dim_message and update fact_train_movement

Revision ID: fcb01b080dc9
Revises: cd0f3e7ca385
Create Date: 2025-12-15 11:53:20.380769

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'fcb01b080dc9'
down_revision: Union[str, Sequence[str], None] = 'cd0f3e7ca385'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Create dim_message
    op.execute("""
        CREATE TABLE IF NOT EXISTS dim_message (
            message_key SERIAL PRIMARY KEY,
            message_id VARCHAR(100),
            message_type VARCHAR(100),
            message_category VARCHAR(100),
            message_code VARCHAR(100),
            priority INTEGER,
            timestamp TIMESTAMP,
            timestamp_tts VARCHAR(50),
            valid_from TIMESTAMP,
            valid_to TIMESTAMP
        );
    """)
    
    # Drop old fact_train_movement
    op.execute("DROP TABLE IF EXISTS fact_train_movement CASCADE;")
    
    # Create new fact_train_movement with all fields
    op.execute("""
        CREATE TABLE fact_train_movement (
            movement_id SERIAL PRIMARY KEY,
            
            station_key INTEGER REFERENCES dim_station(station_key),
            train_key INTEGER REFERENCES dim_train(train_key),
            time_key INTEGER REFERENCES dim_time(time_key),
            message_key INTEGER REFERENCES dim_message(message_key),
            
            event_type VARCHAR(50),
            event_status VARCHAR(50),
            
            planned_time TIMESTAMP,
            actual_time TIMESTAMP,
            
            planned_platform VARCHAR(50),
            actual_platform VARCHAR(50),
            
            delay_minutes INTEGER,
            is_cancelled BOOLEAN,
            cancellation_time TIMESTAMP,
            is_hidden BOOLEAN,
            has_disruption BOOLEAN,
            
            distance_change INTEGER,
            line_number VARCHAR(50),
            planned_path TEXT,
            actual_path TEXT,
            planned_destination VARCHAR(255),
            transition_reference VARCHAR(100)
        );
        
        CREATE INDEX idx_movement_station ON fact_train_movement(station_key);
        CREATE INDEX idx_movement_train ON fact_train_movement(train_key);
        CREATE INDEX idx_movement_time ON fact_train_movement(time_key);
        CREATE INDEX idx_movement_message ON fact_train_movement(message_key);
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS fact_train_movement CASCADE;")
    op.execute("DROP TABLE IF EXISTS dim_message CASCADE;")