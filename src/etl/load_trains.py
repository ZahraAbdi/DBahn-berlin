import psycopg2

def get_train_key(conn, train_data):
    """
    Get or create a train in dim_train table
    train_data is a dictionary with:  train_id, train_number, operator_code, 
    operator_name, train_category, train_category_desc, train_class, 
    train_type, train_type_desc
    
    Returns the train_key
    """
    cursor = conn.cursor()
    
    # extract train info
    train_id = train_data.get('train_id')
    train_number = train_data.get('train_number')
    operator_code = train_data.get('operator_code')
    operator_name = train_data.get('operator_name')
    train_category = train_data.get('train_category')
    train_category_desc = train_data.get('train_category_desc')
    train_class = train_data.get('train_class')
    train_type = train_data.get('train_type')
    train_type_desc = train_data.get('train_type_desc')
    
    # check if train already exists by train_id
    if train_id:
        cursor. execute("""
            SELECT train_key FROM dim_train 
            WHERE train_id = %s
        """, (train_id,))
        
        result = cursor.fetchone()
        
        if result:
            cursor.close()
            return result[0]
    
    # insert new train
    cursor.execute("""
        INSERT INTO dim_train (
            train_id, train_number, operator_code, operator_name,
            train_category, train_category_desc, train_class,
            train_type, train_type_desc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING train_key
    """, (
        train_id, train_number, operator_code, operator_name,
        train_category, train_category_desc, train_class,
        train_type, train_type_desc
    ))
    
    train_key = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    
    return train_key