TABLE_SCHEMAS = {
    'task_1': {
        'tb_users': {
            'uid': 'varchar(255)',
            'registration_date': 'timestamp',
            'country': 'varchar(255)'
        },
        'tb_logins': {
            'user_uid': 'varchar(255)',
            'login': 'varchar(255)',
            'account_type': 'varchar(255)',
            'registration_date': 'timestamp'
        },
        'tb_operations': {
            'operation_type': 'varchar(255)',
            'operation_date': 'timestamp',
            'login': 'varchar(255)',
            'amount': 'float'
        },
        'tb_orders': {
            'login': 'varchar(255)',
            'order_close_date': 'timestamp',
            'profit': 'float'
        }
    }
}

CREATE_TABLE = """
DROP TABLE IF EXISTS {table};
CREATE TABLE {table} (
    {columns}
);
"""
