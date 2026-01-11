import uuid
from datetime import datetime
import bcrypt


from dotenv import load_dotenv
import os
import ydb

def initialize_tables(pool: ydb.QuerySessionPool):
    # Таблица пользователей
    pool.execute_with_retries("""
    CREATE TABLE IF NOT EXISTS users (
        id Utf8,
        username Utf8,
        password Utf8,
        created_at Timestamp,
        PRIMARY KEY (id),
        INDEX username_idx GLOBAL ON (username)
    );
    """)

    # Таблица задач
    pool.execute_with_retries("""
    CREATE TABLE IF NOT EXISTS tasks (
        id Utf8,
        user_id Utf8,
        title Utf8,
        description Utf8,
        status Utf8,
        created_at Timestamp,
        updated_at Timestamp,
        PRIMARY KEY (user_id, id)
    );
    """)


# CRUD
#CREATE

def create_user(
    pool: ydb.QuerySessionPool,
    username: str,
    password: str,
):
    # Проверяем, есть ли такой username
    if get_user_by_username(pool, username):
        raise ValueError(f"Username '{username}' уже существует")
    user_id = str(uuid.uuid4())

    password_hash = bcrypt.hashpw(
        password.encode("utf-8"),
        bcrypt.gensalt()
    ).decode("utf-8")

    query = """
    DECLARE $id AS Utf8;
    DECLARE $username AS Utf8;
    DECLARE $password AS Utf8;

    UPSERT INTO users (
        id,
        username,
        password,
        created_at
    )
    VALUES (
        $id,
        $username,
        $password,
        CurrentUtcTimestamp()
    );
    """

    pool.execute_with_retries(
        query,
        parameters={
            "$id": user_id,
            "$username": username,
            "$password": password_hash,
        }
    )

    return user_id


def create_task(pool: ydb.QuerySessionPool, user_id: str, title: str, description: str):
    task_id = str(uuid.uuid4())
    query = """
    DECLARE $id AS Utf8;
    DECLARE $user_id AS Utf8;
    DECLARE $title AS Utf8;
    DECLARE $description AS Utf8;
    DECLARE $status AS Utf8;

    UPSERT INTO tasks (
        user_id, id, title, description, status, created_at, updated_at
    )
    VALUES (
        $user_id,
        $id,
        $title,
        $description,
        $status,
        CurrentUtcTimestamp(),
        CurrentUtcTimestamp()
    );
    """
    pool.execute_with_retries(
        query,
        parameters={
            "$id": task_id,
            "$user_id": user_id,
            "$title": title,
            "$description": description,
            "$status": "Не сделано",
        }
    )
    
    # pool.execute_with_retries(f"""UPSERT INTO tasks (id, user_id, title, description, status, created_at, uploaded_at) VALUES 
    # ({id}, {user}, {title}, {description}, Не сделано, {datetime.now().isoformat()}, {datetime.now().isoformat()})""")
    # pass
#READ

def get_user_by_id(
    pool: ydb.QuerySessionPool,
    user_id: str,
):
    query = """
    DECLARE $id AS Utf8;

    SELECT
        id,
        username,
        password,
        created_at
    FROM users
    WHERE id = $id;
    """

    result_sets = pool.execute_with_retries(
        query,
        parameters={
            "$id": user_id,
        }
    )

    rows = result_sets[0].rows
    return rows[0] if rows else None

def get_user_by_id(
    pool: ydb.QuerySessionPool,
    user_id: str,
):
    query = """
    DECLARE $id AS Utf8;

    SELECT
        id,
        username,
        password,
        created_at
    FROM users
    WHERE id = $id;
    """

    result_sets = pool.execute_with_retries(
        query,
        parameters={
            "$id": user_id,
        }
    )

    rows = result_sets[0].rows
    return rows[0] if rows else None

def get_user_by_username(
    pool: ydb.QuerySessionPool,
    username: str,
):
    query = """
    DECLARE $username AS Utf8;

    SELECT
        id,
        username,
        password,
        created_at
    FROM users
    WHERE username = $username;
    """

    result_sets = pool.execute_with_retries(
        query,
        parameters={
            "$username": username,
        }
    )

    rows = result_sets[0].rows
    return rows[0] if rows else None


def get_tasks_by_user(pool: ydb.QuerySessionPool, user_id: str):
    query = """
    DECLARE $user_id AS Utf8;

    SELECT
        id,
        title,
        description,
        status,
        created_at,
        updated_at
    FROM tasks
    WHERE user_id = $user_id
    ORDER BY created_at DESC;
    """

    result_sets = pool.execute_with_retries(
        query,
        parameters={
            "$user_id": user_id,
        }
    )

    rows = result_sets[0].rows
    return rows
#UPDATE
def update_task_description(
    pool: ydb.QuerySessionPool,
    user_id: str,
    task_id: str,
    description: str
):
    query = """
    DECLARE $user_id AS Utf8;
    DECLARE $id AS Utf8;
    DECLARE $description AS Utf8;

    UPDATE tasks
    SET
        description = $description,
        updated_at = CurrentUtcTimestamp()
    WHERE user_id = $user_id AND id = $id;
    """

    pool.execute_with_retries(
        query,
        parameters={
            "$user_id": user_id,
            "$id": task_id,
            "$description": description,
        }
    )

def update_task_title(
    pool: ydb.QuerySessionPool,
    user_id: str,
    task_id: str,
    title: str
):
    query = """
    DECLARE $user_id AS Utf8;
    DECLARE $id AS Utf8;
    DECLARE $title AS Utf8;

    UPDATE tasks
    SET
        title = $title,
        updated_at = CurrentUtcTimestamp()
    WHERE user_id = $user_id AND id = $id;
    """

    pool.execute_with_retries(
        query,
        parameters={
            "$user_id": user_id,
            "$id": task_id,
            "$title": title,
        }
    )
def update_task_status(
    pool: ydb.QuerySessionPool,
    user_id: str,
    task_id: str,
    status: str
):
    query = """
    DECLARE $user_id AS Utf8;
    DECLARE $id AS Utf8;
    DECLARE $status AS Utf8;

    UPDATE tasks
    SET
        status = $status,
        updated_at = CurrentUtcTimestamp()
    WHERE user_id = $user_id AND id = $id;
    """

    pool.execute_with_retries(
        query,
        parameters={
            "$user_id": user_id,
            "$id": task_id,
            "$status": status,
        }
    )

#DELETE
def delete_task(pool: ydb.QuerySessionPool, user_id: str, task_id: str):
    query = """
    DECLARE $user_id AS Utf8;
    DECLARE $id AS Utf8;

    DELETE FROM tasks
    WHERE user_id = $user_id AND id = $id;
    """

    pool.execute_with_retries(
        query,
        parameters={
            "$user_id": user_id,
            "$id": task_id,
        }
    )

#Login logic
def verify_password(
    plain_password: str,
    password_hash: str,
) -> bool:
    return bcrypt.checkpw(
        plain_password.encode("utf-8"),
        password_hash.encode("utf-8"),
    )

def authenticate_user(
    pool: ydb.QuerySessionPool,
    username: str,
    password: str,
):
    user = get_user_by_username(pool, username)

    if not user:
        return None

    if not verify_password(password, user.password):
        return None

    return user

