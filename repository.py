import uuid
from datetime import datetime

from dotenv import load_dotenv
import os
import ydb

# CRUD
#CREATE
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