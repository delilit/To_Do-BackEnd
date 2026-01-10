from fastapi import FastAPI
from db import pool
from repository import (
    create_task,
    delete_task,
    get_tasks_by_user,
    update_task_title,
    update_task_description,
    update_task_status,
)
from models import (
    TaskCreate,
    TaskUpdateTitle,
    TaskUpdateDescription,
    TaskUpdateStatus,
)

app = FastAPI()

# временно — один пользователь
USER_ID = "demo-user"


@app.post("/tasks")
def create_task_api(task: TaskCreate):
    create_task(
        pool,
        USER_ID,
        task.title,
        task.description,
    )
    return {"status": "ok"}

@app.get("/tasks")
def get_tasks_api():
    tasks = get_tasks_by_user(pool, USER_ID)

    return [
        {
            "id": row.id,
            "title": row.title,
            "description": row.description,
            "status": row.status,
        }
        for row in tasks
    ]
@app.delete("/tasks/{task_id}")
def delete_task_api(task_id: str):
    delete_task(pool, USER_ID, task_id)
    return {"status": "deleted"}

@app.put("/tasks/{task_id}/title")
def update_title_api(task_id: str, body: TaskUpdateTitle):
    update_task_title(pool, USER_ID, task_id, body.title)
    return {"status": "updated"}

@app.put("/tasks/{task_id}/description")
def update_description_api(task_id: str, body: TaskUpdateDescription):
    update_task_description(pool, USER_ID, task_id, body.description)
    return {"status": "updated"}

@app.put("/tasks/{task_id}/status")
def update_status_api(task_id: str, body: TaskUpdateStatus):
    update_task_status(pool, USER_ID, task_id, body.status)
    return {"status": "updated"}
