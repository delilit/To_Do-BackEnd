from db import pool
from models import (
    UserCreate,
    UserLogin,
    UserResponse,
    TaskCreate,
    TaskUpdateTitle,
    TaskUpdateDescription,
    TaskUpdateStatus,
)
from repository import (
    create_user,
    authenticate_user,
    create_task,
    get_tasks_by_user,
    delete_task,
    update_task_title,
    update_task_description,
    update_task_status,
    get_user_by_id,
    get_user_by_username,
)

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from db import pool
from repository import initialize_tables  # твоя функция

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup_event():
    initialize_tables(pool)
    print("Tables initialized if not exists")


# ===== Dependency =====

def get_current_user_id(x_user_id: str = Header(...)):
    """
    Временная аутентификация через заголовок.
    Позже легко заменить на JWT.
    """
    user = get_user_by_id(pool, x_user_id)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid user")
    return x_user_id


# ===== Users =====

@app.post("/users", response_model=UserResponse)
def register(user: UserCreate):
    user_id = create_user(pool, user.username, user.password)
    return {
        "id": user_id,
        "username": user.username,
    }


@app.post("/login")
def login(body: UserLogin):
    user = authenticate_user(pool, body.username, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # пока возвращаем user_id (позже будет JWT)
    return {"user_id": user.id}


@app.get("/users/{user_id}", response_model=UserResponse)
def get_user_api(user_id: str):
    user = get_user_by_id(pool, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "id": user.id,
        "username": user.username,
    }


@app.get("/users/by-username/{username}", response_model=UserResponse)
def get_user_by_username_api(username: str):
    user = get_user_by_username(pool, username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "id": user.id,
        "username": user.username,
    }


# ===== Tasks =====

@app.post("/tasks")
def create_task_api(
    task: TaskCreate,
    user_id: str = Depends(get_current_user_id),
):
    create_task(
        pool,
        user_id,
        task.title,
        task.description,
    )
    return {"status": "ok"}


@app.get("/tasks")
def get_tasks_api(
    user_id: str = Depends(get_current_user_id),
):
    tasks = get_tasks_by_user(pool, user_id)

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
def delete_task_api(
    task_id: str,
    user_id: str = Depends(get_current_user_id),
):
    delete_task(pool, user_id, task_id)
    return {"status": "deleted"}


@app.put("/tasks/{task_id}/title")
def update_title_api(
    task_id: str,
    body: TaskUpdateTitle,
    user_id: str = Depends(get_current_user_id),
):
    update_task_title(pool, user_id, task_id, body.title)
    return {"status": "updated"}


@app.put("/tasks/{task_id}/description")
def update_description_api(
    task_id: str,
    body: TaskUpdateDescription,
    user_id: str = Depends(get_current_user_id),
):
    update_task_description(pool, user_id, task_id, body.description)
    return {"status": "updated"}


@app.put("/tasks/{task_id}/status")
def update_status_api(
    task_id: str,
    body: TaskUpdateStatus,
    user_id: str = Depends(get_current_user_id),
):
    update_task_status(pool, user_id, task_id, body.status)
    return {"status": "updated"}


