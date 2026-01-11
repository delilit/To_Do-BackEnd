from pydantic import BaseModel

class TaskCreate(BaseModel):
    title: str
    description: str

class TaskUpdateTitle(BaseModel):
    title: str

class TaskUpdateDescription(BaseModel):
    description: str

class TaskUpdateStatus(BaseModel):
    status: str

class TaskResponse(BaseModel):
    id: str
    title: str
    description: str
    status: str

class UserCreate(BaseModel):
    username: str
    password: str

class UserLogin(BaseModel):
    username: str
    password: str

class UserResponse(BaseModel):
    id: str
    username: str

