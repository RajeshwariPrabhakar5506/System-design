import uuid
import os
from typing import List
from fastapi import FastAPI, APIRouter, status, HTTPException, Depends, Query
from sqlalchemy import create_engine, Column, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext

# --- Configuration & Security ---
IS_DOCKER = os.path.exists('/.dockerenv')
DB_HOST = "db_service" if IS_DOCKER else "localhost"
DB_PORT = "5432" if IS_DOCKER else "5632"
DATABASE_URL = f'postgresql://agriadmin:agriadmin123@{DB_HOST}:{DB_PORT}/agri_db'

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# --- Database Setup ---
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- SQLAlchemy Model ---
class User(Base):
    __tablename__ = "users"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False, unique=True)
    password = Column(String(100), nullable=False)

# --- Pydantic Schemas ---
class UserCreate(BaseModel):
    name: str
    email: EmailStr
    password: str

class UserOut(BaseModel):
    id: uuid.UUID
    name: str
    email: EmailStr

    class Config:
        from_attributes = True

class PaginatedUserResponse(BaseModel):
    total_count: int
    skip: int
    limit: int
    users: List[UserOut]

# --- FastAPI App & Router ---
app = FastAPI(title="Agri Project API")
app_v1 = APIRouter(prefix="/api/v1", tags=["v1"])

@app.on_event("startup")
def startup():
    print("Connecting to:", DATABASE_URL) # Check if this URL is actually correct
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully!")
# --- Routes ---

@app_v1.post("/users", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def create_user(user: UserCreate, db: Session = Depends(get_db)):
    # Hash the password before saving
    hashed_password = pwd_context.hash(user.password)
    
    new_user = User(
        name=user.name, 
        email=user.email, 
        password=hashed_password
    )
    
    try:
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        return new_user
    except Exception:
        db.rollback()
        raise HTTPException(status_code=400, detail="Email already registered")

@app_v1.get("/users", response_model=PaginatedUserResponse)
def get_users(
    skip: int = Query(0, ge=0), 
    limit: int = Query(10, ge=1, le=100), 
    db: Session = Depends(get_db)
):
    """
    Fetch a paginated list of users.
    - **skip**: Number of records to pass over (offset)
    - **limit**: Number of records to return
    """
    # Get the total count for the frontend to calculate total pages
    total_count = db.query(func.count(User.id)).scalar()
    
    # Fetch only the slice needed
    db_users = db.query(User).offset(skip).limit(limit).all()
    
    return {
        "total_count": total_count,
        "skip": skip,
        "limit": limit,
        "users": db_users
    }

@app_v1.get("/users/{user_id}", response_model=UserOut)
def get_user_by_id(user_id: uuid.UUID, db: Session = Depends(get_db)):
    user_obj = db.query(User).filter(User.id == user_id).first()
    if not user_obj:
        raise HTTPException(status_code=404, detail="User not found")
    return user_obj

app.include_router(app_v1)