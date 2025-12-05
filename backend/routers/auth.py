"""
Authentication Router

Handles user signup, login, and authentication endpoints.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr, Field
from typing import Optional

from backend.core.database_client import get_db
from backend.core.auth import (
    get_password_hash,
    authenticate_user,
    create_access_token,
    get_current_user
)
from backend.models.user import User as SQLUser


router = APIRouter(prefix="/auth", tags=["Authentication"])


# Pydantic Models
class UserSignup(BaseModel):
    """User registration request"""
    email: EmailStr
    name: str = Field(..., min_length=2, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    password: str = Field(..., min_length=6, max_length=72)  # bcrypt limit is 72 bytes


class UserLogin(BaseModel):
    """User login request"""
    email: EmailStr
    password: str


class Token(BaseModel):
    """JWT token response"""
    access_token: str
    token_type: str = "bearer"
    user: dict


class UserResponse(BaseModel):
    """User information response"""
    id: str
    email: str
    name: str
    phone: Optional[str]
    is_active: bool


# Endpoints

@router.post("/signup", response_model=Token, status_code=status.HTTP_201_CREATED)
def signup(user_data: UserSignup, db: Session = Depends(get_db)):
    """
    Register a new user

    - **email**: Valid email address (unique)
    - **name**: User's full name
    - **phone**: Optional phone number
    - **password**: Password (min 6 characters)

    Returns JWT token and user information
    """
    # Check if user already exists
    existing_user = db.query(SQLUser).filter(SQLUser.email == user_data.email).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    # Create new user
    import uuid

    try:
        hashed_password = get_password_hash(user_data.password)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

    new_user = SQLUser(
        id=str(uuid.uuid4()),
        email=user_data.email,
        name=user_data.name,
        phone=user_data.phone,
        hashed_password=hashed_password,
        is_active=True
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    # Generate access token
    access_token = create_access_token(data={"sub": new_user.id})

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": new_user.to_dict()
    }


@router.post("/login", response_model=Token)
def login(credentials: UserLogin, db: Session = Depends(get_db)):
    """
    Login with email and password

    - **email**: User's email address
    - **password**: User's password

    Returns JWT token and user information
    """
    user = authenticate_user(db, credentials.email, credentials.password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Generate access token
    access_token = create_access_token(data={"sub": user.id})

    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": user.to_dict()
    }


@router.get("/me", response_model=UserResponse)
def get_me(current_user: SQLUser = Depends(get_current_user)):
    """
    Get current authenticated user information

    Requires: Bearer token in Authorization header
    """
    return current_user.to_dict()


@router.post("/logout")
def logout():
    """
    Logout endpoint (client-side token removal)

    Note: Since JWT tokens are stateless, actual logout happens on client side
    by removing the token from storage.
    """
    return {"message": "Logged out successfully. Please remove token from client."}
