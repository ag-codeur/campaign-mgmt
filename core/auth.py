"""
Authentication utilities: password hashing and session token management.
"""

import uuid
from datetime import datetime, timedelta
from passlib.context import CryptContext
from sqlalchemy.orm import Session

SESSION_TTL_HOURS = 8

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def create_session_token(db: Session, user_id: str) -> str:
    """Create a new AuthSession row and return the token UUID."""
    from core.database import AuthSession # local import to avoid circular
    token = str(uuid.uuid4())
    session = AuthSession(
        id=token,
        user_id=user_id,
        created_at=datetime.utcnow(),
        expires_at=datetime.utcnow() + timedelta(hours=SESSION_TTL_HOURS),
        is_revoked=False,
    )
    db.add(session)
    db.commit()
    return token

def get_session_user(db: Session, token: str):
    """Validate a session token and return the associated User, or None."""
    if not token:
        return None
    
    from core.database import AuthSession, User # local import to avoid circular
    session = (
        db.query(AuthSession)
        .filter(AuthSession.id == token, AuthSession.is_revoked == False)
        .first()
    )

    if not session:
        return None
    
    if session.expires_at < datetime.utcnow():
        return None
    
    user = db.query(User).filter(User.id == session.user_id).first()
    return user