"""SQLAlchemy ORM models and database setup for Golden."""

from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

DATABASE_URL = "sqlite:///data/golden.db"

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


class EstablishmentRow(Base):
    __tablename__ = "establishments"

    id = Column(Integer, primary_key=True)
    city = Column(String, nullable=False, index=True)
    establishment_id = Column(String, nullable=False)
    name = Column(String, default="")
    address = Column(String, default="")
    zip = Column(String, default="")
    owner = Column(String, default="")
    establishment_type = Column(String, default="")
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("city", "establishment_id", name="uq_city_establishment"),
    )

    violations = relationship("ViolationRow", back_populates="establishment")
    contacts = relationship("ContactRow", back_populates="establishment")
    outreach = relationship("OutreachRow", back_populates="establishment")


class ViolationRow(Base):
    __tablename__ = "violations"

    id = Column(Integer, primary_key=True)
    establishment_id = Column(Integer, ForeignKey("establishments.id"), nullable=False, index=True)
    city = Column(String, nullable=False, index=True)
    inspection_date = Column(String, default="")
    inspection_type = Column(String, default="")
    in_compliance = Column(Boolean, default=False)
    violation_code = Column(String, default="")
    violation_type = Column(String, default="")
    violation_description = Column(Text, default="")
    problem_description = Column(Text, default="")
    is_corrected = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    establishment = relationship("EstablishmentRow", back_populates="violations")


class ContactRow(Base):
    __tablename__ = "contacts"

    id = Column(Integer, primary_key=True)
    establishment_id = Column(Integer, ForeignKey("establishments.id"), nullable=False, index=True)
    email = Column(String, default="")
    phone = Column(String, default="")
    website = Column(String, default="")
    source = Column(String, default="")
    scraped_at = Column(DateTime)

    establishment = relationship("EstablishmentRow", back_populates="contacts")
    outreach = relationship("OutreachRow", back_populates="contact")


class OutreachRow(Base):
    __tablename__ = "outreach"

    id = Column(Integer, primary_key=True)
    establishment_id = Column(Integer, ForeignKey("establishments.id"), nullable=False, index=True)
    contact_id = Column(Integer, ForeignKey("contacts.id"), nullable=True)
    channel = Column(String, default="")
    status = Column(String, default="")
    subject = Column(String, default="")
    message_body = Column(Text, default="")
    sent_at = Column(DateTime)
    opened_at = Column(DateTime)
    replied_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)

    establishment = relationship("EstablishmentRow", back_populates="outreach")
    contact = relationship("ContactRow", back_populates="outreach")


def init_db():
    """Create all tables."""
    Base.metadata.create_all(engine)


@contextmanager
def get_session():
    """Yield a session that auto-commits on success, rolls back on error."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
