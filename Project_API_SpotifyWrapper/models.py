from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Spotify user identity
    spotify_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    display_name: Mapped[str | None] = mapped_column(String, nullable=True)
    email: Mapped[str | None] = mapped_column(String, nullable=True)

    # Persist auth so server restarts don't break login
    access_token: Mapped[str | None] = mapped_column(String, nullable=True)
    refresh_token: Mapped[str | None] = mapped_column(String, nullable=True)
    token_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    plays = relationship("Play", back_populates="user")


class Artist(Base):
    __tablename__ = "artists"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    spotify_artist_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    name: Mapped[str] = mapped_column(String)


class Track(Base):
    __tablename__ = "tracks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    spotify_track_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    name: Mapped[str] = mapped_column(String)
    album: Mapped[str | None] = mapped_column(String, nullable=True)

    # We store only a "primary" artist for now (first artist in Spotify response).
    primary_artist_id: Mapped[int | None] = mapped_column(ForeignKey("artists.id"), nullable=True)
    primary_artist = relationship("Artist")


class Play(Base):
    __tablename__ = "plays"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    track_id: Mapped[int] = mapped_column(ForeignKey("tracks.id"))

    played_at: Mapped[datetime] = mapped_column(DateTime)

    user = relationship("User", back_populates="plays")
    track = relationship("Track")

    # Prevent duplicate inserts when syncing
    __table_args__ = (
        UniqueConstraint("user_id", "track_id", "played_at", name="uq_play"),
    ) 