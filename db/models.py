from __future__ import annotations

from dataclasses import dataclass
import sqlite3


@dataclass(frozen=True)
class User:
    id: int
    email: str
    password_hash: str
    created_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "User":
        return cls(
            id=int(row["id"]),
            email=str(row["email"] or ""),
            password_hash=str(row["password_hash"] or ""),
            created_at=str(row["created_at"] or ""),
        )


@dataclass(frozen=True)
class Lead:
    id: int
    name: str
    raw_title: str
    normalized_name: str
    domain: str
    website: str
    source: str
    query: str
    source_type: str
    entity_type: str
    extraction_source: str
    parent_source_url: str
    parent_entity_type: str
    child_profile_extracted: int
    quality_score: float
    realtor_confidence: float
    role_validation_reason: str
    validation_reason: str
    discovered_at: str
    status: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Lead":
        return cls(
            id=int(row["id"]),
            name=str(row["name"] or ""),
            raw_title=str(row["raw_title"] or ""),
            normalized_name=str(row["normalized_name"] or ""),
            domain=str(row["domain"] or ""),
            website=str(row["website"] or ""),
            source=str(row["source"] or ""),
            query=str(row["query"] or ""),
            source_type=str(row["source_type"] or ""),
            entity_type=str(row["entity_type"] or ""),
            extraction_source=str(row["extraction_source"] or ""),
            parent_source_url=str(row["parent_source_url"] or ""),
            parent_entity_type=str(row["parent_entity_type"] or ""),
            child_profile_extracted=int(row["child_profile_extracted"] or 0),
            quality_score=float(row["quality_score"] or 0),
            realtor_confidence=float(row["realtor_confidence"] or 0),
            role_validation_reason=str(row["role_validation_reason"] or ""),
            validation_reason=str(row["validation_reason"] or ""),
            discovered_at=str(row["discovered_at"] or ""),
            status=str(row["status"] or ""),
        )


@dataclass(frozen=True)
class Contact:
    id: int
    lead_id: int
    email: str
    phone: str
    source_page: str
    confidence: float

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Contact":
        return cls(
            id=int(row["id"]),
            lead_id=int(row["lead_id"]),
            email=str(row["email"] or ""),
            phone=str(row["phone"] or ""),
            source_page=str(row["source_page"] or ""),
            confidence=float(row["confidence"] or 0),
        )


@dataclass(frozen=True)
class Draft:
    id: int
    lead_id: int
    user_id: int
    email: str
    subject: str
    body: str
    status: str
    created_at: str
    approved_at: str
    sent_at: str
    sender_profile: str = ""
    campaign_prompt: str = ""
    lead_name: str = ""
    lead_domain: str = ""
    lead_website: str = ""

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Draft":
        return cls(
            id=int(row["id"]),
            lead_id=int(row["lead_id"]),
            user_id=int(row["user_id"] or 0),
            email=str(row["email"] or ""),
            subject=str(row["subject"] or ""),
            body=str(row["body"] or ""),
            status=str(row["status"] or ""),
            created_at=str(row["created_at"] or ""),
            approved_at=str(row["approved_at"] or ""),
            sent_at=str(row["sent_at"] or ""),
            sender_profile=str(row["sender_profile"] or ""),
            campaign_prompt=str(row["campaign_prompt"] or ""),
            lead_name=str(row["lead_name"] or ""),
            lead_domain=str(row["lead_domain"] or ""),
            lead_website=str(row["lead_website"] or ""),
        )


@dataclass(frozen=True)
class Run:
    id: int
    worker: str
    args: str
    status: str
    started_at: str
    finished_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Run":
        return cls(
            id=int(row["id"]),
            worker=str(row["worker"] or ""),
            args=str(row["args"] or ""),
            status=str(row["status"] or ""),
            started_at=str(row["started_at"] or ""),
            finished_at=str(row["finished_at"] or ""),
        )


@dataclass(frozen=True)
class GmailConnection:
    id: int
    user_id: int
    email: str
    access_token: str
    refresh_token: str
    token_expiry: str
    scope: str
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "GmailConnection":
        return cls(
            id=int(row["id"]),
            user_id=int(row["user_id"]),
            email=str(row["email"] or ""),
            access_token=str(row["access_token"] or ""),
            refresh_token=str(row["refresh_token"] or ""),
            token_expiry=str(row["token_expiry"] or ""),
            scope=str(row["scope"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
        )
