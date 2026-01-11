"""Data-driven configuration for Pipedrive entities.

All entity definitions are centralized here - no hardcoded logic elsewhere.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class EntityConfig:
    """Configuration for a Pipedrive entity type."""

    name: str
    endpoint: str
    fields_endpoint: str | None = None
    supports_pagination: bool = True
    max_limit: int = 500


# Central configuration for all exportable entities
ENTITIES: dict[str, EntityConfig] = {
    "persons": EntityConfig(
        name="persons",
        endpoint="/v1/persons",
        fields_endpoint="/v1/personFields",
    ),
    "organizations": EntityConfig(
        name="organizations",
        endpoint="/v1/organizations",
        fields_endpoint="/v1/organizationFields",
    ),
    "deals": EntityConfig(
        name="deals",
        endpoint="/v1/deals",
        fields_endpoint="/v1/dealFields",
    ),
    "activities": EntityConfig(
        name="activities",
        endpoint="/v1/activities",
        fields_endpoint="/v1/activityFields",
    ),
    "notes": EntityConfig(
        name="notes",
        endpoint="/v1/notes",
        fields_endpoint=None,
    ),
    "products": EntityConfig(
        name="products",
        endpoint="/v1/products",
        fields_endpoint="/v1/productFields",
    ),
    "files": EntityConfig(
        name="files",
        endpoint="/v1/files",
        fields_endpoint=None,
    ),
    "users": EntityConfig(
        name="users",
        endpoint="/v1/users",
        fields_endpoint=None,
    ),
}

# API configuration
API_BASE_URL = "https://api.pipedrive.com"
DEFAULT_LIMIT = 100
RATE_LIMIT_REQUESTS = 80
RATE_LIMIT_WINDOW = 2.0  # seconds

# Restore configuration
RESTORE_ORDER = ["organizations", "persons", "deals", "activities", "notes", "products"]

# Entities that can be backed up but not restored (read-only from API)
READONLY_ENTITIES = {"users"}

READONLY_FIELDS = {
    "id",
    "add_time",
    "update_time",
    "creator_user_id",
    "first_char",
    "company_id",
    "active_flag",
    "cc_email",
    "org_name",
    "owner_name",
    "person_name",
    "next_activity_date",
    "next_activity_time",
    "next_activity_id",
    "last_activity_id",
    "last_activity_date",
    "activities_count",
    "done_activities_count",
    "undone_activities_count",
    "files_count",
    "notes_count",
    "followers_count",
    "email_messages_count",
    "picture_id",
    "last_incoming_mail_time",
    "last_outgoing_mail_time",
    "open_deals_count",
    "related_open_deals_count",
    "closed_deals_count",
    "related_closed_deals_count",
    "won_deals_count",
    "related_won_deals_count",
    "lost_deals_count",
    "related_lost_deals_count",
}
