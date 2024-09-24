from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List


class BaseDTO(ABC):
    """
    This is a base class for data transfer objects
    """

    @staticmethod
    @abstractmethod
    def from_airtable(data: Dict[str, Any]) -> 'BaseDTO':
        """
        An abstract method for converting an airtable dictionary row to a data transfer object
        :param data:
        :return:
        """
        pass

    @abstractmethod
    def to_database(self) -> Dict[str, Any]:
        """
        An abstract method for converting the data transfer object to a database dictionary row
        :return:
        """
        pass


@dataclass
class DataSourcesDTO(BaseDTO):
    name: Optional[str] = None
    submitted_name: Optional[str] = None
    description: Optional[str] = None
    record_type: Optional[str] = None
    source_url: Optional[str] = None
    airtable_uid: Optional[str] = None
    agency_supplied: Optional[bool] = None
    supplying_entity: Optional[str] = None
    agency_originated: Optional[bool] = None
    originating_entity: Optional[str] = None
    agency_aggregation: Optional[bool] = None
    coverage_start: Optional[str] = None
    coverage_end: Optional[str] = None
    source_last_updated: Optional[str] = None
    retention_schedule: Optional[str] = None
    detail_level: Optional[str] = None
    number_of_records_available: Optional[int] = None
    size: Optional[int] = None
    access_type: Optional[str] = None
    data_portal_type: Optional[str] = None
    access_notes: Optional[str] = None
    record_format: Optional[str] = None
    update_frequency: Optional[str] = None
    update_method: Optional[str] = None
    agency_described_linked_uid: Optional[str] = None
    tags: Optional[str] = None
    readme_url: Optional[str] = None
    scraper_url: Optional[str] = None
    data_source_created: Optional[str] = None
    airtable_source_last_modified: Optional[str] = None
    submission_notes: Optional[str] = None
    rejection_note: Optional[str] = None
    last_approval_editor: Optional[str] = None
    submitter_contact_info: Optional[str] = None
    agency_described_submitted: Optional[bool] = None
    agency_described_not_in_database: Optional[bool] = None
    approval_status: Optional[str] = None
    record_type_other: Optional[str] = None
    data_portal_type_other: Optional[str] = None
    data_source_request: Optional[str] = None
    url_button: Optional[str] = None
    tags_other: Optional[str] = None

    Data: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_airtable(data: Dict[str, Any]) -> 'DataSourcesDTO':
        return DataSourcesDTO(**data)

    def to_database(self) -> Dict[str, Any]:
        return self.Data


@dataclass
class AgenciesDTO(BaseDTO):
    name: Optional[str] = None
    homepage_url: Optional[str] = None
    count_data_sources: Optional[int] = None
    agency_type: Optional[str] = None
    multi_agency: Optional[bool] = None
    submitted_name: Optional[str] = None
    jurisdiction_type: Optional[str] = None
    state_iso: Optional[str] = None
    municipality: Optional[str] = None
    zip_code: Optional[str] = None
    county_fips: Optional[str] = None
    county_name: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    data_sources: Optional[List[str]] = field(default_factory=list)
    no_web_presence: Optional[bool] = None
    airtable_agency_last_modified: Optional[str] = None
    data_sources_last_updated: Optional[str] = None
    approved: Optional[bool] = None
    rejection_reason: Optional[str] = None
    last_approval_editor: Optional[str] = None
    submitter_contact: Optional[str] = None
    agency_created: Optional[str] = None
    county_airtable_uid: Optional[str] = None
    defunct_year: Optional[int] = None
    airtable_uid: Optional[str] = None

    Data: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_airtable(data: Dict[str, Any]) -> 'AgenciesDTO':
        return AgenciesDTO(**data)

    def to_database(self) -> Dict[str, Any]:
        return self.Data


@dataclass
class CountiesDTO(BaseDTO):
    fips: Optional[str] = None
    name: Optional[str] = None
    name_ascii: Optional[str] = None
    state_iso: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    population: Optional[int] = None
    agencies: Optional[List[str]] = field(default_factory=list)
    airtable_uid: Optional[str] = None
    airtable_county_last_modified: Optional[str] = None
    airtable_county_created: Optional[str] = None

    Data: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_airtable(data: Dict[str, Any]) -> 'CountiesDTO':
        return CountiesDTO(**data)

    def to_database(self) -> Dict[str, Any]:
        return self.Data


@dataclass
class RequestsDTO(BaseDTO):
    id: Optional[str] = None
    submission_notes: Optional[str] = None
    request_status: Optional[str] = None
    submitter_contact_info: Optional[str] = None
    agency_described_submitted: Optional[bool] = None
    record_type: Optional[str] = None
    archive_reason: Optional[str] = None
    date_created: Optional[str] = None
    status_last_changed: Optional[str] = None

    Data: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_airtable(data: Dict[str, Any]) -> 'RequestsDTO':
        return RequestsDTO(**data)

    def to_database(self) -> Dict[str, Any]:
        return self.Data