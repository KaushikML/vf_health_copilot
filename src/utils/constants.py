"""Project-wide constants and placeholder-safe defaults."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIGS_DIR = PROJECT_ROOT / "configs"
DATA_DIR = PROJECT_ROOT / "data"
LOCAL_OUTPUT_DIR = PROJECT_ROOT / ".local_outputs"

DEFAULT_TABLE_NAMES = {
    "bronze_vf_ghana_raw": "bronze_vf_ghana_raw",
    "silver_vf_organizations": "silver_vf_organizations",
    "gold_facility_master": "gold_facility_master",
    "gold_ngo_master": "gold_ngo_master",
    "gold_region_summary": "gold_region_summary",
    "gold_facility_facts_long": "gold_facility_facts_long",
}

LIST_LIKE_COLUMNS = [
    "specialties",
    "procedure",
    "equipment",
    "capability",
    "phone_numbers",
    "websites",
    "countries",
    "affiliationTypeIds",
]

FACT_TYPES = ("specialty", "procedure", "equipment", "capability", "description")

QUERY_FAMILIES = (
    "facility lookup",
    "count/ranking",
    "service search",
    "anomaly detection",
    "region gap analysis",
    "planner recommendation",
    "ngo analysis",
    "unsupported",
    "external-data-needed",
)

NULL_LIKE_TOKENS = {
    "",
    "[]",
    "{}",
    "null",
    "none",
    "nan",
    "n/a",
    "na",
    "nil",
    "unknown",
}

REGION_CENTROIDS = {
    "Ahafo": {"lat": 7.0499, "lon": -2.5208},
    "Ashanti": {"lat": 6.6885, "lon": -1.6244},
    "Bono": {"lat": 7.7392, "lon": -2.1046},
    "Bono East": {"lat": 7.8239, "lon": -1.3577},
    "Central": {"lat": 5.5600, "lon": -1.0586},
    "Eastern": {"lat": 6.3871, "lon": -0.6831},
    "Greater Accra": {"lat": 5.6037, "lon": -0.1870},
    "North East": {"lat": 10.4990, "lon": -0.3660},
    "Northern": {"lat": 9.4008, "lon": -0.8393},
    "Oti": {"lat": 8.4339, "lon": 0.2510},
    "Savannah": {"lat": 9.0833, "lon": -1.8167},
    "Upper East": {"lat": 10.7082, "lon": -0.9821},
    "Upper West": {"lat": 10.4000, "lon": -2.5000},
    "Volta": {"lat": 6.5781, "lon": 0.4502},
    "Western": {"lat": 5.5000, "lon": -2.8000},
    "Western North": {"lat": 6.8000, "lon": -2.5000},
}

GHANA_REGION_ALIASES = {
    "greater accra region": "Greater Accra",
    "accra": "Greater Accra",
    "ashanti region": "Ashanti",
    "northern region": "Northern",
    "upper east region": "Upper East",
    "upper west region": "Upper West",
    "western region": "Western",
    "western north region": "Western North",
    "central region": "Central",
    "eastern region": "Eastern",
    "volta region": "Volta",
    "oti region": "Oti",
    "savannah region": "Savannah",
    "north east region": "North East",
    "bono east region": "Bono East",
    "bono region": "Bono",
    "ahafo region": "Ahafo",
}

COUNTRY_ALIASES = {
    "ghana": "Ghana",
    "republic of ghana": "Ghana",
}

DEMO_FACILITIES = [
    {
        "unique_id": "FAC-001",
        "name": "Tema General Hospital",
        "facilityTypeId": "hospital",
        "operatorTypeId": "public",
        "region": "Greater Accra",
        "city": "Tema",
        "country": "Ghana",
        "specialties": '["Cardiology", "Obstetrics"]',
        "procedure": '["Caesarean Section"]',
        "equipment": '["Ultrasound", "Operating Theatre"]',
        "capability": '["Emergency Care"]',
        "description": "General hospital with cardiology clinic, obstetrics unit, ultrasound imaging and emergency care.",
        "doctor_count": 14,
        "capacity": 120,
        "area": 2.5,
        "phone_numbers": '["+233-555-0101"]',
        "websites": '["https://tema.example.org"]',
        "affiliationTypeIds": '["public"]',
    },
    {
        "unique_id": "FAC-002",
        "name": "Tamale Community Clinic",
        "facilityTypeId": "clinic",
        "operatorTypeId": "private",
        "region": "Northern",
        "city": "Tamale",
        "country": "Ghana",
        "specialties": '["Cardiac"]',
        "procedure": '["Catheterization", "Appendectomy"]',
        "equipment": "[]",
        "capability": "[]",
        "description": "Small clinic claiming cardiac services and multiple surgeries with limited supporting detail.",
        "doctor_count": 1,
        "capacity": 8,
        "area": 0.5,
        "phone_numbers": '["+233-555-0202"]',
        "websites": '["https://tamaleclinic.example.org"]',
        "affiliationTypeIds": '["private"]',
    },
    {
        "unique_id": "FAC-003",
        "name": "Sunyani Diagnostic Centre",
        "facilityTypeId": "diagnostic centre",
        "operatorTypeId": "private",
        "region": "Bono",
        "city": "Sunyani",
        "country": "Ghana",
        "specialties": '["Radiology"]',
        "procedure": "[]",
        "equipment": '["X-Ray", "Ultrasound"]',
        "capability": "[]",
        "description": "Diagnostic imaging centre with x-ray and ultrasound support.",
        "doctor_count": 3,
        "capacity": 15,
        "area": 1.0,
        "phone_numbers": '["+233-555-0303"]',
        "websites": '["https://sunyaniimaging.example.org"]',
        "affiliationTypeIds": '["private"]',
    },
    {
        "unique_id": "NGO-001",
        "name": "Northern Rural Outreach NGO",
        "facilityTypeId": "ngo",
        "operatorTypeId": "ngo",
        "region": "Northern",
        "city": "Tamale",
        "country": "Ghana",
        "specialties": "[]",
        "procedure": "[]",
        "equipment": '["Ultrasound"]',
        "capability": '["Emergency Care"]',
        "description": "NGO-led outreach program supporting maternal care referrals and emergency transport coordination.",
        "doctor_count": 0,
        "capacity": 0,
        "area": 0.0,
        "phone_numbers": '["+233-555-0404"]',
        "websites": '["https://northernoutreach.example.org"]',
        "affiliationTypeIds": '["ngo"]',
    },
]
