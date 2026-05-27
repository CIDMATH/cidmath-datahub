"""Static ISO3 → WHO region / UN M49 sub-region lookups.

These mappings are not exposed by ``country_converter`` or ``pycountry`` as of
mid-2026, so we maintain them here. Values are aligned to the controlled
vocabularies in :mod:`cidmath_datahub.reference.geography_intl`:

* ``WHO_REGION_BY_ALPHA3`` uses the **GHO API ``ParentCode``** short forms
  (``AFR``, ``AMR``, ``EMR``, ``EUR``, ``SEAR``, ``WPR``) — not the
  regional-office suffixed forms (``AFRO``/``AMRO``/...). This matches
  :data:`geography_intl.WHO_REGION_CODES`.
* ``UN_SUBREGION_BY_ALPHA3`` uses the UN Statistics Division M49 English
  sub-region names verbatim (e.g. ``"Western Africa"``, ``"Southern Asia"``).
  No controlled vocabulary is enforced downstream; the column is free-form
  text.

Coverage
--------
* WHO covers all 194 member states + 2 associate members (Puerto Rico,
  Tokelau) + Palestine (OPT, reported under EMRO) = 197 entries.
* UN M49 covers every country and most dependent territory with an
  ISO 3166-1 alpha-3 code, including Antarctica.
* Kosovo: GADM uses ``XKO``; the de facto user-assigned ISO 3166-1
  alpha-3 is ``XKX``. Both resolve to the same lookup via
  :func:`_normalize`. Kosovo is not a WHO or UN member, so both helpers
  return ``None``.
* Taiwan (``TWN``): UN M49 places it under "Eastern Asia". WHO does not
  recognize Taiwan; :func:`who_region` returns ``None``.
* Dependent territories without WHO membership (French Polynesia,
  Greenland, Bermuda, ...) return ``None`` from :func:`who_region` rather
  than inheriting from their administering parent. Callers can choose to
  fall back to the parent's region if needed.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# UN M49 sub-region (granular layer)
# ---------------------------------------------------------------------------

UN_SUBREGION_BY_ALPHA3: dict[str, str] = {
    # ---- Northern Africa ----
    "DZA": "Northern Africa",
    "EGY": "Northern Africa",
    "ESH": "Northern Africa",
    "LBY": "Northern Africa",
    "MAR": "Northern Africa",
    "SDN": "Northern Africa",
    "TUN": "Northern Africa",
    # ---- Eastern Africa ----
    "BDI": "Eastern Africa",
    "COM": "Eastern Africa",
    "DJI": "Eastern Africa",
    "IOT": "Eastern Africa",
    "ERI": "Eastern Africa",
    "ETH": "Eastern Africa",
    "KEN": "Eastern Africa",
    "MDG": "Eastern Africa",
    "MWI": "Eastern Africa",
    "MUS": "Eastern Africa",
    "MYT": "Eastern Africa",
    "MOZ": "Eastern Africa",
    "REU": "Eastern Africa",
    "RWA": "Eastern Africa",
    "SYC": "Eastern Africa",
    "SOM": "Eastern Africa",
    "SSD": "Eastern Africa",
    "TZA": "Eastern Africa",
    "UGA": "Eastern Africa",
    "ZMB": "Eastern Africa",
    "ZWE": "Eastern Africa",
    # ---- Middle Africa ----
    "AGO": "Middle Africa",
    "CAF": "Middle Africa",
    "CMR": "Middle Africa",
    "COD": "Middle Africa",
    "COG": "Middle Africa",
    "GAB": "Middle Africa",
    "GNQ": "Middle Africa",
    "STP": "Middle Africa",
    "TCD": "Middle Africa",
    # ---- Southern Africa ----
    "BWA": "Southern Africa",
    "LSO": "Southern Africa",
    "NAM": "Southern Africa",
    "SWZ": "Southern Africa",
    "ZAF": "Southern Africa",
    # ---- Western Africa ----
    "BEN": "Western Africa",
    "BFA": "Western Africa",
    "CIV": "Western Africa",
    "CPV": "Western Africa",
    "GHA": "Western Africa",
    "GIN": "Western Africa",
    "GMB": "Western Africa",
    "GNB": "Western Africa",
    "LBR": "Western Africa",
    "MLI": "Western Africa",
    "MRT": "Western Africa",
    "NER": "Western Africa",
    "NGA": "Western Africa",
    "SEN": "Western Africa",
    "SHN": "Western Africa",
    "SLE": "Western Africa",
    "TGO": "Western Africa",
    # ---- Caribbean ----
    "AIA": "Caribbean",
    "ATG": "Caribbean",
    "ABW": "Caribbean",
    "BES": "Caribbean",
    "BHS": "Caribbean",
    "BLM": "Caribbean",
    "BRB": "Caribbean",
    "CUB": "Caribbean",
    "CUW": "Caribbean",
    "CYM": "Caribbean",
    "DMA": "Caribbean",
    "DOM": "Caribbean",
    "GLP": "Caribbean",
    "GRD": "Caribbean",
    "HTI": "Caribbean",
    "JAM": "Caribbean",
    "KNA": "Caribbean",
    "LCA": "Caribbean",
    "MAF": "Caribbean",
    "MSR": "Caribbean",
    "MTQ": "Caribbean",
    "PRI": "Caribbean",
    "SXM": "Caribbean",
    "TCA": "Caribbean",
    "TTO": "Caribbean",
    "VCT": "Caribbean",
    "VGB": "Caribbean",
    "VIR": "Caribbean",
    # ---- Central America ----
    "BLZ": "Central America",
    "CRI": "Central America",
    "GTM": "Central America",
    "HND": "Central America",
    "MEX": "Central America",
    "NIC": "Central America",
    "PAN": "Central America",
    "SLV": "Central America",
    # ---- South America ----
    "ARG": "South America",
    "BOL": "South America",
    "BRA": "South America",
    "CHL": "South America",
    "COL": "South America",
    "ECU": "South America",
    "FLK": "South America",
    "GUF": "South America",
    "GUY": "South America",
    "PER": "South America",
    "PRY": "South America",
    "SUR": "South America",
    "URY": "South America",
    "VEN": "South America",
    # ---- Northern America ----
    "BMU": "Northern America",
    "CAN": "Northern America",
    "GRL": "Northern America",
    "SPM": "Northern America",
    "USA": "Northern America",
    # ---- Central Asia ----
    "KAZ": "Central Asia",
    "KGZ": "Central Asia",
    "TJK": "Central Asia",
    "TKM": "Central Asia",
    "UZB": "Central Asia",
    # ---- Eastern Asia ----
    "CHN": "Eastern Asia",
    "HKG": "Eastern Asia",
    "JPN": "Eastern Asia",
    "KOR": "Eastern Asia",
    "MAC": "Eastern Asia",
    "MNG": "Eastern Asia",
    "PRK": "Eastern Asia",
    "TWN": "Eastern Asia",
    # ---- South-eastern Asia ----
    "BRN": "South-eastern Asia",
    "IDN": "South-eastern Asia",
    "KHM": "South-eastern Asia",
    "LAO": "South-eastern Asia",
    "MMR": "South-eastern Asia",
    "MYS": "South-eastern Asia",
    "PHL": "South-eastern Asia",
    "SGP": "South-eastern Asia",
    "THA": "South-eastern Asia",
    "TLS": "South-eastern Asia",
    "VNM": "South-eastern Asia",
    # ---- Southern Asia ----
    "AFG": "Southern Asia",
    "BGD": "Southern Asia",
    "BTN": "Southern Asia",
    "IND": "Southern Asia",
    "IRN": "Southern Asia",
    "LKA": "Southern Asia",
    "MDV": "Southern Asia",
    "NPL": "Southern Asia",
    "PAK": "Southern Asia",
    # ---- Western Asia ----
    "ARE": "Western Asia",
    "ARM": "Western Asia",
    "AZE": "Western Asia",
    "BHR": "Western Asia",
    "CYP": "Western Asia",
    "GEO": "Western Asia",
    "IRQ": "Western Asia",
    "ISR": "Western Asia",
    "JOR": "Western Asia",
    "KWT": "Western Asia",
    "LBN": "Western Asia",
    "OMN": "Western Asia",
    "PSE": "Western Asia",
    "QAT": "Western Asia",
    "SAU": "Western Asia",
    "SYR": "Western Asia",
    "TUR": "Western Asia",
    "YEM": "Western Asia",
    # ---- Eastern Europe ----
    "BGR": "Eastern Europe",
    "BLR": "Eastern Europe",
    "CZE": "Eastern Europe",
    "HUN": "Eastern Europe",
    "MDA": "Eastern Europe",
    "POL": "Eastern Europe",
    "ROU": "Eastern Europe",
    "RUS": "Eastern Europe",
    "SVK": "Eastern Europe",
    "UKR": "Eastern Europe",
    # ---- Northern Europe ----
    "ALA": "Northern Europe",
    "DNK": "Northern Europe",
    "EST": "Northern Europe",
    "FIN": "Northern Europe",
    "FRO": "Northern Europe",
    "GBR": "Northern Europe",
    "GGY": "Northern Europe",
    "IMN": "Northern Europe",
    "IRL": "Northern Europe",
    "ISL": "Northern Europe",
    "JEY": "Northern Europe",
    "LTU": "Northern Europe",
    "LVA": "Northern Europe",
    "NOR": "Northern Europe",
    "SJM": "Northern Europe",
    "SWE": "Northern Europe",
    # ---- Southern Europe ----
    "ALB": "Southern Europe",
    "AND": "Southern Europe",
    "BIH": "Southern Europe",
    "ESP": "Southern Europe",
    "GIB": "Southern Europe",
    "GRC": "Southern Europe",
    "HRV": "Southern Europe",
    "ITA": "Southern Europe",
    "MKD": "Southern Europe",
    "MLT": "Southern Europe",
    "MNE": "Southern Europe",
    "PRT": "Southern Europe",
    "SMR": "Southern Europe",
    "SRB": "Southern Europe",
    "SVN": "Southern Europe",
    "VAT": "Southern Europe",
    # ---- Western Europe ----
    "AUT": "Western Europe",
    "BEL": "Western Europe",
    "CHE": "Western Europe",
    "DEU": "Western Europe",
    "FRA": "Western Europe",
    "LIE": "Western Europe",
    "LUX": "Western Europe",
    "MCO": "Western Europe",
    "NLD": "Western Europe",
    # ---- Australia and New Zealand ----
    "AUS": "Australia and New Zealand",
    "CCK": "Australia and New Zealand",
    "CXR": "Australia and New Zealand",
    "HMD": "Australia and New Zealand",
    "NFK": "Australia and New Zealand",
    "NZL": "Australia and New Zealand",
    # ---- Melanesia ----
    "FJI": "Melanesia",
    "NCL": "Melanesia",
    "PNG": "Melanesia",
    "SLB": "Melanesia",
    "VUT": "Melanesia",
    # ---- Micronesia ----
    "FSM": "Micronesia",
    "GUM": "Micronesia",
    "KIR": "Micronesia",
    "MHL": "Micronesia",
    "MNP": "Micronesia",
    "NRU": "Micronesia",
    "PLW": "Micronesia",
    "UMI": "Micronesia",
    # ---- Polynesia ----
    "ASM": "Polynesia",
    "COK": "Polynesia",
    "NIU": "Polynesia",
    "PCN": "Polynesia",
    "PYF": "Polynesia",
    "TKL": "Polynesia",
    "TON": "Polynesia",
    "TUV": "Polynesia",
    "WLF": "Polynesia",
    "WSM": "Polynesia",
    # ---- Antarctica (M49 lists Antarctica + sub-Antarctic islands here) ----
    "ATA": "Antarctica",
    "ATF": "Antarctica",
    "BVT": "Antarctica",
    "SGS": "Antarctica",
}


# ---------------------------------------------------------------------------
# WHO regions (by ISO3) — GHO API ``ParentCode`` short forms
# ---------------------------------------------------------------------------
#   AFR  = Africa                        (WHO AFRO)
#   AMR  = Region of the Americas        (WHO AMRO / PAHO)
#   SEAR = South-East Asia               (WHO SEARO)
#   EUR  = Europe                        (WHO EURO)
#   EMR  = Eastern Mediterranean         (WHO EMRO)
#   WPR  = Western Pacific               (WHO WPRO)

WHO_REGION_BY_ALPHA3: dict[str, str | None] = {
    # ---- AFR (47) ----
    "DZA": "AFR",
    "AGO": "AFR",
    "BEN": "AFR",
    "BWA": "AFR",
    "BFA": "AFR",
    "BDI": "AFR",
    "CPV": "AFR",
    "CMR": "AFR",
    "CAF": "AFR",
    "TCD": "AFR",
    "COM": "AFR",
    "COG": "AFR",
    "COD": "AFR",
    "CIV": "AFR",
    "GNQ": "AFR",
    "ERI": "AFR",
    "SWZ": "AFR",
    "ETH": "AFR",
    "GAB": "AFR",
    "GMB": "AFR",
    "GHA": "AFR",
    "GIN": "AFR",
    "GNB": "AFR",
    "KEN": "AFR",
    "LSO": "AFR",
    "LBR": "AFR",
    "MDG": "AFR",
    "MWI": "AFR",
    "MLI": "AFR",
    "MRT": "AFR",
    "MUS": "AFR",
    "MOZ": "AFR",
    "NAM": "AFR",
    "NER": "AFR",
    "NGA": "AFR",
    "RWA": "AFR",
    "STP": "AFR",
    "SEN": "AFR",
    "SYC": "AFR",
    "SLE": "AFR",
    "ZAF": "AFR",
    "SSD": "AFR",
    "TGO": "AFR",
    "UGA": "AFR",
    "TZA": "AFR",
    "ZMB": "AFR",
    "ZWE": "AFR",
    # ---- AMR (35 member states + Puerto Rico associate) ----
    "ATG": "AMR",
    "ARG": "AMR",
    "BHS": "AMR",
    "BRB": "AMR",
    "BLZ": "AMR",
    "BOL": "AMR",
    "BRA": "AMR",
    "CAN": "AMR",
    "CHL": "AMR",
    "COL": "AMR",
    "CRI": "AMR",
    "CUB": "AMR",
    "DMA": "AMR",
    "DOM": "AMR",
    "ECU": "AMR",
    "SLV": "AMR",
    "GRD": "AMR",
    "GTM": "AMR",
    "GUY": "AMR",
    "HTI": "AMR",
    "HND": "AMR",
    "JAM": "AMR",
    "MEX": "AMR",
    "NIC": "AMR",
    "PAN": "AMR",
    "PRY": "AMR",
    "PER": "AMR",
    "KNA": "AMR",
    "LCA": "AMR",
    "VCT": "AMR",
    "SUR": "AMR",
    "TTO": "AMR",
    "USA": "AMR",
    "URY": "AMR",
    "VEN": "AMR",
    "PRI": "AMR",  # PAHO associate member
    # ---- SEAR (11) ----
    "BGD": "SEAR",
    "BTN": "SEAR",
    "PRK": "SEAR",
    "IND": "SEAR",
    "IDN": "SEAR",
    "MDV": "SEAR",
    "MMR": "SEAR",
    "NPL": "SEAR",
    "LKA": "SEAR",
    "THA": "SEAR",
    "TLS": "SEAR",
    # ---- EUR (53) ----
    "ALB": "EUR",
    "AND": "EUR",
    "ARM": "EUR",
    "AUT": "EUR",
    "AZE": "EUR",
    "BLR": "EUR",
    "BEL": "EUR",
    "BIH": "EUR",
    "BGR": "EUR",
    "HRV": "EUR",
    "CYP": "EUR",
    "CZE": "EUR",
    "DNK": "EUR",
    "EST": "EUR",
    "FIN": "EUR",
    "FRA": "EUR",
    "GEO": "EUR",
    "DEU": "EUR",
    "GRC": "EUR",
    "HUN": "EUR",
    "ISL": "EUR",
    "IRL": "EUR",
    "ISR": "EUR",
    "ITA": "EUR",
    "KAZ": "EUR",
    "KGZ": "EUR",
    "LVA": "EUR",
    "LTU": "EUR",
    "LUX": "EUR",
    "MLT": "EUR",
    "MCO": "EUR",
    "MNE": "EUR",
    "NLD": "EUR",
    "MKD": "EUR",
    "NOR": "EUR",
    "POL": "EUR",
    "PRT": "EUR",
    "MDA": "EUR",
    "ROU": "EUR",
    "RUS": "EUR",
    "SMR": "EUR",
    "SRB": "EUR",
    "SVK": "EUR",
    "SVN": "EUR",
    "ESP": "EUR",
    "SWE": "EUR",
    "CHE": "EUR",
    "TJK": "EUR",
    "TUR": "EUR",
    "TKM": "EUR",
    "UKR": "EUR",
    "GBR": "EUR",
    "UZB": "EUR",
    # ---- EMR (21 member states + Palestine) ----
    "AFG": "EMR",
    "BHR": "EMR",
    "DJI": "EMR",
    "EGY": "EMR",
    "IRN": "EMR",
    "IRQ": "EMR",
    "JOR": "EMR",
    "KWT": "EMR",
    "LBN": "EMR",
    "LBY": "EMR",
    "MAR": "EMR",
    "OMN": "EMR",
    "PAK": "EMR",
    "PSE": "EMR",
    "QAT": "EMR",
    "SAU": "EMR",
    "SOM": "EMR",
    "SDN": "EMR",
    "SYR": "EMR",
    "TUN": "EMR",
    "ARE": "EMR",
    "YEM": "EMR",
    # ---- WPR (27 member states + Tokelau associate) ----
    "AUS": "WPR",
    "BRN": "WPR",
    "KHM": "WPR",
    "CHN": "WPR",
    "COK": "WPR",
    "FJI": "WPR",
    "JPN": "WPR",
    "KIR": "WPR",
    "LAO": "WPR",
    "MYS": "WPR",
    "MHL": "WPR",
    "FSM": "WPR",
    "MNG": "WPR",
    "NRU": "WPR",
    "NZL": "WPR",
    "NIU": "WPR",
    "PLW": "WPR",
    "PNG": "WPR",
    "PHL": "WPR",
    "KOR": "WPR",
    "WSM": "WPR",
    "SGP": "WPR",
    "SLB": "WPR",
    "TON": "WPR",
    "TUV": "WPR",
    "VUT": "WPR",
    "VNM": "WPR",
    "TKL": "WPR",  # WPRO associate member
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# GADM uses XKO for Kosovo; the de facto user-assigned ISO 3166-1 alpha-3 is
# XKX. Normalize to XKX for lookup purposes (both still resolve to None
# because Kosovo is not a WHO/UN member, but this keeps the alias explicit).
_ALPHA3_ALIASES = {
    "XKO": "XKX",
}


def _normalize(alpha3: str) -> str:
    return _ALPHA3_ALIASES.get(alpha3.upper(), alpha3.upper())


# UN M49 sub-region → macro region. The M49 hierarchy is strict: every
# sub-region rolls up to exactly one of these six macro names, which match
# :data:`cidmath_datahub.reference.geography_intl.UN_REGION_NAMES`.
_UN_SUBREGION_TO_REGION: dict[str, str] = {
    "Northern Africa": "Africa",
    "Eastern Africa": "Africa",
    "Middle Africa": "Africa",
    "Southern Africa": "Africa",
    "Western Africa": "Africa",
    "Caribbean": "Americas",
    "Central America": "Americas",
    "South America": "Americas",
    "Northern America": "Americas",
    "Central Asia": "Asia",
    "Eastern Asia": "Asia",
    "South-eastern Asia": "Asia",
    "Southern Asia": "Asia",
    "Western Asia": "Asia",
    "Eastern Europe": "Europe",
    "Northern Europe": "Europe",
    "Southern Europe": "Europe",
    "Western Europe": "Europe",
    "Australia and New Zealand": "Oceania",
    "Melanesia": "Oceania",
    "Micronesia": "Oceania",
    "Polynesia": "Oceania",
    "Antarctica": "Antarctica",
}


# ISO 3166-1 alpha-3 codes that are NOT WHO members but ARE UN members.
# WHO membership ≈ UN membership but not exact. As of 2026, no UN member
# states are absent from WHO; Liechtenstein, the closest historical
# exception, joined WHO in 1975. Kept as a hook for future divergence.
_UN_MEMBER_NOT_WHO_MEMBER: frozenset[str] = frozenset()

# ISO 3166-1 alpha-3 codes that are WHO members but NOT UN members.
# Currently: Cook Islands (COK) and Niue (NIU) — both associate states of
# New Zealand, both WHO members but not UN members.
_WHO_MEMBER_NOT_UN_MEMBER: frozenset[str] = frozenset({"COK", "NIU"})


def who_region(alpha3: str) -> str | None:
    """Return the WHO ParentCode for an ISO 3166-1 alpha-3, or None for non-members."""
    return WHO_REGION_BY_ALPHA3.get(_normalize(alpha3))


def un_subregion(alpha3: str) -> str | None:
    """Return the UN M49 sub-region for an ISO 3166-1 alpha-3, or None."""
    return UN_SUBREGION_BY_ALPHA3.get(_normalize(alpha3))


def un_region(alpha3: str) -> str | None:
    """Return the UN M49 macro region (Africa/Americas/Asia/Europe/Oceania/Antarctica) or None.

    Derived from :func:`un_subregion` via the M49 hierarchy — a sub-region
    always rolls up to exactly one macro region.
    """
    sub = un_subregion(alpha3)
    if sub is None:
        return None
    return _UN_SUBREGION_TO_REGION.get(sub)


def is_un_member(alpha3: str) -> bool:
    """Return True if the ISO 3166-1 alpha-3 is a current UN member state.

    Derived from WHO membership as the practical proxy, with explicit
    overrides for the two well-known divergences:

    * Cook Islands (COK) and Niue (NIU): WHO members, not UN members → False.
    * Reserved for future cases where a UN member is absent from WHO.

    Cleaner than calling out to country_converter for one field, removes
    a runtime dependency, and the proxy is exact under current WHO/UN
    rosters.
    """
    a3 = _normalize(alpha3)
    if a3 in _WHO_MEMBER_NOT_UN_MEMBER:
        return False
    if a3 in _UN_MEMBER_NOT_WHO_MEMBER:
        return True
    return WHO_REGION_BY_ALPHA3.get(a3) is not None
