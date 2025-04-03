from typing import Dict, Optional, Tuple, Any
import structlog

log = structlog.get_logger(__name__)

# --- Address Normalization ---
try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False
    log.warning("geopy library not found. Address normalization disabled.")

if GEOPY_AVAILABLE:
    geolocator = Nominatim(user_agent="pipedrive_etl_pipeline/1.0")
else:
    geolocator = None

def normalize_address(address: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Normalizes a string address using an external geocoding service (Nominatim).

    Args:
        address: The address string to normalize.

    Returns:
        A dictionary containing normalized components, or None if normalization fails or is disabled.
    """
    if not GEOPY_AVAILABLE or not geolocator or not address:
        return None

    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            return {
                'raw_input': address,
                'latitude': location.latitude,
                'longitude': location.longitude,
                'full_address': location.address,
                'components': location.raw
            }
        else:
            log.debug("Address not found by geocoder", address=address)
            return None
    except GeocoderTimedOut:
        log.warning("Geocoding service timed out", address=address)
        return None
    except GeocoderServiceError as e:
        log.error("Geocoding service error", address=address, error=str(e))
        return None
    except Exception as e:
        log.error("Unexpected error during geocoding", address=address, error=str(e), exc_info=True)
        return None


# --- Data Anonymization ---
try:
    from presidio_analyzer import AnalyzerEngine
    from presidio_anonymizer import AnonymizerEngine
    from presidio_anonymizer.entities import OperatorConfig
    PRESIDIO_AVAILABLE = True
except ImportError:
    PRESIDIO_AVAILABLE = False
    log.warning("presidio libraries not found. Data anonymization disabled.")

if PRESIDIO_AVAILABLE:
    analyzer = AnalyzerEngine()
    anonymizer = AnonymizerEngine()
else:
    analyzer = None
    anonymizer = None

def anonymize_text(text: Optional[str], language: str = 'pt') -> Optional[str]:
    """
    Analyzes text for PII and anonymizes found entities using Presidio.

    Args:
        text: The input text to anonymize.
        language: The language code for the analyzer (e.g., 'pt', 'en').

    Returns:
        The anonymized text, or the original text if anonymization fails, is disabled, or no PII found.
    """
    if not PRESIDIO_AVAILABLE or not analyzer or not anonymizer or not text:
        return text 

    try:
        analyzer_results = analyzer.analyze(text=text, language=language)

        if not analyzer_results:
            return text

        operators={"PHONE_NUMBER": OperatorConfig("mask", {"masking_char": "*", "chars_to_mask": 12, "from_end": False})}
        anonymized_result = anonymizer.anonymize(
            text=text,
            analyzer_results=analyzer_results,
            operators=operators 
        )
        return anonymized_result.text

    except Exception as e:
        log.error("Error during text anonymization", error=str(e), exc_info=True)
        return text
