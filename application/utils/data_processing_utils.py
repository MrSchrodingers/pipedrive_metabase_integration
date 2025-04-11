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
        log.error("Unexpected error during geocoding", address=address, exc_info=True)
        return None
