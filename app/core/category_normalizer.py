"""Category normalization utility for news sources."""

from typing import Optional
from app.core.logging import setup_logging

logger = setup_logging()

# Normalized category set (enum-like)
NORMALIZED_CATEGORIES = {
    "politics",
    "economy",
    "society",
    "international",
    "culture",
    "sports",
    "science",
    "technology",
    "health",
    "provinces",
    "other",
}

# Category mappings per source
# Format: {source_name: {raw_category: normalized_category}}
CATEGORY_MAPPINGS = {
    "mehrnews": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "سیاست‌های داخلی": "politics",
        "سیاست‌های خارجی": "politics",
        "داخلی": "politics",
        "خارجی": "international",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد و تجارت": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        "حقوقی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "دين، حوزه، انديشه": "culture",
        "هنر": "culture",
        "سینما": "culture",
        "موسیقی": "culture",
        "ادبیات": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        "والیبال": "sports",
        "بسکتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        "IT": "technology",
        "رایانه": "technology",
        # Provinces
        "استانها": "provinces",
    },
    "isna": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        # Provinces
        "استان ها": "provinces",
        "استانها": "provinces",
    },
    "irna": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "دين، حوزه، انديشه": "culture",
        "هنر": "culture",
        "سینما": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        # Provinces
        "استان ها": "provinces",
        "استانها": "provinces",
        "استان‌ها": "provinces",  # With zero-width non-joiner
    },
    "fars": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        # Provinces
        "استان ها": "provinces",
        "استانها": "provinces",
        "استان‌ها": "provinces",  # With zero-width non-joiner
    },
    "tasnim": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست ایران": "politics",
        "نظامی | دفاعی | امنیتی": "politics",
        "گزارش و تحلیل سیاسی": "politics",
        "مجلس و دولت": "politics",
        "امام و رهبری": "politics",
        "دفاعی و امنیتی": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد ایران": "economy",
        "پول | ارز | بانک": "economy",
        "خودرو": "economy",
        "صنعت و تجارت": "economy",
        "نفت و انرژی": "economy",
        "بازار سهام | بورس": "economy",
        "کشاورزی": "economy",
        "اقتصاد جهان": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "پزشکی": "health",
        "طب سنتی": "health",
        "خانواده و جوانان": "society",
        "تهران": "society",
        "فرهنگیان و مدارس": "society",
        "پلیس": "society",
        "حقوقی و قضایی": "society",
        "علم و تکنولوژی": "science",
        "محیط زیست": "society",
        "سفر": "society",
        "حوادث": "society",
        "آسیب های اجتماعی": "society",
        "بازنشستگان": "society",
        # International
        "بین الملل": "international",
        "بین‌الملل": "international",
        "دیپلماسی ایران": "international",
        "تولیدات دفاتر خارجی": "international",
        "آسیای غربی": "international",
        "افغانستان": "international",
        "آمریکا": "international",
        "اروپا": "international",
        "آسیا-اقیانوسیه": "international",
        "پاکستان و هند": "international",
        "ترکیه و اوراسیا": "international",
        "آفریقا": "international",
        "بیداری اسلامی": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "ادبیات و نشر": "culture",
        "رادیو و تلویزیون": "culture",
        "دین ، قرآن و اندیشه": "culture",
        "سینما و تئاتر": "culture",
        "فرهنگ حماسه و مقاومت": "culture",
        "موسیقی و تجسمی": "culture",
        "حوزه و روحانیت": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال ایران": "sports",
        "فوتبال جهان": "sports",
        "والیبال | بسکتبال | هندبال": "sports",
        "کشتی و وزنه‌برداری": "sports",
        "ورزش های رزمی": "sports",
        "ورزش زنان": "sports",
        "ورزش جهان": "sports",
        "رشته های ورزشی": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "فضا و نجوم": "science",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات | اینترنت | موبایل": "technology",
        # Health
        "سلامت": "health",
        "پزشکی": "health",
        # Provinces
        "استانها": "provinces",
        "استان‌ها": "provinces",
        "استان ها": "provinces",
    },
}


def normalize_category(source: str, raw_category: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Normalize category from raw source category.
    
    Args:
        source: News source name (e.g., "mehrnews", "isna")
        raw_category: Original category string from source
        
    Returns:
        Tuple of (normalized_category, raw_category)
        - normalized_category: Normalized category or "other" if unknown
        - raw_category: Original category (preserved)
    """
    if not raw_category or not raw_category.strip():
        return None, None
    
    raw_category = raw_category.strip()
    
    # Get mappings for this source
    source_mappings = CATEGORY_MAPPINGS.get(source, {})
    
    # Try exact match first
    normalized = source_mappings.get(raw_category)
    
    # If not found, try case-insensitive match
    if not normalized:
        for raw_key, norm_value in source_mappings.items():
            if raw_key.lower() == raw_category.lower():
                normalized = norm_value
                break
    
    # If still not found, try partial match (contains)
    if not normalized:
        raw_lower = raw_category.lower()
        for raw_key, norm_value in source_mappings.items():
            if raw_key.lower() in raw_lower or raw_lower in raw_key.lower():
                normalized = norm_value
                break
    
    # If still not found, check if it contains subcategory separator
    if not normalized and '>' in raw_category:
        # Extract main category (before '>')
        main_cat = raw_category.split('>')[0].strip()
        normalized = source_mappings.get(main_cat)
        if not normalized:
            # Try case-insensitive match for main category
            main_cat_lower = main_cat.lower()
            for raw_key, norm_value in source_mappings.items():
                if raw_key.lower() == main_cat_lower:
                    normalized = norm_value
                    break
    
    # Special handling for MehrNews: categories starting with "استانها"
    if not normalized and source == "mehrnews" and raw_category.startswith("استانها"):
        normalized = "provinces"
    
    # Special handling for IRNA: categories starting with "استان‌ها" or "استانها" or "استان ها"
    if not normalized and source == "irna":
        if raw_category.startswith("استان‌ها") or raw_category.startswith("استانها") or raw_category.startswith("استان ها"):
            normalized = "provinces"
    
    # Default to "other" if no mapping found
    if not normalized:
        normalized = "other"
        logger.warning(
            f"Unknown category '{raw_category}' for source '{source}', mapped to 'other'",
            extra={
                "source": source,
                "raw_category": raw_category,
                "normalized_category": normalized,
            }
        )
    else:
        logger.debug(
            f"Category normalized: '{raw_category}' -> '{normalized}'",
            extra={
                "source": source,
                "raw_category": raw_category,
                "normalized_category": normalized,
            }
        )
    
    return normalized, raw_category

